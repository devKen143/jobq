//! Worker runtime: lease → run handler (with heartbeats) → complete / fail.
//!
//! Built-in handlers, chosen by the payload:
//! - `{"cmd": "echo hi"}` runs a shell command (stdout is the result, non-zero exit fails)
//! - `{"sleep_ms": 500, "fail_rate": 0.2}` simulates work, failing randomly
//! - anything else is echoed back as the result

use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct WorkerConfig {
    pub server: String,
    pub name: String,
    pub queues: Vec<String>,
    pub concurrency: usize,
    pub visibility_ms: u64,
    pub allow_shell: bool,
}

#[derive(Default)]
pub struct WorkerStats {
    pub completed: AtomicU64,
    pub failed: AtomicU64,
}

fn pseudo_random(seed: u64) -> f64 {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (seed >> 7);
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    (x % 10_000) as f64 / 10_000.0
}

pub async fn handle(payload: &Value, id: u64, attempt: u64, allow_shell: bool) -> Result<Value, String> {
    if let Some(cmd) = payload.get("cmd").and_then(Value::as_str) {
        if !allow_shell {
            return Err("shell jobs are disabled on this worker (start it with --allow-shell)".into());
        }
        let mut c = if cfg!(windows) {
            let mut c = tokio::process::Command::new("cmd");
            c.args(["/C", cmd]);
            c
        } else {
            let mut c = tokio::process::Command::new("sh");
            c.args(["-c", cmd]);
            c
        };
        let timeout = payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(60_000);
        let out = tokio::time::timeout(Duration::from_millis(timeout), c.kill_on_drop(true).output())
            .await
            .map_err(|_| format!("command timed out after {timeout} ms"))?
            .map_err(|e| e.to_string())?;
        let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
        return if out.status.success() {
            Ok(json!({"stdout": stdout}))
        } else {
            Err(format!("exit {}: {}", out.status.code().unwrap_or(-1), String::from_utf8_lossy(&out.stderr).trim()))
        };
    }
    if let Some(ms) = payload.get("sleep_ms").and_then(Value::as_u64) {
        tokio::time::sleep(Duration::from_millis(ms.min(600_000))).await;
        let rate = payload.get("fail_rate").and_then(Value::as_f64).unwrap_or(0.0);
        if pseudo_random(id * 31 + attempt) < rate {
            return Err(format!("simulated failure (fail_rate {rate})"));
        }
        return Ok(json!({"slept_ms": ms}));
    }
    Ok(json!({"echo": payload}))
}

async fn post(client: &reqwest::Client, url: String, body: Value) -> Result<Value, String> {
    let r = client.post(url).json(&body).send().await.map_err(|e| e.to_string())?;
    let status = r.status();
    let v: Value = r.json().await.unwrap_or(Value::Null);
    if status.is_success() {
        Ok(v)
    } else {
        Err(v["error"].as_str().unwrap_or("request failed").to_string())
    }
}

/// Runs `concurrency` lease loops until `stop` resolves.
pub async fn run(cfg: WorkerConfig, stats: Arc<WorkerStats>, stop: impl std::future::Future<Output = ()>) {
    let client = reqwest::Client::builder().timeout(Duration::from_secs(40)).build().unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for slot in 0..cfg.concurrency.max(1) {
        let (cfg, client, stats) = (cfg.clone(), client.clone(), stats.clone());
        tasks.spawn(async move {
            let worker = format!("{}#{slot}", cfg.name);
            loop {
                let leased = post(&client, format!("{}/lease", cfg.server), json!({"worker": worker, "queues": cfg.queues, "max": 1, "visibility_ms": cfg.visibility_ms, "wait_ms": 10_000})).await;
                let jobs = match leased {
                    Ok(v) => v["jobs"].as_array().cloned().unwrap_or_default(),
                    Err(e) => {
                        eprintln!("[{worker}] lease failed: {e}; retrying in 1s");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                for job in jobs {
                    let id = job["id"].as_u64().unwrap_or(0);
                    let attempt = job["attempts"].as_u64().unwrap_or(1);
                    // heartbeat at a third of the visibility timeout while the handler runs
                    let hb = {
                        let (client, server, worker, vis) = (client.clone(), cfg.server.clone(), worker.clone(), cfg.visibility_ms);
                        tokio::spawn(async move {
                            loop {
                                tokio::time::sleep(Duration::from_millis((vis / 3).max(100))).await;
                                if post(&client, format!("{server}/jobs/{id}/heartbeat"), json!({"worker": worker, "extend_ms": vis})).await.is_err() {
                                    break;
                                }
                            }
                        })
                    };
                    let outcome = handle(&job["payload"], id, attempt, cfg.allow_shell).await;
                    hb.abort();
                    let r = match &outcome {
                        Ok(result) => post(&client, format!("{}/jobs/{id}/complete", cfg.server), json!({"worker": worker, "result": result})).await,
                        Err(error) => post(&client, format!("{}/jobs/{id}/fail", cfg.server), json!({"worker": worker, "error": error})).await,
                    };
                    match (&outcome, r) {
                        (Ok(_), Ok(_)) => {
                            stats.completed.fetch_add(1, Ordering::Relaxed);
                            println!("[{worker}] job {id} ({}) done", job["queue"].as_str().unwrap_or(""));
                        }
                        (Err(e), Ok(v)) => {
                            stats.failed.fetch_add(1, Ordering::Relaxed);
                            println!("[{worker}] job {id} failed (attempt {attempt}): {e} -> {}", v["state"].as_str().unwrap_or("?"));
                        }
                        (_, Err(e)) => eprintln!("[{worker}] could not report job {id}: {e}"),
                    }
                }
            }
        });
    }
    stop.await;
    tasks.abort_all();
}
