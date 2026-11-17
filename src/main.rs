use jobq::queue::Queue;
use jobq::server::App;
use jobq::worker::{WorkerConfig, WorkerStats};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

const USAGE: &str = "\
jobq — distributed job queue

USAGE:
  jobq server  [--bind 127.0.0.1:7070] [--data ./jobq-data]
  jobq worker  [--server URL] [--queues a,b] [--concurrency 4] [--visibility-ms 30000] [--name host] [--allow-shell]
  jobq enqueue <queue> '<json payload>' [--priority N] [--delay-ms N] [--max-attempts N] [--key idempotency-key]
  jobq stats   [--server URL]
  jobq dead    [--server URL]               list dead-lettered jobs
  jobq retry   <id> [--server URL]          requeue a dead job

  --server defaults to http://127.0.0.1:7070
";

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Err(e) = run(args).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run(args: Vec<String>) -> Result<(), String> {
    let server = flag(&args, "--server").unwrap_or_else(|| "http://127.0.0.1:7070".into());
    let client = reqwest::Client::new();
    let show = |v: Value| println!("{}", serde_json::to_string_pretty(&v).unwrap());
    match args.first().map(String::as_str) {
        Some("server") => {
            let bind = flag(&args, "--bind").unwrap_or_else(|| "127.0.0.1:7070".into());
            let data = flag(&args, "--data").unwrap_or_else(|| "./jobq-data".into());
            let queue = Queue::open(std::path::Path::new(&data)).map_err(|e| e.to_string())?;
            let counts = queue.stats();
            let app = Arc::new(App { queue: Mutex::new(queue), wake: tokio::sync::Notify::new() });
            let listener = tokio::net::TcpListener::bind(&bind).await.map_err(|e| e.to_string())?;
            println!("jobq server on http://{bind} (data: {data}, {} queue(s) recovered)", counts.len());
            let a = app.clone();
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                a.queue.lock().unwrap().flush();
                std::process::exit(0);
            });
            jobq::server::serve(app, listener).await.map_err(|e| e.to_string())
        }
        Some("worker") => {
            let cfg = WorkerConfig {
                server,
                name: flag(&args, "--name").unwrap_or_else(|| format!("worker-{}", std::process::id())),
                queues: flag(&args, "--queues").map(|q| q.split(',').map(str::to_string).collect()).unwrap_or_default(),
                concurrency: flag(&args, "--concurrency").and_then(|c| c.parse().ok()).unwrap_or(4),
                visibility_ms: flag(&args, "--visibility-ms").and_then(|c| c.parse().ok()).unwrap_or(30_000),
                allow_shell: args.iter().any(|a| a == "--allow-shell"),
            };
            println!("{} polling {} for {} with {} slot(s)", cfg.name, cfg.server, if cfg.queues.is_empty() { "all queues".into() } else { cfg.queues.join(",") }, cfg.concurrency);
            jobq::worker::run(cfg, Arc::new(WorkerStats::default()), async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await;
            Ok(())
        }
        Some("enqueue") => {
            let queue = args.get(1).ok_or("missing queue")?;
            let payload: Value = match args.get(2).filter(|a| !a.starts_with("--")) {
                Some(p) => serde_json::from_str(p).map_err(|e| format!("payload must be JSON: {e}"))?,
                None => Value::Null,
            };
            let num = |f: &str| flag(&args, f).and_then(|v| v.parse::<i64>().ok());
            let body = json!({
                "queue": queue, "payload": payload,
                "priority": num("--priority").unwrap_or(0), "delay_ms": num("--delay-ms").unwrap_or(0),
                "max_attempts": num("--max-attempts"), "idempotency_key": flag(&args, "--key"),
            });
            let r = client.post(format!("{server}/jobs")).json(&body).send().await.map_err(|e| e.to_string())?;
            show(r.json().await.map_err(|e| e.to_string())?);
            Ok(())
        }
        Some("stats") => {
            show(client.get(format!("{server}/stats")).send().await.map_err(|e| e.to_string())?.json().await.map_err(|e| e.to_string())?);
            Ok(())
        }
        Some("dead") => {
            show(client.get(format!("{server}/jobs?state=dead")).send().await.map_err(|e| e.to_string())?.json().await.map_err(|e| e.to_string())?);
            Ok(())
        }
        Some("retry") => {
            let id = args.get(1).ok_or("missing id")?;
            show(client.post(format!("{server}/jobs/{id}/retry")).send().await.map_err(|e| e.to_string())?.json().await.map_err(|e| e.to_string())?);
            Ok(())
        }
        _ => {
            eprint!("{USAGE}");
            std::process::exit(2)
        }
    }
}
