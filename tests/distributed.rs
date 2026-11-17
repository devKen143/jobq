use jobq::queue::Queue;
use jobq::server::App;
use jobq::worker::{WorkerConfig, WorkerStats};
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

async fn start() -> String {
    let app = Arc::new(App { queue: Mutex::new(Queue::new()), wake: tokio::sync::Notify::new() });
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(jobq::server::serve(app, l));
    url
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workers_drain_queue_with_retries() {
    let url = start().await;
    let c = reqwest::Client::new();
    for i in 0..40 {
        let payload = if i % 4 == 0 { json!({"sleep_ms": 5, "fail_rate": 1.0}) } else { json!({"sleep_ms": 5}) };
        c.post(format!("{url}/jobs")).json(&json!({"queue": "work", "payload": payload, "max_attempts": 1})).send().await.unwrap();
    }
    let stats = Arc::new(WorkerStats::default());
    let mut handles = Vec::new();
    for w in 0..2 {
        let cfg = WorkerConfig { server: url.clone(), name: format!("w{w}"), queues: vec!["work".into()], concurrency: 3, visibility_ms: 5_000, allow_shell: false };
        let s = stats.clone();
        handles.push(tokio::spawn(jobq::worker::run(cfg, s, tokio::time::sleep(Duration::from_secs(30)))));
    }
    // wait until everything is done or dead
    let mut last = Value::Null;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        last = c.get(format!("{url}/stats")).send().await.unwrap().json().await.unwrap();
        let q = &last["queues"]["work"];
        if q["done"].as_u64().unwrap_or(0) + q["dead"].as_u64().unwrap_or(0) == 40 {
            break;
        }
    }
    assert_eq!(last["queues"]["work"]["done"], 30, "{last}");
    assert_eq!(last["queues"]["work"]["dead"], 10);
    assert_eq!(stats.completed.load(Ordering::Relaxed), 30);
    for h in handles {
        h.abort();
    }
}

#[tokio::test]
async fn long_poll_and_lease_conflicts() {
    let url = start().await;
    let c = reqwest::Client::new();
    let url2 = url.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        reqwest::Client::new().post(format!("{url2}/jobs")).json(&json!({"queue": "q", "payload": {"n": 1}})).send().await.unwrap();
    });
    let t0 = std::time::Instant::now();
    let v: Value = c.post(format!("{url}/lease")).json(&json!({"worker": "a", "wait_ms": 5000})).send().await.unwrap().json().await.unwrap();
    assert!(t0.elapsed() < Duration::from_secs(2));
    let id = v["jobs"][0]["id"].as_u64().unwrap();
    let r = c.post(format!("{url}/jobs/{id}/complete")).json(&json!({"worker": "b"})).send().await.unwrap();
    assert_eq!(r.status(), 409, "another worker cannot ack a job it does not hold");
    let r = c.post(format!("{url}/jobs/{id}/complete")).json(&json!({"worker": "a", "result": 7})).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let j: Value = c.get(format!("{url}/jobs/{id}")).send().await.unwrap().json().await.unwrap();
    assert_eq!(j["state"], "done");
    assert_eq!(j["result"], 7);
}
