//! HTTP API over the queue. Workers on any machine lease jobs with long polling.

use crate::queue::{NewJob, Queue, QueueError, State};
use axum::extract::{Path, Query, State as AxState};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

pub struct App {
    pub queue: Mutex<Queue>,
    pub wake: Notify,
}

type Shared = Arc<App>;

struct ApiError(StatusCode, String);
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}
impl From<QueueError> for ApiError {
    fn from(e: QueueError) -> Self {
        let code = match e {
            QueueError::NotFound(_) => StatusCode::NOT_FOUND,
            QueueError::NotLeasedBy(_) => StatusCode::CONFLICT,
            QueueError::Invalid(_) => StatusCode::BAD_REQUEST,
        };
        ApiError(code, e.to_string())
    }
}

#[derive(Deserialize)]
struct LeaseReq {
    worker: String,
    #[serde(default)]
    queues: Vec<String>,
    #[serde(default = "one")]
    max: usize,
    #[serde(default = "thirty_s")]
    visibility_ms: u64,
    #[serde(default)]
    wait_ms: u64,
}
fn one() -> usize {
    1
}
fn thirty_s() -> u64 {
    30_000
}

#[derive(Deserialize)]
struct WorkerReq {
    worker: String,
    #[serde(default)]
    result: Value,
    #[serde(default)]
    error: String,
    #[serde(default = "thirty_s")]
    extend_ms: u64,
}

#[derive(Deserialize)]
struct ListQuery {
    state: Option<State>,
    queue: Option<String>,
    limit: Option<usize>,
}

async fn enqueue(AxState(app): AxState<Shared>, Json(n): Json<NewJob>) -> Result<(StatusCode, Json<Value>), ApiError> {
    let (id, created) = app.queue.lock().unwrap().enqueue(n, now_ms())?;
    app.wake.notify_waiters();
    Ok((if created { StatusCode::CREATED } else { StatusCode::OK }, Json(json!({"id": id, "created": created}))))
}

async fn get_job(AxState(app): AxState<Shared>, Path(id): Path<u64>) -> Result<Json<Value>, ApiError> {
    let q = app.queue.lock().unwrap();
    let j = q.get(id).ok_or(QueueError::NotFound(id))?;
    Ok(Json(serde_json::to_value(j).unwrap()))
}

async fn list(AxState(app): AxState<Shared>, Query(l): Query<ListQuery>) -> Json<Value> {
    let q = app.queue.lock().unwrap();
    Json(json!({"jobs": q.list(l.state, l.queue.as_deref(), l.limit.unwrap_or(50).min(1000))}))
}

async fn lease(AxState(app): AxState<Shared>, Json(r): Json<LeaseReq>) -> Json<Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(r.wait_ms.min(30_000));
    loop {
        let notified = app.wake.notified();
        let jobs = app.queue.lock().unwrap().lease(&r.queues, &r.worker, r.max.clamp(1, 100), r.visibility_ms, now_ms());
        if !jobs.is_empty() || tokio::time::Instant::now() >= deadline {
            return Json(json!({"jobs": jobs}));
        }
        // wake on enqueue, or re-check periodically for delayed jobs becoming due
        let _ = tokio::time::timeout_at(deadline.min(tokio::time::Instant::now() + Duration::from_millis(250)), notified).await;
    }
}

async fn heartbeat(AxState(app): AxState<Shared>, Path(id): Path<u64>, Json(r): Json<WorkerReq>) -> Result<Json<Value>, ApiError> {
    let until = app.queue.lock().unwrap().heartbeat(id, &r.worker, r.extend_ms, now_ms())?;
    Ok(Json(json!({"lease_until": until})))
}

async fn complete(AxState(app): AxState<Shared>, Path(id): Path<u64>, Json(r): Json<WorkerReq>) -> Result<Json<Value>, ApiError> {
    app.queue.lock().unwrap().complete(id, &r.worker, r.result, now_ms())?;
    Ok(Json(json!({"ok": true})))
}

async fn fail(AxState(app): AxState<Shared>, Path(id): Path<u64>, Json(r): Json<WorkerReq>) -> Result<Json<Value>, ApiError> {
    let state = app.queue.lock().unwrap().fail(id, &r.worker, r.error, now_ms())?;
    Ok(Json(json!({"state": state})))
}

async fn retry(AxState(app): AxState<Shared>, Path(id): Path<u64>) -> Result<Json<Value>, ApiError> {
    app.queue.lock().unwrap().retry(id)?;
    app.wake.notify_waiters();
    Ok(Json(json!({"ok": true})))
}

async fn stats(AxState(app): AxState<Shared>) -> Json<Value> {
    Json(json!({"queues": app.queue.lock().unwrap().stats()}))
}

pub fn router(app: Shared) -> Router {
    Router::new()
        .route("/jobs", post(enqueue).get(list))
        .route("/jobs/:id", get(get_job))
        .route("/jobs/:id/heartbeat", post(heartbeat))
        .route("/jobs/:id/complete", post(complete))
        .route("/jobs/:id/fail", post(fail))
        .route("/jobs/:id/retry", post(retry))
        .route("/lease", post(lease))
        .route("/stats", get(stats))
        .route("/health", get(|| async { "ok" }))
        .with_state(app)
}

/// Background housekeeping: reclaim expired leases, promote delayed jobs, fsync the WAL.
pub fn spawn_housekeeping(app: Shared) {
    tokio::spawn(async move {
        let mut i = 0u64;
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let (promoted, _) = app.queue.lock().unwrap().tick(now_ms());
            if promoted > 0 {
                app.wake.notify_waiters();
            }
            i += 1;
            if i % 5 == 0 {
                app.queue.lock().unwrap().flush();
            }
        }
    });
}

pub async fn serve(app: Shared, listener: tokio::net::TcpListener) -> std::io::Result<()> {
    spawn_housekeeping(app.clone());
    axum::serve(listener, router(app)).await
}
