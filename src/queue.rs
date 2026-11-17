//! The queue state machine. Pure and synchronous: time is passed in, so behaviour
//! (leases expiring, backoff, delays) is fully deterministic in tests.
//!
//! ```text
//!   enqueue ──► Delayed ──(run_at)──► Ready ──lease──► Leased ──complete──► Done
//!                  ▲                                     │
//!                  └──── fail / lease expiry (backoff) ──┤
//!                                                        └── attempts exhausted ──► Dead
//! ```

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Delayed,
    Ready,
    Leased,
    Done,
    Dead,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Job {
    pub id: u64,
    pub queue: String,
    pub payload: Value,
    pub priority: i32,
    pub state: State,
    pub run_at: u64,
    pub attempts: u32,
    pub max_attempts: u32,
    pub lease_until: Option<u64>,
    pub worker: Option<String>,
    pub last_error: Option<String>,
    pub result: Option<Value>,
    pub idempotency_key: Option<String>,
    pub created_at: u64,
    pub finished_at: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct NewJob {
    pub queue: String,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub delay_ms: u64,
    pub max_attempts: Option<u32>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, PartialEq)]
pub enum QueueError {
    NotFound(u64),
    NotLeasedBy(u64),
    Invalid(String),
}

impl std::fmt::Display for QueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueError::NotFound(id) => write!(f, "job {id} not found"),
            QueueError::NotLeasedBy(id) => write!(f, "job {id} is not leased by this worker (the lease may have expired)"),
            QueueError::Invalid(m) => write!(f, "{m}"),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, PartialEq)]
pub struct QueueStats {
    pub delayed: usize,
    pub ready: usize,
    pub leased: usize,
    pub done: usize,
    pub dead: usize,
}

pub const BACKOFF_BASE_MS: u64 = 1_000;
pub const BACKOFF_CAP_MS: u64 = 5 * 60_000;

/// Exponential backoff: 1s, 2s, 4s … capped at 5 minutes.
pub fn backoff(attempt: u32) -> u64 {
    BACKOFF_BASE_MS.saturating_mul(1u64 << attempt.saturating_sub(1).min(20)).min(BACKOFF_CAP_MS)
}

#[derive(Default)]
pub struct Queue {
    jobs: HashMap<u64, Job>,
    ready: HashMap<String, BTreeSet<(i32, u64)>>, // (-priority, id): highest priority, then FIFO
    delayed: BTreeSet<(u64, u64)>,                // (run_at, id)
    leased: BTreeSet<(u64, u64)>,                 // (lease_until, id)
    idem: HashMap<String, u64>,
    next_id: u64,
    wal: Option<std::io::BufWriter<std::fs::File>>,
    wal_path: Option<PathBuf>,
    pub retention_done: usize,
}

impl Queue {
    pub fn new() -> Self {
        Queue { next_id: 1, retention_done: 10_000, ..Default::default() }
    }

    /// Opens a queue backed by a write-ahead log in `dir`: replays it, then compacts it.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("jobs.wal");
        let mut q = Queue::new();
        if let Ok(f) = std::fs::File::open(&path) {
            for line in std::io::BufReader::new(f).lines() {
                let line = line?;
                // a torn final line from a crash is skipped
                if let Ok(job) = serde_json::from_str::<Job>(&line) {
                    q.next_id = q.next_id.max(job.id + 1);
                    q.index(job);
                }
            }
        }
        let tmp = path.with_extension("tmp");
        {
            let mut w = std::io::BufWriter::new(std::fs::File::create(&tmp)?);
            let mut ids: Vec<&u64> = q.jobs.keys().collect();
            ids.sort();
            for id in ids {
                writeln!(w, "{}", serde_json::to_string(&q.jobs[id])?)?;
            }
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        q.wal = Some(std::io::BufWriter::new(std::fs::OpenOptions::new().append(true).open(&path)?));
        q.wal_path = Some(path);
        Ok(q)
    }

    pub fn flush(&mut self) {
        if let Some(w) = &mut self.wal {
            let _ = w.flush();
            let _ = w.get_ref().sync_data();
        }
    }

    pub fn wal_path(&self) -> Option<&Path> {
        self.wal_path.as_deref()
    }

    fn persist(&mut self, id: u64) {
        if let (Some(w), Some(job)) = (&mut self.wal, self.jobs.get(&id)) {
            if let Ok(s) = serde_json::to_string(job) {
                let _ = writeln!(w, "{s}");
            }
        }
    }

    /// Removes a job from whatever secondary index it is in.
    fn unindex(&mut self, id: u64) {
        if let Some(j) = self.jobs.get(&id) {
            match j.state {
                State::Ready => {
                    if let Some(s) = self.ready.get_mut(&j.queue) {
                        s.remove(&(-j.priority, id));
                    }
                }
                State::Delayed => {
                    self.delayed.remove(&(j.run_at, id));
                }
                State::Leased => {
                    if let Some(l) = j.lease_until {
                        self.leased.remove(&(l, id));
                    }
                }
                _ => {}
            }
        }
    }

    fn index(&mut self, job: Job) {
        let id = job.id;
        self.unindex(id);
        match job.state {
            State::Ready => {
                self.ready.entry(job.queue.clone()).or_default().insert((-job.priority, id));
            }
            State::Delayed => {
                self.delayed.insert((job.run_at, id));
            }
            State::Leased => {
                self.leased.insert((job.lease_until.unwrap_or(0), id));
            }
            _ => {}
        }
        if let Some(k) = &job.idempotency_key {
            self.idem.insert(k.clone(), id);
        }
        self.jobs.insert(id, job);
    }

    fn update(&mut self, mut job: Job, f: impl FnOnce(&mut Job)) {
        f(&mut job);
        let id = job.id;
        self.index(job);
        self.persist(id);
    }

    pub fn get(&self, id: u64) -> Option<&Job> {
        self.jobs.get(&id)
    }

    /// Returns (job id, created?) — an existing id when the idempotency key was seen before.
    pub fn enqueue(&mut self, n: NewJob, now: u64) -> Result<(u64, bool), QueueError> {
        if n.queue.is_empty() || n.queue.len() > 100 {
            return Err(QueueError::Invalid("queue name must be 1-100 characters".into()));
        }
        if let Some(k) = &n.idempotency_key {
            if let Some(&id) = self.idem.get(k) {
                return Ok((id, false));
            }
        }
        let id = self.next_id;
        self.next_id += 1;
        let delayed = n.delay_ms > 0;
        let job = Job {
            id,
            queue: n.queue,
            payload: n.payload,
            priority: n.priority,
            state: if delayed { State::Delayed } else { State::Ready },
            run_at: now + n.delay_ms,
            attempts: 0,
            max_attempts: n.max_attempts.unwrap_or(3).clamp(1, 100),
            lease_until: None,
            worker: None,
            last_error: None,
            result: None,
            idempotency_key: n.idempotency_key,
            created_at: now,
            finished_at: None,
        };
        self.index(job);
        self.persist(id);
        Ok((id, true))
    }

    /// Promotes due delayed jobs and reclaims expired leases. Returns (promoted, reclaimed).
    pub fn tick(&mut self, now: u64) -> (usize, usize) {
        let due: Vec<u64> = self.delayed.iter().take_while(|(t, _)| *t <= now).map(|(_, id)| *id).collect();
        for id in &due {
            let j = self.jobs[id].clone();
            self.update(j, |j| j.state = State::Ready);
        }
        let expired: Vec<u64> = self.leased.iter().take_while(|(t, _)| *t <= now).map(|(_, id)| *id).collect();
        for id in &expired {
            let j = self.jobs[id].clone();
            self.retry_or_bury(j, "lease expired (worker crashed or stopped heartbeating)".into(), now);
        }
        (due.len(), expired.len())
    }

    fn retry_or_bury(&mut self, job: Job, error: String, now: u64) {
        let dead = job.attempts >= job.max_attempts;
        self.update(job, |j| {
            j.last_error = Some(error);
            j.lease_until = None;
            j.worker = None;
            if dead {
                j.state = State::Dead;
                j.finished_at = Some(now);
            } else {
                j.state = State::Delayed;
                j.run_at = now + backoff(j.attempts);
            }
        });
    }

    /// Leases up to `max` jobs from `queues` (empty = all queues) to `worker`.
    pub fn lease(&mut self, queues: &[String], worker: &str, max: usize, visibility_ms: u64, now: u64) -> Vec<Job> {
        self.tick(now);
        let mut candidates: Vec<(i32, u64)> = Vec::new();
        for (name, set) in &self.ready {
            if queues.is_empty() || queues.iter().any(|q| q == name) {
                candidates.extend(set.iter().take(max).copied());
            }
        }
        candidates.sort();
        candidates.truncate(max);
        let mut out = Vec::new();
        for (_, id) in candidates {
            let j = self.jobs[&id].clone();
            self.update(j, |j| {
                j.state = State::Leased;
                j.attempts += 1;
                j.lease_until = Some(now + visibility_ms.max(1));
                j.worker = Some(worker.to_string());
            });
            out.push(self.jobs[&id].clone());
        }
        out
    }

    fn owned(&self, id: u64, worker: &str) -> Result<Job, QueueError> {
        let j = self.jobs.get(&id).ok_or(QueueError::NotFound(id))?;
        if j.state != State::Leased || j.worker.as_deref() != Some(worker) {
            return Err(QueueError::NotLeasedBy(id));
        }
        Ok(j.clone())
    }

    pub fn heartbeat(&mut self, id: u64, worker: &str, extend_ms: u64, now: u64) -> Result<u64, QueueError> {
        let j = self.owned(id, worker)?;
        let until = now + extend_ms.max(1);
        self.update(j, |j| j.lease_until = Some(until));
        Ok(until)
    }

    pub fn complete(&mut self, id: u64, worker: &str, result: Value, now: u64) -> Result<(), QueueError> {
        let j = self.owned(id, worker)?;
        self.update(j, |j| {
            j.state = State::Done;
            j.result = Some(result);
            j.lease_until = None;
            j.finished_at = Some(now);
        });
        self.prune_done();
        Ok(())
    }

    pub fn fail(&mut self, id: u64, worker: &str, error: String, now: u64) -> Result<State, QueueError> {
        let j = self.owned(id, worker)?;
        self.retry_or_bury(j, error, now);
        Ok(self.jobs[&id].state)
    }

    /// Moves a dead job back to ready with a fresh attempt budget.
    pub fn retry(&mut self, id: u64) -> Result<(), QueueError> {
        let j = self.jobs.get(&id).ok_or(QueueError::NotFound(id))?.clone();
        if j.state != State::Dead {
            return Err(QueueError::Invalid(format!("job {id} is {:?}, only dead jobs can be retried", j.state)));
        }
        self.update(j, |j| {
            j.state = State::Ready;
            j.attempts = 0;
            j.finished_at = None;
        });
        Ok(())
    }

    /// Keeps memory bounded: forgets the oldest finished jobs beyond the retention limit.
    fn prune_done(&mut self) {
        let done: usize = self.jobs.values().filter(|j| j.state == State::Done).count();
        if done <= self.retention_done {
            return;
        }
        let mut ids: Vec<(u64, u64)> = self.jobs.values().filter(|j| j.state == State::Done).map(|j| (j.finished_at.unwrap_or(0), j.id)).collect();
        ids.sort();
        for (_, id) in ids.into_iter().take(done - self.retention_done) {
            if let Some(j) = self.jobs.remove(&id) {
                if let Some(k) = j.idempotency_key {
                    self.idem.remove(&k);
                }
            }
        }
    }

    pub fn stats(&self) -> BTreeMap<String, QueueStats> {
        let mut out: BTreeMap<String, QueueStats> = BTreeMap::new();
        for j in self.jobs.values() {
            let s = out.entry(j.queue.clone()).or_default();
            match j.state {
                State::Delayed => s.delayed += 1,
                State::Ready => s.ready += 1,
                State::Leased => s.leased += 1,
                State::Done => s.done += 1,
                State::Dead => s.dead += 1,
            }
        }
        out
    }

    pub fn list(&self, state: Option<State>, queue: Option<&str>, limit: usize) -> Vec<Job> {
        let mut v: Vec<Job> = self
            .jobs
            .values()
            .filter(|j| state.map_or(true, |s| j.state == s) && queue.map_or(true, |q| j.queue == q))
            .cloned()
            .collect();
        v.sort_by_key(|j| std::cmp::Reverse(j.id));
        v.truncate(limit);
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn job(q: &str, prio: i32) -> NewJob {
        NewJob { queue: q.into(), payload: json!({}), priority: prio, ..Default::default() }
    }

    #[test]
    fn priority_then_fifo() {
        let mut q = Queue::new();
        let a = q.enqueue(job("q", 0), 0).unwrap().0;
        let b = q.enqueue(job("q", 5), 0).unwrap().0;
        let c = q.enqueue(job("q", 0), 0).unwrap().0;
        let got: Vec<u64> = q.lease(&[], "w", 10, 1000, 0).iter().map(|j| j.id).collect();
        assert_eq!(got, vec![b, a, c]);
        assert!(q.lease(&[], "w", 10, 1000, 0).is_empty());
    }

    #[test]
    fn delayed_jobs_wait() {
        let mut q = Queue::new();
        q.enqueue(NewJob { delay_ms: 5000, ..job("q", 0) }, 1000).unwrap();
        assert!(q.lease(&[], "w", 1, 1000, 5999).is_empty());
        assert_eq!(q.lease(&[], "w", 1, 1000, 6000).len(), 1);
    }

    #[test]
    fn failures_back_off_then_die() {
        let mut q = Queue::new();
        let id = q.enqueue(NewJob { max_attempts: Some(3), ..job("q", 0) }, 0).unwrap().0;
        let mut now = 0;
        for attempt in 1..=3 {
            let leased = q.lease(&[], "w", 1, 10_000, now);
            assert_eq!(leased[0].attempts, attempt);
            let s = q.fail(id, "w", format!("boom {attempt}"), now).unwrap();
            if attempt < 3 {
                assert_eq!(s, State::Delayed);
                assert_eq!(q.get(id).unwrap().run_at, now + backoff(attempt));
                assert!(q.lease(&[], "w", 1, 10_000, now + backoff(attempt) - 1).is_empty());
                now += backoff(attempt);
            } else {
                assert_eq!(s, State::Dead);
            }
        }
        assert_eq!(q.stats()["q"].dead, 1);
        q.retry(id).unwrap();
        assert_eq!(q.lease(&[], "w", 1, 10, now)[0].attempts, 1);
    }

    #[test]
    fn expired_leases_are_reclaimed_and_late_acks_rejected() {
        let mut q = Queue::new();
        let id = q.enqueue(job("q", 0), 0).unwrap().0;
        q.lease(&[], "crashy", 1, 1000, 0);
        assert_eq!(q.heartbeat(id, "crashy", 1000, 500).unwrap(), 1500);
        assert_eq!(q.tick(1499), (0, 0));
        assert_eq!(q.tick(1500), (0, 1));
        assert_eq!(q.complete(id, "crashy", json!(null), 1600), Err(QueueError::NotLeasedBy(id)));
        let j = q.lease(&[], "healthy", 1, 1000, 1500 + backoff(1));
        assert_eq!(j[0].worker.as_deref(), Some("healthy"));
        q.complete(id, "healthy", json!({"ok": true}), 4000).unwrap();
        assert_eq!(q.get(id).unwrap().state, State::Done);
    }

    #[test]
    fn idempotency_and_queue_filter() {
        let mut q = Queue::new();
        let k = Some("order-42".to_string());
        let (a, new_a) = q.enqueue(NewJob { idempotency_key: k.clone(), ..job("emails", 0) }, 0).unwrap();
        let (b, new_b) = q.enqueue(NewJob { idempotency_key: k, ..job("emails", 0) }, 0).unwrap();
        assert!(new_a && !new_b && a == b);
        q.enqueue(job("images", 0), 0).unwrap();
        assert!(q.lease(&["videos".into()], "w", 5, 10, 0).is_empty());
        assert_eq!(q.lease(&["images".into()], "w", 5, 10, 0)[0].queue, "images");
    }

    #[test]
    fn wal_replay_restores_state() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut q = Queue::open(dir.path()).unwrap();
            let a = q.enqueue(job("q", 1), 0).unwrap().0;
            q.enqueue(NewJob { delay_ms: 100, ..job("q", 0) }, 0).unwrap();
            q.lease(&[], "w", 1, 1000, 0);
            q.complete(a, "w", json!("done"), 10).unwrap();
            q.enqueue(job("q", 0), 0).unwrap();
            q.flush();
        }
        // simulate a torn last line
        std::fs::OpenOptions::new().append(true).open(dir.path().join("jobs.wal")).unwrap().write_all(b"{\"id\":99,\"que").unwrap();
        let mut q = Queue::open(dir.path()).unwrap();
        assert_eq!(q.stats()["q"], QueueStats { delayed: 1, ready: 1, leased: 0, done: 1, dead: 0 });
        assert_eq!(q.enqueue(job("q", 0), 0).unwrap().0, 4, "ids keep increasing after restart");
    }
}
