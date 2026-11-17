export default {
  name: 'jobq',
  stack: 'Rust · tokio · axum · WAL',
  tagline: 'A distributed job queue: workers on any machine lease jobs over HTTP, heartbeat while they run, and the server retries or dead-letters whatever they drop.',
  repo: 'https://github.com/devKen143/jobq',
  description:
    'Producers enqueue JSON jobs with a priority, an optional delay and an idempotency key. Workers long-poll for leases. A leased job is invisible to everyone else until its visibility timeout; if the worker crashes and stops heartbeating, the lease expires and the job is retried with exponential backoff, then moved to a dead-letter queue when its attempts run out. Every state change is written to a write-ahead log.',
  features: [
    'At-least-once delivery with leases, visibility timeouts and heartbeats',
    'Retries with exponential backoff (1s, 2s, 4s … capped at 5 min) and a dead-letter queue with manual retry',
    'Priorities (FIFO within a priority), delayed jobs and multiple named queues',
    'Idempotency keys so a retried enqueue never creates a duplicate',
    'Late acks from an expired lease are rejected with 409, so two workers never both "own" a job',
    'Write-ahead log replayed and compacted at startup; a torn last line is ignored',
    'Worker binary with shell, sleep/simulate and echo handlers, plus a CLI for enqueue/stats/dead/retry',
  ],
  demoNote: 'A live model of queue.rs. Crash a worker mid-job and watch its lease expire and the job come back; raise the failure rate to fill the dead-letter queue.',
  architecture: [
    { title: 'queue.rs', text: 'A pure state machine with injected time. Ready jobs live in a BTreeSet ordered by (−priority, id); delayed jobs and leases in sets ordered by deadline, so ticks are cheap.' },
    { title: 'server.rs', text: 'axum routes for enqueue, lease, heartbeat, complete, fail and retry. Lease requests long-poll on a tokio Notify that enqueues wake.' },
    { title: 'worker.rs', text: 'N concurrent lease loops per process. Each running job has a heartbeat task at ⅓ of the visibility timeout, aborted when the handler returns.' },
    { title: 'Durability', text: 'Each mutation appends the full job record to the WAL (last write wins). Startup replays it, rewrites a compact snapshot, and fsyncs every second.' },
  ],
  usage: `git clone https://github.com/devKen143/jobq && cd jobq
cargo run --release -- server --data ./jobq-data

# on any machine that can reach the server
cargo run --release -- worker --server http://10.0.0.5:7070 --queues emails,thumbs --concurrency 8

cargo run --release -- enqueue emails '{"to":"ada@example.com"}' --priority 5 --key welcome-ada
cargo run --release -- enqueue thumbs '{"sleep_ms": 800, "fail_rate": 0.3}' --max-attempts 4
cargo run --release -- stats
curl -s localhost:7070/jobs?state=dead`,
}
