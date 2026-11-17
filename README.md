# jobq

A distributed job queue in Rust (tokio + axum). Workers on any machine lease jobs over HTTP, heartbeat while they run, and the server retries or dead-letters anything they drop.

## Features
- At-least-once delivery: leases with visibility timeouts and heartbeats; expired leases are reclaimed
- Retries with exponential backoff (1 s, 2 s, 4 s … max 5 min), dead-letter queue, manual retry
- Priorities (FIFO within a priority), delayed jobs, multiple named queues
- Idempotency keys on enqueue
- Acks from a worker whose lease expired are rejected (409)
- Write-ahead log, replayed and compacted at startup
- Long-poll leasing, so idle workers don't busy-poll

## Usage
```bash
cargo run --release -- server --bind 0.0.0.0:7070 --data ./jobq-data
cargo run --release -- worker --server http://HOST:7070 --queues emails --concurrency 8 [--allow-shell]
cargo run --release -- enqueue emails '{"to":"ada@example.com"}' --priority 5 --key welcome-ada
cargo run --release -- enqueue build '{"cmd":"cargo --version"}'
cargo run --release -- stats
cargo run --release -- dead
cargo run --release -- retry 42
```

## HTTP API
| Method | Path | Body |
|---|---|---|
| POST | `/jobs` | `{queue, payload, priority?, delay_ms?, max_attempts?, idempotency_key?}` |
| GET | `/jobs?state=dead&queue=q&limit=50` | |
| GET | `/jobs/:id` | |
| POST | `/lease` | `{worker, queues?, max?, visibility_ms?, wait_ms?}` |
| POST | `/jobs/:id/heartbeat` | `{worker, extend_ms?}` |
| POST | `/jobs/:id/complete` | `{worker, result?}` |
| POST | `/jobs/:id/fail` | `{worker, error}` |
| POST | `/jobs/:id/retry` | |
| GET | `/stats` | |

## Worker handlers
`{"cmd": "..."}` runs a shell command (only with `--allow-shell`), `{"sleep_ms": n, "fail_rate": 0.2}` simulates work, and anything else is echoed back.

## Tests
`cargo test` — state-machine tests with injected time (priority, delay, backoff, dead-letter, lease expiry, idempotency, WAL replay) and HTTP tests with real workers.

## License
MIT
