import { useEffect, useRef, useState } from 'react'

const TICK = 100 // ms of simulated time per frame
const VISIBILITY = 3000
const backoff = (attempt) => Math.min(1000 * 2 ** (attempt - 1), 300000)
const STATES = ['delayed', 'ready', 'leased', 'done', 'dead']
const COLORS = { delayed: 'var(--warn)', ready: 'var(--accent)', leased: '#2563eb', done: 'var(--ok)', dead: 'var(--err)' }

function initial() {
  return { now: 0, nextId: 1, jobs: {}, workers: [0, 1, 2].map((i) => ({ id: `w${i}`, alive: true, job: null, busyUntil: 0 })), log: [] }
}

function step(s, failRate) {
  const now = s.now + TICK
  const jobs = { ...s.jobs }
  const log = [...s.log]
  const say = (m) => log.unshift(`${(now / 1000).toFixed(1)}s ${m}`)
  // tick: promote delayed, reclaim expired leases (queue.rs::tick)
  for (const j of Object.values(jobs)) {
    if (j.state === 'delayed' && j.runAt <= now) jobs[j.id] = { ...j, state: 'ready' }
    if (j.state === 'leased' && j.leaseUntil <= now) {
      const dead = j.attempts >= j.max
      jobs[j.id] = { ...j, state: dead ? 'dead' : 'delayed', runAt: now + backoff(j.attempts), worker: null, error: 'lease expired' }
      say(`job ${j.id} lease expired → ${dead ? 'dead-letter' : `retry in ${backoff(j.attempts) / 1000}s`}`)
    }
  }
  const workers = s.workers.map((w) => ({ ...w }))
  for (const w of workers) {
    if (!w.alive) continue
    const cur = w.job && jobs[w.job]
    if (cur && cur.state === 'leased' && cur.worker === w.id) {
      jobs[cur.id] = { ...cur, leaseUntil: now + VISIBILITY } // heartbeat
      if (now >= w.busyUntil) {
        if (Math.random() < failRate) {
          const dead = cur.attempts >= cur.max
          jobs[cur.id] = { ...cur, state: dead ? 'dead' : 'delayed', runAt: now + backoff(cur.attempts), worker: null, error: 'handler error' }
          say(`${w.id} failed job ${cur.id} (attempt ${cur.attempts}) → ${dead ? 'dead-letter' : 'backoff'}`)
        } else {
          jobs[cur.id] = { ...cur, state: 'done', worker: null }
          say(`${w.id} completed job ${cur.id}`)
        }
        w.job = null
      }
      continue
    }
    w.job = null
    // lease: highest priority first, then FIFO
    const next = Object.values(jobs).filter((j) => j.state === 'ready').sort((a, b) => b.priority - a.priority || a.id - b.id)[0]
    if (next) {
      jobs[next.id] = { ...next, state: 'leased', attempts: next.attempts + 1, worker: w.id, leaseUntil: now + VISIBILITY }
      w.job = next.id
      w.busyUntil = now + 600 + Math.random() * 1400
    }
  }
  return { ...s, now, jobs, workers, log: log.slice(0, 14) }
}

export default function Demo() {
  const [s, setS] = useState(initial)
  const [running, setRunning] = useState(true)
  const [failRate, setFailRate] = useState(0.2)
  const fr = useRef(failRate)
  fr.current = failRate
  useEffect(() => {
    if (!running) return
    const t = setInterval(() => setS((x) => step(x, fr.current)), TICK)
    return () => clearInterval(t)
  }, [running])

  const enqueue = (n, priority = 0, delay = 0) =>
    setS((x) => {
      const jobs = { ...x.jobs }
      let id = x.nextId
      for (let i = 0; i < n; i++, id++) jobs[id] = { id, priority, state: delay ? 'delayed' : 'ready', runAt: x.now + delay, attempts: 0, max: 3, worker: null }
      return { ...x, jobs, nextId: id, log: [`${(x.now / 1000).toFixed(1)}s enqueued ${n} job(s)${priority ? ` priority ${priority}` : ''}${delay ? ` delayed ${delay / 1000}s` : ''}`, ...x.log].slice(0, 14) }
    })
  const toggleWorker = (i) => setS((x) => ({ ...x, workers: x.workers.map((w, k) => (k === i ? { ...w, alive: !w.alive, job: w.alive ? w.job : null } : w)), log: [`${(x.now / 1000).toFixed(1)}s ${x.workers[i].id} ${x.workers[i].alive ? 'CRASHED (stops heartbeating)' : 'restarted'}`, ...x.log].slice(0, 14) }))
  const retryDead = () => setS((x) => ({ ...x, jobs: Object.fromEntries(Object.entries(x.jobs).map(([k, j]) => [k, j.state === 'dead' ? { ...j, state: 'ready', attempts: 0 } : j])) }))
  const byState = (st) => Object.values(s.jobs).filter((j) => j.state === st)

  return (
    <div>
      <div className="row">
        <button className="primary" onClick={() => enqueue(5)}>Enqueue 5</button>
        <button onClick={() => enqueue(1, 10)}>Enqueue priority 10</button>
        <button onClick={() => enqueue(3, 0, 4000)}>Enqueue 3 delayed 4s</button>
        <button onClick={retryDead}>Retry dead</button>
        <button onClick={() => setRunning(!running)}>{running ? 'Pause' : 'Resume'}</button>
        <button onClick={() => setS(initial())}>Reset</button>
        <label style={{ fontSize: 14 }}>failure rate {Math.round(failRate * 100)}% <input type="range" min="0" max="1" step="0.05" value={failRate} onChange={(e) => setFailRate(Number(e.target.value))} /></label>
      </div>
      <div className="row">
        {s.workers.map((w, i) => (
          <span key={w.id} className="card" style={{ padding: '6px 10px', opacity: w.alive ? 1 : 0.55 }}>
            <b>{w.id}</b> {w.alive ? (w.job ? <span className="pill" style={{ color: COLORS.leased }}>job {w.job}</span> : <span className="muted">idle</span>) : <span className="pill err">down</span>}{' '}
            <button onClick={() => toggleWorker(i)}>{w.alive ? 'Crash' : 'Restart'}</button>
          </span>
        ))}
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, minmax(0, 1fr))', gap: 8, marginTop: 8 }}>
        {STATES.map((st) => (
          <div key={st} className="card" style={{ padding: 10, minHeight: 120 }}>
            <div style={{ fontSize: 13, fontWeight: 600, color: COLORS[st] }}>{st} ({byState(st).length})</div>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 3, marginTop: 6 }}>
              {byState(st).slice(-40).map((j) => (
                <span key={j.id} title={`job ${j.id} attempts ${j.attempts}${j.error ? ` · ${j.error}` : ''}`} style={{ fontSize: 11, fontFamily: 'var(--mono)', padding: '1px 5px', borderRadius: 4, background: 'var(--surface-2)', border: j.priority ? '1px solid var(--accent)' : '1px solid transparent' }}>
                  {j.id}{j.attempts > 1 ? `·${j.attempts}` : ''}
                </span>
              ))}
            </div>
          </div>
        ))}
      </div>
      <div className="log">{s.log.join('\n') || 'Enqueue some jobs to start.'}</div>
    </div>
  )
}
