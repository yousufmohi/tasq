# taskq: Design

Status: **approved 2026-11-03 and implemented.** Where implementation refined
the design, this document was updated in place, and `docs/MILESTONES.md`
records why. Numbers here are configuration defaults unless a test or
benchmark is cited. Measured results live in `results/failure/` and
`bench/results/`, and are summarised in the README and `docs/FAILURE_MODES.md`.

## 1. Goals and non-goals

**Goals**

- At-least-once execution of jobs that Redis acknowledged at enqueue time.
- Bounded, observable duplicate execution, with tools that let handlers make
  duplicates harmless.
- Crash recovery with no operator in the loop.
- Every multi-step state change is atomic, and every job state transition is
  checked against an explicit table.
- Failure behaviour that tests prove and benchmarks measure.

**Non-goals**

- Exactly-once delivery or execution. This is impossible against arbitrary
  side effects, and section 3 explains why.
- Durability beyond what the Redis deployment provides. taskq inherits the
  persistence and replication guarantees of Redis and documents them (section 8).
- Redis Cluster support. Keys are hash-tagged per queue so a later port is
  possible, but only a single primary, optionally with replicas, is tested.
- Workflows/DAGs, cron schedules, a UI, Kubernetes manifests.

## 2. Environment (inspected 2026-11-03)

| Item | Value |
|---|---|
| Host | Windows 11 Pro, WSL2 kernel 6.18.54.1-microsoft-standard-WSL2 |
| Distro | Ubuntu 22.04.4 LTS (`Ubuntu`) |
| CPU | AMD Ryzen 5 7600X, 6 cores / 12 threads (as seen by WSL) |
| Memory | 15 GiB visible to WSL |
| Disk | 1007 GB ext4 VHD, 948 GB free |
| git / gcc / make | 2.34.1 / 11.4.0 / 4.3 (gcc is needed for `-race`, cgo) |
| Go | **not installed** |
| Docker | Docker Desktop 29.7.2 runs on Windows, but **WSL integration is off for `Ubuntu`** (`docker` is a stub in the distro) |
| redis-cli, jq | not installed (not required; `docker exec` is enough) |

**Prerequisites before milestone 1**

1. Docker Desktop → Settings → Resources → WSL integration → enable `Ubuntu`.
   Testcontainers, the failure tests, and Compose all need this.
2. Install the current stable Go toolchain from go.dev into `/usr/local/go`
   (needs sudo) or `~/.local/go` (no sudo). I'll pin the exact version in `go.mod`.

**Benchmark caveat:** Redis runs in a Docker Desktop VM, and the clients run in
WSL2 or in containers on the same host. Results will measure this laptop
setup, not production network latency. Every result file records the
host, versions, and configuration.

## 3. Delivery guarantees

### What taskq guarantees

1. **At-least-once execution of acknowledged enqueues**: once `Enqueue` returns
   success, the job will be run to a terminal state (`succeeded` or `dead`),
   *provided the write that created it survives in Redis* (section 8).
2. **No silent drop on worker failure**: a worker that crashes, hangs, or is
   partitioned at any point after a job is delivered leaves the job in the
   stream's pending entries list (PEL). Another worker reclaims it after
   `visibility_timeout`.
3. **Fenced state transitions**: only the current lease holder can move a job
   out of `active`. A zombie worker whose lease was taken over can't overwrite
   the new owner's result (section 6.3).
4. **Stable idempotency key**: every delivery of a job carries the same job ID
   and the same idempotency key.

### What taskq does *not* guarantee

- **Exactly-once execution.** Duplicates happen in these known places:

| # | Scenario | Why a duplicate occurs |
|---|---|---|
| D1 | Worker finishes the side effect and then crashes before `complete` runs | Redis can't tell "ran but didn't report" apart from "never ran" |
| D2 | Lease expires while the handler is still running (GC pause, slow I/O, partition from Redis) | another worker reclaims and runs it concurrently |
| D3 | `complete` succeeds on the primary but a failover loses it (async replication) | the new primary still has the job in the PEL |
| D4 | Producer retries an enqueue after a timeout with no idempotency key, or after the dedup key expired or was lost in a failover | two distinct jobs exist |
| D5 | Graceful-shutdown `release` returns the job to the queue but the handler ignores context cancellation and finishes anyway | job is both finished and re-queued |

- **Ordering.** Concurrency, retries, and reclaims reorder jobs. FIFO within a
  priority level holds only for first delivery.
- **Durability beyond Redis.** See section 8.

### How idempotency limits the impact

There are two separate layers:

1. **Enqueue dedup** (handles D4): `Enqueue(..., IdempotencyKey: k)` checks
   `idem:{k}` atomically. A retried enqueue returns the original job ID
   instead of creating a second job. The key has a TTL (default 24h). It only
   covers retries inside that window, and the key itself can be lost in a
   failover.
2. **Side-effect dedup** (handles D1, D2, D3, D5): the queue can't make an
   arbitrary side effect idempotent. It provides:
   - `job.IdempotencyKey()`, which is stable across deliveries. Handlers forward
     it to downstreams that support idempotency (payment APIs, `INSERT ... ON
     CONFLICT DO NOTHING`, etc.). **This is the only approach that closes D1.**
   - an `idem.Once(ctx, key, fn)` helper backed by Redis (a claim key with a
     TTL plus a done marker). It suppresses re-execution for duplicates that
     arrive *after* the first run recorded completion. It narrows D2, D3 and
     D5 but can't close D1, because the side effect and the marker aren't in
     one transaction. This limit is stated in the helper's doc comment.

The failure tests report **two numbers**: *duplicate deliveries* (handler
invocations minus unique jobs) and *duplicate applied effects* (writes a
key-deduplicating sink accepted minus unique jobs). The gap between the two
shows what idempotency buys you.

## 4. Redis data model

All keys for a queue share the hash tag `{q}`, which keeps them in one slot for
multi-key Lua.

| Key | Type | Contents |
|---|---|---|
| `taskq:{q}:s:{p}` | Stream | one stream per priority `p` ∈ {`high`,`default`,`low`}; entry = `{id: <job id>}` |
| consumer group `workers` | on each stream | PEL holds delivered, unacked entries: owner, idle time, delivery count |
| `taskq:{q}:job:{id}` | Hash | job record (below); the source of truth for state |
| `taskq:{q}:sched` | ZSET | score = run-at (ms, Redis clock); delayed **and** retry-waiting jobs |
| `taskq:{q}:dead` | ZSET | score = dead-at ms; member = job id (DLQ index, paged by score) |
| `taskq:{q}:idem:{key}` | String | job id; `SET NX` with TTL (enqueue dedup) |
| `taskq:{q}:rl` | String | GCRA theoretical arrival time (rate limiter) |
| `taskq:{q}:cfg` | Hash | queue config: max depth, rate, burst, weights, visibility timeout |
| `taskq:queues` | Set | known queue names (for the API and metrics) |

**Job hash fields:** `id, queue, type, payload, priority, state, attempts,
max_attempts, idem_key, created_ms, run_at_ms, started_ms, finished_ms,
lease_token, lease_owner, last_error, stream_entry, traceparent, timeout_ms`.

**Why the payload lives in the hash and the stream holds only an ID:** a retry or
replay re-adds a 1-field entry instead of copying the payload. State lookups need
the hash anyway, and the stream stays a pure *delivery* mechanism while
the hash holds the *state*. The cost is one extra `HGETALL` per delivery, which the
`start` script performs as part of its own call, so there's no extra round trip.

**Memory:** `XACK` alone doesn't free stream memory, so `complete`, `fail`
and `release` also run `XDEL`. Succeeded job hashes get a TTL (`retention`,
default 24h). Dead jobs persist until replayed or purged.

**Required Redis configuration:** `maxmemory-policy noeviction`. Under any
eviction policy, Redis would silently delete job hashes or streams under memory
pressure, which breaks guarantee 1. With `noeviction`, writes fail loudly and
taskq surfaces that failure as a rejected enqueue. The worker checks the policy on startup and
refuses to start if it's wrong (overridable for tests).

**Clock:** every timestamp that affects scheduling comes from Redis `TIME` inside
scripts. Workers never compare their own wall clocks with each other's. In Redis 7
scripts replicate as effects, so calling `TIME` in a script is safe.

**Redis version:** pin `redis:7.4`, which provides `XAUTOCLAIM` (returns deleted IDs),
`WAITAOF`, and functions/scripts with effect replication. Redis 8.2's `XACKDEL` would
merge ack+delete, but Lua already makes that atomic, so the newer version adds nothing.

## 5. Job lifecycle and state machine

```
            enqueue (run_at > now)            enqueue (run_at <= now)
                 │                                     │
                 ▼            promote                  ▼
            ┌─────────┐ ─────────────────────▶ ┌─────────┐
            │scheduled│                        │ pending │◀────────────┐
            └─────────┘                        └────┬────┘             │
                                                    │ start            │ release
            ┌─────────┐   promote                   ▼                  │ (shutdown)
            │  retry  │ ──────────────────▶   ┌──────────┐ ────────────┘
            └─────────┘ ◀──── fail ────────── │  active  │ ◀─┐ start after reclaim
                 (attempts < max, retryable)  └──┬────┬──┘ ──┘ (new lease)
                                     complete    │    │ fail (attempts >= max
                                                 ▼    ▼  or non-retryable)
                                        ┌──────────┐ ┌──────┐
                                        │succeeded │ │ dead │──── replay ──▶ pending
                                        └──────────┘ └──────┘
```

| From | To | Event | Script |
|---|---|---|---|
| — | `scheduled` / `pending` | enqueue | `enqueue.lua` |
| `scheduled`, `retry` | `pending` | run_at reached | `promote.lua` |
| `pending` | `active` | worker starts delivery | `start.lua` |
| `active` | `active` | reclaimed delivery started by new owner (new lease token) | `start.lua` |
| `pending`, `active` | `dead` | started with `attempts >= max_attempts` (crash loop / poison pill) | `start.lua` |
| `active` | `succeeded` | handler returned nil | `complete.lua` |
| `active` | `retry` | handler error, attempts left, retryable | `fail.lua` |
| `active` | `dead` | attempts exhausted or `taskq.Permanent(err)` | `fail.lua` |
| `active` | `pending` | graceful shutdown timed out; attempt refunded | `release.lua` |
| `dead` | `pending` | operator replay; attempts reset | `replay.lua` |

`succeeded` is terminal. `dead` is terminal except through an explicit replay.

**Enforcement:** the table lives once in Go (`internal/state`). Each script
receives the allowed from-states as `ARGV` from that table, so the two can't
drift. A script rejects any other current state with an error code instead of
writing. Unit tests cover the table exhaustively (every from×to pair).
Integration tests try every illegal transition against real Redis and assert
it was rejected with no partial writes.

**Attempts are counted at `start`, not at failure.** A job that crashes its
worker (OOM, segfault in cgo, `os.Exit`) never reaches `fail`. If
the count were taken at failure, a poison pill would crash-loop forever. Counting at start
means `max_attempts` bounds total *starts*, including starts that crashed.

## 6. Core mechanisms

### 6.1 Enqueue: `enqueue.lua` (atomic)

In one script: (1) if an idempotency key is given and `idem:{k}` exists, return
the existing ID; (2) backpressure check: if `XLEN` of the priority streams + `ZCARD sched`
≥ `max_depth`, return `QUEUE_FULL`; (3) `HSET` the job; (4) `XADD` to the
priority stream, or `ZADD sched` if delayed; (5) `SET idem:{k} id NX PX ttl`.

*Why atomic:* if the steps were split, a crash between `HSET` and `XADD` would leave an orphan
hash that never runs, and two concurrent producers with the same key could both
pass the existence check. `MULTI` can't express "check, then conditionally
write". `WATCH`/`MULTI` could, but it retries under contention, and Lua is
simpler to reason about.

**Optional durability:** `EnqueueOptions.Durability{LocalFsync, Replicas,
Timeout}` follows the script with `WAITAOF 1 <replicas>` or `WAIT <replicas>`.
Both only cover writes made earlier on *the same connection*, so the
script (as `EVAL`, which a flushed script cache can't fail) and the wait go
in one pipeline. If the wait times out, the result is **uncertain**, not
failed. The client returns the job ID with `ErrDurabilityUncertain`, and the
producer can safely retry with the same idempotency key. (See section 8 for
what this does and doesn't buy, and `TestFailoverDemo` for measurements.)

### 6.2 Fetch with priorities and starvation prevention

Each queue has three streams. Each fetch:

1. Compute a weighted-random order over priorities (default weights
   high:6, default:3, low:1) in Go.
2. In one round trip (`fetch.lua`), try non-blocking `XREADGROUP COUNT n`
   on each stream in that order and take from the first non-empty one.
   `n` is the number of free concurrency slots, so a worker never claims
   more than it can start.
3. If all are empty, block on all three with `XREADGROUP BLOCK 1000`.
   COUNT applies per stream, so this can return up to three entries; any
   beyond the free slots are buffered. They're already in this worker's PEL,
   and `start.lua`'s ownership check makes them harmless if they wait too long.

Under sustained backlog on every level, `low` is tried first in ~10% of
fetches. Its expected share of throughput therefore never drops below
its weight, so it can't starve. A test asserts the observed share stays within
a tolerance band of the weights.

*Rejected:* **strict priority**, which starves low under sustained high load
(available as `weights: strict` for users who want it). **Aging** (bumping old
jobs' priority) needs a periodic rescoring scan and gives no simple bound. A
**single ZSET used as a priority queue** loses consumer groups, the PEL, and
`XAUTOCLAIM`, so I'd have to rebuild them.

*Cost, as implemented:* one round trip per fetch, for up to `n` jobs. The
first version tried each stream from the client, one entry per round trip.
Under 50 ms of injected latency it took 39.4 s to drain 300 jobs (development test logs; the final recorded run is in `results/failure/TestToxiproxyLatency.json`). Moving the
reads into `fetch.lua` cut that to 21.4 s, and batching up to the free slots
cut it to 9.1 s (`TestToxiproxyLatency`). Batching doesn't change the
weighting: which stream a fetch picks is still weighted, and batch size
doesn't depend on the stream picked.

### 6.3 Start, leases, and fencing: `start.lua`

The worker receives `(stream, entry_id, job_id)` from `XREADGROUP`/`XAUTOCLAIM`
and runs `start.lua`, which atomically:

- loads the hash. If it's missing (expired or purged), it `XACK`+`XDEL`s and
  returns `SKIP`.
- if the hash's `stream_entry` isn't this entry, it `XACK`+`XDEL`s and returns
  `SKIP stale_entry`. Invariant: a job has at most one live stream entry, and
  only that entry can start it, so a stray duplicate entry can't cause a
  second concurrent run.
- if the state is `succeeded` or `dead` (a duplicate entry, or the earlier owner
  finished just before the reclaim), it `XACK`+`XDEL`s and returns `SKIP`. This is
  counted as `taskq_duplicate_deliveries_suppressed_total`.
- if `attempts >= max_attempts`, moves the job to `dead` ("attempts exhausted
  before start") and acks the entry.
- otherwise sets `state=active`, `attempts++`, a fresh random `lease_token`,
  `lease_owner=<consumer>`, and `started_ms=TIME`, then returns the job.

**There's a gap between `XREADGROUP` and `start.lua`.** If the worker crashes
inside it, the entry is in the PEL while the hash still says `pending`. That's
safe because recovery is driven by PEL idle time, not by hash state. The entry is
reclaimed like any other.

*Rejected:* running `XREADGROUP` inside Lua to close that gap. Blocking
reads aren't allowed in scripts, so I'd need a polling loop. The gap is already safe.

**Fencing token:** `complete`, `fail`, `release` and `heartbeat` all require
the caller's `lease_token` to equal the hash's. When worker B reclaims and starts
a job, the token rotates, and from then on any late call from zombie worker A is
rejected (`LEASE_LOST`). A may already have performed its side effect (D2), which
is why the token protects *queue state*, not the outside world.

Edge case: if B has reclaimed the PEL entry but hasn't yet run `start`, A's
token is still current. If A completes in that window, the job is `succeeded`,
and B's `start` sees a terminal state and skips. That's correct.

### 6.4 Heartbeats and crash recovery

- **Lease = PEL idle time.** A delivered entry's idle time resets whenever it's
  claimed. Each worker runs `heartbeat.lua` for its in-flight jobs every
  `visibility_timeout / 3` (default 30s / 10s). The script checks that
  `XPENDING <id> <id>` still names this consumer and that the token matches,
  then runs `XCLAIM ... 0 <id> JUSTID` to itself (resetting idle time without
  bumping the delivery counter) and updates `lease_until`.
  **The ownership check is essential:** an unconditional `XCLAIM` with
  min-idle 0 would steal back a job that another worker had legitimately
  reclaimed.
- **Reclaim:** every worker runs `XAUTOCLAIM <stream> workers <me>
  <visibility_timeout> <cursor> COUNT n` on a jittered interval, walking the
  cursor to the end. It then passes the claimed entries through `start.lua` like
  fresh ones. Deleted IDs that `XAUTOCLAIM` returns are logged and ignored.
- **Self-fencing:** if a worker can't heartbeat successfully for
  `visibility_timeout - safety_margin`, it cancels the handler's context,
  because someone else may already own the job. This narrows D2 but can't
  eliminate it: handlers can ignore cancellation, and the lease timer runs on
  the worker's clock while idle time runs on Redis's.
- **Recovery time** after a crash ≈ `visibility_timeout + reclaim_interval`.
  The tradeoff: a shorter timeout recovers faster but reclaims more falsely
  (duplicates) during GC pauses and latency spikes. The benchmark measures both
  sides at several timeouts.

*Rejected:* **per-job lock keys with TTL (Redlock-style)**, which add a second
source of truth that can disagree with the PEL. **Stream delivery count as the
attempt counter**: `XAUTOCLAIM` increments it, but `JUSTID` and releases
complicate it, and it's lost when an entry is re-added on retry.

### 6.5 Completion and failure: `complete.lua`, `fail.lua`

- `complete`: token check → `state=succeeded`, `finished_ms` → `XACK`+`XDEL`
  → `PEXPIRE` the hash by `retention`.
- `fail`: token check → if retryable and attempts remain: `state=retry`,
  `ZADD sched (TIME + delay)` and `XACK`+`XDEL`; otherwise `state=dead`,
  `ZADD dead` and `XACK`+`XDEL`.

*Why atomic:* if ack and reschedule were split, a crash between them would either
**lose** the job (acked, never rescheduled) or **duplicate** it (rescheduled
and then reclaimed from the PEL).

**Backoff:** full jitter. `delay = rand(0, min(cap, base·2^(attempt-1)))`, with
defaults `base=1s`, `cap=10m`. The delay is computed in Go, and the absolute
time comes from Redis `TIME` in the script. *Rejected:* no jitter (synchronised
retry storms against a recovering downstream); "equal jitter" (smaller spread,
worse contention); decorrelated jitter (comparable, harder to explain and test).
A deterministic RNG in tests lets them assert the distribution bounds.

### 6.6 Delayed jobs: `promote.lua`

Every worker periodically (default every 250ms, jittered) runs
`promote.lua`: `ZRANGEBYSCORE sched -inf TIME LIMIT 0 100`, and for each
job `ZREM`, verify `state ∈ {scheduled, retry}`, `XADD` to its priority stream,
and set `state=pending`.

*Why atomic:* if `ZREM` and `XADD` were separate, a crash between them would lose the
job, and two promoters racing between the read and the remove would add it
twice. Because scripts are serialised, many workers can promote concurrently
without coordinating or electing a leader.
*Bound:* `LIMIT 100` caps the time Redis is blocked by one script, and the
loop runs again immediately if it hit the limit.
*Precision:* a job runs at `run_at + [0, promote_interval) + queue wait`. This
is documented, not hidden.

### 6.7 Rate limiting and backpressure

- **Rate limit (per queue, global across workers):** GCRA in `ratelimit.lua`
  using Redis `TIME`. A worker acquires a token *before* fetching, so a
  rate-limited job is never pulled into a PEL to sit idle. If the fetch then
  comes back empty, the token is wasted. Burst capacity absorbs this, and it's
  documented. *Rejected:* a fixed-window counter (2× burst at window edges);
  local per-worker token buckets (the global rate becomes rate × workers).
- **Producer backpressure:** the `max_depth` check in `enqueue.lua`, exposed as
  `ErrQueueFull` and HTTP `429` with `Retry-After`.
- **Consumer backpressure:** each worker has a fixed concurrency semaphore
  and fetches only when a slot is free (no prefetch beyond capacity). Undelivered
  jobs therefore stay in the stream, where any worker can take them, instead of
  queuing up behind one slow worker.

### 6.8 Dead-letter queue

The DLQ is `dead` (a ZSET) plus the job hashes, which keep `last_error` and
`attempts`. The API supports listing (paged by dead-at), inspecting one job,
replaying one or N jobs (`replay.lua`: `ZREM dead`, reset `attempts`, set
`state=pending`, `XADD`, all atomic so a replay can't happen twice or vanish),
and purging by age.

### 6.9 Graceful shutdown

On SIGTERM/SIGINT the worker:

1. stops fetching, reclaiming and promoting;
2. keeps heartbeating in-flight jobs and waits up to `drain_timeout`
   (default 25s, below Compose's `stop_grace_period: 30s`);
3. on timeout, cancels handler contexts, waits a short grace period, then
   runs `release.lua` on whatever is still running: token check, `XACK`+`XDEL`,
   re-`XADD`, `state=pending`, and the attempt refunded, so a deploy doesn't burn
   retries. This sends unfinished jobs back immediately instead of after
   `visibility_timeout`;
4. removes its consumer from the group **only if it has no pending entries**.
   This is checked and deleted atomically in Lua, because `XGROUP DELCONSUMER`
   silently drops the consumer's PEL entries, which would turn a shutdown bug
   into job loss.

A separate GC removes consumers that are idle for longer than `consumer_ttl`
and have zero pending entries, using the same atomic check.

## 7. Atomicity inventory

| Script | Multi-step change | Failure it prevents |
|---|---|---|
| `enqueue.lua` | dedup check + depth check + HSET + XADD/ZADD + SET idem | orphan hash, duplicate job on concurrent retries, depth limit overshoot |
| `promote.lua` | ZRANGEBYSCORE + ZREM + XADD + HSET | lost or double-promoted delayed jobs |
| `start.lua` | state check + attempts++ + token rotation (+ ack on skip/dead) | two owners believing they're current; uncounted crash attempts |
| `heartbeat.lua` | ownership check + XCLAIM JUSTID | stealing back a reclaimed job |
| `complete.lua` | token check + state + XACK + XDEL + PEXPIRE | acked-but-not-succeeded, or succeeded-but-still-in-PEL |
| `fail.lua` | token check + XACK + XDEL + ZADD sched/dead + state | lost or duplicated retries |
| `release.lua` | token check + XACK + XDEL + XADD + state + attempt refund | lost job on shutdown |
| `replay.lua` | ZREM dead + reset + XADD + state | double replay, vanished replay |
| `ratelimit.lua` | read TAT + compare + write | rate overshoot under concurrent workers |
| `delconsumer.lua` | pending==0 check + DELCONSUMER | dropping PEL entries |
| `fetch.lua` | weighted non-blocking reads (*latency, not atomicity*) | one round trip per stream tried |

Atomicity outside Lua: `Durability` pipelines `EVAL` + `WAIT`/`WAITAOF` on one
connection. That isn't for atomicity; WAIT only observes the connection's own
earlier writes.

**Two properties of Lua that the scripts rely on:**

- A script is atomic with respect to other clients, but **there's no rollback**:
  if it errors halfway, earlier writes stay. Every script therefore validates
  all inputs and reads first and only then writes. Integration tests drive the
  rejection paths (illegal states for complete/fail/replay/promote, stale lease
  tokens for complete/fail/heartbeat) and assert that nothing was written.
- Scripts block Redis while they run, so every loop is bounded (`LIMIT`/`COUNT`).
- Under `noeviction` at `maxmemory`, Redis rejects a script with OOM only on its
  *first* write (`scriptVerifyOOM`: once a script has written, it is allowed to
  finish). Combined with validating before writing, a full Redis yields a clean
  rejection, never a half-applied change (`TestEnqueueFailsLoudlyWhenRedisIsFull`
  checks that no orphan hash is left).
- Scripts derive damaged-field fallbacks (an unknown priority becomes `default`)
  instead of raising mid-script, because a Lua error after a write is a partial write.

Script effects replicate and persist to AOF as a `MULTI`/`EXEC` unit, so a
replica or AOF reload sees all of a script's writes or none of them.

## 8. Persistence and failover: what can be lost

Redis acknowledges a write once it's applied in memory on the primary.
Durability depends on configuration:

| Config | Single-node crash (SIGKILL / power loss) |
|---|---|
| no persistence | everything |
| RDB snapshots | everything since the last snapshot |
| AOF `appendfsync everysec` (Compose default) | up to ~1s of writes (more if fsync stalls) |
| AOF `appendfsync always` | nothing that was acknowledged, at a large throughput cost |
| `everysec` + per-enqueue `WAITAOF 1 0` | acknowledged *enqueues* survive; acks, retries and promotions may still roll back |

**Replication is asynchronous.** When a primary fails and Sentinel (or an
operator) promotes a replica, writes inside the replication lag window are lost:

- **Lost enqueues** → acknowledged jobs vanish. *This is the only way taskq
  loses an acknowledged job*, and it's a Redis-level loss.
- **Lost `complete`/`fail`** → the job reappears in the PEL and is re-run (D3).
- **Lost `promote`/`start`/token rotation** → re-executed, never lost.
- **Lost idempotency keys** → a producer retry can create a duplicate (D4).
- **Split brain:** an old primary partitioned with some clients keeps
  accepting writes, and those are discarded when it rejoins as a replica.
  `min-replicas-to-write 1` + `min-replicas-max-lag 10` bound this window:
  the old primary refuses writes once it has gone 10s without hearing from a replica.

**Mitigations and their limits:**

- `WAIT 1 <timeout>` after enqueue confirms that at least one replica received
  the write. It *reduces* the loss window but isn't a consensus guarantee:
  failover can still promote a replica that didn't receive it (for example, the
  acking replica is the one that died). The Redis documentation says plainly that
  `WAIT` doesn't make Redis strongly consistent.
- The cost: one replica round trip per enqueue. The benchmark measures it.
- Jobs whose loss is unacceptable belong in a system with consensus
  replication (Kafka with `acks=all` + `min.insync.replicas`, or a database
  using an outbox). This is a stated tradeoff, not an oversight.

## 9. Observability

- **Logs:** `log/slog` JSON with `queue, job_id, type, attempt,
  lease_owner, trace_id` on every job-scoped line.
- **Metrics (Prometheus):** counters for enqueued/started/succeeded/retried/dead/
  reclaimed/released/duplicate-suppressed/lease-lost/rate-limited/rejected;
  histograms for queue wait (enqueue→start, from Redis timestamps) and
  handler duration; gauges per queue, sampled by a collector, for stream
  lag (`XINFO GROUPS`), PEL size, scheduled size and DLQ size; and a script
  latency histogram labelled by script.
- **Tracing (OpenTelemetry):** the producer span's W3C `traceparent` is stored
  on the job. The worker span is a new root that is **linked** to the producer
  span, not a child of it, because a job can run hours later and many times.
  This follows OTel messaging semantic conventions. OTLP export is used when
  `OTEL_EXPORTER_OTLP_ENDPOINT` is set, with a no-op otherwise.

## 10. Components and dependencies

```
cmd/taskq-api      HTTP API (net/http 1.22+ routing, no framework)
cmd/taskq-worker   example worker binary (also used as the SIGKILL target)
cmd/taskq-bench    load generator + result writer
taskq/             client library: Enqueue, Inspect, DLQ ops
worker/            runtime: fetch, start, heartbeat, reclaim, promote, shutdown
internal/scripts   embedded Lua + loader (EVALSHA with NOSCRIPT fallback)
internal/state     transition table
idem/              Once() helper
test/integration   testcontainers-based
test/failure       SIGKILL, Redis restart, Toxiproxy
bench/             scripts, hardware capture, raw results (JSON/CSV)
deploy/            compose, prometheus.yml, grafana provisioning
```

**HTTP API**

| Method & path | Purpose |
|---|---|
| `POST /v1/queues/{q}/jobs` | enqueue → `202 {id}`; `200` on dedup hit; `429` queue full |
| `GET /v1/queues/{q}/jobs/{id}` | job state |
| `GET /v1/queues/{q}/stats` | depth per priority, PEL, scheduled, DLQ |
| `GET /v1/queues/{q}/dead?after=&limit=` | list DLQ |
| `POST /v1/queues/{q}/dead/{id}/replay` | replay one |
| `POST /v1/queues/{q}/dead/replay?limit=` | replay batch |
| `DELETE /v1/queues/{q}/dead?older_than=` | purge |
| `GET /healthz`, `/readyz`, `/metrics` | ops |

**Dependencies:** `github.com/redis/go-redis/v9`, `prometheus/client_golang`,
`go.opentelemetry.io/otel` (+ OTLP exporter), `testcontainers-go` (+ redis and
toxiproxy modules, tests only). Logging, routing and JSON use only the
standard library. Job IDs are 128-bit random hex, with no UUID dependency.

## 11. Test and measurement plan

- **Unit:** transition table, backoff bounds, priority weighting, GCRA math,
  config validation.
- **Integration (testcontainers, real Redis):** every script's happy and
  error paths; illegal transitions; concurrent enqueue dedup; concurrent
  promoters; heartbeat-vs-reclaim race; DelConsumer safety.
- **Race detector:** `go test -race ./...` everywhere, enforced in `make test`.
- **Failure tests** (each ends with the same invariant check):
  *every job whose enqueue returned success is in a terminal state, and every
  `succeeded` job ran ≥1 time*. They report the duplicate deliveries and
  duplicate applied effects.
  - SIGKILL 1..k worker processes mid-job (workers run as subprocesses, since
    goroutines can't be SIGKILLed).
  - Redis restart: graceful (`SHUTDOWN`) and SIGKILL, under `everysec` and
    `always`. The test asserts zero loss where the config guarantees it and
    *reports* the observed loss where it doesn't (`everysec` + SIGKILL).
  - Toxiproxy: latency (e.g. 50ms ± jitter), latency above the visibility timeout
    (forced false reclaim → duplicates expected and counted), a partition via the
    `timeout` toxic, and connection resets.
  - Failover demo (primary + replica): kill the primary mid-load and promote the
    replica. Reports loss with and without `WAIT`. This is a documented
    demonstration, not a zero-loss assertion.
- **Load tests (`taskq-bench`):** a fixed job mix and payload size; worker
  processes ∈ {1,2,4,8} × concurrency; enqueue rate open-loop
  (fixed arrival rate, which avoids coordinated omission) and closed-loop
  (max throughput). Reports throughput, p50/p95/p99 of end-to-end latency
  (enqueue→complete, Redis clock) and queue wait. **Recovery time** = time from
  SIGKILL until every orphaned job is complete, reported against
  `visibility_timeout`. Each run writes raw per-job samples plus a summary JSON
  with git SHA, Go/Redis/Docker versions, CPU, memory and config. Percentiles
  are computed from raw samples (sorted), not from Prometheus buckets.

## 12. Milestones

Each milestone ends with `go test -race` green and one focused commit, and I stop to review it with you.

1. **Walking skeleton:** module, Redis in Compose, `enqueue.lua`, fetch, `start`,
   `complete`, integration test (enqueue → process → ack).
2. **State machine + failure path:** transition table, fencing tokens, `fail.lua`,
   backoff/jitter, retries via `sched`, DLQ + replay.
3. **Crash recovery:** heartbeats, `XAUTOCLAIM` reclaim, self-fencing, first SIGKILL test.
4. **Delayed jobs:** `promote.lua`, concurrency test with many promoters.
5. **Priorities + rate limit + backpressure.**
6. **Graceful shutdown:** drain, release, safe consumer deletion.
7. **Idempotency:** enqueue dedup, `idem.Once`, dual duplicate counters.
8. **HTTP API.**
9. **Observability:** slog, Prometheus, OTel.
10. **Full Compose stack:** API, workers, Prometheus, and Grafana with a provisioned dashboard.
11. **Failure suite:** Redis restarts, Toxiproxy, failover demo.
12. **Load tests + results.**
13. **README, FAILURE_MODES.md, resume bullets** (measured numbers only).

## 13. Decisions (approved 2026-11-03)

1. **Trace backend:** Jaeger in Compose behind `--profile tracing`. The default
   stack stays Redis, API, workers, Prometheus and Grafana.
2. **Failover test:** manual replica promotion (primary + one replica). This is a
   demonstration that reports loss with and without `WAIT`. No Sentinel.
3. **Default durability:** AOF `appendfsync everysec`, with `WAITAOF`/`WAIT`
   opt-in per enqueue. `always` is exercised in the Redis-restart failure tests
   and in benchmarks for comparison.
4. **Repository:** local git only, with no remote until the owner sets one up.
