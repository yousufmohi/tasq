# Failure modes

Each row describes a failure, what taskq does about it, whether acknowledged
work can be lost or duplicated, the test that proves the behaviour, and what
was measured. Nothing here is estimated: every number is copied from one of
three result sets, all generated from a clean checkout of commit `9c54507` on
the machine described in their `env.json` (Ryzen 5 7600X, WSL2, Redis 7.4.12
in Docker Desktop):

- **[F]** `results/failure/20261104T180504Z/summary.md`: the failure suite
  (`bench/failure.sh 5`), 5 runs, all passed. Per-run reports are in
  `run<N>/*.json` and full logs in `run<N>.log.gz`. Ranges below are min–max
  over the 5 runs.
- **[T]** `results/tests/20261104T182333Z/summary.md`: the unit + integration
  suite (`bench/integration.sh 3`), 3 runs. Every test named below passed in
  all 3 runs. "Asserted" means the test fails unless that statement holds.
- **[B]** `bench/results/20261104T174524Z/summary.md`: the benchmarks.

Terms used below:
- **Acked** means `Enqueue` returned success.
- **Lost** means an acked job that never ran to completion and never will. The checker
  looks it up both in the queue Redis and in an independent recorder Redis,
  which survives failures injected into the queue Redis.
- **Duplicates** come in three kinds, because idempotency is layered:
  - *executions*: handler invocations beyond one per job;
  - *naive effects*: a non-idempotent side effect repeated;
  - *Once effects*: the same effect guarded by `idem.Once`. The idempotent
    downstream (`SADD` of the idempotency key) can never repeat by construction.

Across the 8 failure scenarios that assert zero loss, the 5 runs acked
154,278 jobs in total and lost **0** ([F], "Scenarios that assert zero loss").

## Worker failures

| Failure | Behaviour | Lost? | Duplicates? | Proven by | Measured |
|---|---|---|---|---|---|
| Worker process killed (SIGKILL/OOM) mid-job | Its PEL entries go idle. Another worker's `XAUTOCLAIM` reclaims them after the visibility timeout, and `start.lua` rotates the lease token and counts the attempt. | No | Executions yes. Effects only if the kill lands between the effect and `complete` (D1). | `TestSIGKILLWorkersMidJob`, `TestReclaimAbandonedJob` | [F] 10 SIGKILLs across 4 processes, 401 jobs per run: **0 lost** in every run, 42–54 duplicate executions, 0–1 duplicate naive effects, 0 duplicate `idem.Once` effects, 27–32 jobs reclaimed |
| Worker dies after `XREADGROUP`, before `start` | The entry is in the PEL while the hash says `pending`. Reclaim goes by PEL idle time, so it's recovered the same way. | No | No (it never started) | `TestReclaimUnstartedDelivery` | [T] asserted: attempts = 1 after recovery |
| Job crashes its worker on every attempt (poison pill) | Attempts are counted at `start`. Once they're exhausted, `start.lua` dead-letters the job instead of running it. | No (dead, inspectable) | Bounded by `MaxAttempts` | `TestPoisonPillIsDeadLettered`, poison job in `TestSIGKILLWorkersMidJob` | [T] asserted: dead after 2 starts with `MaxAttempts: 2`. [F] asserted in every run: dead after exactly 3 starts with `MaxAttempts: 3` |
| Handler returns an error | `fail.lua` acks the entry and schedules a retry with full-jitter backoff, or dead-letters it. | No | Retries are intended | `TestRetryThenSucceed`, `TestExhaustedAttemptsDeadLetter`, `TestRetryIsScheduledWithBackoff` | [T] |
| Handler error marked `taskq.Permanent` | Dead-lettered immediately | No | No | `TestPermanentErrorSkipsRetries` | [T] asserted: attempts = 1 with `MaxAttempts: 10` |
| Handler panics | Recovered and treated as a failure. The worker keeps running. | No | No | `TestHandlerPanicIsContained` | [T] |
| Handler hangs | The per-job timeout cancels its context, which counts as a failure. | No | No | `TestJobTimeoutCountsAsFailure` | [T] |
| Job runs longer than the visibility timeout | Heartbeats (`XCLAIM … JUSTID`) keep resetting the idle time. | No | No | `TestHeartbeatKeepsLongJobAlive` | [T] asserted: a job running 4× the visibility timeout next to an idle peer worker runs once (attempts = 1) |
| Worker stalls past the visibility timeout (GC pause, VM freeze), then resumes | Another worker reclaims the job and rotates the token. The stalled worker's `start`, heartbeat, `complete` and `fail` are all rejected (`not_owner` / `LEASE_LOST`) and write nothing. | No | Possible (D2): two handlers can overlap | `TestCompleteIsFencedByLeaseToken`, `TestFailIsFencedByLeaseToken`, `TestHeartbeatDoesNotStealReclaimedEntry`, `TestLeaseLossCancelsHandler` | [T] |
| Graceful shutdown (deploy) | Intake stops, leases stay renewed, handlers drain for up to `DrainTimeout`. Survivors are cancelled and released with their attempt refunded. The consumer is deleted only if its PEL is empty. | No | No for cooperative handlers | `TestRunDrainsInFlightOnCancel`, `TestShutdownReleasesInterruptedJob`, `TestShutdownIdleDeletesConsumer` | [T] |
| Handler ignores cancellation at shutdown | Its job is released after `ReleaseGrace`. Its late completion is fenced because the token was cleared. Logged at ERROR. | No | Possible (D5) | `TestShutdownReleasesUncooperativeHandler` | [T] asserted: shutdown returns within 2 s although the handler never returns on its own |
| Crashed workers leave consumers behind | The consumer GC removes consumers that are idle past `ConsumerTTL` *and* own no pending entries, atomically. | No | No | `TestConsumerGC`, `TestDelConsumerRefusesWhilePending` | [T] |

Recovery time after a worker SIGKILL under load is benchmarked separately
([B], recovery table): with 4 workers it tracked the visibility timeout,
2,351 ms median (2,230–2,532) at 2 s, 5,283 ms (5,232–5,411) at 5 s and
10,441 ms (10,201–10,752) at 10 s, 5 runs each, with every job completed.

## Network failures (Toxiproxy)

| Failure | Behaviour | Lost? | Duplicates? | Proven by | Measured |
|---|---|---|---|---|---|
| 50 ms ± 10 ms latency each way, worker ↔ Redis | Slower, still correct. One round trip per fetch (`fetch.lua`), and each fetch takes up to the number of free slots. | No | No | `TestToxiproxyLatency` | [F] 300 jobs, 2 workers × 8: **0 lost, 0 duplicates** in every run; drained in 9.2–9.7 s |
| Silent partition of one worker (no data, connections held) for 4× VT | The partitioned worker's heartbeats fail, and it cancels its handlers at VT − HB (self-fencing). The healthy worker reclaims after VT. | No | Yes (D2); bounded by idempotency | `TestToxiproxyPartitionOneWorker` | [F] 60 jobs, 6–8 in flight at the cut: **0 lost** in every run; 8 reclaimed, 8 duplicate executions and **8 duplicate naive effects, 0 duplicate `idem.Once` effects, 0 at the idempotent downstream** in every run; all done 4.0 s after the cut |
| Connection resets (proxy flapping 150 ms down / 150 ms up, 10×) | go-redis reconnects. Failed `complete` calls leave the job in the PEL for reclaim. | No | A few | `TestToxiproxyConnectionResets` | [F] 400 jobs: **0 lost** in every run; 1–4 duplicate executions |

## Redis failures

| Failure | Behaviour | Lost? | Duplicates? | Proven by | Measured |
|---|---|---|---|---|---|
| Graceful restart (SIGTERM), AOF `everysec` | Redis fsyncs the AOF on shutdown. Producers and workers ride through the outage. | **No** (asserted) | A few: a `complete` that hits the outage is redelivered | `TestRedisRestart/graceful_aof_everysec` | [F] 7,759–8,294 acked: **0 lost**; 2–7 duplicate naive effects; outage 523–571 ms |
| Redis process SIGKILL, AOF `always` | Every write is fsynced before the reply | **No** (asserted) | A few, as above | `TestRedisRestart/sigkill_aof_always` | [F] 4,227–4,723 acked: **0 lost**. Throughput cost of `always`: 2,150–2,283 enqueues acked in the 1.5 s before the disruption, against 3,843–3,998 with `everysec` (graceful case), same 8 producers |
| Redis process SIGKILL, AOF `everysec` | The process dies but the kernel page cache survives, so already-written AOF data is kept. | Observed none (reported, not asserted) | A few, as above | `TestRedisRestart/sigkill_aof_everysec` | [F] 7,692–7,972 acked: 0 lost in every run |
| **Host crash** (power loss / kernel crash), AOF `always` | Simulated with LazyFS: Redis is SIGKILLed and every write that was never fsynced is dropped. Every acked write had been fsynced. | **No** (asserted) | A few | `TestRedisRestart/hostcrash_aof_always` | [F] 6,930–7,320 acked: **0 lost**; 0–2 duplicate executions |
| **Host crash**, AOF `everysec` | Writes since the last background fsync are gone: acknowledged enqueues *and* completions. Lost completions make finished jobs run again. | **Yes** | **Yes** (lost completions) | `TestRedisRestart/hostcrash_aof_everysec` (reported) | [F] 7,203–7,700 acked: **1,443–2,407 lost**; the oldest lost job had been acked 498–898 ms before the crash. 380–643 duplicate executions and the same number of duplicate naive effects; 0 duplicate `idem.Once` effects (its records live in the separate recorder Redis) |
| Host crash, `appendfsync no` (positive control) | Nothing is ever fsynced | **Yes** | — | `TestRedisRestart/hostcrash_aof_no_control`: proves the simulation drops unsynced data | [F] 7,372–7,586 acked: 2,823–2,904 lost |
| Redis loses everything (no persistence, or failover to an empty replica) | Acked-but-unexecuted jobs are gone. Workers get NOGROUP and **recreate the consumer groups** instead of failing forever. | **Yes** | — | `TestRedisRestart/sigkill_no_persistence_control`: proves the harness detects loss | [F] 7,759–8,094 acked: 3,011–3,128 lost |
| Enqueue reply lost (Redis applied the write, then the connection failed before the reply) | The producer sees an error although the job exists and will run | No | **Yes, if the producer retries without an idempotency key** (D4) | `TestEnqueueIdempotencyKeyDeduplicates`, `TestConcurrentEnqueueSameKeyCreatesOneJob`; observed in `TestRedisRestart` | [F] `applied_not_acked` (jobs that ran although `Enqueue` returned an error): 0–8 per run with SIGKILL + `always`, 0–4 on a graceful restart, 0–1 on a host crash with `always` |
| Failover with async replication (primary dies, replica promoted) | Writes acked by the primary but not yet replicated are gone. | **Yes** | Lost completions mean re-runs (D3) | `TestFailoverDemo/async_replication` (demonstration) | [F] see the next section |
| Same, with `min-replicas-to-write 1`, `min-replicas-max-lag 1` | The primary refuses writes once its replica has been silent for 1 s, which bounds the loss window | Bounded | — | `TestFailoverDemo/min_replicas_to_write` (demonstration) | [F] 4–8 of 15,453–17,787 acked lost; 485–499 enqueues returned errors (the async mode, which refuses nothing, gets 16: the producers' in-flight calls when the primary dies) |
| Same, with `Durability{Replicas: 1}` (`WAIT 1`) | An enqueue is acked only after the replica confirms it. Unconfirmed ones return `ErrDurabilityUncertain`. | **No** (asserted, one-replica topology) | — | `TestFailoverDemo/wait_1_replica` | [F] 9,158–10,295 acked: **0 lost** in every run; 80–95 enqueues returned `ErrDurabilityUncertain`, of which 72–94 were missing after the failover |
| Redis memory full (`maxmemory`, `noeviction`) | Enqueue fails loudly with OOM. Nothing is evicted. The script never half-writes: Redis raises OOM only on a script's *first* write (`scriptVerifyOOM`), and every taskq script validates before writing. | No | — | `TestEnqueueFailsLoudlyWhenRedisIsFull` | [T] asserted: no orphan job hash is left |
| Redis configured to evict | The worker refuses to start, because eviction would silently delete jobs | — | — | `TestWorkerRefusesEvictingRedis` | [T] |
| Script cache flushed (restart, failover) | go-redis falls back from `EVALSHA` to `EVAL` on NOSCRIPT. The durability pipeline uses `EVAL` directly. | No | No | `TestScriptsSurviveScriptFlush` | [T] |

### What the failover numbers mean

The failover demonstration partitions the replication link for 3 s while 16
producers keep writing to the primary, then kills the primary and promotes
the replica. With asynchronous replication, the primary acknowledges every
write during the partition and none of them reach the replica. So the loss is
whatever was acknowledged while the replica was cut off: in [F], 44,607–59,391
lost, against 44,090–58,847 acked during the partition (the 445–573 extra
were acknowledged before the partition but had not reached the replica).

That count is set by the test's design (partition length × write rate),
not by taskq. Measured this way, it says nothing about how much a real
failover would lose. The point is the mechanism. Without `WAIT`,
an acknowledged write is only as safe as the replication lag at the moment
the primary dies. `min-replicas-to-write` stops accepting writes once the
replica has been silent for a second. `Durability{Replicas: 1}` turns the
writes the replica didn't confirm into explicit `ErrDurabilityUncertain`
errors instead of acknowledgements: 0 acked jobs lost.

## Concurrency hazards

| Hazard | Behaviour | Proven by | Measured |
|---|---|---|---|
| Two promoters move the same due job | `promote.lua` does `ZRANGEBYSCORE`+`ZREM`+`XADD` atomically | `TestConcurrentPromotersPromoteExactlyOnce` | [T] asserted: 20 promoters, 2,000 jobs, each with exactly one stream entry |
| Two operators replay the same dead job | `replay.lua` checks state and moves the job atomically | `TestReplayDead` | [T] asserted: 8 concurrent replays, exactly 1 succeeds |
| Producers race past the depth limit | The depth check and the write are one script | `TestBackpressureUnderConcurrentProducers` | [T] asserted: 20×20 enqueues at `MaxDepth` 100, exactly 100 accepted |
| Producers race on one idempotency key | Check and set are one script | `TestConcurrentEnqueueSameKeyCreatesOneJob` | [T] asserted: 50 producers, 1 job |
| A stray duplicate stream entry for a job | Only the entry the hash points to can start it. Others are acked and dropped. | `TestStartRejectsStaleEntry` | [T] |
| A heartbeat steals back a reclaimed entry | The heartbeat checks PEL ownership and the token before `XCLAIM` | `TestHeartbeatDoesNotStealReclaimedEntry` | [T] |
| An illegal state transition | Every script checks the Go transition table and writes nothing on rejection | `TestIllegalTransitionsWriteNothing` (every illegal from-state × script), `internal/state` unit tests | [T] |
| Many workers share a rate limit | GCRA in Lua on the Redis clock: global, not per worker | `TestRateLimitIsGlobalAcrossWorkers` | [T] asserted: 40 starts at 20/s over 16 slots span at least 1.755 s, and no 1 s window holds more than 21 starts |
| Low priority starves | Weighted-random fetch order gives each level about its weight's share | `TestPriorityWeightsPreventStarvation` | [T] asserted: low gets 20–65 of the first 400 jobs (expected 40; strict priority would give 0) |

## Not covered by tests

- **A lost `complete` after failover (D3).** The failover demonstration
  runs no workers, so it measures lost enqueues only. Lost completions *are*
  measured for a host crash (380–643 re-executions, above), which loses
  writes the same way.
- **Clock skew between hosts.** Scheduling uses the Redis clock (`TIME` in
  scripts), so skew only shifts `RunAt` values supplied by producers. This is reasoned
  from the design, not tested.
- **Redis Cluster.** Keys are hash-tagged per queue, but only a single primary
  (with optional replica) is tested.
- **Real hardware power loss.** The host-crash rows use LazyFS, which drops
  exactly the writes that were never fsynced. A real disk can also lose or
  reorder writes it acknowledged, if its write cache lies about fsync. That
  is not simulated.
