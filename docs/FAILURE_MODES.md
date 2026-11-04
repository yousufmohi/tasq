# Failure modes

Each row describes a failure, what taskq does about it, whether acknowledged work can
be lost or duplicated, and the test that proves the behaviour. "Measured"
numbers come from one recorded run of `make failure` at commit `f2c12c1`
(`results/failure/*.json`) unless marked otherwise. Nothing here is estimated.

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

## Worker failures

| Failure | Behaviour | Lost? | Duplicates? | Proven by | Measured |
|---|---|---|---|---|---|
| Worker process killed (SIGKILL/OOM) mid-job | Its PEL entries go idle. Another worker's `XAUTOCLAIM` reclaims them after the visibility timeout, and `start.lua` rotates the lease token and counts the attempt. | No | Executions yes. Effects only if the kill lands between the effect and `complete` (D1). | `TestSIGKILLWorkersMidJob`, `TestReclaimAbandonedJob` | 10 SIGKILLs across 4 processes, 401 jobs: **0 lost**, 46 duplicate executions, 0 duplicate naive effects, 32 jobs reclaimed |
| Worker dies after `XREADGROUP`, before `start` | The entry is in the PEL while the hash says `pending`. Reclaim goes by PEL idle time, so it's recovered the same way. | No | No (it never started) | `TestReclaimUnstartedDelivery` | attempts = 1 after recovery |
| Job crashes its worker on every attempt (poison pill) | Attempts are counted at `start`. Once they're exhausted, `start.lua` dead-letters the job instead of running it. | No (dead, inspectable) | Bounded by `MaxAttempts` | `TestPoisonPillIsDeadLettered`, poison job in `TestSIGKILLWorkersMidJob` | dead after exactly 3 starts with `MaxAttempts: 3` |
| Handler returns an error | `fail.lua` acks the entry and schedules a retry with full-jitter backoff, or dead-letters it. | No | Retries are intended | `TestRetryThenSucceed`, `TestExhaustedAttemptsDeadLetter`, `TestRetryIsScheduledWithBackoff` | |
| Handler error marked `taskq.Permanent` | Dead-lettered immediately | No | No | `TestPermanentErrorSkipsRetries` | attempts = 1 |
| Handler panics | Recovered and treated as a failure. The worker keeps running. | No | No | `TestHandlerPanicIsContained` | |
| Handler hangs | The per-job timeout cancels its context, which counts as a failure. | No | No | `TestJobTimeoutCountsAsFailure` | |
| Job runs longer than the visibility timeout | Heartbeats (`XCLAIM … JUSTID`) keep resetting the idle time. | No | No | `TestHeartbeatKeepsLongJobAlive` | 4×VT job with an idle peer: 1 attempt |
| Worker stalls past the visibility timeout (GC pause, VM freeze), then resumes | Another worker reclaims the job and rotates the token. The stalled worker's `start`, heartbeat, `complete` and `fail` are all rejected (`not_owner` / `LEASE_LOST`) and write nothing. | No | Possible (D2): two handlers can overlap | `TestCompleteIsFencedByLeaseToken`, `TestFailIsFencedByLeaseToken`, `TestHeartbeatDoesNotStealReclaimedEntry`, `TestLeaseLossCancelsHandler` | |
| Graceful shutdown (deploy) | Intake stops, leases stay renewed, handlers drain for up to `DrainTimeout`. Survivors are cancelled and released with their attempt refunded. The consumer is deleted only if its PEL is empty. | No | No for cooperative handlers | `TestShutdownReleasesInterruptedJob`, `TestShutdownIdleDeletesConsumer`, manual `docker compose stop` check | manual check (not saved as a result file): 20 in-flight 3 s jobs drained on `docker compose stop` |
| Handler ignores cancellation at shutdown | Its job is released after `ReleaseGrace`. Its late completion is fenced because the token was cleared. Logged at ERROR. | No | Possible (D5) | `TestShutdownReleasesUncooperativeHandler` | asserted: shutdown returns in < 2 s although the handler never returns on its own |
| Crashed workers leave consumers behind | The consumer GC removes consumers that are idle past `ConsumerTTL` *and* own no pending entries, atomically. | No | No | `TestConsumerGC`, `TestDelConsumerRefusesWhilePending` | |

## Network failures (Toxiproxy)

| Failure | Behaviour | Lost? | Duplicates? | Proven by | Measured |
|---|---|---|---|---|---|
| 50 ms ± 10 ms latency each way, worker ↔ Redis | Slower, still correct. One round trip per fetch (`fetch.lua`), and each fetch takes up to the number of free slots. | No | No | `TestToxiproxyLatency` | 300 jobs, 2 workers × 8: **0 lost, 0 duplicates**, drained in 9.6 s. Before the `fetch.lua` fixes, the same test took 39.4 s in development logs (not saved). |
| Silent partition of one worker (no data, connections held) for 4× VT | The partitioned worker's heartbeats fail, and it cancels its handlers at VT − HB (self-fencing). The healthy worker reclaims after VT. | No | Yes (D2); bounded by idempotency | `TestToxiproxyPartitionOneWorker` | 60 jobs, 8 in flight at the cut: **0 lost**, 8 reclaimed, 8 duplicate executions, **8 duplicate naive effects, 0 duplicate `idem.Once` effects, 0 at the idempotent downstream**; all done 4.0 s after the cut |
| Connection resets (proxy flapping 150 ms down / 150 ms up, 10×) | go-redis reconnects. Failed `complete` calls leave the job in the PEL for reclaim. | No | Rare | `TestToxiproxyConnectionResets` | 400 jobs: **0 lost**, 1 duplicate execution |

## Redis failures

| Failure | Behaviour | Lost? | Duplicates? | Proven by | Measured |
|---|---|---|---|---|---|
| Graceful restart (SIGTERM), AOF `everysec` | Redis fsyncs the AOF on shutdown. Producers and workers ride through the outage (0.39–0.55 s across the restart runs). | **No** | A few: a `complete` that hits the outage is redelivered | `TestRedisRestart/graceful_aof_everysec` | 7,978 acked: **0 lost**, 2 duplicate naive effects |
| Redis process SIGKILL, AOF `always` | Every write is fsynced before the reply | **No** (asserted) | A few, as above | `TestRedisRestart/sigkill_aof_always` | 4,454 acked: **0 lost**. Throughput cost of `always`: 2,214 enqueues acked in the 1.5 s before the kill, vs 3,970 with `everysec` (graceful run) |
| Redis process SIGKILL, AOF `everysec` | The process dies but the kernel page cache survives, so already-written AOF data is kept. The ~1 s `everysec` window applies to **host/kernel crashes**, which a container test can't simulate. | Observed 0 (reported, not asserted) | A few, as above | `TestRedisRestart/sigkill_aof_everysec` | 8,119 acked: 0 lost |
| Redis loses everything (no persistence, or failover to an empty replica) | Acked-but-unexecuted jobs are gone. Workers get NOGROUP and **recreate the consumer groups** instead of failing forever. | **Yes** | — | `TestRedisRestart/sigkill_no_persistence_control`, the positive control proving the harness detects loss | 7,880 acked: 3,161 lost; 1,000 had already run and were forgotten; 3,719 succeeded |
| Enqueue reply lost (Redis applied the write, then died before replying) | The producer sees an error although the job exists and will run | No | **Yes, if the producer retries without an idempotency key** (D4) | `TestEnqueueIdempotencyKeyDeduplicates`, `TestConcurrentEnqueueSameKeyCreatesOneJob`; observed in `TestRedisRestart/sigkill_aof_always` | Not in the final run. In the run recorded at commit `01c78da` (`git show 01c78da:results/failure/TestRedisRestart_sigkill_aof_always.json`), 8 enqueues that *returned errors* had in fact been applied and executed (4,446 applied vs 4,438 acked) |
| Failover with async replication (primary dies, replica promoted) | Writes acked by the primary but not yet replicated are gone. **This is the one way taskq loses acked jobs.** | **Yes** | Lost acks/completions mean re-runs (D3) | `TestFailoverDemo/async_replication` (demonstration) | replication partitioned 3 s, then primary killed: 60,768 of 78,514 acked lost |
| Same, with `min-replicas-to-write 1`, `min-replicas-max-lag 1` | The primary refuses writes once its replica has been silent for 1 s, which bounds the loss window | Bounded | — | `TestFailoverDemo/min_replicas_to_write` (demonstration) | 3 of 17,675 acked lost; 484 enqueues refused during the partition |
| Same, with `Durability{Replicas: 1}` (`WAIT 1`) | An enqueue is acked only after the replica confirms it. Unconfirmed ones return `ErrDurabilityUncertain`. | **No** (asserted, one-replica topology) | — | `TestFailoverDemo/wait_1_replica` | 10,440 acked: **0 lost**; 80 flagged uncertain, of which 79 were missing after failover. Throughput: 10,424 acked in the first 1 s vs 17,726 async |
| Redis memory full (`maxmemory`, `noeviction`) | Enqueue fails loudly with OOM. Nothing is evicted. The script never half-writes: Redis raises OOM only on a script's *first* write (`scriptVerifyOOM`), and every taskq script validates before writing. | No | — | `TestEnqueueFailsLoudlyWhenRedisIsFull` | |
| Redis configured to evict | The worker refuses to start, because eviction would silently delete jobs | — | — | `TestWorkerRefusesEvictingRedis` | |
| Script cache flushed (restart, failover) | go-redis falls back from `EVALSHA` to `EVAL` on NOSCRIPT. The durability pipeline uses `EVAL` directly. | No | No | `TestScriptsSurviveScriptFlush` | |

## Concurrency hazards

| Hazard | Behaviour | Proven by | Measured |
|---|---|---|---|
| Two promoters move the same due job | `promote.lua` does `ZRANGEBYSCORE`+`ZREM`+`XADD` atomically | `TestConcurrentPromotersPromoteExactlyOnce` | 20 promoters, 2,000 jobs: each exactly one stream entry |
| Two operators replay the same dead job | `replay.lua` checks state and moves the job atomically | `TestReplayDead` | 8 concurrent replays: exactly 1 succeeds |
| Producers race past the depth limit | The depth check and the write are one script | `TestBackpressureUnderConcurrentProducers` | 20×20 enqueues at `MaxDepth` 100: exactly 100 accepted |
| Producers race on one idempotency key | Check and set are one script | `TestConcurrentEnqueueSameKeyCreatesOneJob` | 50 producers: 1 job |
| A stray duplicate stream entry for a job | Only the entry the hash points to can start it. Others are acked and dropped. | `TestStartRejectsStaleEntry` | |
| A heartbeat steals back a reclaimed entry | The heartbeat checks PEL ownership and the token before `XCLAIM` | `TestHeartbeatDoesNotStealReclaimedEntry` | |
| An illegal state transition | Every script checks the Go transition table and writes nothing on rejection | `TestIllegalTransitionsWriteNothing` (every illegal from-state × script), `internal/state` unit tests | |
| Many workers share a rate limit | GCRA in Lua on the Redis clock: global, not per worker | `TestRateLimitIsGlobalAcrossWorkers` | asserted: 40 starts at 20/s over 16 slots span ≥ 1.76 s, and no 1 s window holds more than 21 starts |
| Low priority starves | Weighted-random fetch order gives each level about its weight's share | `TestPriorityWeightsPreventStarvation` | asserted: low gets 20–65 of the first 400 jobs (expected 40; strict priority would give 0) |

## Not covered by tests

- **Host or kernel crash with AOF `everysec`.** Up to ~1 s of acked writes can be
  lost. This needs a VM-level kill, which the container harness can't do.
- **A lost `complete` after failover (D3).** It re-runs completed jobs. This is implied by
  the same replication behaviour the failover demo measures for enqueues, but
  not measured separately.
- **Clock skew between hosts.** Scheduling uses the Redis clock (`TIME` in
  scripts), so skew only shifts `RunAt` values supplied by producers. This is reasoned
  from the design, not tested.
- **Redis Cluster.** Keys are hash-tagged per queue, but only a single primary
  (with optional replica) is tested.
