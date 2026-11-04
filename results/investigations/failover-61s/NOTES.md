# The 61-second TestFailoverDemo failure

## Background

One earlier run of `TestFailoverDemo/wait_1_replica` failed after 61 s. Its
output was not saved, and the failure did not recur in 26 later runs. Commit
`bbb0488` added a diagnostic for the replication-setup step but did not
claim a fix.

## Reproduction (`repro-30x.log.gz`)

Command: `go test -race -count=30 -tags failure -v -run TestFailoverDemo ./test/failure/`

Code: working tree based on `ee7bb00`, with the changes that became
`50a1ca5` except the Toxiproxy retry. The failover test itself was unchanged
apart from report fields.

Result: 90 subtests (30 × 3 modes). 89 passed, 1 failed:

```
--- FAIL: TestFailoverDemo/min_replicas_to_write (61.02s)
failover_test.go:69: run toxiproxy: generic container: start container:
  started hook: wait until ready: context deadline exceeded
```

The Toxiproxy container's own log (included in the log file) shows it
started its proxy and its HTTP API within the same second. The readiness
check, an HTTP GET of `/version` through the port Docker Desktop publishes
on the host, still did not succeed in 60 s. 60 s is the testcontainers
default startup timeout, so the subtest failed at 61 s.

The fault is in the environment. Docker Desktop occasionally never brings up
the host port-forward for a new container, while the container itself runs
normally. The same fault was already seen and worked around for Redis
containers (`internal/testutil.StartRedisWith`). Toxiproxy containers had no
workaround.

Reproduced here in `min_replicas_to_write`, not in `wait_1_replica`. The
original failure had the same duration (61 s = 60 s readiness timeout + about
1 s of setup) and both subtests start a Toxiproxy container the same way. Its
output was not saved, so the original cause can't be confirmed.

## Fix (`50a1ca5`)

`runToxiproxy` (`test/failure/toxiproxy_test.go`) waits at most 15 s for the
API to be reachable from the host. It replaces an unreachable container, up
to three attempts, and logs each replacement.

## Verification (`verify-10x.log.gz`)

10 more repetitions with the fix: 30 of 30 subtests passed. The fault did
not occur during these runs (no replacement was logged), so this run shows
no regression, not that the retry works.

The retry did fire later. In a 5-run failure suite from a clean checkout of
`50a1ca5` (`clean-50a1ca5-20261104T172523Z-run3.log.gz`, the full
`go test -v` log of run 3), one Toxiproxy container was unreachable from the
host after 15 s and was replaced:

```
toxiproxy_test.go:142: toxiproxy attempt 1 not reachable from the host, replacing it: ...
```

Every test in that run passed. That suite's reports were written to the
wrong directory (fixed in the next commit), so only this log is kept. The
published failure results come from the later commit, and their run logs
record any replacement the same way (search for `not reachable from the host`).
