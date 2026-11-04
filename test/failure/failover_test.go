//go:build failure

package failure

import (
	"context"
	"strings"
	"testing"
	"time"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/toxiproxy"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"

	"github.com/yousufmo27/taskq"
	"github.com/yousufmo27/taskq/internal/keys"
	"github.com/yousufmo27/taskq/internal/testutil"
	"github.com/yousufmo27/taskq/internal/verify"
)

// TestFailoverDemo demonstrates what asynchronous replication loses in a
// failover, and what WAIT and min-replicas-to-write change.
//
// Topology: a primary and one replica whose replication link runs through
// Toxiproxy. 16 producer goroutines enqueue to the primary. After 1s the
// replication link is partitioned (connections reset and refused) while
// producers keep writing to the primary for 3s. Then the primary is
// SIGKILLed and the replica is promoted with REPLICAOF NO ONE (what
// Sentinel would do). Every acked job is looked up on the new primary.
//
// Modes:
//   - async_replication: the primary acks writes it never sends; all of
//     them are lost. Expected and reported, not asserted.
//   - min_replicas_to_write: the primary refuses writes once its replica
//     has been silent for 1s, bounding the loss window. Reported.
//   - wait_1_replica: Enqueue uses Durability{Replicas: 1}; during the
//     partition WAIT times out, so those enqueues are returned as
//     ErrDurabilityUncertain instead of acked. Asserted: zero acked jobs
//     lost (with one replica, the one that acked is the one promoted).
func TestFailoverDemo(t *testing.T) {
	for _, mode := range []struct {
		name string
		args []string
		dur  taskq.Durability
	}{
		{name: "async_replication"},
		{name: "min_replicas_to_write", args: []string{"--min-replicas-to-write", "1", "--min-replicas-max-lag", "1"}},
		{name: "wait_1_replica", dur: taskq.Durability{Replicas: 1, Timeout: 500 * time.Millisecond}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			nw, err := tcnetwork.New(ctx)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = nw.Remove(context.Background()) })
			primary, err := testutil.StartRedisWith(ctx, testutil.RedisOptions{
				Network: nw, Aliases: []string{"primary"}, Args: mode.args})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = primary.Terminate() })
			tp, err := toxiproxy.Run(ctx, toxiproxyImage,
				tcnetwork.WithNetwork([]string{"toxiproxy"}, nw),
				toxiproxy.WithProxy("replication", "primary:6379"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = testcontainers.TerminateContainer(tp) })
			replica, err := testutil.StartRedisWith(ctx, testutil.RedisOptions{
				Network: nw, Aliases: []string{"replica"},
				Args: []string{"--replicaof", "toxiproxy", "8666"},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = replica.Terminate() })

			prdb := primary.Client(t)
			rrdb := replica.Client(t)
			t0 := time.Now()
			var info string
			for deadline := time.Now().Add(30 * time.Second); ; {
				info, _ = rrdb.Info(ctx, "replication").Result()
				if strings.Contains(info, "master_link_status:up") {
					break
				}
				if time.Now().After(deadline) {
					// Setup failure, not a result: say so, with the replica's view.
					t.Fatalf("SETUP: replication link not up after 30s; replica INFO replication:\n%s", info)
				}
				time.Sleep(100 * time.Millisecond)
			}
			t.Logf("replication link up after %v", time.Since(t0).Round(time.Millisecond))
			uri, _ := tp.URI(ctx)
			proxy, err := toxiclient.NewClient(uri).Proxy("replication")
			if err != nil {
				t.Fatal(err)
			}

			q := testutil.Queue(t)
			c := taskq.NewClient(prdb)
			p := startProducer(t, c, q, 16, 0, taskq.EnqueueOptions{Type: "demo", Durability: mode.dur})
			time.Sleep(time.Second)
			ackedBeforePartition := len(p.Acked())
			if err := proxy.Disable(); err != nil { // partition replication
				t.Fatal(err)
			}
			time.Sleep(3 * time.Second)
			ackedDuringPartition := len(p.Acked()) - ackedBeforePartition
			if err := primary.Kill(ctx); err != nil {
				t.Fatal(err)
			}
			p.Stop()
			if err := rrdb.Do(ctx, "REPLICAOF", "NO", "ONE").Err(); err != nil {
				t.Fatal(err)
			}
			acked := p.Acked()
			lost := countMissing(t, rrdb, q, acked)

			rep := verify.Report{
				Test:  "TestFailoverDemo/" + mode.name,
				Acked: len(acked), Lost: lost,
				Extra: map[string]any{
					"mode":                             mode.name,
					"redis_args":                       mode.args,
					"producers":                        16,
					"acked_before_partition_1s":        ackedBeforePartition,
					"acked_during_partition_3s":        ackedDuringPartition,
					"uncertain_acks":                   len(p.uncertain),
					"uncertain_missing_on_new_primary": countMissing(t, rrdb, q, p.uncertain),
					"enqueue_errors":                   p.failed,
					"note":                             "demonstration: loss without WAIT is expected Redis behaviour, not a taskq bug",
				},
			}
			verify.Publish(t, rep)
			if mode.dur.Replicas > 0 && lost != 0 {
				t.Fatalf("WAIT-acked jobs lost on failover: %d", lost)
			}
			if mode.name == "async_replication" && lost == 0 {
				t.Fatal("demo did not reproduce asynchronous-replication loss; the setup is wrong")
			}
		})
	}
}

// countMissing returns how many job hashes are absent on rdb.
func countMissing(t *testing.T, rdb *redis.Client, q string, ids []string) int {
	t.Helper()
	ctx := context.Background()
	pipe := rdb.Pipeline()
	cmds := make([]*redis.IntCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.Exists(ctx, keys.Job(q, id))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		t.Fatal(err)
	}
	n := 0
	for _, c := range cmds {
		if c.Val() == 0 {
			n++
		}
	}
	return n
}
