//go:build integration

package worker

import (
	"context"
	"testing"
	"time"

	"github.com/yousufmohi/tasq"
	"github.com/yousufmohi/tasq/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

// Regression test (found in the audit): an entry buffered from a
// multi-stream read sits in this worker's own PEL. If it waits there past
// the visibility timeout, this worker's own XAUTOCLAIM claims it. The entry
// must then leave the buffer, or the same process would start it twice.
func TestReclaimRemovesEntryFromOwnBuffer(t *testing.T) {
	rdb := testutil.Shared(t)
	q := testutil.Queue(t)
	ctx := context.Background()
	w, err := New(rdb, Options{
		Queue: q, VisibilityTimeout: 300 * time.Millisecond, HeartbeatInterval: 100 * time.Millisecond,
	}, func(context.Context, *taskq.Job) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := w.ensureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := taskq.NewClient(rdb).Enqueue(ctx, q, taskq.EnqueueOptions{Type: "x"}); err != nil {
		t.Fatal(err)
	}
	// Read the entry as this worker's consumer and park it in the buffer,
	// as a multi-stream read with no free slot would.
	ds, err := w.read(ctx, w.streams, -1)
	if err != nil || len(ds) != 1 {
		t.Fatalf("read: %v %v", ds, err)
	}
	w.buffer = append(w.buffer, ds[0])

	time.Sleep(400 * time.Millisecond) // idle past the visibility timeout
	var got *delivery
	for range 6 { // walk the per-stream cursors until the claim happens
		if got, err = w.reclaim(ctx); err != nil || got != nil {
			break
		}
	}
	if err != nil || got == nil || got.msg.ID != ds[0].msg.ID {
		t.Fatalf("reclaim = %+v, %v; want the buffered entry", got, err)
	}
	if len(w.buffer) != 0 {
		t.Fatalf("entry still buffered after being reclaimed: it would start twice")
	}
}
