package worker

import (
	"math/rand/v2"
	"testing"
	"time"
)

func TestWeightedOrderIsPermutation(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 1000 {
		o := weightedOrder(DefaultWeights, false, r.IntN)
		seen := [3]bool{}
		for _, i := range o {
			seen[i] = true
		}
		if !seen[0] || !seen[1] || !seen[2] {
			t.Fatalf("not a permutation: %v", o)
		}
	}
}

// The first-choice frequencies must match the weights: this is what
// prevents starvation when every level is backlogged.
func TestWeightedOrderFirstChoiceMatchesWeights(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	const n = 100000
	var first [3]int
	for range n {
		first[weightedOrder(DefaultWeights, false, r.IntN)[0]]++
	}
	want := [3]float64{0.6, 0.3, 0.1}
	for i, c := range first {
		got := float64(c) / n
		if got < want[i]-0.01 || got > want[i]+0.01 {
			t.Errorf("level %d first %.3f of the time, want %.2f±0.01", i, got, want[i])
		}
	}
}

func TestWeightedOrderStrictAndZero(t *testing.T) {
	if o := weightedOrder(DefaultWeights, true, rand.IntN); o != [3]int{0, 1, 2} {
		t.Fatalf("strict: %v", o)
	}
	// Zero weight: low is never tried first.
	r := rand.New(rand.NewPCG(5, 6))
	for range 1000 {
		if o := weightedOrder(Weights{High: 1, Default: 1}, false, r.IntN); o[2] != 2 {
			t.Fatalf("zero-weight level not last: %v", o)
		}
	}
	// All zero: priority order.
	if o := weightedOrder(Weights{}, false, rand.IntN); o != [3]int{0, 1, 2} {
		t.Fatalf("all zero: %v", o)
	}
}

func TestJitterBounds(t *testing.T) {
	d := time.Second
	for range 1000 {
		j := jitter(d)
		if j < 800*time.Millisecond || j > 1200*time.Millisecond {
			t.Fatalf("jitter %v outside ±20%%", j)
		}
	}
}

func TestOptionsDefaults(t *testing.T) {
	o := Options{Queue: "q"}
	if err := o.setDefaults(); err != nil {
		t.Fatal(err)
	}
	if o.VisibilityTimeout != 30*time.Second || o.HeartbeatInterval != 10*time.Second ||
		o.Weights != DefaultWeights || o.Concurrency != 1 {
		t.Fatalf("defaults: %+v", o)
	}
	bad := Options{Queue: "q", Weights: Weights{High: -1}}
	if err := bad.setDefaults(); err == nil {
		t.Fatal("negative weight accepted")
	}
}
