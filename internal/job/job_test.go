package job

import (
	"testing"
	"time"

	"github.com/yousufmohi/tasq/internal/state"
)

func TestFromFlatRoundTrip(t *testing.T) {
	j, err := FromFlat([]any{
		"id", "abc", "queue", "q", "type", "email", "payload", "{}",
		"state", "active", "attempts", "2", "max_attempts", "5",
		"created_ms", "1700000000123", "lease_token", "tok",
	})
	if err != nil {
		t.Fatal(err)
	}
	if j.ID != "abc" || j.State != state.Active || j.Attempts != 2 || j.MaxAttempts != 5 {
		t.Fatalf("bad decode: %+v", j)
	}
	if !j.CreatedAt.Equal(time.UnixMilli(1700000000123)) {
		t.Fatalf("created = %v", j.CreatedAt)
	}
	if !j.StartedAt.IsZero() {
		t.Fatal("missing started_ms should decode to zero time")
	}
}

func TestFromFlatRejectsMalformed(t *testing.T) {
	cases := [][]any{
		{"id"},                                 // odd
		{"id", 5},                              // non-string
		{"queue", "q"},                         // no id
		{"id", "x", "attempts", "nope"},        // bad int
		{"id", "x", "created_ms", "yesterday"}, // bad time
	}
	for i, c := range cases {
		if _, err := FromFlat(c); err == nil {
			t.Errorf("case %d: expected error", i)
		}
	}
}

func TestNewIDUnique(t *testing.T) {
	seen := map[string]bool{}
	for range 1000 {
		id := NewID()
		if len(id) != 32 || seen[id] {
			t.Fatalf("bad or duplicate id %q", id)
		}
		seen[id] = true
	}
}
