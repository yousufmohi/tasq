package state

import (
	"strings"
	"testing"
)

// expected is written out independently of Table so that an accidental edit
// to Table fails this test. Every (state, event) pair not listed is illegal.
var expected = map[[2]string]State{
	{"scheduled", "promote"}: Pending,
	{"retry", "promote"}:     Pending,

	{"pending", "start"}: Active,
	{"active", "start"}:  Active, // reclaimed delivery, new lease

	{"pending", "exhausted"}: Dead,
	{"active", "exhausted"}:  Dead,

	{"active", "complete"}:   Succeeded,
	{"active", "fail"}:       Retry,
	{"active", "fail_final"}: Dead,
	{"active", "release"}:    Pending,

	{"dead", "replay"}: Pending,
}

func TestTableIsExhaustivelyAsExpected(t *testing.T) {
	for _, s := range All {
		for e := range Table {
			want, legal := expected[[2]string{string(s), string(e)}]
			if got := Allowed(s, e); got != legal {
				t.Errorf("Allowed(%s, %s) = %v, want %v", s, e, got, legal)
			}
			if legal && Table[e].To != want {
				t.Errorf("%s --%s--> %s, want %s", s, e, Table[e].To, want)
			}
		}
	}
	// Every expected pair must refer to a real event.
	for k := range expected {
		if _, ok := Table[Event(k[1])]; !ok {
			t.Errorf("expected references unknown event %q", k[1])
		}
	}
}

func TestSucceededHasNoExits(t *testing.T) {
	for e := range Table {
		if Allowed(Succeeded, e) {
			t.Errorf("succeeded must be terminal, but %s is allowed", e)
		}
	}
}

func TestDeadOnlyExitsViaReplay(t *testing.T) {
	for e := range Table {
		if Allowed(Dead, e) && e != Replay {
			t.Errorf("dead may only leave via replay, but %s is allowed", e)
		}
	}
}

func TestEveryStateReachable(t *testing.T) {
	reached := map[State]bool{Pending: true, Scheduled: true} // creation
	for changed := true; changed; {
		changed = false
		for _, tr := range Table {
			for _, f := range tr.From {
				if reached[f] && !reached[tr.To] {
					reached[tr.To] = true
					changed = true
				}
			}
		}
	}
	for _, s := range All {
		if !reached[s] {
			t.Errorf("state %s is unreachable", s)
		}
	}
}

func TestSourcesCSVIsSortedAndUnknownPanics(t *testing.T) {
	if got := SourcesCSV(Start); got != "active,pending" {
		t.Fatalf("SourcesCSV(Start) = %q", got)
	}
	for e := range Table {
		if strings.Contains(SourcesCSV(e), " ") {
			t.Fatalf("SourcesCSV(%s) contains a space", e)
		}
	}
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for unknown event")
		}
	}()
	SourcesCSV("nope")
}
