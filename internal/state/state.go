// Package state defines the job state machine. It is the single source of
// truth for legal transitions: Lua scripts receive the allowed source states
// for their event from Sources, so Go and Lua cannot drift apart.
package state

import (
	"sort"
	"strings"
)

// State is a job's lifecycle state as stored in the job hash.
type State string

const (
	Scheduled State = "scheduled" // waiting in the sched ZSET for its run time
	Pending   State = "pending"   // has exactly one live stream entry, not started
	Active    State = "active"    // started by a worker holding the lease token
	Retry     State = "retry"     // failed, waiting in the sched ZSET for backoff
	Succeeded State = "succeeded" // terminal
	Dead      State = "dead"      // terminal except via Replay
)

// All lists every state, in lifecycle order.
var All = []State{Scheduled, Pending, Active, Retry, Succeeded, Dead}

// Event is something that moves a job between states. Each event is applied
// by exactly one Lua script.
type Event string

const (
	Promote   Event = "promote"   // run_at reached (promote.lua)
	Start     Event = "start"     // worker starts a delivery, incl. after reclaim (start.lua)
	Exhausted Event = "exhausted" // started with attempts >= max_attempts (start.lua)
	Complete  Event = "complete"  // handler returned nil (complete.lua)
	Fail      Event = "fail"      // handler error, attempts remain (fail.lua)
	FailFinal Event = "fail_final"
	Release   Event = "release" // graceful shutdown hands the job back (release.lua)
	Replay    Event = "replay"  // operator replays a dead job (replay.lua)
)

// Transition is the set of states an event may be applied in, and the
// resulting state.
type Transition struct {
	From []State
	To   State
}

// Table is the complete transition table. Creation (enqueue) is not an
// event: a job is born Pending or Scheduled.
var Table = map[Event]Transition{
	Promote:   {From: []State{Scheduled, Retry}, To: Pending},
	Start:     {From: []State{Pending, Active}, To: Active},
	Exhausted: {From: []State{Pending, Active}, To: Dead},
	Complete:  {From: []State{Active}, To: Succeeded},
	Fail:      {From: []State{Active}, To: Retry},
	FailFinal: {From: []State{Active}, To: Dead},
	Release:   {From: []State{Active}, To: Pending},
	Replay:    {From: []State{Dead}, To: Pending},
}

// Allowed reports whether event e may be applied to a job in state s.
func Allowed(s State, e Event) bool {
	t, ok := Table[e]
	if !ok {
		return false
	}
	for _, f := range t.From {
		if f == s {
			return true
		}
	}
	return false
}

// SourcesCSV returns the allowed source states for e as a comma-separated
// list, the form the Lua scripts consume.
func SourcesCSV(e Event) string {
	t, ok := Table[e]
	if !ok {
		panic("state: unknown event " + string(e))
	}
	s := make([]string, len(t.From))
	for i, f := range t.From {
		s[i] = string(f)
	}
	sort.Strings(s)
	return strings.Join(s, ",")
}

// Terminal reports whether no automatic transition leaves s.
func Terminal(s State) bool { return s == Succeeded || s == Dead }
