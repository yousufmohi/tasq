// Package scripts embeds the Lua scripts that perform every multi-step job
// state change. Each script is prefixed with prelude.lua. go-redis runs them
// with EVALSHA and falls back to EVAL on NOSCRIPT (e.g. after a Redis restart
// flushed the script cache).
package scripts

import (
	_ "embed"
	"fmt"

	"github.com/redis/go-redis/v9"
)

var (
	//go:embed prelude.lua
	prelude string
	//go:embed enqueue.lua
	enqueueSrc string
	//go:embed start.lua
	startSrc string
	//go:embed complete.lua
	completeSrc string
)

var (
	Enqueue  = redis.NewScript(prelude + enqueueSrc)
	Start    = redis.NewScript(prelude + startSrc)
	Complete = redis.NewScript(prelude + completeSrc)
)

// Status extracts the leading status string from a script reply of the form
// {status, ...}.
func Status(v any) (string, []any, error) {
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 {
		return "", nil, fmt.Errorf("scripts: unexpected reply %T %v", v, v)
	}
	s, ok := arr[0].(string)
	if !ok {
		return "", nil, fmt.Errorf("scripts: non-string status %T", arr[0])
	}
	return s, arr[1:], nil
}
