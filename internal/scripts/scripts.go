// Package scripts embeds the Lua scripts that perform every multi-step job
// state change. Each script is prefixed with prelude.lua. go-redis runs them
// with EVALSHA and falls back to EVAL on NOSCRIPT (e.g. after a Redis restart
// flushed the script cache).
package scripts

import (
	_ "embed"
	"fmt"
	"strconv"

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
	//go:embed fail.lua
	failSrc string
	//go:embed promote.lua
	promoteSrc string
	//go:embed replay.lua
	replaySrc string
	//go:embed purge.lua
	purgeSrc string
	//go:embed heartbeat.lua
	heartbeatSrc string
	//go:embed ratelimit.lua
	ratelimitSrc string
	//go:embed release.lua
	releaseSrc string
	//go:embed delconsumer.lua
	delconsumerSrc string
)

var (
	Enqueue     = redis.NewScript(prelude + enqueueSrc)
	Start       = redis.NewScript(prelude + startSrc)
	Complete    = redis.NewScript(prelude + completeSrc)
	Fail        = redis.NewScript(prelude + failSrc)
	Promote     = redis.NewScript(prelude + promoteSrc)
	Replay      = redis.NewScript(prelude + replaySrc)
	Purge       = redis.NewScript(prelude + purgeSrc)
	Heartbeat   = redis.NewScript(prelude + heartbeatSrc)
	RateLimit   = redis.NewScript(prelude + ratelimitSrc)
	Release     = redis.NewScript(prelude + releaseSrc)
	DelConsumer = redis.NewScript(prelude + delconsumerSrc)
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

// Int converts a script reply element (integer or numeric string) to int64.
func Int(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case string:
		return strconv.ParseInt(x, 10, 64)
	default:
		return 0, fmt.Errorf("scripts: not an integer: %T %v", v, v)
	}
}
