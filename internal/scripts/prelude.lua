-- Shared helpers, prepended to every script at load time.
--
-- Script discipline (Redis Lua has no rollback: a runtime error leaves earlier
-- writes in place):
--   1. read and validate everything first;
--   2. only then write;
--   3. report expected outcomes as status replies, never as errors.

-- Current time in ms from the Redis clock. Workers never compare their own
-- clocks with each other. TIME is allowed in scripts because Redis 7 replicates
-- script effects, not the script source.
local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

-- Integer-valued double to a decimal string (avoids "1.7e+12").
local function fmt(n)
  return string.format('%.0f', n)
end

-- Is state s in the comma-separated list csv? The list comes from the Go
-- transition table (internal/state).
local function allowed(s, csv)
  if not s then return false end
  return string.find(',' .. csv .. ',', ',' .. s .. ',', 1, true) ~= nil
end

-- Acknowledge and delete a stream entry. XACK alone leaves the entry in the
-- stream (memory is only freed by XDEL/XTRIM).
local function ack_del(stream, group, eid)
  redis.call('XACK', stream, group, eid)
  redis.call('XDEL', stream, eid)
end
