-- start: turn a delivered stream entry into an active job with a new lease.
--
-- KEYS[1] job hash   KEYS[2] stream   KEYS[3] dead
-- ARGV[1] group  ARGV[2] entry id  ARGV[3] consumer  ARGV[4] new lease token
-- ARGV[5] allowed source states (csv, from state.Start; state.Exhausted has
--         the same sources)
--
-- Returns {'OK', <HGETALL>} | {'DEAD'} | {'SKIP', reason}.
--
-- Skips that ack and delete the entry (it can never become runnable):
--   missing      the hash expired or was purged
--   stale_entry  the hash points at a different entry (at most one live entry
--                per job is an invariant; only that entry may start it)
--   <state>      the job is terminal, or is scheduled/retry and will be
--                re-added to the stream by promote
-- Skips that leave the entry alone (someone else is responsible for it):
--   not_owner    another consumer has claimed this entry since we read it
--                (e.g. we stalled longer than the visibility timeout)
--   not_pending  the entry was acked meanwhile
--
-- DEAD: the job has used all its attempts without ever reporting a result,
-- i.e. it crashed or lost its lease every time (a poison pill). Counting
-- attempts here, not in fail.lua, is what bounds crash loops.
--
-- Atomic because: check-state + ownership + attempts++ + token rotation must
-- be one step, or two workers could both believe they hold the current lease.

if redis.call('EXISTS', KEYS[1]) == 0 then
  ack_del(KEYS[2], ARGV[1], ARGV[2])
  return {'SKIP', 'missing'}
end

local f = redis.call('HMGET', KEYS[1], 'state', 'stream_entry', 'attempts',
  'max_attempts', 'id')
local st, entry = f[1], f[2]

if entry ~= ARGV[2] then
  ack_del(KEYS[2], ARGV[1], ARGV[2])
  return {'SKIP', 'stale_entry'}
end
if not allowed(st, ARGV[5]) then
  ack_del(KEYS[2], ARGV[1], ARGV[2])
  return {'SKIP', st}
end

local p = redis.call('XPENDING', KEYS[2], ARGV[1], ARGV[2], ARGV[2], 1)
if #p == 0 then
  return {'SKIP', 'not_pending'}
end
if p[1][2] ~= ARGV[3] then
  return {'SKIP', 'not_owner'}
end

local now = fmt(now_ms())
local attempts = tonumber(f[3]) or 0
local max = tonumber(f[4]) or 1
if attempts >= max then
  ack_del(KEYS[2], ARGV[1], ARGV[2])
  redis.call('HSET', KEYS[1], 'state', 'dead', 'finished_ms', now,
    'last_error', 'attempts exhausted: ' .. attempts ..
      ' starts without a reported result (worker crash or lost lease)')
  redis.call('HDEL', KEYS[1], 'stream_entry', 'lease_token')
  redis.call('ZADD', KEYS[3], now, f[5])
  return {'DEAD'}
end

redis.call('HSET', KEYS[1],
  'state', 'active', 'attempts', tostring(attempts + 1),
  'lease_token', ARGV[4], 'lease_owner', ARGV[3], 'started_ms', now)
return {'OK', redis.call('HGETALL', KEYS[1])}
