-- promote: move due jobs from the sched ZSET into their priority stream.
--
-- KEYS[1] sched
-- ARGV[1] queue key prefix  ARGV[2] max jobs per call
-- ARGV[3] allowed source states (csv, from state.Promote)
--
-- Returns {moved, scanned}. If scanned == limit there may be more due jobs,
-- and the caller runs it again immediately.
--
-- Atomic because: split, a crash between ZREM and XADD loses the job, and
-- two promoters racing between ZRANGEBYSCORE and ZREM would both XADD it.
-- Scripts run serially, so any number of workers may promote concurrently
-- without a leader.
--
-- Job and stream keys are derived from ARGV[1]; they share the queue's hash
-- tag and therefore the slot of KEYS[1].
-- Bounded by ARGV[2] so one call never blocks Redis for long.

local now = now_ms()
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', fmt(now),
  'LIMIT', 0, tonumber(ARGV[2]))
local moved = 0
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  local jk = ARGV[1] .. 'job:' .. id
  local f = redis.call('HMGET', jk, 'state', 'priority')
  -- A missing hash or a job in any other state has nothing to promote; the
  -- ZSET member was stale and removing it is the only correct action.
  if allowed(f[1], ARGV[3]) then
    local eid = redis.call('XADD', ARGV[1] .. 's:' .. f[2], '*', 'id', id)
    redis.call('HSET', jk, 'state', 'pending', 'stream_entry', eid)
    moved = moved + 1
  end
end
return {moved, #ids}
