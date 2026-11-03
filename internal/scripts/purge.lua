-- purge: delete dead jobs that died at or before a cutoff.
--
-- KEYS[1] dead
-- ARGV[1] queue key prefix  ARGV[2] cutoff ms (inclusive)  ARGV[3] limit
--
-- Returns the number deleted. The caller loops while it equals the limit.
--
-- Atomic because: split, a crash between ZREM and DEL leaks a hash nothing
-- indexes; the other order leaves index entries pointing at nothing.

local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[2],
  'LIMIT', 0, tonumber(ARGV[3]))
for _, id in ipairs(ids) do
  local jk = ARGV[1] .. 'job:' .. id
  -- Only delete a hash that is still dead; a replayed job must survive even
  -- if a stale index entry somehow remained.
  if redis.call('HGET', jk, 'state') == 'dead' then
    redis.call('DEL', jk)
  end
  redis.call('ZREM', KEYS[1], id)
end
return #ids
