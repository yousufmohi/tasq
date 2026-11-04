-- fetch: read up to ARGV[3] new entries from the first non-empty priority
-- stream, trying them in the order given (the worker's weighted-random
-- order), in one round trip.
--
-- KEYS[1..n] streams in the order to try
-- ARGV[1] group  ARGV[2] consumer  ARGV[3] max entries (the worker's free
--         slots, so it never takes more than it can start)
--
-- Returns {stream, {{entry id, {field, value, ...}}, ...}} or false when
-- every stream is empty.
--
-- Not about atomicity: this exists for latency. Doing the same reads from
-- the client costs one round trip per empty stream, and fetching one entry
-- per round trip caps a worker at 1/RTT starts per second regardless of its
-- concurrency; under 50ms of network latency both dominated the cost per
-- job (found by TestToxiproxyLatency). XREADGROUP without BLOCK is allowed
-- in scripts. As with any XREADGROUP, the entries join this consumer's PEL
-- as soon as they are returned; if the reply is lost, reclaim recovers them.

for i = 1, #KEYS do
  local r = redis.call('XREADGROUP', 'GROUP', ARGV[1], ARGV[2],
    'COUNT', tonumber(ARGV[3]), 'STREAMS', KEYS[i], '>')
  if r then
    return {KEYS[i], r[1][2]}
  end
end
return false
