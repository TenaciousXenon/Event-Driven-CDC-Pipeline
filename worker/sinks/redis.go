package sinks

import (
	"context"
	"distributed-cdc/worker/processing"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	"strconv"
)

// Decimal string comparison avoids Lua's floating-point precision limit.
// One hash makes data+version updates atomic; deleted rows retain their version.
var cacheScript = redis.NewScript(`
local prev = redis.call('HGET', KEYS[1], 'version')
local incoming = ARGV[1]
if prev and ((#incoming < #prev) or (#incoming == #prev and incoming <= prev)) then
 return 0
end
redis.call('HSET', KEYS[1], 'version', incoming, 'deleted', ARGV[2])
if ARGV[2] == 'true' then
 redis.call('HDEL', KEYS[1], 'data')
else
 redis.call('HSET', KEYS[1], 'data', ARGV[3])
end
return 1
`)

type Redis struct {
	Client *redis.Client
	Prefix string
}

func (s *Redis) Apply(ctx context.Context, e processing.Event) error {
	raw, err := json.Marshal(e.Data)
	if err != nil {
		return err
	}
	return cacheScript.Run(ctx, s.Client, []string{s.Prefix + e.ID}, strconv.FormatInt(e.Version, 10), strconv.FormatBool(e.Deleted), string(raw)).Err()
}
