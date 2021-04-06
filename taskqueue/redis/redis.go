package redis

import (
	"github.com/garyburd/redigo/redis"
)

func NewTryFetchTaskScript() *redis.Script {

	return redis.NewScript(4, `
local readyTaskQueues = {KEYS[1], KEYS[2], KEYS[3]}
for i = 1, 3 do
	local readyTask
end
	`)
}
