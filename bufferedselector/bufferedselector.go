package bufferedselector

import (
	"sync/atomic"

	"github.com/garyburd/redigo/redis"
	"github.com/pkg/errors"
)

type (
	BufferedSelectorManager struct {
		pool                 *redis.Pool
		namespace            string
		trySelectNewTimeMsec int64
		selectNew            int64
		timeoutMsec          int64
	}

	BufferedSelectorManagerOption func(*BufferedSelectorManager)
)

const (
	RES_CODE_FAILED  = int64(0)
	RES_CODE_SUCCESS = int64(1)

	SELECT_OLD = int64(0)
	SELECT_NEW = int64(1)

	DEFAULT_TIMEOUT_MSEC = int64(60000)

	DUMMY_SELECTOR_ID = ""
)

var (
	TimeoutMsecOption = func(timeoutMsec int64) BufferedSelectorManagerOption {
		return func(manager *BufferedSelectorManager) {
			manager.timeoutMsec = timeoutMsec
		}
	}
)

var (
	selectNewScript = redis.NewScript(1, `
local oldSelectorsKey = KEYS[1]
local selectorID = ARGV[1]
local nowTimeMsec = ARGV[2]
local timeoutMsec = ARGV[3]

-- no old selectors, just select new
if (redis.call("EXISTS", oldSelectorsKey) == 0) then
	return 1
end

redis.call("ZREMRANGEBYSCORE", oldSelectorsKey, "-inf", nowTimeMsec-timeoutMsec)

if (redis.call("EXISTS", oldSelectorsKey) == 0) then
	return 1
end

redis.call("ZADD", oldSelectorsKey, nowTimeMsec, selectorID)
return 0
	`)
)

func NewBufferedSelectorManager(pool *redis.Pool, namespace string, trySelectNewTimeMsec int64,
	opts ...BufferedSelectorManagerOption) (*BufferedSelectorManager, error) {

	manager := BufferedSelectorManager{
		pool:                 pool,
		namespace:            namespace,
		trySelectNewTimeMsec: trySelectNewTimeMsec,
		selectNew:            SELECT_OLD,
		timeoutMsec:          DEFAULT_TIMEOUT_MSEC,
	}

	for _, opt := range opts {
		opt(&manager)
	}

	err := manager.init()
	if err != nil {
		return nil, err
	}

	return &manager, nil
}

func (manager *BufferedSelectorManager) SelectNew(selectorID string, nowTimeMsec int64) (bool, error) {

	selectNew := atomic.LoadInt64(&manager.selectNew)
	if selectNew == SELECT_NEW {
		return true, nil
	}

	conn := manager.pool.Get()
	defer conn.Close()

	selectNew, err := redis.Int64(selectNewScript.Do(conn, manager.getOldSelectorsKey(),
		selectorID, nowTimeMsec, manager.timeoutMsec))
	if err != nil {
		return false, errors.WithStack(err)
	}

	if selectNew == SELECT_NEW {
		atomic.StoreInt64(&manager.selectNew, SELECT_NEW)
		return true, nil
	}

	return false, nil
}

func (manager *BufferedSelectorManager) Countervail(selectorID string) error {

	conn := manager.pool.Get()
	defer conn.Close()

	_, err := conn.Do("ZREM", manager.getOldSelectorsKey(), selectorID)
	return errors.WithStack(err)
}

func (manager *BufferedSelectorManager) init() error {

	conn := manager.pool.Get()
	defer conn.Close()

	// add a dummy selector
	_, err := conn.Do("ZADD", manager.getOldSelectorsKey(), manager.trySelectNewTimeMsec, DUMMY_SELECTOR_ID)
	return errors.WithStack(err)
}

func (manager *BufferedSelectorManager) getOldSelectorsKey() string {
	return manager.namespace + ":old-selectors"
}
