package lock

import (
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/pkg/errors"
)

type (
	LockManager struct {
		AsyncErrCh chan error

		namespace      string // Redis namespace
		memberID       uint64 // identify process
		pool           *redis.Pool
		leaseCycleMsec int64
		stopRenewalCh  chan struct{} // use to stop renewal goroutine
		needRenewal    bool
	}

	RetCode int64
)

const (
	RES_CODE_SUCCESS = -1 * (iota + 1)
	RES_CODE_UNLOCK_NON_EXISTENT_LOCK
	RES_CODE_UNLOCK_ANOTHER_MEMBER
	RES_CODE_RENEWAL_NON_EXISTENT_LOCK
	RES_CODE_SUCCESS_REENTRANT
)

const (
	UNLOCK_NOTIFY = uint8(1)
)

var (
	ErrUnlockNonExistentKey = errors.New("unlock non-existent key error")
	ErrUnlockAnotherMember  = errors.New("unlock another member error")
	ErrAsyncRenewal         = errors.New("renewal key async error")
	ErrAsyncLock            = errors.New("lock key async error")
)

var (
	lockScript = redis.NewScript(1, `
local lockKey = KEYS[1]
local memberID = ARGV[1]
local leaseMsec = ARGV[2]

if (redis.call("EXISTS", lockKey) == 1) then
	-- another member holds the lock
	if (redis.call("HEXISTS", lockKey, memberID) == 0) then
		return redis.call("PTTL", lockKey)
	end
end

-- reentrant lock
redis.call("PEXPIRE", lockKey, leaseMsec)
if redis.call("HINCRBY", lockKey, memberID, 1) == 1 then
	return -1
end

return -5
	`)
	unlockScript = redis.NewScript(2, `
local lockKey = KEYS[1]
local pubsubKey = KEYS[2]
local memberID = ARGV[1]
local pubMsg = ARGV[2]

-- try to unlock a non-existent key
if (redis.call("EXISTS", lockKey) == 0) then
	return -2
end

-- try to unlock another member
if (redis.call("HEXISTS", lockKey, memberID) == 0) then
	return -3
end

-- still holds the lock
if (redis.call("HINCRBY", lockKey, memberID, -1) ~= 0) then
	return -5
end

-- unlock success
redis.call("PUBLISH", pubsubKey, pubMsg)
return -1
	`)
	renewalScript = redis.NewScript(1, `
local lockKey = KEYS[1]
local memberID = ARGV[1]
local leaseMsec = ARGV[2]

if (redis.call("HEXISTS", lockKey, memberID) == 0) then
	return -4
end

redis.call("PEXPIRE", lockKey, leaseMsec)
return -1
	`)
)

// Example:
// lm := NewLockManager(...)
// go func() {
// 	for err := range lm.AsyncErrCh {
// 		log.Println(err)
// 	}
// }
// err := lm.Lock()
// if err != nil {
// 	...
// }
//
// lm.Unlock()
func NewLockManager(pool *redis.Pool, namespace string, memberID uint64, leaseCycleMsec int64, needRenewal bool) *LockManager {
	return &LockManager{
		AsyncErrCh: make(chan error, 1),

		namespace:      namespace,
		memberID:       memberID,
		pool:           pool,
		leaseCycleMsec: leaseCycleMsec,
		stopRenewalCh:  make(chan struct{}),
		needRenewal:    needRenewal,
	}
}

func (manager *LockManager) TryLock() (bool, int64, error) {

	conn := manager.pool.Get()
	defer conn.Close()

	res, err := redis.Int64(lockScript.Do(conn, manager.getLockKey(), manager.memberID,
		manager.leaseCycleMsec))
	if err != nil {
		return false, 0, errors.WithStack(err)
	}

	switch res {
	case RES_CODE_SUCCESS:
		// acquire lock for the first time
		if manager.needRenewal {
			go manager.renewal()
		}
		return true, 0, nil
	case RES_CODE_SUCCESS_REENTRANT:
		// reentrant lock
		return true, 0, nil
	default:
		// return lock key TTL
		return false, res, nil
	}
}

func (manager *LockManager) Unlock() error {

	conn := manager.pool.Get()
	defer conn.Close()

	res, err := redis.Int64(unlockScript.Do(conn, manager.getLockKey(), manager.getUnlockNotifyKey(), manager.memberID, UNLOCK_NOTIFY))
	if err != nil {
		return err
	}

	switch res {
	case RES_CODE_UNLOCK_NON_EXISTENT_LOCK:
		return ErrUnlockNonExistentKey
	case RES_CODE_UNLOCK_ANOTHER_MEMBER:
		return ErrUnlockAnotherMember
	case RES_CODE_SUCCESS_REENTRANT:
		return nil
	}

	// stop renewal
	if manager.needRenewal {
		manager.stopRenewalCh <- struct{}{}
	}

	return nil
}

func (manager *LockManager) LockWithTimeout(timeoutMsec int64) (bool, error) {

	timeoutTimer := time.NewTimer(time.Duration(timeoutMsec) * time.Millisecond)
	defer timeoutTimer.Stop()

	cancelCh := make(chan struct{})
	defer close(cancelCh)

	resCh := make(chan struct {
		success bool
		err     error
	})
	go func() {
		success, _, err := manager.lock(cancelCh)
		resCh <- struct {
			success bool
			err     error
		}{
			success: success,
			err:     err,
		}
	}()

	select {
	case <-timeoutTimer.C:
		// cancel lock goroutine with defer
		return false, nil
	case res := <-resCh:
		return res.success, res.err
	}
}

func (manager *LockManager) Lock() error {
	_, _, err := manager.lock(make(chan struct{}))
	return err
}

func (manager *LockManager) lock(cancelCh chan struct{}) (bool, int64, error) {

	success, shouldWaitMsec, err := manager.TryLock()
	if err != nil {
		return success, shouldWaitMsec, err
	}

	if success {
		return true, 0, nil
	}

	conn := manager.pool.Get()
	defer conn.Close()

	psConn := redis.PubSubConn{
		Conn: conn,
	}

	if err := psConn.Subscribe(manager.getUnlockNotifyKey()); err != nil {
		return false, 0, err
	}

	defer psConn.Unsubscribe(manager.getUnlockNotifyKey())

	msgCh := make(chan redis.Message)
	go func() {
		for {
			switch n := psConn.Receive().(type) {
			case error:
				manager.AsyncErrCh <- errors.WithMessage(ErrAsyncLock, n.Error())
				return
			case redis.Message:
				msgCh <- n
			case redis.Subscription:
				switch n.Count {
				case 0:
					return
				}
			}
		}
	}()

	// TODO(ian.miao): 1 sec should be reconsidered
	subPingTicker := time.NewTicker(time.Second)
	defer subPingTicker.Stop()

	waitTimer := time.NewTimer(time.Duration(shouldWaitMsec) * time.Millisecond)
	defer waitTimer.Stop()

	for {
		select {
		case <-subPingTicker.C:
			if err := psConn.Ping(""); err != nil {
				return false, 0, err
			}
		case <-cancelCh:
			return false, 0, nil
		case <-msgCh:
		case <-waitTimer.C:
		}

		success, shouldWaitMsec, err = manager.TryLock()
		if err != nil {
			return success, shouldWaitMsec, err
		}

		if success {
			return true, 0, nil
		}

		if !waitTimer.Stop() {
			<-waitTimer.C
		}

		waitTimer.Reset(time.Duration(shouldWaitMsec) * time.Millisecond)
	}
}

func (manager *LockManager) renewal() {

	ticker := time.NewTicker(time.Duration(manager.leaseCycleMsec) * time.Millisecond / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			conn := manager.pool.Get()
			defer conn.Close()

			res, err := redis.Int64(renewalScript.Do(conn, manager.getLockKey(),
				manager.memberID, manager.leaseCycleMsec))
			if err != nil {
				manager.AsyncErrCh <- errors.WithMessage(ErrAsyncRenewal, err.Error())
				return
			}

			switch res {
			case RES_CODE_RENEWAL_NON_EXISTENT_LOCK:
				return
			}
		case <-manager.stopRenewalCh:
			return
		}
	}
}

func (manager *LockManager) getLockKey() string {
	return manager.namespace + ":lock"
}

func (manager *LockManager) getUnlockNotifyKey() string {
	return manager.namespace + ":unlock-notify"
}
