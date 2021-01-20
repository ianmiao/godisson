package lock

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_LockManager_TryLock(t *testing.T) {

	t.Parallel()

	t.Run("lock unlock", func(t *testing.T) {

		t.Parallel()

		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		memberID := uint64(r.Int63n(math.MaxInt64))
		namespace := fmt.Sprintf("namespace_%d", r.Int())
		leaseCycleMsec := int64(10000)
		pool := redis.NewPool(func() (redis.Conn, error) {
			return redis.DialURL(os.Getenv("REDIS_ADDR"))
		}, 3)
		defer pool.Close()

		lm := NewLockManager(pool, namespace, memberID, leaseCycleMsec,
			AsyncErrListenerOption(func(err error) {
				assert.NoError(t, err)
			}),
		)
		defer lm.Close()

		success, shouldWaitMsec, err := lm.TryLock()
		require.NoError(t, err)

		assert.Zero(t, shouldWaitMsec)
		assert.True(t, success)

		err = lm.Unlock()
		require.NoError(t, err)
	})
	t.Run("lock failed", func(t *testing.T) {

		t.Parallel()

		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		memberID1 := uint64(r.Int63n(math.MaxInt64))
		memberID2 := memberID1 + 1
		namespace := fmt.Sprintf("namespace_%d", r.Int())
		leaseCycleMsec := int64(10000)
		pool := redis.NewPool(func() (redis.Conn, error) {
			return redis.DialURL(os.Getenv("REDIS_ADDR"))
		}, 3)
		defer pool.Close()

		lm1 := NewLockManager(pool, namespace, memberID1, leaseCycleMsec,
			AsyncErrListenerOption(func(err error) {
				assert.NoError(t, err)
			}),
		)
		defer lm1.Close()

		lm2 := NewLockManager(pool, namespace, memberID2, leaseCycleMsec,
			AsyncErrListenerOption(func(err error) {
				assert.NoError(t, err)
			}),
		)
		defer lm2.Close()

		success, shouldWaitMsec, err := lm1.TryLock()
		require.NoError(t, err)

		assert.Zero(t, shouldWaitMsec)
		assert.True(t, success)

		success, shouldWaitMsec, err = lm2.TryLock()
		require.NoError(t, err)

		assert.InDelta(t, leaseCycleMsec, shouldWaitMsec, 10.0)
		assert.False(t, success)
	})
	t.Run("reentrant lock", func(t *testing.T) {

		t.Parallel()

		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		memberID1 := uint64(r.Int63n(math.MaxInt64))
		memberID2 := memberID1 + 1
		namespace := fmt.Sprintf("namespace_%d", r.Int())
		leaseCycleMsec := int64(10000)
		pool := redis.NewPool(func() (redis.Conn, error) {
			return redis.DialURL(os.Getenv("REDIS_ADDR"))
		}, 3)
		defer pool.Close()

		lm1 := NewLockManager(pool, namespace, memberID1, leaseCycleMsec,
			AsyncErrListenerOption(func(err error) {
				assert.NoError(t, err)
			}),
		)
		defer lm1.Close()

		lm2 := NewLockManager(pool, namespace, memberID2, leaseCycleMsec,
			AsyncErrListenerOption(func(err error) {
				assert.NoError(t, err)
			}),
		)
		defer lm2.Close()

		success, shouldWaitMsec, err := lm1.TryLock()
		require.NoError(t, err)

		assert.Zero(t, shouldWaitMsec)
		assert.True(t, success)

		success, shouldWaitMsec, err = lm1.TryLock()
		require.NoError(t, err)

		assert.Zero(t, shouldWaitMsec)
		assert.True(t, success)

		err = lm1.Unlock()
		require.NoError(t, err)

		success, shouldWaitMsec, err = lm2.TryLock()
		require.NoError(t, err)

		assert.InDelta(t, leaseCycleMsec, shouldWaitMsec, 10.0)
		assert.False(t, success)

		err = lm1.Unlock()
		require.NoError(t, err)

		success, shouldWaitMsec, err = lm2.TryLock()
		require.NoError(t, err)

		assert.Zero(t, shouldWaitMsec)
		assert.True(t, success)
	})
}

func Test_LockManager_Lock(t *testing.T) {

	t.Parallel()

	t.Run("block until acquire", func(t *testing.T) {

		t.Parallel()

		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		memberID1 := uint64(r.Int63n(math.MaxInt64))
		memberID2 := memberID1 + 1
		namespace := fmt.Sprintf("namespace_%d", r.Int())
		leaseCycleMsec := int64(10000)
		pool := redis.NewPool(func() (redis.Conn, error) {
			return redis.DialURL(os.Getenv("REDIS_ADDR"))
		}, 3)
		defer pool.Close()

		lm1 := NewLockManager(pool, namespace, memberID1, leaseCycleMsec,
			AsyncErrListenerOption(func(err error) {
				assert.NoError(t, err)
			}),
		)
		defer lm1.Close()

		lm2 := NewLockManager(pool, namespace, memberID2, leaseCycleMsec,
			AsyncErrListenerOption(func(err error) {
				assert.NoError(t, err)
			}),
		)
		defer lm2.Close()

		err := lm1.Lock()
		require.NoError(t, err)

		unlockNotifyCh := make(chan struct{})
		unlockNotifiedCh := make(chan struct{})
		lm2LockCh := make(chan struct{})
		lm2LockedCh := make(chan struct{})

		go func() {
			close(lm2LockCh)
			err := lm2.Lock()
			require.NoError(t, err)
			close(lm2LockedCh)
		}()

		go func() {
			conn := pool.Get()
			defer conn.Close()

			psConn := redis.PubSubConn{
				Conn: conn,
			}

			err := psConn.Subscribe(lm1.getUnlockNotifyKey())
			require.NoError(t, err)

			close(unlockNotifyCh)

			for {
				switch n := psConn.Receive().(type) {
				case error:
					require.NoError(t, n)
				case redis.Message:
					unlockNotifiedCh <- struct{}{}
				}
			}
		}()

		<-unlockNotifyCh
		<-lm2LockCh

		select {
		case <-lm2LockedCh:
			assert.Error(t, errors.New("lm2 acquire lock unexpectedly"))
		default:
		}

		lm1.Unlock()
		<-unlockNotifiedCh
		<-lm2LockedCh
		lm2.Unlock()
	})
}

func Test_LockManager_LockWithTimeout(t *testing.T) {

	t.Parallel()

	t.Run("lock failed if timeout", func(t *testing.T) {

		t.Parallel()

		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		memberID1 := uint64(r.Int63n(math.MaxInt64))
		memberID2 := memberID1 + 1
		namespace := fmt.Sprintf("namespace_%d", r.Int())
		leaseCycleMsec := int64(10000)
		pool := redis.NewPool(func() (redis.Conn, error) {
			return redis.DialURL(os.Getenv("REDIS_ADDR"))
		}, 3)
		defer pool.Close()

		lm1 := NewLockManager(pool, namespace, memberID1, leaseCycleMsec,
			AsyncErrListenerOption(func(err error) {
				assert.NoError(t, err)
			}),
		)
		defer lm1.Close()

		lm2 := NewLockManager(pool, namespace, memberID2, leaseCycleMsec,
			AsyncErrListenerOption(func(err error) {
				assert.NoError(t, err)
			}),
		)
		defer lm2.Close()

		err := lm1.Lock()
		require.NoError(t, err)

		cancelCh := make(chan struct{})
		lockNotifyCh := make(chan struct{})
		lockFailedNotifiedCh := make(chan struct{})

		go func() {
			close(lockNotifyCh)
			success, _, err := lm2.lock(cancelCh)
			require.NoError(t, err)

			assert.False(t, success)
			close(lockFailedNotifiedCh)
		}()

		<-lockNotifyCh
		close(cancelCh)
		<-lockFailedNotifiedCh
	})
}

func Test_LockManager_renewalScript(t *testing.T) {

	t.Parallel()

	t.Run("return `RES_CODE_RENEWAL_NON_EXISTENT_LOCK` if lock not exists", func(t *testing.T) {

		t.Parallel()

		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		memberID := uint64(r.Int63n(math.MaxInt64))
		namespace := fmt.Sprintf("namespace_%d", r.Int())
		leaseCycleMsec := int64(1000)
		pool := redis.NewPool(func() (redis.Conn, error) {
			return redis.DialURL(os.Getenv("REDIS_ADDR"))
		}, 3)
		defer pool.Close()

		lm := LockManager{
			memberID:  memberID,
			namespace: namespace,
		}

		conn := pool.Get()
		defer conn.Close()

		res, err := redis.Int64(renewalScript.Do(conn, lm.getLockKey(), memberID, leaseCycleMsec))
		require.NoError(t, err)

		assert.Equal(t, RES_CODE_RENEWAL_NON_EXISTENT_LOCK, res)
	})
	t.Run("return `RES_CODE_SUCCESS` if lock exists", func(t *testing.T) {

		t.Parallel()

		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		memberID := uint64(r.Int63n(math.MaxInt64))
		namespace := fmt.Sprintf("namespace_%d", r.Int())
		leaseCycleMsec := int64(1000)
		pool := redis.NewPool(func() (redis.Conn, error) {
			return redis.DialURL(os.Getenv("REDIS_ADDR"))
		}, 3)
		defer pool.Close()

		lm := LockManager{
			asyncErrCh: make(chan error, 1),
			asyncErrListener: func(err error) {
				assert.NoError(t, err)
			},
			namespace:       namespace,
			memberID:        memberID,
			pool:            pool,
			leaseCycleMsec:  leaseCycleMsec,
			renewalNotifyCh: make(chan RenewalNotify),
			needRenewal:     false,
			inflight:        sync.WaitGroup{},
		}

		err := lm.Lock()
		require.NoError(t, err)

		conn := pool.Get()
		defer conn.Close()

		res, err := redis.Int64(renewalScript.Do(conn, lm.getLockKey(), memberID, leaseCycleMsec))
		require.NoError(t, err)

		assert.Equal(t, RES_CODE_SUCCESS, res)
	})
}
