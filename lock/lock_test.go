package lock

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
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

		lm := NewLockManager(pool, namespace, memberID, leaseCycleMsec, AsyncErrListenerOption(func(_ *LockManager, err error) {
			assert.NoError(t, err)
		}))

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

		lm1 := NewLockManager(pool, namespace, memberID1, leaseCycleMsec, AsyncErrListenerOption(func(_ *LockManager, err error) {
			assert.NoError(t, err)
		}))

		lm2 := NewLockManager(pool, namespace, memberID2, leaseCycleMsec, AsyncErrListenerOption(func(_ *LockManager, err error) {
			assert.NoError(t, err)
		}))

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

		lm1 := NewLockManager(pool, namespace, memberID1, leaseCycleMsec, AsyncErrListenerOption(func(_ *LockManager, err error) {
			assert.NoError(t, err)
		}))

		lm2 := NewLockManager(pool, namespace, memberID2, leaseCycleMsec, AsyncErrListenerOption(func(_ *LockManager, err error) {
			assert.NoError(t, err)
		}))

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
