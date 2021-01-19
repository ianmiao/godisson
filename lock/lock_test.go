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

	t.Run("lock unlock", func(t *testing.T) {

		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		memberID := uint64(r.Int63n(math.MaxInt64))
		namespace := fmt.Sprintf("namespace_%d", r.Int())
		leaseCycleMsec := int64(10000)
		needRenewal := false
		pool := redis.NewPool(func() (redis.Conn, error) {
			return redis.DialURL(os.Getenv("REDIS_ADDR"))
		}, 3)

		lm := NewLockManager(pool, namespace, memberID, leaseCycleMsec, needRenewal)
		success, shouldWaitMsec, err := lm.TryLock()
		require.NoError(t, err)

		assert.Zero(t, shouldWaitMsec)
		assert.True(t, success)

		err = lm.Unlock()
		require.NoError(t, err)
	})
}
