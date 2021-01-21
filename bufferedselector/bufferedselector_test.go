package bufferedselector

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_BufferedSelectorManager(t *testing.T) {

	t.Parallel()

	t.Run("select old before `trySelectNewTimeMsec`", func(t *testing.T) {

		t.Parallel()

		now := time.Now()
		r := rand.New(rand.NewSource(now.Unix()))

		selectorID := fmt.Sprintf("selector_id_%d", r.Int())
		namespace := fmt.Sprintf("namespace_%d", r.Int())
		trySelectNewTimeMsec := now.Add(time.Hour).UnixNano() / int64(time.Millisecond)
		pool := redis.NewPool(func() (redis.Conn, error) {
			return redis.DialURL(os.Getenv("REDIS_ADDR"))
		}, 3)
		defer pool.Close()

		manager, err := NewBufferedSelectorManager(pool, namespace, trySelectNewTimeMsec)
		require.NoError(t, err)

		gotSelectNew, err := manager.SelectNew(selectorID, now.UnixNano()/int64(time.Millisecond))
		require.NoError(t, err)

		assert.False(t, gotSelectNew)
	})
	t.Run("select old if other old selectors still exist", func(t *testing.T) {

		t.Parallel()

		now := time.Now()
		r := rand.New(rand.NewSource(now.Unix()))

		selectorID := fmt.Sprintf("selector_id_%d", r.Int())
		anotherSelectorID := fmt.Sprintf("another_selector_id_%d", r.Int())
		namespace := fmt.Sprintf("namespace_%d", r.Int())
		trySelectNewTimeMsec := now.UnixNano() / int64(time.Millisecond)
		timeoutMsec := int64(10000)
		pool := redis.NewPool(func() (redis.Conn, error) {
			return redis.DialURL(os.Getenv("REDIS_ADDR"))
		}, 3)
		defer pool.Close()

		manager, err := NewBufferedSelectorManager(pool, namespace, trySelectNewTimeMsec,
			TimeoutMsecOption(timeoutMsec))
		require.NoError(t, err)

		gotSelectNew, err := manager.SelectNew(anotherSelectorID,
			now.Add(time.Duration(timeoutMsec/2)).UnixNano()/int64(time.Millisecond))
		require.NoError(t, err)

		assert.False(t, gotSelectNew)

		gotSelectNew, err = manager.SelectNew(selectorID,
			now.Add(time.Duration(timeoutMsec)).UnixNano()/int64(time.Millisecond))
		require.NoError(t, err)

		assert.False(t, gotSelectNew)
	})
	t.Run("select new if all old selectors expire", func(t *testing.T) {

		t.Parallel()

		now := time.Now()
		r := rand.New(rand.NewSource(now.Unix()))

		selectorID := fmt.Sprintf("selector_id_%d", r.Int())
		namespace := fmt.Sprintf("namespace_%d", r.Int())
		trySelectNewTimeMsec := now.UnixNano() / int64(time.Millisecond)
		timeoutMsec := int64(10000)
		pool := redis.NewPool(func() (redis.Conn, error) {
			return redis.DialURL(os.Getenv("REDIS_ADDR"))
		}, 3)
		defer pool.Close()

		manager, err := NewBufferedSelectorManager(pool, namespace, trySelectNewTimeMsec,
			TimeoutMsecOption(timeoutMsec))
		require.NoError(t, err)

		gotSelectNew, err := manager.SelectNew(selectorID, now.UnixNano()/int64(time.Millisecond)+timeoutMsec)
		require.NoError(t, err)

		assert.True(t, gotSelectNew)
	})
}
