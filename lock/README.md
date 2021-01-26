# Distributed lock with watch dog

```go
pool := redis.NewPool(func() (redis.Conn, error) {
	return redis.DialURL(os.Getenv("REDIS_ADDR"))
}, 3)
defer pool.Close()

lm := godisson.NewLockManager(pool, "redis_namespace", uuid/*member ID*/, 
	int64(time.Second/time.Millisecond), AsyncErrListenerOption(func(err error) {
		assert.NoError(t, err)
	}),
	RenewalOption(),
)
defer lm.Close()

err := lm.Lock()
if err != nil {
	// handle err
}
defer lm.Unlock()
```
