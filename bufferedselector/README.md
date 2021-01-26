```go
// select old before `trySelectNewTimeMsec`
// try select new after `trySelectNewTimeMsec` until all old selectors operation finish

pool := redis.NewPool(func() (redis.Conn, error) {
	return redis.DialURL(os.Getenv("REDIS_ADDR"))
}, 3)
defer pool.Close()

mamanger, err := NewBufferedSelectorManager(pool, namespace, trySelectNewTimeMsec)
if err != nil {
	//...
}

// select old
manager.SelectNew(oldSelectorID, timeBeforeTrySelectNewTimeMsec)

// still select old since old selector not finished
manager.SelectNew(trySelectorID, timeAfterTrySelectNewTimeMsec)

manager.Countervail(oldSelectorID)
manager.Countervail(trySelectorID)

// select new
manager.SelectNew(newSelectorID, timeAfterTrySelectNewTimeMsec)
```
