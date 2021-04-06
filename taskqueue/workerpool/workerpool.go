package workerpool

import (
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/ianmiao/godisson/taskqueue/task"
)

type (
	WorkerPool struct {
		namespace      string
		pool           *redis.Pool
		taskConfigMap  map[string]task.TaskConfig
		taskMiddleware task.TaskMiddleware
		workerChs      []workerChan

		inFlight sync.WaitGroup
	}

	WorkerPoolOption func(*WorkerPool)

	Worker struct {
		workerPool *WorkerPool
		closeCh    workerChan
	}

	workerChan chan struct{}
)

func NewWorkerPool(namespace string, pool *redis.Pool, opts ...WorkerPoolOption) *WorkerPool {

	wp := WorkerPool{
		namespace:      namespace,
		pool:           pool,
		taskConfigMap:  map[string]task.TaskConfig{},
		taskMiddleware: task.DefaultTaskMiddleware,
	}

	for _, opt := range opts {
		opt(&wp)
	}

	return &wp
}

func TaskConfigOption(tcs ...task.TaskConfig) WorkerPoolOption {
	return func(wp *WorkerPool) {
		tcMap := make(map[string]task.TaskConfig, len(tcs))
		for _, tc := range tcs {
			tcMap[tc.Name] = tc
		}
	}
}

func WorkerOption(num int) WorkerPoolOption {
	return func(wp *WorkerPool) {
		wp.workerChs = make(workerChan, num)
		for i := range wp.workerChs {
			wp.workerChs[i] = make(workerChan)
		}
	}
}

func MiddlewareOption(mws ...task.TaskMiddleware) WorkerPoolOption {
	return func(wp *WorkerPool) {
		wp.taskMiddleware = task.MergeTaskMiddlewares(mws)
	}
}

func (wp *WorkerPool) GracefulStop() {
	for _, workerCh := range wp.workerChs {
		close(workerCh)
	}
	wp.inFlight.Wait()
}

func (w *Worker) run() {

	defer w.workerPool.inFlight.Done()

	ticker := time.NewTicker(0)
	defer ticker.Stop()

	for {
		select {
		case <-w.closeCh:
			return
		case <-ticker.C:

			task := w.tryFetchTask()
			if task == nil {
				if !ticker.Stop() {
					<-ticker.C
				}
				ticker.Reset(w.getBackoffInMilliseconds())
				continue
			}

			w.processTask(task)
		}
	}
}
