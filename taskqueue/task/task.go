package task

type (
	TaskKind uint8

	TaskPriority uint8

	Task struct {
		Name string `json:"name"`
		Data []byte `json:"data"`

		ID      string `json:"id"`
		Fails   int64  `json:"fails,string"`
		LastErr string `json:"last_err"`
	}

	TaskHandler func(*Task) error

	TaskMiddleware func(*Task, TaskHandler) error

	TaskConfig struct {
		Name           string
		Kind           TaskKind
		Priority       TaskPriority
		MaxConcurrency int64
		MaxFails       int64
		Handler        TaskHandler
	}
)

const (
	TASK_KIND_NORMAL TaskKind = iota
	TASK_KIND_SCHEDULED
	TASK_KIND_PERIODIC
)

const (
	TASK_PRIORITY_LOW TaskPriority = iota
	TASK_PRIORITY_MEDIUM
	TASK_PRIORITY_HIGH
)

var (
	DefaultTaskMiddleware = func(task *Task, handler *TaskHandler) error {
		return handler(task)
	}
)

// inspired by gRPC interceptor chain
func MergeTaskMiddlewares(mws ...TaskMiddleware) TaskMiddleware {

	if len(mws) == 0 {
		return DefaultTaskMiddleware
	}

	if len(mws) == 1 {
		return mws[0]
	}

	return func(task *Task, handler TaskHandler) error {

		var (
			wrapHandler TaskHandler
			currIdx     int
		)

		wrapHandler = func(task *Task, handler TaskHandler) error {
			if currIdx == len(mws) {
				return handler(task)
			}

			currIdx++
			err := mws[currIdx](task, wrapHandler)
			currIdx--
			return err
		}

		return mws[0](task, wrapHandler)
	}
}
