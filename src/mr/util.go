package mr

type Task struct {
	FilePath   string
	TaskStatus int
	WorkerId   int
	TaskId     int
}

const (
	TASK_STATUS_READY   = 0
	TASK_STATUS_QUEUE   = 1
	TASK_STATUS_RUNNING = 2
	TASK_STATUS_FINISH  = 3
	TASK_STATUS_ERROR   = 4
)

type RegisterArgs struct{}

type RegisterReply struct {
	WorkerId int
}
