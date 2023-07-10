package mr

import "time"

type Task struct {
	FilePath   string
	TaskStatus int
	WorkerId   int
	TaskId     int
	StartTime  time.Time
	Result     []KeyValue
}

const (
	TASK_STATUS_READY   = 0
	TASK_STATUS_QUEUE   = 1
	TASK_STATUS_RUNNING = 2
	TASK_STATUS_FINISH  = 3
	TASK_STATUS_ERROR   = 4
)

const (
	TASK_TYPE_MAP    = 0
	TASK_TYPE_REDUCE = 1
)

type RegisterArgs struct{}

type RegisterReply struct {
	WorkerId int
}

type TaskArgs struct {
	WorkId int
}

type TaskReply struct {
	FilePath string
	TaskType int
}

type DoneArgs struct {
	WorderId int
	TaskId   int
	Result   []KeyValue
}

type DoneReply struct{}
