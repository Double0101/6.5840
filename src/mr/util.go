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

const (
	PHASE_MAP    = 0
	PHASE_REDUCE = 1
	PHASE_DONE   = 2
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
	TaskId   int
}

type DoneArgs struct {
	WorderId     int
	TaskId       int
	TaskType     int
	ResultMap    []KeyValue
	ResultReduce string
}

type DoneReply struct{}
