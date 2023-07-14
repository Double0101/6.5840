package mr

import (
	"fmt"
	"time"
)

type Task struct {
	FilePath   string
	ReduceN    int
	TaskStatus int
	WorkerId   int
	TaskId     int
	StartTime  time.Time
}

const (
	TASK_STATUS_READY   = 0
	TASK_STATUS_QUEUE   = 1
	TASK_STATUS_RUNNING = 2
	TASK_STATUS_FINISH  = 3
	TASK_STATUS_ERROR   = 4
)

const (
	MAX_RUN_TIME = time.Second * 10
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
	NoReduce int
}

type StatusArgs struct {
	WorderId   int
	TaskId     int
	TaskType   int
	TaskStatus int
}

type StatusReply struct{}

func ReduceFileName(midx, ridx int) string {
	return fmt.Sprintf("mr-%d-%d", midx, ridx)
}

func MergeFileName(ridx int) string {
	return fmt.Sprintf("mr-out-%d", ridx)
}
