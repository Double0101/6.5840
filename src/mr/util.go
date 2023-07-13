package mr

import (
	"fmt"
	"time"
)

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
	TASK_STATUS_RUNNING = 1
	TASK_STATUS_FINISH  = 2
	TASK_STATUS_ERROR   = 3
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
