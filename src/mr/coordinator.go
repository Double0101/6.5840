package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	Files     []string
	NoReduce  int
	NoFile    int
	Tasks     []Task
	MapRemain int
	Phase     int
	Finished  bool
	Mu        sync.Mutex
	NoWorker  int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	reply.WorkerId = c.NoWorker
	c.NoWorker += 1
	return nil
}

func (c *Coordinator) mapTaskCanRun(idx int) bool {
	switch c.Tasks[idx].TaskStatus {
	case TASK_STATUS_READY:
	case TASK_STATUS_ERROR:
		return true
	case TASK_STATUS_QUEUE:
	case TASK_STATUS_RUNNING:
		if time.Now().Sub(c.Tasks[idx].StartTime) > MAX_RUN_TIME {
			return true
		} else {
			return false
		}
	default:
		return false
	}
	return false
}

func (c *Coordinator) DispatchTask(args *TaskArgs, reply *TaskReply) error {
	if c.Phase == PHASE_REDUCE {

	} else {
		if c.MapRemain != 0 {
			// dispatch map task
			idx := 0
			for idx < c.NoFile {
				c.Mu.Lock()
				defer c.Mu.Unlock()
				if c.mapTaskCanRun(idx) {
					c.Tasks[idx].TaskStatus = TASK_STATUS_QUEUE
					c.Tasks[idx].StartTime = time.Now()
					reply.TaskType = TASK_TYPE_MAP
					reply.FilePath = c.Files[idx]
					reply.TaskId = idx
					reply.NoReduce = c.NoReduce
					return nil
				}
				idx += 1
			}
		}
	}
	return nil
}

func (c *Coordinator) initReduceTask() {
	c.Mu.Lock()
	c.Tasks = make([]Task, c.NoReduce)
	for idx := 0; idx < c.NoReduce; idx += 1 {
		c.Tasks[idx].TaskId = idx
		c.Tasks[idx].TaskStatus = TASK_STATUS_READY
		c.Tasks[idx].ReduceN = idx
		c.Tasks[idx].WorkerId = -1
	}
	c.Mu.Unlock()
}

func (c *Coordinator) UpdateTaskStatus(args *StatusArgs, reply *StatusReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	if args.WorderId != c.Tasks[args.TaskId].WorkerId ||
		c.Tasks[args.TaskId].TaskStatus == TASK_STATUS_FINISH {
		return nil
	}

	if args.TaskStatus == TASK_STATUS_FINISH {
		if c.Phase == PHASE_MAP &&
			args.TaskType == TASK_TYPE_MAP &&
			c.Tasks[args.TaskId].TaskStatus != TASK_STATUS_FINISH {
			c.Tasks[args.TaskId].TaskStatus = TASK_STATUS_FINISH
			c.MapRemain -= 1
			if c.MapRemain == 0 {
				go c.initReduceTask()
			}
		}
	} else if args.TaskStatus == TASK_STATUS_ERROR {
		c.Tasks[args.TaskId].WorkerId = -1
	}
	c.Tasks[args.TaskId].TaskStatus = args.TaskStatus
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.Mu.Lock()
	defer c.Mu.Unlock()
	ret = c.Finished
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Files = files
	c.NoFile = len(files)
	c.NoReduce = nReduce
	c.Mu = sync.Mutex{}
	c.MapRemain = c.NoFile
	c.Finished = false
	c.NoWorker = 0
	c.Tasks = make([]Task, len(files))
	for i, f := range files {
		c.Tasks[i].TaskId = i
		c.Tasks[i].FilePath = f
		c.Tasks[i].TaskStatus = TASK_STATUS_READY
		c.Tasks[i].WorkerId = -1
	}

	c.server()
	return &c
}
