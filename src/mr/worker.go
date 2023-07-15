package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type Work struct {
	Id       int
	MapF     func(string, string) []KeyValue
	ReduceF  func(string, []string) string
	TaskType int
	TaskId   int
	FilePath string
	ReduceN  int
	NoReduce int
	NoMap    int
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (w *Work) Register() {
	args := &RegisterArgs{}
	reply := &RegisterReply{}
	if ok := call("Coordinator.Register", args, reply); !ok {
		log.Fatal("register to coordinator failed!")
	}
	w.Id = reply.WorkerId
}

func (w *Work) GetOneTask() {
	args := &TaskArgs{}
	reply := &TaskReply{}
	if ok := call("Coordinator.DispatchTask", args, reply); !ok {
		log.Fatal("get task failed!")
	}
	w.TaskType = reply.TaskType
	w.FilePath = reply.FilePath
	w.ReduceN = reply.ReduceN
	w.TaskId = reply.TaskId
	w.NoReduce = reply.NoReduce
	w.NoMap = reply.NoMap
}

func (w *Work) UpdateStatus(status int) {
	args := &StatusArgs{}
	reply := &StatusReply{}
	args.TaskId = w.TaskId
	args.TaskType = w.TaskType
	args.WorderId = w.Id
	args.TaskStatus = status
	if ok := call("Coordinator.UpdateTaskStatus", args, reply); !ok {
		log.Fatal("update task status failed!")
	}
}

func (w *Work) RunMapTask() {
	w.UpdateStatus(TASK_STATUS_RUNNING)
	content, err := ioutil.ReadFile(w.FilePath)
	if err != nil {
		w.UpdateStatus(TASK_STATUS_ERROR)
		return
	}
	kvs := w.MapF(w.FilePath, string(content))
	res := make([][]KeyValue, w.NoReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % w.NoReduce
		res[idx] = append(res[idx], kv)
	}

	// output result to file
	for idx, kvl := range res {
		ofn := ReduceFileName(w.TaskId, idx)
		if _, err := os.Stat(ofn); os.IsExist(err) {
			if errt := os.Remove(ofn); errt != nil {
				w.UpdateStatus(TASK_STATUS_ERROR)
				return
			}
		}
		f, err := os.Create(ofn)
		if err != nil {
			w.UpdateStatus(TASK_STATUS_ERROR)
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range kvl {
			if err := enc.Encode(&kv); err != nil {
				w.UpdateStatus(TASK_STATUS_ERROR)
				return
			}
		}
		if err := f.Close(); err != nil {
			w.UpdateStatus(TASK_STATUS_ERROR)
			return
		}
	}
	w.UpdateStatus(TASK_STATUS_FINISH)
}

func (w *Work) RunReduceTask() {
	w.UpdateStatus(TASK_STATUS_RUNNING)
	values := make(map[string][]string)
	for tid := 0; tid < w.NoMap; tid += 1 {
		fn := ReduceFileName(tid, w.ReduceN)
		f, err := os.Open(fn)
		if err != nil {
			w.UpdateStatus(TASK_STATUS_ERROR)
			return
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := values[kv.Key]; !ok {
				values[kv.Key] = make([]string, 0, 100)
			}
			values[kv.Key] = append(values[kv.Key], kv.Value)
		}
	}
	res := make([]string, 0, 100)
	for k, vs := range values {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.ReduceF(k, vs)))
	}
	if err := ioutil.WriteFile(MergeFileName(w.ReduceN), []byte(strings.Join(res, "")), 0600); err != nil {
		w.UpdateStatus(TASK_STATUS_ERROR)
	}
	w.UpdateStatus(TASK_STATUS_FINISH)
}

func (w *Work) Run() {
	if w.TaskType == TASK_TYPE_MAP {
		w.RunMapTask()
	} else {
		w.RunReduceTask()
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := Work{}
	w.MapF = mapf
	w.ReduceF = reducef

	// register to coordinator
	w.Register()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	w.GetOneTask()
	w.Run()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
