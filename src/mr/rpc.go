package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type TaskType string
type WorkerID string

func (t TaskType) String() string {
	return string(t)
}

func (id WorkerID) String() string {
	return string(id)
}

const (
	TaskMap    TaskType = "MAP"
	TaskReduce TaskType = "REDUCE"
)

type Task struct {
	Type         TaskType // MAP or REDUCE
	Index        int
	MapInputFile string

	WorkerID WorkerID
	Deadline time.Time
}

type ApplyTaskRequest struct {
	WorkerID      WorkerID
	LastTaskType  TaskType
	LastTaskIndex int
}

type ApplyTaskResponse struct {
	TaskType     TaskType
	TaskIndex    int
	MapInputFile string
	MapNum       int
	ReduceNum    int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func tmpMapOutFile(worker string, mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%s-%d-%d", worker, mapIndex, reduceIndex)
}

func finalMapOutFile(mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
}

func tmpReduceOutFile(worker string, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%s-out-%d", worker, reduceIndex)
}

func finalReduceOutFile(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
}

func genTaskID(t string, index int) string {
	return fmt.Sprintf("%s-%d", t, index)
}
