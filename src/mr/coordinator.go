package mr

import (
	"log"
	"math"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Stage string

const (
	StageMap    Stage = "MAP"
	StageReduce Stage = "Stage"
)

type Coordinator struct {
	mu             sync.RWMutex
	stage          Stage // 工作阶段
	nMap           int
	nReduce        int
	tasks          map[string]Task
	availableTasks chan Task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ApplyTask(req *ApplyTaskRequest, resp *ApplyTaskResponse) error {
	// 记录 Worker 的上一个 Task 已经运行完成
	if req.LastTaskType != "" {
		c.mu.Lock()
		lastTaskID := genTaskID(req.LastTaskType.String(), req.LastTaskIndex)
		if task, exists := c.tasks[lastTaskID]; exists && task.WorkerID == req.WorkerID {
			log.Printf(
				"Mark %s task %d as finished on worker %s\n",
				task.Type, task.Index, req.WorkerID)
			// 将该 Worker 的临时产出文件标记为最终产出文件
			switch req.LastTaskType {
			case TaskMap:
				for ri := 0; ri < c.nReduce; ri++ {
					err := os.Rename(
						tmpMapOutFile(req.WorkerID.String(), req.LastTaskIndex, ri),
						finalMapOutFile(req.LastTaskIndex, ri))
					if err != nil {
						log.Fatalf(
							"Failed to mark map output file `%s` as final: %e",
							tmpMapOutFile(req.WorkerID.String(), req.LastTaskIndex, ri), err)
					}
				}
			case TaskReduce:
				err := os.Rename(
					tmpReduceOutFile(req.WorkerID.String(), req.LastTaskIndex),
					finalReduceOutFile(req.LastTaskIndex))
				if err != nil {
					log.Fatalf(
						"Failed to mark reduce output file `%s` as final: %e",
						tmpReduceOutFile(req.WorkerID.String(), req.LastTaskIndex), err)
				}
			}
			delete(c.tasks, lastTaskID)

			// 当前阶段所有 Task 已完成，进入下一阶段
			if len(c.tasks) == 0 {
				c.transit()
			}
		}
		c.mu.Unlock()
	}

	// 获取一个可用 Task 并返回
	task, ok := <-c.availableTasks
	if !ok {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	log.Printf("Assign %s task %d to worker %s\n", task.Type, task.Index, req.WorkerID)
	task.WorkerID = req.WorkerID
	task.Deadline = time.Now().Add(10 * time.Second)
	c.tasks[genTaskID(task.Type.String(), task.Index)] = task
	resp.TaskType = task.Type
	resp.TaskIndex = task.Index
	resp.MapInputFile = task.MapInputFile
	resp.MapNum = c.nMap
	resp.ReduceNum = c.nReduce
	return nil
}

func (c *Coordinator) transit() {
	switch c.stage {
	case StageMap:
		log.Printf("All map tasks finished. Transit to Reduce stage\n")
		c.stage = StageReduce
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				Type:  TaskReduce,
				Index: i,
			}
			c.tasks[genTaskID(task.Type.String(), task.Index)] = task
			c.availableTasks <- task
		}
	case StageReduce:
		log.Printf("all reduce tasks finished. prepare to exit\n")
		close(c.availableTasks)
		c.stage = ""
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stage == ""
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mu:             sync.RWMutex{},
		stage:          StageMap,
		nMap:           len(files),
		nReduce:        nReduce,
		tasks:          make(map[string]Task),
		availableTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}
	for i, file := range files {
		task := Task{
			Type:         TaskMap,
			Index:        i,
			MapInputFile: file,
		}
		c.tasks[genTaskID(task.Type.String(), task.Index)] = task
		c.availableTasks <- task
	}
	log.Printf("Coordinator start\n")
	c.server()
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)

			c.mu.Lock()
			for _, task := range c.tasks {
				if task.WorkerID != "" && time.Now().After(task.Deadline) {
					// 回收并重新分配
					log.Printf(
						"Found timed-out %s task %d previously running on worker %s. Prepare to re-assign",
						task.Type, task.Index, task.WorkerID)
					task.WorkerID = ""
					c.availableTasks <- task
				}
			}
			c.mu.Unlock()
		}
	}()
	return &c
}
