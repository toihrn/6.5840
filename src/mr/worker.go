package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use iHash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func iHash(key string) int {
	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	if err != nil {
		log.Fatalf("iHash write err: %v", err)
	}
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapF func(string, string) []KeyValue, reduceF func(string, []string) string) {
	// 单机运行，直接使用 PID 作为 Worker ID，方便 debug
	id := strconv.Itoa(os.Getpid())
	log.Printf("Worker %s started\n", id)

	// 进入循环，向 Coordinator 申请 Task
	var lastTaskType TaskType
	var lastTaskIndex int
	for {
		req := ApplyTaskRequest{
			WorkerID:      WorkerID(id),
			LastTaskType:  lastTaskType,
			LastTaskIndex: lastTaskIndex,
		}
		resp := ApplyTaskResponse{}
		call("Coordinator.ApplyTask", &req, &resp)

		if resp.TaskType == "" {
			// MR 作业已完成，退出
			log.Printf("Received job finish signal from coordinator")
			break
		}

		log.Printf("Received %s task %d from coordinator", resp.TaskType, resp.TaskIndex)
		if resp.TaskType == TaskMap {
			// 读取输入数据
			file, err := os.Open(resp.MapInputFile)
			if err != nil {
				log.Fatalf("Failed to open map input file %s: %e", resp.MapInputFile, err)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("Failed to read map input file %s: %e", resp.MapInputFile, err)
			}
			// 传递输入数据至 MAP 函数，得到中间结果
			kva := mapF(resp.MapInputFile, string(content))
			// 按 Key 的 Hash 值对中间结果进行分桶
			hashedKva := make(map[int][]KeyValue)
			for _, kv := range kva {
				hashed := iHash(kv.Key) % resp.ReduceNum
				hashedKva[hashed] = append(hashedKva[hashed], kv)
			}
			// 写出中间结果文件
			for i := 0; i < resp.ReduceNum; i++ {
				oFile, _ := os.Create(tmpMapOutFile(id, resp.TaskIndex, i))
				for _, kv := range hashedKva[i] {
					_, err := fmt.Fprintf(oFile, "%v\t%v\n", kv.Key, kv.Value)
					if err != nil {
						log.Fatalf("Failed to write map output file %s: %e\n", oFile.Name(), err)
					}
				}
				_ = oFile.Close()
			}
		} else if resp.TaskType == TaskReduce {
			// 读取输入数据
			var lines []string
			for mi := 0; mi < resp.MapNum; mi++ {
				inputFile := finalMapOutFile(mi, resp.TaskIndex)
				file, err := os.Open(inputFile)
				if err != nil {
					log.Fatalf("Failed to open map output file %s: %e\n", inputFile, err)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("Failed to read map output file %s: %e\n", inputFile, err)
				}
				lines = append(lines, strings.Split(string(content), "\n")...)
			}
			var kva []KeyValue
			for _, line := range lines {
				if strings.TrimSpace(line) == "" {
					continue
				}
				parts := strings.Split(line, "\t")
				kva = append(kva, KeyValue{
					Key:   parts[0],
					Value: parts[1],
				})
			}
			sort.Sort(ByKey(kva))

			oFile, _ := os.Create(tmpReduceOutFile(id, resp.TaskIndex))
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reduceF(kva[i].Key, values)

				_, err := fmt.Fprintf(oFile, "%v %v\n", kva[i].Key, output)
				if err != nil {
					log.Fatalf("Failed to write reduce output file %s: %e\n", oFile.Name(), err)
				}
				i = j
			}
			_ = oFile.Close()
		}
		lastTaskType = resp.TaskType
		lastTaskIndex = resp.TaskIndex
		log.Printf("Finished %s task %d\n", resp.TaskType, resp.TaskIndex)
	}

	log.Printf("Worker %s exit\n", id)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcName string, req interface{}, resp interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer func() { _ = c.Close() }()
	err = c.Call(rpcName, req, resp)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
