package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

const (
	MapIntermediateTempFilePattern = "mr-%d-%d-*"  // mr-<mapTaskId>-<reduceTaskId>-* (temp file)
	MapIntermediateFilePattern     = "mr-%d-%d"    // mr-<mapTaskId>-<reduceTaskId> (final file)
	ReduceOutputTempFilePattern    = "mr-out-%d-*" // mr-out-<reduceTaskId>-* (temp file)
	ReduceOutputFilePattern        = "mr-out-%d"   // mr-out-<reduceTaskId> (final file)
)

type KV []KeyValue

// implement sort.Interface for KV

func (kv KV) Len() int { return len(kv) }

func (kv KV) Less(i, j int) bool { return kv[i].Key < kv[j].Key }

func (kv KV) Swap(i, j int) { kv[i], kv[j] = kv[j], kv[i] }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ConcurrentWorker struct {
	mapf    func(string, string) []KeyValue // map function
	reducef func(string, []string) string   // reduce function

	idle chan struct{} // signal channel for the worker to wait for a task
	exit chan struct{} // signal channel for the worker to exit

	taskType TaskType
	taskId   int
	nMap     int
	nReduce  int

	files []string // input files

}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// create a worker instance, and run it forever.
	worker := ConcurrentWorker{
		mapf:    mapf,
		reducef: reducef,
		idle:    make(chan struct{}, 1),
		exit:    make(chan struct{}, 1),
	}
	worker.idle <- struct{}{}
	worker.runForever()
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

	log.Printf("call error: %v\n", err)
	return false
}

// communicate with the coordinator to get the next task.
// execute the task, and report the result.
// repeat until there are no more tasks.
func (w *ConcurrentWorker) runForever() {
	for {
		select {
		case <-w.idle:
			// ask the coordinator for a task
			hasTask, err := w.requestTask()
			if err != nil {
				w.idle <- struct{}{}
				log.Printf("failed to request task: %v\n", err)
			}

			// if the request is successful, execute the task
			if hasTask {
				go w.doTask()
			}
		case <-time.After(1 * time.Second):
			// send heartbeat to the coordinator
			//w.sendHeartbeat()
		case <-w.exit:
			return
		}

		// wait for a while before the next loop
		time.Sleep(1 * time.Second)
	}
}

// ask the coordinator for a task
func (w *ConcurrentWorker) requestTask() (bool, error) {
	args := &Args{
		Type: RpcTypeRequestTask,
	}
	reply := &Reply{}

	if !call("Coordinator.RemoteCall", args, reply) {
		return false, fmt.Errorf("failed to request task")
	}

	if reply.Type == RpcTypeExit {
		w.exit <- struct{}{}
		return false, nil
	} else if reply.Type == RpcTypeNoTask {
		return false, nil
	}

	w.taskType = reply.TaskTyp
	w.taskId = reply.TaskId
	w.nMap = reply.NMap
	w.nReduce = reply.NReduce
	w.files = reply.Files

	return true, nil
}

func (w *ConcurrentWorker) doTask() error {
	var err error = nil
	if w.taskType == TaskTypeMap {
		err = w.doMapTask()
	} else if w.taskType == TaskTypeReduce {
		err = w.doReduceTask()
	}

	// report the result to the coordinator
	if err != nil {
		log.Printf("task failed: %v\n", err)
		w.failTask()
	} else {
		w.finishTask()
	}

	// mark the worker as idle
	w.idle <- struct{}{}

	return err
}

// execute the map task
func (w *ConcurrentWorker) doMapTask() error {
	// read input files and call map function
	kvs := []KeyValue{}
	for _, file := range w.files {
		content, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		text := string(content)
		kvs = append(kvs, w.mapf(file, text)...)
	}

	sort.Sort(KV(kvs))

	// create temp intermediate files
	files := make([]*os.File, w.nReduce)
	encoders := make([]*json.Encoder, w.nReduce)
	for i := 0; i < w.nReduce; i++ {
		// i is reduce id

		f, err := os.CreateTemp(".", fmt.Sprintf(MapIntermediateTempFilePattern, w.taskId, i))
		files[i] = f
		if err != nil {
			return err
		}
		defer files[i].Close()
		encoders[i] = json.NewEncoder(files[i])
	}

	// write to intermediate files
	for _, kv := range kvs {
		h := ihash(kv.Key) % w.nReduce
		err := encoders[h].Encode(&kv)
		if err != nil {
			return err
		}
	}

	// rename temp files
	for i := 0; i < w.nReduce; i++ {
		filename := fmt.Sprintf(MapIntermediateFilePattern, w.taskId, i)
		err := os.Rename(files[i].Name(), filename)
		if err != nil {
			return err
		}
	}
	return nil
}

// execute the reduce task
func (w *ConcurrentWorker) doReduceTask() error {
	// read intermediate files and call reduce function
	kvm := make(map[string][]string)

	for i := 0; i < w.nMap; i++ {
		// read intermediate files
		filename := fmt.Sprintf(MapIntermediateFilePattern, i, w.taskId)
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer file.Close()

		// decode intermediate files
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kvm[kv.Key] = append(kvm[kv.Key], kv.Value)
		}
	}

	// call reduce function
	kvs := []KeyValue{}
	for k, v := range kvm {
		output := w.reducef(k, v)
		kvs = append(kvs, KeyValue{k, output})
	}

	sort.Sort(KV(kvs))

	// create output file

	filename := fmt.Sprintf(ReduceOutputTempFilePattern, w.taskId)
	file, err := os.CreateTemp(".", filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// write to output file
	for _, kv := range kvs {
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}

	// rename output file
	err = os.Rename(file.Name(), fmt.Sprintf(ReduceOutputFilePattern, w.taskId))
	if err != nil {
		return err
	}

	return nil
}

/*
// send heartbeat to the coordinator
func (w *ConcurrentWorker) sendHeartbeat() error {
	args := &Args{
		Type:    RpcTypeHeartbeat,
		TaskTyp: w.taskType,
		TaskId:  w.taskId,
	}
	reply := &Reply{}

	if !call("Coordinator.RemoteCall", args, reply) {
		return fmt.Errorf("failed to send heartbeat")
	}

	if reply.Type == RpcTypeExit {
		w.exit <- struct{}{}
	}
	return nil
}
*/

// report failure to the coordinator
func (w *ConcurrentWorker) failTask() error {
	args := &Args{
		Type:    RpcTypeFailTask,
		TaskTyp: w.taskType,
		TaskId:  w.taskId,
	}
	reply := &Reply{}

	if !call("Coordinator.RemoteCall", args, reply) {
		return fmt.Errorf("failed to report failure")
	}

	if reply.Type == RpcTypeExit {
		w.exit <- struct{}{}
	}
	return nil
}

// report success to the coordinator
func (w *ConcurrentWorker) finishTask() error {
	args := &Args{
		Type:    RpcTypeFinishTask,
		TaskTyp: w.taskType,
		TaskId:  w.taskId,
	}
	reply := &Reply{}

	if !call("Coordinator.RemoteCall", args, reply) {
		return fmt.Errorf("failed to report success")
	}

	if reply.Type == RpcTypeExit {
		w.exit <- struct{}{}
	}
	return nil
}
