package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// RPC definitions.

type RpcType string

// RPC types
const (
	RpcTypeRequestTask RpcType = "request_task" // worker requests a task
	RpcTypeFailTask    RpcType = "fail_task"    // worker tells coordinator that a task has failed
	RpcTypeFinishTask  RpcType = "finish_task"  // worker tells coordinator that a task has been completed
	RpcTypeNoTask      RpcType = "no_task"      // coordinator tells worker that there is no task available
	//RpcTypeHeartbeat   RpcType = "heartbeat"    // worker sends a heartbeat to the coordinator
	RpcTypeExit RpcType = "exit" // coordinator tells worker to exit
)

type Args struct {
	Type    RpcType
	TaskId  int
	TaskTyp TaskType
}

type Reply struct {
	Type    RpcType
	TaskId  int
	TaskTyp TaskType
	Files   []string // input files name for map, reduce task

	// intermediate files are named as mr-X-Y, where X is the map task number, Y is the reduce task number
	NMap    int // number of map tasks
	NReduce int // number of reduce tasks
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
