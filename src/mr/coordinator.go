package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// max wait time before a task is to be rescheduled.
// a task should be rescheduled if it takes too long to complete
const TaskTimeoutSeconds = 10

// max wait time for a worker to exit after the entire job has been completed.
const MaxWorkerExitWaitSeconds = 3

type TaskType string

// define two task types: map and reduce.
const (
	TaskTypeMap    TaskType = "map"
	TaskTypeReduce TaskType = "reduce"
)

type Task struct {
	Type      TaskType // type of the task: map or reduce
	Scheduled bool     // whether the task has been scheduled
	BeginTime int64    // the time when the task was scheduled
	Done      bool     // whether the task has been completed
}

type Coordinator struct {
	mu          sync.Mutex // lock to protect shared access to the coordinator state
	files       []string   // input files
	nMap        int        // number of map tasks
	nReduce     int        // number of reduce tasks
	mapDone     bool       // whether all map tasks have been completed
	reduceDone  bool       // whether all reduce tasks have been completed
	done        bool       // whether the entire job has been completed
	mapTasks    []Task     // map tasks, the index is the task id
	reduceTasks []Task     // reduce tasks, the index is the task id
}

// Your code here -- RPC handlers for the worker to call.

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
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nMap = len(files)
	c.nReduce = nReduce
	c.files = make([]string, c.nMap)
	copy(c.files, files)

	c.mapDone = false
	c.reduceDone = false
	c.done = false

	// init map tasks
	c.mapTasks = make([]Task, c.nMap)
	for i := 0; i < c.nMap; i++ {
		c.mapTasks[i] = Task{
			Type:      TaskTypeMap,
			Scheduled: false,
			BeginTime: 0,
			Done:      false}
	}

	// init reduce tasks
	c.reduceTasks = make([]Task, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks[i] = Task{
			Type:      TaskTypeReduce,
			Scheduled: false,
			BeginTime: 0,
			Done:      false}
	}

	c.server()
	go c.runForever()
	return &c
}

// monitor the status of map tasks and reschedule them if necessary.
func (c *Coordinator) checkMapTasks() {
	now := time.Now().Unix()
	nDone := 0

	for i := 0; i < c.nMap; i++ {
		task := &c.mapTasks[i]

		// if the task has not been scheduled or has been completed, skip it.
		if !task.Scheduled || task.Done {
			if task.Done {
				nDone++
			}
			continue
		}

		// if the task has been scheduled but not completed, check if it has timed out.
		// if it has, reschedule the task.
		if now-task.BeginTime > TaskTimeoutSeconds {
			task.Scheduled = false
		}
	}

	// if all map tasks have been completed, set the map phase as done.
	if nDone == c.nMap {
		c.mapDone = true
	}
}

// monitor the status of reduce tasks and reschedule them if necessary.
func (c *Coordinator) checkReduceTasks() {
	now := time.Now().Unix()
	nDone := 0

	for i := 0; i < c.nReduce; i++ {
		task := &c.reduceTasks[i]

		// if the task has not been scheduled or has been completed, skip it.
		if !task.Scheduled || task.Done {
			if task.Done {
				nDone++
			}
			continue
		}

		// if the task has been scheduled but not completed, check if it has timed out.
		// if it has, reschedule the task.
		if now-task.BeginTime > TaskTimeoutSeconds {
			task.Scheduled = false
		}
	}

	// if all reduce tasks have been completed, set the reduce phase as done.
	if nDone == c.nReduce {
		c.reduceDone = true
	}
}

// monitor the status of the tasks and reschedule them if necessary.
func (c *Coordinator) runForever() {
	// run forever until the entire job has been completed.
	for {
		time.Sleep(1 * time.Second)

		// lock the coordinator state.
		c.mu.Lock()

		// if the entire job has been completed, exit the loop.
		if c.done {
			c.mu.Unlock()
			break
		}

		if !c.mapDone {
			c.checkMapTasks()
		}

		if !c.reduceDone {
			c.checkReduceTasks()
		}

		// check if the entire job has been completed.
		if c.mapDone && c.reduceDone {
			// release the lock before waiting for workers to exit.
			c.mu.Unlock()

			// wait for a while to make sure all workers have exited.
			time.Sleep(MaxWorkerExitWaitSeconds)

			// set the entire job as completed.
			c.mu.Lock()
			c.done = true
		}

		c.mu.Unlock()
	}
}

func (c *Coordinator) handleRequestTask(args *Args, reply *Reply) error {
	if args.Type != RpcTypeRequestTask {
		return fmt.Errorf("RpcTypeRequestTask expected, but got %v", args.Type)
	}

	if !c.mapDone {
		// find the first unscheduled map task and schedule it.
		for i := 0; i < c.nMap; i++ {
			// i is the task id.

			task := &c.mapTasks[i]
			if !task.Scheduled {
				task.Scheduled = true
				task.BeginTime = time.Now().Unix()
				reply.Type = RpcTypeRequestTask
				reply.TaskId = i
				reply.TaskTyp = TaskTypeMap
				reply.NMap = c.nMap
				reply.NReduce = c.nReduce
				// the input file for the map task is the i-th file in the files array.
				reply.Files = []string{c.files[i]}
				return nil
			}
		}
	} else if !c.reduceDone {
		// if the map phase has been completed, start the reduce phase.
		for i := 0; i < c.nReduce; i++ {
			// i is the task id.

			task := &c.reduceTasks[i]
			if !task.Scheduled {
				task.Scheduled = true
				task.BeginTime = time.Now().Unix()
				reply.Type = RpcTypeRequestTask
				reply.TaskId = i
				reply.TaskTyp = TaskTypeReduce
				reply.NMap = c.nMap
				reply.NReduce = c.nReduce

				// reply.Files is not used for reduce tasks.
				// there is no need to specify the input files for the reduce task.
				// the intermediate files are named as mr-X-Y, where X is the map task number, Y is the reduce task number.
				// the reduce task should read all the intermediate files with Y as the reduce task number.
				// for example, if the reduce task number is 0, the intermediate files are mr-*-0.
				return nil
			}
		}
	} else {
		// if both map and reduce phases have been completed, tell the worker to exit.
		reply.Type = RpcTypeExit
	}
	return nil
}

func (c *Coordinator) handleFailTask(args *Args, reply *Reply) error {
	if args.Type != RpcTypeFailTask {
		return fmt.Errorf("RpcTypeFailTask expected, but got %v", args.Type)
	}

	// if a task has failed, reschedule it.
	if args.TaskTyp == TaskTypeMap {
		c.mapTasks[args.TaskId].Scheduled = false
	} else if args.TaskTyp == TaskTypeReduce {
		c.reduceTasks[args.TaskId].Scheduled = false
	}

	reply.Type = RpcTypeFailTask
	return nil
}

func (c *Coordinator) handleFinishTask(args *Args, reply *Reply) error {
	if args.Type != RpcTypeFinishTask {
		return fmt.Errorf("RpcTypeFinishTask expected, but got %v", args.Type)
	}

	// if a task has been completed, mark it as done.
	if args.TaskTyp == TaskTypeMap {
		c.mapTasks[args.TaskId].Done = true
	} else if args.TaskTyp == TaskTypeReduce {
		c.reduceTasks[args.TaskId].Done = true
	}

	reply.Type = RpcTypeFinishTask
	return nil
}

func (c *Coordinator) RemoteCall(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// if all map, reduce tasks have been completed, tell the worker to exit.
	if c.mapDone && c.reduceDone {
		reply.Type = RpcTypeExit
		return nil
	}

	// default reply type is no task.
	reply.Type = RpcTypeNoTask

	switch args.Type {
	case RpcTypeRequestTask:
		return c.handleRequestTask(args, reply)
	case RpcTypeFailTask:
		return c.handleFailTask(args, reply)
	case RpcTypeFinishTask:
		return c.handleFinishTask(args, reply)
	default:
		return fmt.Errorf("invalid RPC type: %v", args.Type)
	}
}
