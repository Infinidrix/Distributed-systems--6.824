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

const (
	PENDING = iota
	ONGOING
	COMPLETED
)

type Task struct {
	taskId         uint
	status         int
	initTime       uint32
	resource       string
	assignedWorker string
	taskType       int
}

type TaskIdentifier struct {
	Resource string
	TaskType int
}

type Coordinator struct {
	// Your definitions here.
	m                  sync.Mutex
	pendingTasks       []*Task
	ongoingMapTasks    []*Task
	ongoingReduceTasks []*Task
	completedTasks     []*Task
	ReduceInputs       map[string]string // reduceInputs[mapfile] = worker sockname
	allTasks           map[TaskIdentifier]*Task
	nReduce            uint
	nWork              uint
	count              int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) GetSocket(args *NULLArgs, reply *GetSocketReply) error {
	c.m.Lock()
	defer c.m.Unlock()
	reply.SockName = randomWorkerSock() + fmt.Sprint(c.count)
	c.count += 1
	// fmt.Printf("Assigned a socket %v to worker\n", reply.SockName)
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.m.Lock()
	defer c.m.Unlock()
	if len(c.pendingTasks) != 0 {
		curr := c.pendingTasks[0]
		c.pendingTasks = c.pendingTasks[1:]
		curr.assignedWorker = args.Sockname
		curr.initTime = uint32(time.Now().Unix())
		curr.status = ONGOING
		if curr.taskType == MAP {
			c.ongoingMapTasks = append(c.ongoingMapTasks, curr)
		} else {
			c.ongoingReduceTasks = append(c.ongoingReduceTasks, curr)
		}
		reply.ResourceName = curr.resource
		reply.TaskType = curr.taskType
		reply.NReduce = int(c.nReduce)
		reply.TaskId = int(curr.taskId)
		return nil
	}
	reply.ResourceName = "IDLING"
	reply.TaskType = IDLE_TASK
	return nil
}

func (c *Coordinator) GetReduceInputs(args *NULLArgs, reply *ReduceInputs) error {
	c.m.Lock()
	defer c.m.Unlock()
	reply.Inputs = c.ReduceInputs
	reply.Total = int(c.nWork)
	return nil
}

func (c *Coordinator) heartbeat() {
	for !c.Done() {
		c.m.Lock()
		for len(c.ongoingMapTasks) != 0 {
			curr := c.ongoingMapTasks[0]
			if curr.status == COMPLETED {
				c.ongoingMapTasks = c.ongoingMapTasks[1:]
				c.completedTasks = append(c.completedTasks, curr)
			} else if uint32(time.Now().Unix())-curr.initTime > 20 {
				curr.status = PENDING
				c.ongoingMapTasks = c.ongoingMapTasks[1:]
				c.pendingTasks = append(c.pendingTasks, curr)
			} else {
				break
			}
		}

		for len(c.ongoingReduceTasks) != 0 && len(c.ongoingMapTasks) == 0 && len(c.pendingTasks) == 0 {
			curr := c.ongoingReduceTasks[0]
			if curr.status == COMPLETED {
				c.ongoingReduceTasks = c.ongoingReduceTasks[1:]
				c.completedTasks = append(c.completedTasks, curr)
			} else if uint32(time.Now().Unix())-curr.initTime > 70 {
				curr.status = PENDING
				c.ongoingReduceTasks = c.ongoingReduceTasks[1:]
				c.pendingTasks = append(c.pendingTasks, curr)
			} else {
				break
			}
		}
		c.m.Unlock()
		time.Sleep(time.Second)
	}
}

func (c *Coordinator) ReportFailedWorker(args *FailedWorkerArgs, reply *NULLArgs) error {
	c.m.Lock()
	defer c.m.Unlock()
	taskId := args.TaskID
	task := c.allTasks[taskId]
	if task.status == PENDING {
		return nil
	}
	fmt.Printf("Reassigning %v from %v\n", task.resource, task.assignedWorker)
	task.status = PENDING
	c.pendingTasks = append(c.pendingTasks, task)
	return nil
}

func (c *Coordinator) CompletedTask(args *GetTaskReply, reply *CompletedReply) error {
	c.m.Lock()
	defer c.m.Unlock()
	task := c.allTasks[TaskIdentifier{Resource: args.ResourceName, TaskType: args.TaskType}]

	fmt.Printf("%v completed task: %v with value %v\n", task.assignedWorker, task.taskId, args.ResourceName)
	task.status = COMPLETED
	task.initTime = uint32(time.Now().Unix())
	if task.taskType == MAP {
		c.ReduceInputs[task.resource] = task.assignedWorker
	}
	reply.Status = 0
	return nil
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	c.m.Lock()
	defer c.m.Unlock()
	// Your code here.
	if len(c.ongoingMapTasks) == 0 && len(c.ongoingReduceTasks) == 0 && len(c.pendingTasks) == 0 {
		return true
	}
	//fmt.printf("\t%v %v %v\n", len(c.ongoingMapTasks), len(c.ongoingReduceTasks), len(c.pendingTasks))
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = uint(nReduce)
	c.ReduceInputs = make(map[string]string)
	c.nWork = uint(len(files))
	c.allTasks = make(map[TaskIdentifier]*Task)
	c.count = 0
	//fmt.printf("Reduce Tasks: %d\n", c.nReduce)
	// Your code here.
	for i := 0; i < len(files); i++ {
		newTask := Task{initTime: uint32(time.Now().Unix()),
			status:   PENDING,
			resource: files[i],
			taskId:   uint(i),
			taskType: MAP,
		}
		c.pendingTasks = append(c.pendingTasks, &newTask)
		c.allTasks[TaskIdentifier{Resource: newTask.resource, TaskType: newTask.taskType}] = &newTask
	}
	for i := 0; i < nReduce; i++ {
		newTask := Task{
			initTime: uint32(time.Now().Unix()),
			status:   PENDING,
			resource: fmt.Sprintf("%d", i),
			taskId:   uint(i),
			taskType: REDUCE,
		}
		c.pendingTasks = append(c.pendingTasks, &newTask)
		c.allTasks[TaskIdentifier{Resource: newTask.resource, TaskType: newTask.taskType}] = &newTask
	}
	// for i := 0; i < 10; i++ {
	// fmt.Printf("Random number %v\n", randomWorkerSock())
	// }
	c.server()
	go c.heartbeat()
	return &c
}
