package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"math/rand"
	"os"
	"strconv"
)

const (
	MAP = iota
	REDUCE
	IDLE_TASK
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type NULLArgs struct {
}
type ReduceInputs struct {
	Inputs map[string]string
	Total  int
}

type GetReduceInputArgs struct {
	MapName   string
	ReduceInd int
}

type GetSocketReply struct {
	SockName string
}
type GetReduceInputReply struct {
	Inputs []KeyValue
}
type GetTaskArgs struct {
	Sockname string
}
type GetTaskReply struct {
	TaskId       int
	ResourceName string
	TaskType     int
	Status       int
	NReduce      int
}
type AddPeerArgs struct {
	Sockname string
	MapTask  string
}

type AddPeerReply struct {
	Status int
}

type CompletedReply struct {
	Status int
}

type FailedWorkerArgs struct {
	TaskID TaskIdentifier
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func randomWorkerSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(rand.Intn(900) + rand.Int())
	return s
}
