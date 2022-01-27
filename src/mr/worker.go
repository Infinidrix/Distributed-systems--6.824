package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type WorkerServer struct {
	mu          sync.Mutex
	sockname    string
	nReduce     int
	peerworkers map[string]string
	results     map[string][][]KeyValue // map[mapTask][reduceTaskNo] = [keyvalue]
}

var workerServer WorkerServer

func (w *WorkerServer) server() {
	rpc.Register(w)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := CallSockGet()
	w.sockname = sockname
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (w *WorkerServer) GetReduceInput(args *GetReduceInputArgs, reply *GetReduceInputReply) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if val, ok := w.results[args.MapName]; ok {
		reply.Inputs = val[args.ReduceInd]
	} else {
		// keys := make([]string, 0)
		// for key := range w.results {
		// 	keys = append(keys, key)
		// }
		//fmt.printf("We have an issue with the MapName %v\nKeys: %v\n", args.MapName, keys)
		// fmt.Printf("We have an issue on worker %v with key %v given val %v\n", w.sockname, args.MapName, w.results)
	}

	return nil
}

func (w *WorkerServer) performMap(task GetTaskReply, mapf func(string, string) []KeyValue) {
	w.mu.Lock()
	defer w.mu.Unlock()
	filename := task.ResourceName
	dat, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	result := mapf(filename, string(dat))
	//fmt.printf("The result of the Map task has %v elements\n", len(result))
	w.results[task.ResourceName] = make([][]KeyValue, w.nReduce)
	for _, element := range result {
		reduce_ind := ihash(element.Key) % w.nReduce
		w.results[task.ResourceName][reduce_ind] = append(w.results[task.ResourceName][reduce_ind], element)
	}
	//fmt.printf("Parsed all of ID: %v\n", task.TaskId)
	// fmt.Printf("Mapped: %v of len %v\n", task.ResourceName, len(w.results[task.ResourceName]))
	CallFinished(task)
}

func CallFinished(task GetTaskReply) {
	reply := CompletedReply{Status: 0}

	// send the RPC request, wait for the reply.
	if !call("Coordinator.CompletedTask", &task, &reply) {
		panic("Error on RPC call to CompletedTask")
	}
}

func (w *WorkerServer) performReduce(task GetTaskReply, reducef func(string, []string) string) {
	var status map[string]bool = make(map[string]bool)
	var result []KeyValue = make([]KeyValue, 0)
	for {
		// w.mu.Lock()
		inputNodes := callReduceInputs()
		//fmt.printf("The inputs from ReduceInputs: %v with current len: %v\n", len(inputNodes.Inputs), len(status))
		if inputNodes.Total == len(status) {
			break
		}
		for key, val := range inputNodes.Inputs {
			if _, ok := status[key]; ok {
				continue
			}
			input, ok := CallReduceInput(val, key, task.TaskId)
			if !ok {
				continue
			}
			result = append(result, input.Inputs...)
			status[key] = true
		}
		// w.mu.Unlock()
		time.Sleep(time.Second)
	}
	//fmt.printf("Reduce task with %v values on task %v\n", len(result), task.TaskId)
	if len(result) == 0 {
		CallFinished(task)
		return
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	file, err := os.Create("mr-out-" + fmt.Sprint(task.TaskId))
	if err != nil {
		panic(err)
	}
	defer file.Close()

	curr := result[0].Key
	var values []string = make([]string, 0)
	for _, elem := range result {
		if curr == elem.Key {
			values = append(values, elem.Value)
			continue
		}
		finalResult := reducef(curr, values)
		file.WriteString(fmt.Sprintf("%v %v\n", curr, finalResult))
		curr = elem.Key
		values = make([]string, 1)
		values[0] = elem.Value
	}
	finalResult := reducef(curr, values)
	_, er := file.WriteString(fmt.Sprintf("%v %v\n", curr, finalResult))
	fmt.Printf("Finished Reduce task %v errors %v\n", task.TaskId, er)
	CallFinished(task)
}

func CallReduceInput(sockname string, mapname string, id int) (GetReduceInputReply, bool) {
	args := GetReduceInputArgs{MapName: mapname, ReduceInd: id}
	reply := GetReduceInputReply{}

	res := callWorker("WorkerServer.GetReduceInput", sockname, &args, &reply)
	if !res {
		// fmt.Printf("Something wrong with get reduce input call on worker\n")
		callFailedWorker(mapname)
		return reply, false
	}
	return reply, true
}

func callReduceInputs() ReduceInputs {
	args := NULLArgs{}
	reply := ReduceInputs{}

	// send the RPC request, wait for the reply.
	if !call("Coordinator.GetReduceInputs", &args, &reply) {
		panic("Error on RPC call to GetReduceInputs")
	}
	return reply
}

func callFailedWorker(mapname string) {
	args := FailedWorkerArgs{}
	args.TaskID.Resource = mapname
	args.TaskID.TaskType = MAP
	reply := NULLArgs{}

	if !call("Coordinator.ReportFailedWorker", &args, &reply) {
		panic("Error on RPC call to ReportFailedWorker")
	}
}

func (w *WorkerServer) AddPeerConnection(args *AddPeerArgs, reply *AddPeerReply) error {
	// locks
	w.peerworkers[args.MapTask] = args.Sockname
	return nil
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerServer.results = make(map[string][][]KeyValue)
	workerServer.peerworkers = make(map[string]string)
	// Your worker implementation here.
	workerServer.server()
	// uncomment to send the Example RPC to the coordinator.
	for {
		reply, err := CallExample()
		if err != nil {
			panic(err)
		}
		if reply.TaskType == IDLE_TASK {
			time.Sleep(time.Second)
		} else if reply.TaskType == MAP {
			workerServer.nReduce = reply.NReduce
			workerServer.performMap(reply, mapf)
		} else {
			go workerServer.performReduce(reply, reducef)
			time.Sleep(time.Second)
		}

	}

}

func CallSockGet() string {
	args := NULLArgs{}

	// declare a reply structure.
	reply := GetSocketReply{}

	// send the RPC request, wait for the reply.
	if !call("Coordinator.GetSocket", &args, &reply) {
		panic("Error on RPC call to GetTask")
	}
	return reply.SockName
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() (GetTaskReply, error) {

	// declare an argument structure.
	args := GetTaskArgs{}

	// fill in the argument(s).
	args.Sockname = workerServer.sockname

	// declare a reply structure.
	reply := GetTaskReply{}

	// send the RPC request, wait for the reply.
	if !call("Coordinator.GetTask", &args, &reply) {
		panic("Error on RPC call to GetTask")
	}
	return reply, nil
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()

	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Fatal("dialing:", err)
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func callWorker(rpcname string, sockname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")

	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// fmt.Printf("Dialing: %v\n", err)
		return false
		// log.Fatal("dialing:", err)

	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
