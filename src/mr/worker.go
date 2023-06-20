package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

	// Your worker implementation here.
	for true {
		// request task
		taskRequest := TaskRequest{} // not sure what information we need to send here
		task := Task{}
		// TODO: handle the error case
		fmt.Print("Requesting task from master\n")
		call("Master.GetTask", &taskRequest, &task)
		fmt.Printf("Received a %v task\n", task.Type)
		if task.Type == "map" {
			// should we read from the file to get contents, or get contents from master?

			// read input file contents
			file, err := os.Open(task.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", task.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.Filename)
			}
			file.Close()

			// call Map function
			output := mapf(task.Filename, string(content))

			// make nReduce intermediate files
			var ofiles []*os.File
			for i := 0; i < task.NReduce; i++ {
				oname := fmt.Sprintf("mr-%d-%d", task.N, i)
				ofile, _ := os.Create(oname)
				ofiles = append(ofiles, ofile)
			}

			// write each keyVal pair to the appropriate file based on hash
			for _, kv := range output {
				enc := json.NewEncoder(ofiles[ihash(kv.Key)%task.NReduce])
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode json to intermediate file")
				}
			}

			// notify master that the task is finished
			finishTaskRequest := FinishTaskRequest{}
			finishTaskRequest.N = task.N
			finishTaskRequest.Filename = task.Filename

			finishTaskReply := FinishTaskReply{}

			call("Master.FinishTask", &finishTaskRequest, &finishTaskReply)
			fmt.Printf("Finished map task %v\n\n", task.N)
		} else if task.Type == "done" {
			break
		} else {
			fmt.Print("Unrecognized task type\n")
		}

		// wait for a sec before asking for another task
		time.Sleep(1 * time.Second)
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
