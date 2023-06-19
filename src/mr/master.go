package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	mapTasks       []int // index of file to map
	activeMapTasks []int
	mapFiles       []string
	nReduce        int
	mutex          sync.Mutex
}

func (m *Master) GetTask(args *TaskRequest, reply *Task) error {
	m.mutex.Lock()
	if len(m.mapTasks) > 0 {
		reply.Type = "map"
		reply.N = m.mapTasks[0]
		reply.Filename = m.mapFiles[m.mapTasks[0]]
		reply.NReduce = m.nReduce
		m.activeMapTasks = append(m.activeMapTasks, m.mapTasks[0])
		m.mapTasks = m.mapTasks[1:]
	} else if len(m.activeMapTasks) > 0 {
		// TODO
	}
	m.mutex.Unlock()
	return nil
}

func (m *Master) FinishTask(args *FinishTaskRequest, reply *FinishTaskReply) error {
	// if we find the finished task in the active list, remove it
	// this could be easily improved with a dictionary lookup later
	m.mutex.Lock()
	for i := 0; i < len(m.activeMapTasks); i++ {
		if args.N == m.activeMapTasks[i] {
			m.activeMapTasks = append(m.activeMapTasks[:i], m.activeMapTasks[i+1:]...)
			break
		}
	}
	m.mutex.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mapFiles = files
	m.nReduce = nReduce

	// Your code here.
	for i := 0; i < len(files); i++ {
		m.mapTasks = append(m.mapTasks, i)
	}

	m.server()
	return &m
}
