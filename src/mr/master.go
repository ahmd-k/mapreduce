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

type ActiveMapTask struct {
	N         int       // index of the file to map
	StartTime time.Time // when the task started
}

type Master struct {
	mapTasks       []int // index of file to map
	activeMapTasks []ActiveMapTask
	mapFiles       []string
	nReduce        int
	mutex          sync.Mutex
}

func (m *Master) GetTask(args *TaskRequest, reply *Task) error {
	m.mutex.Lock()
	fmt.Print("Received request for task\n")
	if len(m.mapTasks) > 0 {
		reply.Type = "map"
		reply.N = m.mapTasks[0]
		reply.Filename = m.mapFiles[m.mapTasks[0]]
		reply.NReduce = m.nReduce
		m.activeMapTasks = append(m.activeMapTasks, ActiveMapTask{m.mapTasks[0], time.Now()})
		m.mapTasks = m.mapTasks[1:]
	} else if len(m.activeMapTasks) > 0 {
		// Look for stale tasks and reassign them to the current requester
		for i := 0; i < len(m.activeMapTasks); i++ {
			if time.Since(m.activeMapTasks[i].StartTime) > 10*time.Second {
				fmt.Printf("Reassigning stale map task %v\n", m.activeMapTasks[i].N)
				m.activeMapTasks[i].StartTime = time.Now()
				reply.Type = "map"
				reply.N = m.activeMapTasks[i].N
				reply.Filename = m.mapFiles[m.activeMapTasks[i].N]
				reply.NReduce = m.nReduce
				break
			}
		}
	} else {
		fmt.Print("Telling worker to shut down\n")
		reply.Type = "done"
	}
	fmt.Printf("Assigned %v task %v\n\n", reply.Type, reply.N)
	m.mutex.Unlock()
	return nil
}

func (m *Master) FinishTask(args *FinishTaskRequest, reply *FinishTaskReply) error {
	// if we find the finished task in the active list, remove it
	// this could be easily improved with a dictionary lookup later
	m.mutex.Lock()
	for i := 0; i < len(m.activeMapTasks); i++ {
		if args.N == m.activeMapTasks[i].N {
			// TODO: reject finish if its been over 10 seconds?
			fmt.Printf("Received notification that map task %v has finished\n\n", args.N)
			m.activeMapTasks = append(m.activeMapTasks[:i], m.activeMapTasks[i+1:]...)
			break
		}
		fmt.Print("Finished task was not found in activeMapTasks\n\n")
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
