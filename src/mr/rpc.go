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

// Add your RPC definitions here.
type TaskRequest struct {
}

type Task struct {
	Type     string // either map or reduce
	N        int    // task number/file index
	Filename string // file for the map task
	NReduce  int    // number of output files for the map task
}

type FinishTaskRequest struct {
	N        int //task number
	Filename string
}

type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
