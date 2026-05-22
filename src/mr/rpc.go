package mr

import (
	"os"
	"strconv"
	"time"
)

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

type Task int

const (
	Exit Task = iota
	Wait
	Map
	Reduce
)

type Status int

const (
	Unassigned Status = iota
	Assigned
	Finished
)

type MapReduceTask struct {
	Task      Task
	Status    Status
	TimeStamp time.Time
	Index     int

	InputFiles  []string
	OutputFiles []string
}

type TaskReplyReq struct {
	TaskNum int
	Task    MapReduceTask
	NReduce int
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())

	return s
}
