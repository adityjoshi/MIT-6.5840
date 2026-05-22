package mr

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
