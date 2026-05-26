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

type Coordinator struct {
	// Your definitions here

	inputFiles []string
	nReduce    int
	mapTasks   []MapReduceTask
	reduceTask []MapReduceTask

	mapDone    int
	reduceDone int

	allMapComplete    bool
	allReduceComplete bool

	mutex sync.Mutex
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
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inputFiles:        files,
		nReduce:           nReduce,
		mapTasks:          make([]MapReduceTask, len(files)),
		reduceTask:        make([]MapReduceTask, nReduce),
		mapDone:           0,
		reduceDone:        0,
		mutex:             sync.Mutex{},
		allMapComplete:    false,
		allReduceComplete: false,
	}

	for i := range c.mapTasks {
		c.mapTasks[i] = MapReduceTask{
			Task:        Map,
			Status:      Unassigned,
			TimeStamp:   time.Now(),
			Index:       i,
			InputFiles:  []string{files[i]},
			OutputFiles: nil,
		}
	}

	for i := range c.reduceTask {
		c.reduceTask[i] = MapReduceTask{
			Task:        Reduce,
			Status:      Unassigned,
			TimeStamp:   time.Now(),
			Index:       i,
			InputFiles:  generateInputFiles(i, len(files)),
			OutputFiles: []string{fmt.Sprintf("mr-out-%d", i)},
		}
	}

	c.server(sockname)
	return &c
}

func generateInputFiles(i int, file int) []string {
	var inputFiles []string

	for j := 0; j < file; j++ {
		inputFiles = append(inputFiles, fmt.Sprintf("mr-%d-%d", j, i))
	}
	return inputFiles
}
