package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := TaskReplyReq{}
		reply := TaskReplyReq{}

		res := call("Coordinator.RequestTask", &args, &reply)

		if !res {
			break
		}

		switch reply.Task.Task {
		case Map:
			MapTask(&reply, mapf)
		case Reduce:
			ReduceTask(&reply, reducef)
		case Wait:
			time.Sleep(1 * time.Second)
		case Exit:
			os.Exit(0)
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func MapTask(reply *TaskReplyReq, mapf func(string, string) []KeyValue) {
	file, err := os.Open(reply.Task.InputFiles[0])
	if err != nil {
		log.Fatalf("unable to open the given file %v", reply.Task.InputFiles[0])
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("unable to read the contents of the file")
	}

	file.Close()

	kva := mapf(reply.Task.InputFiles[0], string(content))
	intermediate := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % reply.NReduce
		intermediate[r] = append(intermediate[r], kv)
	}

	for r, kva := range intermediate {
		outName := fmt.Sprintf("mr-%d-%d", reply.Task.Index, r)
		outFile, _ := os.CreateTemp("", outName)
		enc := json.NewEncoder(outFile)
		for _, kv := range kva {
			enc.Encode(&kv)
		}
		outFile.Close()
		os.Rename(outFile.Name(), outName)
	}
	reply.Task.Status = Finished
	replyEx := TaskReplyReq{}
	call("Coordinator.NotifyTaskComplete", &reply, &replyEx)
}

func ReduceTask(reply *TaskReplyReq, reducef func(string, []string) string) {

	/*
	*
	* intermediate will store all the key values pairs from all the map output
	* */
	intermediate := []KeyValue{}

	for m := 0; m < len(reply.Task.InputFiles); m++ {
		/*
		*
		* open every file assigned to the reducer
		*
		* */
		file, err := os.Open(reply.Task.InputFiles[m])
		if err != nil {
			log.Fatalf("error opening the file %v", reply.Task.InputFiles[m])
		}
		decodeFile := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := decodeFile.Decode(&kv); err != nil {
				break
			}
			// appending everything into the intermediate fair
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	// sorting the intermediate array using key so that the same key comes together
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	outName := fmt.Sprintf("mr-out-%d", reply.Task.Index)
	outFile, _ := os.CreateTemp("", outName)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		// moving j ahead to find all same keys
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(outFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	outFile.Close()
	os.Rename(outFile.Name(), outName)

	reply.Task.Status = Finished
	replyEx := TaskReplyReq{}
	call("Coordinator.NotifyTaskComplete", &reply, &replyEx)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
