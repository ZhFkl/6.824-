package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
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
func reportTask(task RequestTaskReply, success bool) {
	args := ReportTaskArgs{
		reportId: task.TaskID,
		success:  success,
	}

	reply := ReportTaskReply{}
	call(
		"Coordinator.ReportTask",
		&args,
		&reply,
	)
}

func atomicWrite(filename string, data []byte) error {
	file, err := os.CreateTemp(".", "mr-tmp-*")
	if err != nil {
		return err
	}

	tempName := file.Name()
	defer os.Remove(tempName)

	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(tempName, filename)
}

func executeMapTask(task RequestTaskReply, mapf func(string, string) []KeyValue) error {
	content, err := os.ReadFile((task.FileName))
	if err != nil {
		return err
	}
	kva := mapf(task.FileName, string(content))
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % task.NReduce
		buckets[reduceID] = append(buckets[reduceID], kv)
	}

	for reduceID, bucket := range buckets {
		data, err := json.Marshal((bucket))
		if err != nil {
			return err
		}
		filename := fmt.Sprintf(
			"mr-%d-%d",
			task.TaskID,
			reduceID,
		)

		if err := atomicWrite(filename, data); err != nil {
			return fmt.Errorf("write %s, %w", filename, err)
		}
	}
	return nil
}

func readIntermediate(filename string) ([]KeyValue, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var kva []KeyValue
	if err := json.Unmarshal(data, &kva); err != nil {
		return nil, err
	}

	return kva, nil

}

func executeReduceTask(task RequestTaskReply, reducef func(string, []string) string) error {
	var intermediate []KeyValue
	reduceID := task.TaskID
	for mapID := 0; mapID < task.NMap; mapID++ {
		filename := fmt.Sprintf("mr-%d-%d", mapID, reduceID)

		kva, err := readIntermediate(filename)
		if err != nil {
			return fmt.Errorf("readIntermediate wrong %s, %w", filename, err)
		}

		intermediate = append(intermediate, kva...)
	}

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	var output bytes.Buffer

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		key := intermediate[i].Key
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		result := reducef(key, values)
		if _, err := fmt.Fprintf(&output, "%s %s\n", key, result); err != nil {
			return err
		}
		i = j
	}
	outputName := fmt.Sprintf("mr-out-%d", reduceID)
	return atomicWrite(outputName, output.Bytes())
}

func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}

		ok := call(
			"Coordinator.RequestTask",
			&args,
			&reply,
		)

		if !ok {
			return
		}
		switch reply.Type {
		case TaskMap:
			err := executeMapTask(reply, mapf)
			reportTask(reply, err == nil)
		case TaskReduce:
			err := executeReduceTask(reply, reducef)
			reportTask(reply, err == nil)

		case TaskWait:
			time.Sleep(500 * time.Millisecond)

		case TaskExit:
			return
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

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
