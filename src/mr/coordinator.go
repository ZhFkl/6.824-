package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// zhe li hai yao ding yi task de jie gou lei xing
type TaskState int

const (
	Idle TaskState = iota
	Running
	Finished
)

type Task struct {
	ID        int
	Filename  string
	State     TaskState
	StartTime time.Time
}

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	DonePhase
)

type Coordinator struct {
	// Your definitions here.
	// zhe li hai yao ding yi xian zai de jie duan zhuang tai
	// zhe li yao you dong xi
	mu    sync.Mutex
	phase Phase
	// map task de shu zu
	mapTasks    []Task
	reduceTasks []Task
	nmap        int
	nreduce     int
	// reduce task de shu zu
	//

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
		phase:       MapPhase,
		nmap:        len(files),
		nreduce:     nReduce,
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, len(files)),
	}
	// wan cheng ci shi de chu shi hua
	for id, filename := range files {
		c.mapTasks[id] = Task{
			ID:       id,
			Filename: filename,
			State:    Idle,
		}
	}

	for id := 0; id < nReduce; id++ {
		c.reduceTasks[id] = Task{
			ID:    id,
			State: Idle,
		}
	}
	// Your code here.

	c.server(sockname)
	return &c
}
