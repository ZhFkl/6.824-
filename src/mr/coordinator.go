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
	nMapDone    int
	nReduceDone int
	// reduce task de shu zu
	//

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == DonePhase
}

// han shu RequestTask
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// pan duan ci shi you mei you sheng yu de task
	c.mu.Lock()
	defer c.mu.Unlock()

	// ci shi na shang suo , wei shen me yao you suo ? yin wei ci shi ru guo kai le hen duo ge workder
	// xian cheng na me ci shi dui yi done de zhe ge task de num jiu shi yige jing zheng guan xi
	switch c.phase {
	// pan duan zhe li shi map phase hai shi reduce phase

	case MapPhase:
		// ru guo ci shi shi mappahse zen me ban ?
		// shou xian shi pan duan ci shi yao bu yao qie huan zhuang tai
		// shen me jiao zuo qie huan zhuang tai jiu shi cong ci shi de map phase ran hou
		// qie huan cheng ci shid e reduce zhuang tai

		// done == nmapnum jiu shi qie huan
		for i := range c.mapTasks {
			if c.mapTasks[i].State == Idle {
				c.mapTasks[i].State = Running
				c.mapTasks[i].StartTime = time.Now()
				reply.Type = TaskMap
				reply.TaskID = c.mapTasks[i].ID
				reply.FileName = c.mapTasks[i].Filename
				reply.NMap = c.nmap
				reply.NReduce = c.nreduce
				//fmt.Printf("分配了第%d 个map个任务 \n",reply.TaskID)
				return nil
			}
		}

		if c.nMapDone == c.nmap {
			c.phase = ReducePhase
			//log.Printf("进入到Reduce阶段\n")
		} else {
			reply.Type = TaskWait
			return nil
		}
		fallthrough

	case ReducePhase:
		// zhao dao kong xian de reduce ren wu
		for i := range c.reduceTasks {
			if c.reduceTasks[i].State == Idle {
				c.reduceTasks[i].State = Running
				c.reduceTasks[i].StartTime = time.Now()
				reply.Type = TaskReduce
				reply.TaskID = c.reduceTasks[i].ID
				reply.FileName = "" // reduce 任务不需要特定输入文件
				reply.NMap = c.nmap
				reply.NReduce = c.nreduce
				//fmt.Printf("分配了第%d 个reduce个任务 \n",reply.TaskID)
				return nil
			}
		}
		// ru guo mei zhao dao na me ci shi jiu dai biao le suo you ren wu yi jing fen pei wan cheng
		// zhi hou jiu dneg dai jiu xinig le
		if c.nReduceDone == c.nreduce {
			c.phase = DonePhase
			reply.Type = TaskExit
			return nil
		}
		// 还有 reduce 在运行，让 worker 等待
		reply.Type = TaskWait
		return nil

	case DonePhase:
		reply.Type = TaskExit
		return nil

	}
	return nil
}

// func ReportTask

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	// ci shi shi worker lai hui bao ci shi de task you mei you wan cheng
	// tong guo reporttask reportreply
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Type == TaskMap {

		if args.ReportId >= 0 && args.ReportId < c.nmap && c.mapTasks[args.ReportId].State == Running {
			if args.Success {
				c.mapTasks[args.ReportId].State = Finished
				c.nMapDone++
				//log.Printf("完成了一个第 %d 个map 任务\n",c.nMapDone)
			} else {
				c.mapTasks[args.ReportId].State = Idle
			}
		}

	} else if args.Type == TaskReduce {

		if args.ReportId >= 0 && args.ReportId < c.nreduce && c.reduceTasks[args.ReportId].State == Running {
			if args.Success {
				c.reduceTasks[args.ReportId].State = Finished
				c.nReduceDone++
				//log.Printf("完成了一个第 %d 个Reduce任务\n",c.nReduceDone)
			} else {
				c.reduceTasks[args.ReportId].State = Idle
			}
		}
	}

	return nil
}

// func to check if the task if overtime

func (c *Coordinator) checkTimeout() {
	for {
		time.Sleep(5 * time.Second)
		c.mu.Lock()
		now := time.Now()

		if c.phase == MapPhase || c.phase == ReducePhase {
			tasks := c.mapTasks
			if c.phase == ReducePhase {
				tasks = c.reduceTasks
			}

			for i := range tasks {
				if tasks[i].State == Running && now.Sub(tasks[i].StartTime) > 10*time.Second {
					tasks[i].State = Idle
				}
			}
		}

		c.mu.Unlock()
	}
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
		reduceTasks: make([]Task, nReduce),
		nMapDone:    0,
		nReduceDone: 0,
	}

	log.Printf("finish the coordinator make\n")
	// wan cheng ci shi de chu shi hua
	for id, filename := range files {
		c.mapTasks[id] = Task{
			ID:       id,
			Filename: filename,
			State:    Idle,
		}
	}
	log.Printf("finish the maptask init\n")
	for id := 0; id < nReduce; id++ {
		c.reduceTasks[id] = Task{
			ID:    id,
			State: Idle,
		}
	}
	////log.Printf("finish the reducetask init\n")
	fmt.Printf("一共有 %d 个map 任务， 有 %d 个reduce 任务\n", c.nmap, c.nreduce)
	// Your code here.
	go c.checkTimeout()
	c.server(sockname)
	return &c
}
