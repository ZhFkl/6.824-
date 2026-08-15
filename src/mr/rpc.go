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

// ding yi tasktype

type TaskType int

const (
	TaskInvalid TaskType = iota
	TaskMap
	TaskReduce
	TaskWait
	TaskExit
)

// ding yi requesttaskarg
type RequestTaskArgs struct {
	WorkerID int
}

// ding yi requesttaksreply
type RequestTaskReply struct {
	Type     TaskType
	TaskID   int
	FileName string
	nmap     int
	nreduce  int
}

// ding yi report task arg
type ReportTaskArgs struct {
	reportId int
	success  bool
}

// ding yi report task reply
type ReportTaskReply struct {
	Type    TaskType
	TaskID  int
	success bool
}

// Add your RPC definitions here.
