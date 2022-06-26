package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

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

type GetMapTaskArgs struct {
}

type GetMapTaskReply struct {
	File        string
	NumOfReduce int
}

type GetReduceTaskArgs struct {
}

type GetReduceTaskReply struct {
	File string
}

type TaskArgs struct {
}

type TaskReply struct {
	File          string
	Timeout       time.Duration
	TaskType      string
	ReduceTaskNum int
	NumOfReduce   int
}

// 定义超时任务
type TaskTimeoutArgs struct {
	TaskType      string // map or reduce
	File          string // map处理的文件
	ReduceTaskNum int    // reduce处理的任务号
	Result        string // 处理的结果是success or failed
}
type TaskTimeoutReply struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
