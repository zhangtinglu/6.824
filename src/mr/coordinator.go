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
	// Your definitions here.
	mapTasks         []string // 剩余的map任务数
	mapTasksDoing    []string // 正在执行的map任务数
	m                sync.Mutex
	numOfReduce      int   // 总的reduce数量
	reduceTasks      []int // 剩余的reduce任务数,默认0...9
	reduceTasksDoing []int // 正在执行的reduce任务数
	timeout          time.Duration
	state            bool // 是否所有任务执行状态完成
}

func DeleteFromString(s1 []string, s2 string) []string {
	for i := 0; i < len(s1); i++ {
		if s1[i] == s2 {
			return append(s1[:i], s1[i+1:]...)
		}
	}
	return nil
}

func DeleteFromInt(s1 []int, s2 int) []int {
	for i := 0; i < len(s1); i++ {
		if s1[i] == s2 {
			return append(s1[:i], s1[i+1:]...)
		}
	}
	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandleMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	c.m.Lock()
	if len(c.mapTasks) > 0 {
		reply.File = c.mapTasks[0]
		reply.NumOfReduce = c.numOfReduce
		c.mapTasks = c.mapTasks[1:]
		fmt.Printf("Send Map Task: %s, Left tasks: %d, Reply numOfReduce: %d\n", reply.File, len(c.mapTasks), reply.NumOfReduce)
	}
	c.m.Unlock()
	return nil
}

func (c *Coordinator) HandleReduceTask(args *GetReduceTaskArgs, reply *GetReduceTaskReply) error {
	c.m.Lock()
	if len(c.reduceTasks) > 0 {
		reply.File = fmt.Sprintf("mr-out-%d", c.reduceTasks)
		c.reduceTasks = c.reduceTasks[1:]
		fmt.Printf("Send Task: %s, reduce tasks left: %d\n", reply.File, c.reduceTasks)
	}
	c.m.Unlock()
	return nil
}

func (c *Coordinator) HandleTask(args *TaskArgs, reply *TaskReply) error {
	// 1. 检查map任务, 如果有就直接分配
	// 2. 检查mapDoing任务, 如果有则返回 wait，客户端等所有的map任务执行完成
	// 3. map任务全部执行完成，开始执行reduce任务
	// 4. 如果reduce也执行完成，返回done
	reply.Timeout = c.timeout
	reply.NumOfReduce = c.numOfReduce
	c.m.Lock()
	if len(c.mapTasks) > 0 {
		reply.TaskType = "map"
		reply.File = c.mapTasks[0]
		c.mapTasksDoing = append(c.mapTasksDoing, reply.File)
		c.mapTasks = c.mapTasks[1:]
		fmt.Printf("Send Task: %s, Left tasks: %d\n", reply.File, len(c.mapTasks))
	} else if len(c.mapTasks) == 0 && len(c.mapTasksDoing) > 0 {
		reply.TaskType = "wait"
		fmt.Println("client wait")
	} else if len(c.reduceTasks) > 0 {
		reply.TaskType = "reduce"
		reply.ReduceTaskNum = c.reduceTasks[0]
		c.reduceTasksDoing = append(c.reduceTasksDoing, reply.ReduceTaskNum)
		c.reduceTasks = c.reduceTasks[1:]
		fmt.Printf("Send Reduce Task: %d\n", reply.ReduceTaskNum)
	} else if len(c.reduceTasks) == 0 && len(c.reduceTasksDoing) == 0 {
		reply.TaskType = "done"
		fmt.Println("client done")
		c.state = true
	}
	c.m.Unlock()
	return nil
}

func (c *Coordinator) HandleTaskTimeout(args *TaskTimeoutArgs, reply *TaskTimeoutReply) error {
	// 1. 如果map任务超时，重新添加到待执行，从doing中删除
	// 2. 如果reduce任务超时，同上
	c.m.Lock()
	if args.TaskType == "map" {
		c.mapTasksDoing = DeleteFromString(c.mapTasksDoing, args.File)
		if args.Result == "failed" {
			c.mapTasks = append(c.mapTasks, args.File)
		}
	} else if args.TaskType == "reduce" {
		c.reduceTasksDoing = DeleteFromInt(c.reduceTasksDoing, args.ReduceTaskNum)
		if args.Result == "failed" {
			c.reduceTasks = append(c.reduceTasks, args.ReduceTaskNum)
		}
	}
	c.m.Unlock()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.m.Lock()
	if c.state {
		ret = true
	}
	c.m.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, numOfReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    files,
		numOfReduce: numOfReduce,
	}
	for i := 0; i < numOfReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, i)
	}
	c.server()
	return &c
}
