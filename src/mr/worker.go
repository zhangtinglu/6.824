package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// for循环 获取任务，知道rpc失败 or rpc返回done
	for {
		reply, err := CallTask()
		if err != nil {
			log.Fatal("RPC Error, Worker Exit")
		}

		if reply.TaskType == "done" {
			fmt.Println("all tasks done")
			os.Exit(0)
		}

		if reply.TaskType == "wait" {
			time.Sleep(time.Second)
			continue
		} else if reply.TaskType == "map" {
			fmt.Printf("[Map] %10s, %d, %d\n", reply.File, reply.NumOfReduce, reply.Timeout)
			ok := execMap(&reply, mapf)
			if ok {
				CallTaskTimeout(&reply, "success")
			} else {
				CallTaskTimeout(&reply, "failed")
			}
		} else if reply.TaskType == "reduce" {
			fmt.Printf("[Reduce] %d, %d, %d\n", reply.ReduceTaskNum, reply.NumOfReduce, reply.Timeout)
			ok := execReduce(&reply, reducef)
			// time.Sleep(1 * time.Second)
			if ok {
				CallTaskTimeout(&reply, "success")
			} else {
				CallTaskTimeout(&reply, "failed")
			}
		} else {
			fmt.Errorf("unknown task type %s", reply.TaskType)
		}
	}

}

func execMap(reply *TaskReply, mapf func(string, string) []KeyValue) bool {
	contents, err := ioutil.ReadFile(reply.File)
	if err != nil {
		log.Fatal("read file failed")
		return false
	}

	kvs := mapf(reply.File, string(contents))

	err = saveFile(kvs, reply.NumOfReduce, reply.File[3:len(reply.File)-4])
	if err != nil {
		log.Fatal("save file failed")
		return false
	}
	return true
}

// TODO 找到对应的文件，每个文件遍历，计算长度
func execReduce(reply *TaskReply, reducef func(string, []string) string) bool {
	var intermediate []KeyValue
	for _, file := range getFilesByNumber(reply.ReduceTaskNum) {
		f, _ := os.Open(file)
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil && err != io.EOF {
				return false
			}
			if err == io.EOF {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reply.ReduceTaskNum)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	return true
}

func getFilesByNumber(num int) []string {
	regString := fmt.Sprintf("mr_out_*_%d", num)
	match, _ := filepath.Glob(regString)
	return match
}

func CallTask() (TaskReply, error) {
	args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.HandleTask", &args, &reply)
	if ok {
		return reply, nil
	} else {
		return reply, fmt.Errorf("failed")
	}
}

func CallTaskTimeout(taskReply *TaskReply, result string) {
	args := TaskTimeoutArgs{}
	args.File = taskReply.File
	args.ReduceTaskNum = taskReply.ReduceTaskNum
	args.Result = result
	args.TaskType = taskReply.TaskType
	reply := TaskTimeoutReply{}
	call("Coordinator.HandleTaskTimeout", &args, &reply)
}

func saveFile(kvs []KeyValue, numOfBuckets int, inputFile string) error {
	buckets := map[int][]KeyValue{}
	for i := 0; i < numOfBuckets; i++ {
		buckets[i] = []KeyValue{}
	}

	for _, kv := range kvs {
		n := ihash(kv.Key) % numOfBuckets
		buckets[n] = append(buckets[n], kv)
	}

	//  对数据进行存储
	for i := 0; i < numOfBuckets; i++ {
		oname := fmt.Sprintf("mr_out_%s_%d", inputFile, i)
		f, err := os.OpenFile(oname, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal(err)
		}

		enc := json.NewEncoder(f)
		for _, kv := range buckets[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	return nil
}

func CallReduceTask() (GetReduceTaskReply, error) {
	args := GetReduceTaskArgs{}
	reply := GetReduceTaskReply{}
	ok := call("Coordinator.HandleReduceTask", &args, &reply)
	if ok {
		fmt.Println(reply.File)
		return reply, nil
	} else {
		return reply, fmt.Errorf("failed")
	}
}

func CallMapTask() (GetMapTaskReply, error) {
	args := GetMapTaskArgs{}
	reply := GetMapTaskReply{}
	ok := call("Coordinator.HandleMapTask", &args, &reply)
	if ok {
		fmt.Println(reply.File, reply.NumOfReduce)
		return reply, nil
	} else {
		return reply, fmt.Errorf("failed")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
