package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"os"
	"io/ioutil"
	"encoding/json"
	"strings"
	"strconv"
	"time"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type SliceMap []map[string]string

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for ;; {
		TaskReply:= GetTask()
		fmt.Println(TaskReply.WorkerStatus)
		if(TaskReply.WorkerStatus == 2) {
			break
		} else if(TaskReply.WorkerStatus == 1) {
			time.Sleep(time.Millisecond * 100)
		} else {
			var completionArgs TaskCompletionArgs = TaskCompletionArgs{}
			completionArgs.Status = 0
			completionArgs.TaskType = TaskReply.TaskType
			completionArgs.TaskUID = TaskReply.TaskUID
			completionArgs.TaskId = TaskReply.TaskId
			if(TaskReply.TaskType == 0) {
				fileMapOut:= ProcessMap(TaskReply.FileName, TaskReply.NReduce, mapf)
				completionArgs.FileMap = fileMapOut
			} else {
				fileMapOut:= ProcessReduce(TaskReply.TaskId, reducef)
				completionArgs.FileMap = fileMapOut
			}
			//update task status
			CompleteTask(completionArgs)
			time.Sleep(time.Second)
		}		
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

func GetTask() TaskRequestReply {

	// declare an argument structure.
	args := TaskRequestArgs{}

	// fill in the argument(s).

	// declare a reply structure.
	reply := TaskRequestReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.RequestTask", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.fileName %s\n", reply.FileName)
	return reply
}

func CompleteTask(args TaskCompletionArgs) {

	// fill in the argument(s).

	// declare a reply structure.
	reply := TaskCompletionReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.UpdateTask", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Status %v\n", reply.Status)
}

func ProcessMap(FileName string, nReduce int, mapf func(string, string) []KeyValue) map[int]string {
	fmt.Println("Enter ProcessMap")
	FileMap:= make(map[int]string)
	ContentMap:= make(map[int]SliceMap)
	TempFileMap:= make(map[int]*os.File)
	//process map
	file, err := os.Open(FileName)
	if err != nil {
		log.Fatalf("cannot open %v", FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", FileName)
	}
	file.Close()
	mapOut := mapf(FileName, string(content))
	for i:=0; i< len(mapOut); i++ {
		reduceTask := ihash(mapOut[i].Key) % nReduce
		tmpFileName, ok := FileMap[reduceTask]
		if (!ok) {
			//create temp file
			tmpfile, _ := ioutil.TempFile(".", "tempFile_",)
			defer tmpfile.Close()
			os.Chmod(tmpfile.Name(),0777)
			tmpFileName = tmpfile.Name()
			FileMap[reduceTask] = tmpFileName
			//create content map key
			ContentMap[reduceTask] = make(SliceMap, 0)
			TempFileMap[reduceTask] = tmpfile
		}
		//update content map for reduce task id
		tmpMap := map[string]string {mapOut[i].Key: mapOut[i].Value}
		ContentMap[reduceTask] = append(ContentMap[reduceTask], tmpMap)		
	}

	//write the content to file for each reduced task
	for k, v:= range ContentMap {
		// fmt.Println(k)
		// fmt.Println(v)
		WriteToFile(TempFileMap[k], v)
	}
	fmt.Println("Exit ProcessMap")
	return FileMap
}

func ProcessReduce(ReduceID int, reducef func(string, []string) string) map[int]string {
	fmt.Println("Enter ProcessReduce")
	FileMap:= make(map[int]string)
	var kva []KeyValue
	//get all files in cur directory
	curFiles, _ := ioutil.ReadDir(".")
	for _, f := range(curFiles) {
		reduceFilePattern := strconv.Itoa(ReduceID) + ".json"
		if(strings.Contains(f.Name(), "mr-")) {
			//file matches with mr- pattern, check for reduce task ID in the file
			if(strings.Contains(f.Name(), reduceFilePattern)) {
				//reduce file found with a reduce task ID
				reduceFileName := f.Name()
				fmt.Println(reduceFileName) //DEBUG:
				file, _ := os.Open(reduceFileName)
				dec := json.NewDecoder(file)
				for {
					var kv map[string]string
					if err := dec.Decode(&kv); err != nil {
						break
					}
					var key string
					for k := range kv {
						key = k 	
					}
					kva = append(kva, KeyValue{Key: key, Value:kv[key]})
				}
				file.Close()
			}
		}
	}
	//sort key values
	sort.Slice(kva, 
	func(i, j int) bool {			
		return kva[i].Key < kva[j].Key 
	})
	// call reduce for each key
	tmpFileName := CallReduceForKey(kva, reducef)
	FileMap[ReduceID] = tmpFileName
	fmt.Println("Exit ProcessReduce")
	return FileMap
}

func WriteToFile(file *os.File, tmpMap []map[string]string) {
	fmt.Println("Enter WriteToFile")
	enc := json.NewEncoder(file)
	for _,kv:= range tmpMap {
		enc.Encode(&kv)
	}
	fmt.Println("Exit WriteToFile")
}

func CallReduceForKey(kva []KeyValue, reducef func(string, []string) string) string {
	i := 0
	tmpMap:= make([]KeyValue, 0)
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		tmpMap = append(tmpMap, KeyValue{Key : kva[i].Key, Value: output})
		i = j
	}
	tmpfile, _ := ioutil.TempFile(".", "tempFile_",)
	defer tmpfile.Close()
	os.Chmod(tmpfile.Name(),0777)
	tmpFileName := tmpfile.Name()
	//WriteToFile(tmpfile, tmpMap)
	for _,kv:= range tmpMap {
		fmt.Fprintf(tmpfile, "%v %v\n", kv.Key, kv.Value)
	}
	return tmpFileName
	
}