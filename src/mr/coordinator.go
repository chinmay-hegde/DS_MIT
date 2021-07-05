package mr

import "log"
import "fmt"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "math/rand"
import "strconv"
import "sync"

type MapTasks struct {
	TaskUID int
	FileName string
	Status int // 0->pending, 1->running, 2->completed
	StartTime time.Time
}

type ReduceTasks struct {
	TaskUID int
	Status int // 0->pending, 1->running, 2->completed
	StartTime time.Time
}

type Coordinator struct {
	MapSlice []MapTasks
	ReduceSlice []ReduceTasks
	MapTaskCount int
	ReduceTaskCount int
	NReduce int
	TaskLock sync.Mutex 
}
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
	c.TaskLock.Lock()
	if  ((c.ReduceTaskCount == 0) && (c.MapTaskCount==0) ) {
		ret = true
	}
	c.TaskLock.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.MapTaskCount = len(files)
	c.ReduceTaskCount = nReduce
	c.NReduce = nReduce
	// Initialize MapTasks Slice here
	c.MapSlice = make([]MapTasks,c.MapTaskCount)
	for i:=0; i<c.MapTaskCount; i++ {
		c.MapSlice[i].Status = 0
		c.MapSlice[i].FileName = files[i]
	}
	c.ReduceSlice = make([]ReduceTasks,nReduce)
	for i:=0; i<nReduce; i++ {
		c.ReduceSlice[i].Status = 0
	}
	c.server()
	return &c
}

func (c *Coordinator) RequestTask(args *TaskRequestArgs, reply *TaskRequestReply) error {
	fmt.Println("In RequestTask")
	c.TaskLock.Lock()
	fmt.Printf("RequestTask: MapTaskCount-> %v\n", c.MapTaskCount)
	if (c.MapTaskCount > 0){
		fmt.Println("Inside get map task") //DEBUG:
		// Send map task to the worker
		reply.NReduce = c.NReduce
		getMapTask(c.MapSlice,reply)
	} else if(c.ReduceTaskCount > 0){
		// Send Reduce task to the worker
		fmt.Println("Inside get reduce task") //DEBUG:
		getReduceTask(c.ReduceSlice,reply)
	} else{
		// Send exit code to the worker
		reply.WorkerStatus = 2
	}
	c.TaskLock.Unlock()

	return nil
}

func (c *Coordinator) UpdateTask(args *TaskCompletionArgs, reply *TaskCompletionReply) error {
	fmt.Println("Enter UpdateTask")
	if (args.TaskType == 0){
		// Map task completed
		taskId := args.TaskId
		if (args.TaskUID == c.MapSlice[taskId].TaskUID){
			fmt.Println("Valid Worker")
			c.MapSlice[taskId].Status = 2
			c.TaskLock.Lock()
			c.MapTaskCount--
			c.TaskLock.Unlock()
			// Rename Temp files
			for k,v:= range args.FileMap{
				newFilename := "mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(k) + ".json"
				os.Rename(v,newFilename)
			}
		}
	} else{
		// Reduce task completed
		taskId := args.TaskId
		if (args.TaskUID == c.ReduceSlice[taskId].TaskUID){
			c.ReduceSlice[taskId].Status = 2
			c.TaskLock.Lock()
			c.ReduceTaskCount--
			c.TaskLock.Unlock()
			// Rename Temp files
			for k,v:= range args.FileMap{
				newFilename := "mr-out-" + strconv.Itoa(k)
				os.Rename(v,newFilename)
			}
		}

	}

	reply.Status = 0
	fmt.Println("Exit UpdateTask")
	return nil
}

func getMapTask(mapSlice []MapTasks, reply *TaskRequestReply){
	fmt.Println("Enter getMapTask")
	// TODO: Find any Pending or Timedout map task
	mapTaskCount := len(mapSlice)
	for i:=0;i<mapTaskCount;i++ {
		if (mapSlice[i].Status == 0){
			// Send pending map task
			reply.FileName = mapSlice[i].FileName
			reply.TaskType = 0
			reply.TaskId = i
			reply.TaskUID = getTaskUID()
			reply.WorkerStatus = 0
			mapSlice[i].Status = 1
			mapSlice[i].TaskUID = reply.TaskUID
			mapSlice[i].StartTime = time.Now()
			break
		} else if (mapSlice[i].Status == 1){
			//Check if time elapsed is more than 10secs
			timeElapsed := int(time.Since(mapSlice[i].StartTime))/1e9
			if (timeElapsed > 10){
				reply.FileName = mapSlice[i].FileName
				reply.TaskType = 0
				reply.TaskId = i
				reply.TaskUID = getTaskUID()
				reply.WorkerStatus = 0
				mapSlice[i].TaskUID = reply.TaskUID
				mapSlice[i].StartTime = time.Now()
				break
			}
		} else{
			reply.WorkerStatus = 1
		}
	}
	fmt.Println("Exit getMapTask")
}

func getReduceTask(reduceSlice []ReduceTasks, reply *TaskRequestReply){
	fmt.Println("Enter getReduceTask")
	// TODO: Find any Pending or Timedout reduce task
	reduceTaskCount := len(reduceSlice)
	for i:=0;i<reduceTaskCount;i++ {
		if (reduceSlice[i].Status == 0){
			// Send pending reduce task
			reply.FileName = ""
			reply.TaskType = 1
			reply.TaskId = i
			reply.TaskUID = getTaskUID()
			reply.WorkerStatus = 0
			reduceSlice[i].Status = 1
			reduceSlice[i].TaskUID = reply.TaskUID
			reduceSlice[i].StartTime = time.Now()
			break
		} else if (reduceSlice[i].Status == 1){
			//Check if time elapsed is more than 10secs
			timeElapsed := int(time.Since(reduceSlice[i].StartTime))/1e9
			if (timeElapsed > 10){
				reply.FileName = ""
				reply.TaskType = 1
				reply.TaskId = i
				reply.TaskUID = getTaskUID()
				reply.WorkerStatus = 0
				reduceSlice[i].TaskUID = reply.TaskUID
				reduceSlice[i].StartTime = time.Now()
				break
			}
			
		} else{
			reply.WorkerStatus = 1
		}
	}
	fmt.Println("Exit getReduceTask")
}

func getTaskUID() int{
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(1e9)
}