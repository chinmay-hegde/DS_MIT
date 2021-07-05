package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskRequestArgs struct {
}

type TaskRequestReply struct {
	FileName string
	TaskType int // 0-> Map, 1->Reduce
	TaskId int
	TaskUID int
	WorkerStatus int // 0-> TaskReceived, 1 -> Wait&Request, 2->Exit
	NReduce int
}

type TaskCompletionArgs struct {
	Status int // 0 -> Done, 1->Error
	TaskId int 
	TaskType int // 0-> Map, 1->Reduce
	TaskUID int
	FileMap map[int]string // reduce task ID -> tmpFile1234
}

type TaskCompletionReply struct {
	Status int // 0 -> Wait
}


// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	// s := "~/Desktop/Disributed_Systems/Assignments/map_reduce/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
