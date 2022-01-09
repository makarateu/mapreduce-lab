package mapreduce

import (
	"hash/fnv"
	"net/rpc"
	"strconv"

	"github.com/golang/glog"
)

// Data types, utility functions, and RPC request/responses.

const (
	fprefix string = "mrtmp." // Prefix used for all output.
)

// A KeyValue pairs a string key and value in a struct.
type KeyValue struct {
	Key   string
	Value string
}

// mrPhase is the phase of the mapreduce.
type mrPhase int

const (
	mapPhase    mrPhase = 0
	reducePhase mrPhase = 1
)

// ------------ Utility Functions ------------

// Hash function used for partitioning output.
func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

// Returns the name of the intermediate file that a map task produces for a
// reduce task.
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return fprefix + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// Utility function to die if an error is not nil.
func dieIfError(err error) {
	if err != nil {
		glog.Fatalln("Fatal: ", err)
	}
}

// ------------ RPC Request/Responses ------------

// Worker call a Register RPC with a RegisterRequest to register themselves with
// the manager.
type RegisterRequest struct {
	Address string // RPC address of the worker, used by the manager to give out work.
}

// Empty RegisterReply; a non-error RPC means the worker is registered with the
// manager.
type RegisterReply struct {
}

// The manager calls a Shutdown RPC on workers to tell them the MR is over and
// they should shut down.
type ShutdownRequest struct {
}

// Workers reply to shutdown requests with the number of tasks they have worked
// on in their lifetime.
type ShutdownReply struct {
	Tasks int
}

// Manager calls a DoWork RPC on the worker to give it a map or reduce task. The
// RPC blocks until the work is complete.
type WorkRequest struct {
	Phase    mrPhase // Either mapPhase or reducePhase.
	JobName  string  // The name of the mapreduce job.
	TaskId   int     // For map, which mapper this worker is. For reduce, which reducer this worker is.
	File     string  // For map, the input file to read. For reducer, ignored.
	NumOther int     // For map, the total number of reducers. For reduce, the total number of mappers.
}

// The reply is empty; a non-error RPC means the task was successful.
type WorkReply struct {
}

// ------------ Implementation Details Utility Functions ------------

// Function that wraps calling an RPC so that every call site doesn't require
// the same logging. Returns true iff the RPC succeeds.
func call(endpoint string, fn string, req interface{}, rep interface{}) bool {
	c, e := rpc.Dial("unix", endpoint)
	if e != nil {
		glog.Errorln(e)
		return false
	}

	defer c.Close()
	e = c.Call(fn, req, rep)
	if e != nil {
		glog.Errorln(e)
		return false
	}
	return true
}

// mergeName constructs the name of the output file for a reduce task.
func mergeName(jobName string, reduceTask int) string {
	return fprefix + jobName + "-res-" + strconv.Itoa(reduceTask)
}
