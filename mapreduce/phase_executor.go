package mapreduce

import (
	"fmt"

	"github.com/golang/glog"
)

// Structure representing a phase's job.
type phaseJob struct {
	JobName     string      // Name of the parent job.
	MapFiles    []string    // List of all map files.
	NumReduce   int         // Number of reducers.
	Phase       mrPhase     // The current phase.
	WorkerQueue chan string // A queue of workers populated by the manager.
	TaskQueue   chan int    // A queue of tasks.
}

// Returns how many tasks there are for this phase, and how many tasks there are
// for the other phase. If the phase is map, ntasks will be M, and numOther will
// be R. If the phase is reduce, ntasks will be R, and numOther will be M.
func nwork(phase mrPhase, spec MapReduceSpec) (ntasks, numOther int) {
	switch phase {
	case mapPhase:
		ntasks = len(spec.Files)
		numOther = spec.R
	case reducePhase:
		ntasks = spec.R
		numOther = len(spec.Files)
	}
	return
}

func executePhase(
	spec MapReduceSpec, // spec of the MapReduce.
	phase mrPhase, // current phase.
	registerChan chan string, // channel for worker registration.
) {
	// executePhase() starts the tasks for a given phase (mapPhse or reducePhase)
	// and waits for all tasks to complete.
	// 	- mapFiles is the names of file that are inputs to the mappers, one
	// 		for each map task.
	// 	- nReduce is the number of reduce tasks.
	// 	- registerChan is a stream of registered workers. Each element in the
	// 	  channel is a worker's RPC address, which can be passed to call().
	// 		registerChan will yield all currently-registered workers and any
	// 		new ones as they appear.

	// executePhase's obligation is: given n tasks, schedule these tasks on
	// workers.

	ntasks, numOther := nwork(phase, spec)
	glog.V(1).Infof("Executing Phase %v %v tasks (%d I/Os)", phase, ntasks, numOther)
	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, executePhase() should return.

	// Create task queue and work queue. Populate task queue.
	parallelism := 100 // Max parallelism of 100; in practice this is very tiny.
	wq := make(chan string, parallelism)
	tq := make(chan int, ntasks)
	for i := 0; i < ntasks; i++ {
		tq <- i
	}

	// Start forwarding worker registration to the worker queue.
	done := make(chan bool)
	go forward(wq, registerChan, done)

	// Start matching workers to tasks.
	match(phaseJob{
		JobName:     spec.JobName,
		MapFiles:    spec.Files,
		NumReduce:   spec.R,
		Phase:       phase,
		TaskQueue:   tq,
		WorkerQueue: wq})

	done <- true // shut down forwarding when match returns.
	glog.V(1).Infof("Executing Phase %v done", phase)
}

func match(j phaseJob) {
	// Schedule all tasks from the task queue to workers from the worker queue.
	// TODO: Implement this function.
}

// Forwards the worker registration channel to a worker queue.
func forward(a chan string, r chan string, done chan bool) {
	for {
		select {
		case w := <-r:
			fmt.Println("New worker!", w)
			a <- w
		case <-done:
			return
		}
	}
}

// Calls DoWork on the given worker to execute a map task. Returns true if the
// RPC completed successfully.
func call_map(
	j string, // The mapreduce job.
	w string, // The worker to call DoWork on.
	f string, // The file for the worker to use.
	t int, // The task id of the map task.
	n int, // The number of reducers.
) (success bool) {
	req := WorkRequest{
		Phase:    mapPhase,
		JobName:  j,
		TaskId:   t,
		File:     f,
		NumOther: n}
	var reply WorkReply
	success = call(w, "Worker.DoWork", &req, &reply)
	glog.V(1).Infof("Worker %s done with map task %d", w, t)
	return
}

// Calls DoWork on the given worker to execute a reduce task. Returns true if
// the RPC completed successfully.
func call_reduce(
	j string, // The mapreduce job.
	w string, // The worker to call DoWork on.
	t int, // The reduce task id.
	n int, // The number of mappers.
) (success bool) {
	req := WorkRequest{
		Phase:    reducePhase,
		JobName:  j,
		TaskId:   t,
		NumOther: n}
	var reply WorkReply
	success = call(w, "Worker.DoWork", &req, &reply)
	glog.V(1).Infof("Worker %s done with reduce task %d", w, t)
	return
}
