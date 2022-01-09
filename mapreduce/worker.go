package mapreduce

import (
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/golang/glog"
)

// Implementation of a MapReduce worker. Clients should call RunWorker to start
// a worker. RunWorker blocks until the worker is done.

// Stats is shared among workers and computes how many workers are active in
// parallel. Used only for testing.
type Stats struct {
	sync.Mutex
	curr int
	max  int
}

// Add one concurrent worker. Returns immediately if there is another worker
// running a task. Otherwise waits for about a second to let other workers
// arrive. Used to ensure that there are parallel workers in short tests with
// small amounts of data.
func (s *Stats) add() {
	s.Lock()
	s.curr += 1
	if s.curr > s.max {
		s.max = s.curr
	}

	if s.max > 1 {
		// There is parallelism; return.
		s.Unlock()
		return
	}
	// Sleep for a second to let other workers arrive.
	s.Unlock()
	time.Sleep(time.Second)
}

// Subtracts one concurrent worker.
func (s *Stats) sub() {
	s.Lock()
	defer s.Unlock()
	s.curr -= 1
}

// Generic config for a worker: pointers to its map/reduce fns.
type WorkerConfig struct {
	MapFn    func(string, string) []KeyValue
	ReduceFn func(string, []string) string
}

// Worker class.
type Worker struct {
	sync.Mutex

	id     string       // ID of this worker.
	config WorkerConfig // Static worker config.

	tasks int // Count of tasks this worker has executed.

	failThreshold int  // Worker will fail after servicing this may RPCs.
	active        bool // Whether the worker has an active task.

	l    net.Listener
	s    *Stats    // Concurrency stats; may be null.
	done chan bool // Workers sets to true when complete.
}

// DoWork RPC implementation. Runs a map or reduce task, as specified in the
// WorkRequest. Returns when the task is done.
func (w *Worker) DoWork(req *WorkRequest, rep *WorkReply) error {
	glog.V(1).Infof("Worker %s: phase %v, task %d, on %s with I/Os %d",
		w.id, req.Phase, req.TaskId, req.File, req.NumOther)

	w.Lock()
	w.tasks += 1
	a := w.active
	w.active = true
	w.Unlock()

	if a {
		glog.Fatalf("Worker %s has more than one task.", w.id)
	}

	if w.s != nil {
		w.s.add()
	}

	switch req.Phase {
	case mapPhase:
		mapper(req.JobName, req.TaskId, req.File, req.NumOther, w.config.MapFn)
	case reducePhase:
		reducer(req.JobName, req.TaskId, mergeName(req.JobName, req.TaskId), req.NumOther, w.config.ReduceFn)
	}

	w.Lock()
	w.active = false
	w.Unlock()

	if w.s != nil {
		w.s.sub()
	}

	glog.V(1).Infof("Worker %s: phase %v, task %d, on %s done",
		w.id, req.Phase, req.TaskId, req.File)
	return nil
}

// Shutdown RPC. Called by the manager to shutdown the worker.
func (w *Worker) Shutdown(req *ShutdownRequest, rep *ShutdownReply) error {
	glog.V(2).Infof("Worker %s shutting down", w.id)
	w.Lock()
	defer w.Unlock()
	rep.Tasks = w.tasks
	w.done <- true
	return nil
}

// Registers a worker with the manager. Called once, at start up.
func (w *Worker) register(manager string) {
	req := &RegisterRequest{Address: w.id}
	reply := &RegisterReply{}
	ok := call(manager, "Manager.Register", req, reply)
	if !ok {
		glog.Errorf("Failed to register %s", w.id)
	}
}

// Runs a worker.
func RunWorker(
	manager string, // RPC address of manager to talk to.
	id string, // Id of this worker.
	config WorkerConfig, // Static config of the worker.
	failThreshold int, // How many work items this worker may work on before failing. Used only in tests.
	stats *Stats, // Pointer to a parallelism stats object in tests; shared between in-process workers.
) {
	glog.V(1).Infof("Starting worker %s with manager %s", id, manager)
	done := make(chan bool)
	w := &Worker{
		id:            id,
		config:        config,
		done:          done,
		failThreshold: failThreshold,
		s:             stats}
	r := rpc.NewServer()
	r.Register(w)
	os.Remove(id)
	l, e := net.Listen("unix", id)
	dieIfError(e)
	w.l = l
	w.register(manager)

	go func() {
		_ = <-done
		w.l.Close()
	}()

	// Enter run loop
	for {
		w.Lock()
		if w.failThreshold == 0 {
			w.Unlock()
			w.l.Close()
			break
		}
		w.Unlock()
		c, e := w.l.Accept()
		if e != nil {
			break
		}
		w.Lock()
		w.failThreshold--
		w.Unlock()
		go r.ServeConn(c)
	}
	glog.V(1).Infof("Worker %s done", id)
}
