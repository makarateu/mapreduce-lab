package mapreduce

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sort"
	"sync"

	"github.com/golang/glog"
)

// Spec of the MapReduce
type MapReduceSpec struct {
	JobName  string                          // Name of the job.
	Files    []string                        // Input files.
	R        int                             // Number of reduce shards.
	MapFn    func(string, string) []KeyValue // Map function.
	ReduceFn func(string, []string) string   // Reduce function.
}

// Counters from the mapreduce. For this lab, this is just a list of ints--the
// number of tasks executed by each worker.
type MapReduceResult struct {
	Counters []int
}

// The MapReduce Manager.
type Manager struct {
	sync.Mutex            // Make the Manager lock-able.
	cond       *sync.Cond // Must use the Manager mutex as its Locker.

	// Spec of the MapReduce
	spec MapReduceSpec

	address  string   // The mnager's address.
	workers  []string // List of worker RPC addresses.
	counters []int    // Worker counters.

	done chan bool    // Done channel.
	quit chan bool    // Listen for shutdown (kill).
	l    net.Listener // Listener for RPCs.
}

// Construct a new mnager that will listen on the specified address; private
// helper function.
func manager(address string, spec MapReduceSpec) (m *Manager) {
	m = &Manager{
		spec:    spec,
		address: address,
		done:    make(chan bool),
		quit:    make(chan bool)}
	m.cond = sync.NewCond(m)
	return
}

// Run a local mapreduce with the given specification. Stages will be run
// sequentially with no parallelism.
func LocalMapReduce(spec MapReduceSpec) (m *Manager) {
	m = manager("manager", spec)
	go m.mr(func(phase mrPhase) {
		switch phase {
		case mapPhase:
			for i, f := range m.spec.Files {
				// Call the mapper for each file.
				mapper(m.spec.JobName, i, f, m.spec.R, m.spec.MapFn)
			}
		case reducePhase:
			for i := 0; i < m.spec.R; i++ {
				// Call the reducer for each reduce task.
				reducer(m.spec.JobName, i, mergeName(m.spec.JobName, i), len(m.spec.Files), m.spec.ReduceFn)
			}
		}
	}, func() {
		m.merge()
		m.counters = []int{len(m.spec.Files) + m.spec.R}
	})
	return
}

// Run a distributed mapreduce. The manager will listen on the given address for
// workers to register.
func MapReduce(address string, spec MapReduceSpec) (m *Manager) {
	flag.Parse()
	m = manager(address, spec)
	m.start() // Begin listening for RPCs and forwarding them to the task scheduler.
	go m.mr(
		func(phase mrPhase) {
			c := make(chan string)
			go m.forward(c)                // Forward worker registrations to the task scheduler.
			executePhase(m.spec, phase, c) // Run the task scheduler.
		},
		func() {
			m.merge()
			m.counters = m.shutdownWorkers()
			m.stop()
		})
	return
}

// Waits for a mapreduce to complete. Returns the counters.
func (m *Manager) Wait() (r MapReduceResult) {
	_ = <-m.done
	r.Counters = m.counters
	return
}

// Run a mapreduce. The scheduler function should be capable of scheduling a
// map/reduce phase. It will be called twice: once to schedule the map stage and
// once to schedule the reduce phase. The done function is called once after the
// reduce phase is complete.
func (m *Manager) mr(scheduler func(phase mrPhase), done func()) {
	glog.Infof("Starting MapReduce job: %s", m.spec.JobName)

	scheduler(mapPhase)
	scheduler(reducePhase)
	done()

	glog.Infof("MapReduce %s done.", m.spec.JobName)

	m.done <- true
}

// RPC called by workers when registering to work.
func (m *Manager) Register(req *RegisterRequest, rep *RegisterReply) (err error) {
	m.Lock()
	defer m.Unlock()

	glog.V(1).Infof("Registering worker at %s", req.Address)
	// Add worker to list of pending workers.
	m.workers = append(m.workers, req.Address)
	// Notify that a worker has arrived.
	m.cond.Broadcast()
	return
}

// Function that forwards worker registrations to the scheduler.
func (m *Manager) forward(wc chan string) {
	i := 0
	for {
		m.Lock()
		if len(m.workers) > i {
			// There are workers that can be sent to scheduler.
			w := m.workers[i]
			go func() { wc <- w }() // Forward worker outside the lock.
			i = i + 1
		} else {
			// Wait for a new worker to register. Will be woken by a call
			// to Brodcast.
			m.cond.Wait()
		}
		m.Unlock()
	}
}

// ------ RPC based Manager -------

// Makes the manager start listening on its provided address.
func (m *Manager) start() {
	r := rpc.NewServer()
	r.Register(m)
	os.Remove(m.address)
	l, e := net.Listen("unix", m.address)
	dieIfError(e)
	m.l = l

	// Serve on separate thread.
	go func() {
	loop:
		for {
			select {
			case <-m.quit:
				break loop
			default:
			}
			c, e := m.l.Accept()
			if e != nil {
				glog.V(2).Infoln("Manager accept error: ", e)
				break
			}
			go func() {
				r.ServeConn(c)
				c.Close()
			}()
		}
		glog.V(2).Infoln("Manager done.")
	}()
}

// Stops and shuts down the manager. Exposed as an RPC.
func (m *Manager) Shutdown(req *ShutdownRequest, rep *ShutdownReply) (err error) {
	glog.V(1).Infof("Shutting down Manager for %s", m.spec.JobName)
	close(m.quit)
	m.l.Close()
	return nil
}

// Stops a listening manager by issuing a Shutdown rpc.
func (m *Manager) stop() {
	var reply ShutdownReply
	if ok := call(m.address, "Manager.Shutdown", &ShutdownRequest{}, &reply); !ok {
		glog.Errorf("Error on shutdown for %s", m.spec.JobName)
	}
	glog.V(2).Infof("Manager for %s shut down.", m.spec.JobName)
}

// Issues a shutdown RPC to workers, which should cause them to exit.
func (m *Manager) shutdownWorkers() (counters []int) {
	m.Lock()
	defer m.Unlock()
	counters = make([]int, 0, len(m.workers))
	for _, w := range m.workers {
		glog.V(2).Infof("Shutting down worker %s...", w)
		var reply ShutdownReply
		if ok := call(w, "Worker.Shutdown", &ShutdownRequest{}, &reply); !ok {
			glog.Errorf("Could not shut down worker %s", w)
		} else {
			counters = append(counters, reply.Tasks)
		}
	}
	return
}

// ------- Utility Functions to Merge Output -------

// Merges output from all reducer shards into a single file. This is convenient
// for debugging.
func (m *Manager) merge() {
	glog.V(1).Infof("Merging output for %s", m.spec.JobName)
	kvs := make(map[string]string)
	var keys []string
	for i := 0; i < m.spec.R; i++ {
		fn := mergeName(m.spec.JobName, i)
		glog.Infof("Merging %s", fn)
		f, e := os.Open(fn)
		dieIfError(e)
		d := json.NewDecoder(f)
		var kv KeyValue
		for {
			e = d.Decode(&kv)
			if e != nil {
				break
			}
			kvs[kv.Key] = kv.Value
			keys = append(keys, kv.Key)
		}
		f.Close()
	}
	sort.Strings(keys)

	of, e := os.Create(fprefix + m.spec.JobName)
	dieIfError(e)
	w := bufio.NewWriter(of)
	for _, k := range keys {
		fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
	}
	w.Flush()
	of.Close()
}

// Remove all temporary files.
func (m *Manager) Cleanup() {
	for i := range m.spec.Files {
		for j := 0; j < m.spec.R; j++ {
			e := os.Remove(reduceName(m.spec.JobName, i, j))
			dieIfError(e)
		}
	}
	for i := 0; i < m.spec.R; i++ {
		e := os.Remove(mergeName(m.spec.JobName, i))
		dieIfError(e)
	}
	e := os.Remove(fprefix + m.spec.JobName)
	dieIfError(e)
}
