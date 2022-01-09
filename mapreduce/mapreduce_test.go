package mapreduce

import (
	"flag"
	"fmt"
	"testing"
	"time"

	"bufio"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/golang/glog"
)

func init() {
	testing.Init()
	flag.Parse()
	if testing.Verbose() {
		flag.Set("alsologtostderr", "true")
		flag.Set("v", "5")
	}
	glog.V(1).Infoln("Debug logging enabled.")
}

const (
	nNumber = 100000
	nMap    = 20
	nReduce = 10
)

// Create input file with N numbers
// Check if we have N numbers in output file

// Split in words
func MapFunc(file string, value string) (res []KeyValue) {
	glog.V(8).Infof("Map %v\n", value)
	words := strings.Fields(value)
	for _, w := range words {
		kv := KeyValue{w, ""}
		res = append(res, kv)
	}
	return
}

// Just return key
func ReduceFunc(key string, values []string) string {
	for _, e := range values {
		glog.V(8).Infof("Reduce %s %v\n", key, e)
	}
	return ""
}

// Checks input file agaist output file: each input number should show up
// in the output file in string sorted order
func check(t *testing.T, files []string) {
	output, err := os.Open("mrtmp.test")
	dieIfError(err)
	defer output.Close()

	var lines []string
	for _, f := range files {
		input, err := os.Open(f)
		dieIfError(err)
		defer input.Close()
		inputScanner := bufio.NewScanner(input)
		for inputScanner.Scan() {
			lines = append(lines, inputScanner.Text())
		}
	}

	sort.Strings(lines)

	outputScanner := bufio.NewScanner(output)
	i := 0
	for outputScanner.Scan() {
		var v1 int
		var v2 int
		text := outputScanner.Text()
		n, err := fmt.Sscanf(lines[i], "%d", &v1)
		if n == 1 && err == nil {
			n, err = fmt.Sscanf(text, "%d", &v2)
		}
		if err != nil || v1 != v2 {
			t.Fatalf("line %d: %d != %d err %v\n", i, v1, v2, err)
		}
		i++
	}
	if i != nNumber {
		t.Fatalf("Expected %d lines in output\n", nNumber)
	}
}

// Workers report back how many RPCs they have processed in the Shutdown reply.
// Check that they processed at least 1 DoTask RPC.
func checkWorker(t *testing.T, l []int) {
	for _, tasks := range l {
		if tasks == 0 {
			t.Fatalf("A worker didn't do any work\n")
		}
	}
}

// Make input file
func makeInputs(num int) []string {
	var names []string
	var i = 0
	for f := 0; f < num; f++ {
		names = append(names, fmt.Sprintf("dist-mrinput-%d.txt", f))
		file, err := os.Create(names[f])
		if err != nil {
			log.Fatal("mkInput: ", err)
		}
		w := bufio.NewWriter(file)
		for i < (f+1)*(nNumber/num) {
			fmt.Fprintf(w, "%d\n", i)
			i++
		}
		w.Flush()
		file.Close()
	}
	return names
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp. can't use current directory since
// AFS doesn't support UNIX-domain sockets.
func port(suffix string) string {
	s := "/var/tmp/dist-class-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

func setup() *Manager {
	files := makeInputs(nMap)
	address := port("manager")
	spec := MapReduceSpec{
		JobName:  "test",
		Files:    files,
		R:        nReduce,
		MapFn:    MapFunc,
		ReduceFn: ReduceFunc}
	mr := MapReduce(address, spec)
	return mr
}

func setupLocal(m int, r int) *Manager {
	f := makeInputs(m)
	spec := MapReduceSpec{
		JobName:  "test",
		Files:    f,
		R:        r,
		MapFn:    MapFunc,
		ReduceFn: ReduceFunc}
	mr := LocalMapReduce(spec)
	return mr
}

func cleanup(mr *Manager) {
	mr.Cleanup()
	for _, f := range mr.spec.Files {
		e := os.Remove(f)
		if e != nil {
			fmt.Printf("Error removing %s: %s\n", f, e)
		}
	}
}

func TestSequentialSingle(t *testing.T) {
	mr := setupLocal(1, 1)
	counters := mr.Wait().Counters
	check(t, mr.spec.Files)
	checkWorker(t, counters)
	cleanup(mr)
}

func TestSequentialMany(t *testing.T) {
	mr := setupLocal(5, 3)
	counters := mr.Wait().Counters
	check(t, mr.spec.Files)
	checkWorker(t, counters)
	cleanup(mr)
}

func TestParallelBasic(t *testing.T) {
	mr := setup()
	for i := 0; i < 2; i++ {
		go RunWorker(mr.address, port("worker"+strconv.Itoa(i)),
			WorkerConfig{MapFunc, ReduceFunc}, -1, nil)
	}
	counters := mr.Wait().Counters
	check(t, mr.spec.Files)
	checkWorker(t, counters)
	cleanup(mr)
}

func TestParallelCheck(t *testing.T) {
	mr := setup()
	s := &Stats{}
	for i := 0; i < 2; i++ {
		go RunWorker(mr.address, port("worker"+strconv.Itoa(i)),
			WorkerConfig{MapFunc, ReduceFunc}, -1, s)
	}
	counters := mr.Wait().Counters
	check(t, mr.spec.Files)
	checkWorker(t, counters)

	s.Lock()
	if s.max < 2 {
		t.Fatalf("workers did not execute in parallel")
	}
	s.Unlock()

	cleanup(mr)
}

func TestOneFailure(t *testing.T) {
	mr := setup()
	// Start 2 workers that fail after 10 tasks
	go RunWorker(mr.address, port("worker"+strconv.Itoa(0)),
		WorkerConfig{MapFunc, ReduceFunc}, 10, nil)
	go RunWorker(mr.address, port("worker"+strconv.Itoa(1)),
		WorkerConfig{MapFunc, ReduceFunc}, -1, nil)
	counters := mr.Wait().Counters
	check(t, mr.spec.Files)
	checkWorker(t, counters)
	cleanup(mr)
}

func TestManyFailures(t *testing.T) {
	mr := setup()
	i := 0
	done := false
	for !done {
		select {
		case done = <-mr.done:
			check(t, mr.spec.Files)
			cleanup(mr)
			break
		default:
			// Start 2 workers each sec. The workers fail after 10 tasks
			w := port("worker" + strconv.Itoa(i))
			go RunWorker(mr.address, w, WorkerConfig{MapFunc, ReduceFunc}, 10, nil)
			i++
			w = port("worker" + strconv.Itoa(i))
			go RunWorker(mr.address, w, WorkerConfig{MapFunc, ReduceFunc}, 10, nil)
			i++
			time.Sleep(1 * time.Second)
		}
	}
}
