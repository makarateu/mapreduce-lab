package mapreduce

import (
	"github.com/golang/glog"
)

func mapper(
	jobName string, // Name of the MapReduce job.
	mapTask int, // Id of this map task.
	inFile string, // Input file for this map task.
	r int, // Number of reducers (R in the paper).
	mapFn func(filename string, contents string) []KeyValue,
) {

	// mapper runs a map task:
	// 	1. Read an input shard/file (inFile).
	// 	2. Call the mapFn on the input.
	// 	3. Shuffle the mapFn's output into r files.
	//
	// The output files from a map task should be named according to the map task
	// that produced the output and the reduce task that will consume it. In
	// api.go, the function reduceName(jobName, mapTask, r) will produce the
	// correct file name for the reduce task r.
	//
	// Go's ioutil provides functions for reading/writing files. For example, you
	// can read the contents of a file to a slice of bytes with the following:
	//
	//   contents, err := ioutil.ReadFile(filename)
	//
	// The output of a mapFn is a KeyValue pair. In order to shuffle the key to
	// the correct reduce task, call ihash() on each key and mod the result by
	// the number of reduce tasks. The resulting value is the id of the reduce
	// task that should receive the result.
	//
	// 	reducerId := ihash(kv.Key) % r
	//
	// The output files should be formatted as JSON for both the map and reduce
	// phase. You can write a struct to JSON by creating a JSON encoder:
	//
	//	outputFile = os.Create(filename)
	// 	encoder := json.NewEncoder(outputFile)
	// 	err := encoder.Encode(&myStruct)
	//
	// Since you will have r output files, you will likely have r encoders.
	//
	// Remember to close your files after you have finished writing.

	glog.V(1).Infof("Mapper %d for %s processing %s for %d shards",
		mapTask, jobName, inFile, r)

	// Your code here (Part 1.1).

	// 1. Create output files (remember to close them).
	// 2. Read input file and apply mapFn to input file.
	// 3. Create r output files and corresponding JSON encoders.
	// 4. Write results of mapFn with encoders.

	glog.V(1).Infof("Mapper %d for %s done with %s", mapTask, jobName, inFile)
}
