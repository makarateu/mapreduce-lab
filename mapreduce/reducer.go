package mapreduce

import (
	"github.com/golang/glog"
)

func reducer(
	jobName string, // MapReduce job name.
	reduceTask int, // ID of the reducer task.
	outFile string, // Output file to write.
	m int, // Number of mappers (M in the paper).
	reduceFn func(key string, values []string) string, // Reduce function to run.
) {
	// reducer runs a reduce task:
	// 	1. Read all the input shards for the task.
	// 	2. Sort the KVs by key.
	// 	3. Call the reduceFn for each key.
	// 	4. Write the reduceFn's output to a file.
	//
	// For each map task, the reducer will have to read an input file. The names of
	// these files are generated from the map task id and the reduce task id. The
	// function reduceName(jobName, mapTask, reduceTask) gives the correct file
	// for each mapper/reducer pair.
	//
	// The mapper's output is JSON-encoded KVs. To decode JSON, you can use a
	// JSON Decoder.
	//
	//  inputFile, err := os.Open(filename)
	// 	decoder := json.NewDecoder(inputFile)
	//
	// 	var kv KeyValue
	// 	err := decoderReader.Decode(&kv)
	// 	if err == io.EOF {
	// 		...handle error
	// 	}
	// 	...process kv
	//
	// Given _every_ mapper's input, the reduceFn should be called once with the
	// key and the slice of all values associated with the key.
	//
	// The output of the reduceFn will be a string value; it should be written with
	// its key to the output for the reducer (as a KeyValue). The output files
	// should be encoded in JSON, similar to the output of the mapper.
	//
	// The output for the reducer must be sorted by key. The Go sort package will
	// be useful for this.
	//
	// The otuput file for this reducer should outFile. Make sure you close the
	// output file.

	glog.V(1).Infof("Reducer %d for %s processing %d input shards to %s",
		reduceTask, jobName, m, outFile)

	// Your code here (Part 1.2).

	// 1. Open output file and output encoder.
	// 2. Create decoders for all input files.
	// 3. Now process all input files to build a sorted map to reduce.
	// 4. Actually reduce.
	// 5. Sort the output (you will probably need to create an additional data
	// 		structure).
	// 6. Finally, write the sorted output.

	glog.V(1).Infof("Reducer %d for %s done with %s",
		reduceTask, jobName, outFile)
}
