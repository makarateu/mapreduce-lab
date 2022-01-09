package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func makeMapperInput() (fn string) {
	fn = "dist-mrinput.txt"
	file, err := os.Create(fn)
	dieIfError(err)
	w := bufio.NewWriter(file)
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(w, "%d\n", i)
	}
	w.Flush()
	file.Close()
	return
}

func mapper_mapF(filename string, contents string) (res []KeyValue) {
	vals := strings.Fields(contents)
	for _, w := range vals {
		kv := KeyValue{Key: w, Value: ""}
		res = append(res, kv)
	}
	return
}

func cleanup_mapper(f string, r int) {
	dieIfError(os.Remove(f))
	for i := 0; i < r; i++ {
		os.Remove(reduceName("mrtmp", 1, i))
	}
}

func check_mapper(r int, t *testing.T) {
	// We should have as many files as reducers, each produced by the maptask.
	ifiles := make([]*os.File, r)
	readers := make([]*json.Decoder, r)
	var err error
	for i := range ifiles {
		fname := reduceName("mrtmp", 1, i)
		ifiles[i], err = os.Open(fname)
		dieIfError(err)
		readers[i] = json.NewDecoder(ifiles[i])
	}

	// Read input from all reduce shards.
	// Check that each number appears at most once.
	all := make(map[string]bool)
	var vals []int
	var kv KeyValue
	for _, r := range readers {
		for {
			err = r.Decode(&kv)
			if err != nil {
				if err != io.EOF {
					dieIfError(err)
				}
				break
			}
			if p := all[kv.Key]; p {
				t.Fatalf("Duplicate key %s", kv.Key)
			}
			all[kv.Key] = true
			i, err := strconv.Atoi(kv.Key)
			dieIfError(err)
			vals = append(vals, i)
		}
	}

	// Validate all numbers appear.
	sort.Ints(vals)
	for i := 1; i < len(vals); i++ {
		if vals[i] != vals[i-1]+1 {
			t.Fatalf("Keys missing between %d and %d", vals[i], vals[i-1])
		}
	}
	if vals[len(vals)-1] != 1000-1 {
		t.Fatalf("Range of values incorrect: %d-%d", vals[0], vals[len(vals)-1])
	}
}

func TestMapperOneReduce(t *testing.T) {
	f := makeMapperInput()
	r := 1
	mapper("mrtmp", 1, f, r, mapper_mapF)
	check_mapper(r, t)
	cleanup_mapper(f, r)
}

func TestMapperManyReduce(t *testing.T) {
	f := makeMapperInput()
	r := 3
	mapper("mrtmp", 1, f, r, mapper_mapF)
	check_mapper(r, t)
	cleanup_mapper(f, r)
}
