package mapreduce

import (
	"encoding/json"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
)

func makeReducerInput(m int) {
	// Write m mapper outputs.
	for i := 0; i < m; i++ {
		var kvs []KeyValue
		for j := 0; j < 100; j++ {
			kvs = append(kvs, KeyValue{Key: strconv.Itoa(i), Value: "."})
		}
		fn := reduceName("mrtmp", i, 1)
		of, err := os.Create(fn)
		dieIfError(err)
		w := json.NewEncoder(of)
		for _, kv := range kvs {
			dieIfError(w.Encode(kv))
		}
		of.Close()
	}
}

func reducer_reduceF(key string, values []string) string {
	return strings.Join(values, "")
}

func cleanup_reducer(f string, m int) {
	dieIfError(os.Remove(f))
	for i := 0; i < m; i++ {
		os.Remove(reduceName("mrtmp", i, 1))
	}
}

func check_reducer(fn string, m int, t *testing.T) {
	// Read file, validate it is sorted by key.
	f, e := os.Open(fn)
	dieIfError(e)
	d := json.NewDecoder(f)
	var kv KeyValue
	last := ""
	i := 0
	for {
		e = d.Decode(&kv)
		if e != nil {
			if e != io.EOF {
				dieIfError(e)
			}
			break
		}
		if kv.Key < last {
			t.Fatalf("Key %s out of order (last %s)", kv.Key, last)
		}
		last = kv.Key
		if len(kv.Value) != 100 {
			t.Fatalf("Incorrect length %d for key %s", len(kv.Value), kv.Key)
		}
		i++
	}
	if i != m {
		t.Fatalf("Incorrect number of keys %d (expected %d)", i, m)
	}
	f.Close()
}

func TestReducerOneMapper(t *testing.T) {
	m := 1
	makeReducerInput(m)
	reducer("mrtmp", 1, "mrtmp-reducer", m, reducer_reduceF)
	check_reducer("mrtmp-reducer", m, t)
	cleanup_reducer("mrtmp-reducer", m)
}

func TestReducerManyMappers(t *testing.T) {
	m := 3
	makeReducerInput(m)
	reducer("mrtmp", 1, "mrtmp-reducer", m, reducer_reduceF)
	check_reducer("mrtmp-reducer", m, t)
	cleanup_reducer("mrtmp-reducer", m)
}
