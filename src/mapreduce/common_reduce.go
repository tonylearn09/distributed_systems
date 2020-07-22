package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

// for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	// collect all the intermediate files corresponding to
	// the reduce task
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
		name := reduceName(jobName, i, reduceTask)
		file, err := os.Open(name)
		if nil != err {
			log.Fatal(err)
		}

		dec := json.NewDecoder(file)
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				log.Fatal(err)
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// sort by key
	sort.Sort(ByKey(intermediate))

	reduceFile, err := os.Create(outFile)
	if err != nil {
		log.Fatal(err)
	}

	enc := json.NewEncoder(reduceFile)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// call reduceF once per distinct key
		output := reduceF(intermediate[i].Key, values)
		kv := KeyValue{intermediate[i].Key, output}
		enc.Encode(kv)

		i = j
	}

	reduceFile.Close()

}
