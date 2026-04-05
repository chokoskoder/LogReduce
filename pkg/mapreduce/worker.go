package mapreduce

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"sort"
	"time"
)

/*
what does the worker need to know:
manager's channels to communicate
a map function
a reduce function
how many reduce partitions to create when running a map task
workerID
*/
type Worker struct {
	ID                string
	ManagerRequestCh  chan TaskRequest
	ManagerCompleteCh chan TaskCompletion
	NReduce           int
	NMap              int
	MapFn             MapFunc
	ReduceFn          ReduceFunc
	OutputDir         string
}

type (
	MapFunc    func(split string) []KeyValue
	ReduceFunc func(Key string, value [][]byte) []byte
	KeyValue   struct {
		Key   string
		Value []byte // opaque bytes , the system never reads them - (MAJOR DOUBT HERE)
	}
)

/*
what does the worker loop needs to do:
1.sending a TaskRequest
2.reading the TaskResponse
3.check the task type - map or reduce - and execute the right function
4.send a completion
5.if WaitAndRetry then sleep briefly and loop
6.if JobDone then return
*/
func (w *Worker) Run() {
	responseCh := make(chan TaskResponse)
	request := TaskRequest{
		WorkerID:   w.ID,
		ResponseCh: responseCh,
	}

	for {
		w.ManagerRequestCh <- request
		response := <-responseCh

		switch response.Response {
		case TaskAssigned:
			if response.Task.Type == MapTask {
				w.MapExecution(response.Task)
			} else if response.Task.Type == ReduceTask {
				w.ReduceExecution(response.Task)
			}
			w.ManagerCompleteCh <- TaskCompletion{
				TaskID:   response.Task.TaskID,
				WorkerID: w.ID,
			}

		case WaitAndRetry:
			time.Sleep(100 * time.Millisecond)
			continue

		case JobDone:
			return
		}
	}
}

/*
what does map execution do :
1. call the map func on the contents of the input split
2.get back key-value pairs
3.partitions them by hashing the key
4.writes each partition to an intermediate file with a deterministic name
*/
func (w *Worker) MapExecution(task Task) error {
	KeyValue := w.MapFn(task.InputSplit)
	Files := make([]*os.File, w.NReduce)
	encoders := make([]*gob.Encoder, w.NReduce)
	for i := range w.NReduce {
		file, err := os.Create(fmt.Sprintf("%s/temp-mr-map-%d-%d", w.OutputDir, task.TaskID, i))
		// create temp files for now
		// We need to add a folder name here so that not everything comes out in the same thing
		if err != nil {
			return err
		}
		encoders[i] = gob.NewEncoder(file)
		Files[i] = file
	}

	for _, i := range KeyValue {
		keyHash := partitionKey(i.Key, w.NReduce)
		err := encoders[keyHash].Encode(i)
		if err != nil {
			return err
		}
	}

	for _, f := range Files {
		f.Close()
	}

	for i := range Files {
		err := os.Rename(fmt.Sprintf("%s/temp-mr-map-%d-%d", w.OutputDir, task.TaskID, i), fmt.Sprintf("%s/mr-map-%d-%d", w.OutputDir, task.TaskID, i))
		if err != nil {
			return err
		}
	}

	return nil
}

func partitionKey(key string, nReduce int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % nReduce
}

/*
what does reduce execution do:
1.Reads all intermediate files for its partition across all the map tasks
2.	sorts by key
3.calls the reduce function for each unique key
4.writes the result to an output file
*/

func (w *Worker) ReduceExecution(task Task) error {
	// how are we planning to read nall the intermediate data?
	// we need a way to decode so we will need gob decoders
	// we will need to get the reducefn key for each of them but how ?
	// see the naming is deterministic yeah ? ->
	decoders := make([]*gob.Decoder, w.NMap)
	Files := make([]*os.File, w.NMap)

	// create a slice of struct KeyValue so that I can populate its
	var reduceExecKeyValue []KeyValue
	for mapIndex := 0; mapIndex < w.NMap; mapIndex++ {
		path, err := os.Open(fmt.Sprintf("%s/mr-map-%d-%d", w.OutputDir, mapIndex, task.TaskID))
		if err != nil {
			return err
		}
		Files[mapIndex] = path
		decoders[mapIndex] = gob.NewDecoder(path)

		for {
			var kv KeyValue
			err := decoders[mapIndex].Decode(&kv)

			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			reduceExecKeyValue = append(reduceExecKeyValue, kv)
		}
	}

	sort.Slice(reduceExecKeyValue, func(i, j int) bool {
		return reduceExecKeyValue[i].Key < reduceExecKeyValue[j].Key
	})

	for _, f := range Files {
		f.Close()
	}

	currentKey := reduceExecKeyValue[0].Key
	var values [][]byte
	file, err := os.Create(fmt.Sprintf("%s/temp-mr-reduce-%d", w.OutputDir, task.TaskID))
	for _, kv := range reduceExecKeyValue {
		if kv.Key == currentKey {
			values = append(values, kv.Value)
		} else {
			result := w.ReduceFn(currentKey, values)
			enc := gob.NewEncoder(file)
			enc.Encode(result)
			file.Close()
			currentKey = kv.Key
			values = [][]byte{kv.Value}

		}
	}

	lastResult := w.ReduceFn(currentKey, values)
	enc := gob.NewEncoder(file)
	enc.Encode(lastResult)
	file.Close()

	err = os.Rename(fmt.Sprintf("%d/temp-mr-reduce-%d", w.OutputDir, task.TaskID), fmt.Sprintf("%d/mr-reduce-%d", task.TaskID))

	return nil
}
