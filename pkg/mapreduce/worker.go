package mapreduce

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"os"
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
	MapFn             MapFunc
	ReduceFn          ReduceFunc
	OutputDir         string
}

type (
	MapFunc    func(split string) []KeyValue
	ReduceFunc func(key string, value [][]byte) []byte
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
		file, err := os.Create(fmt.Sprintf("%s/temp-mr-map-%s-%d", w.OutputDir, task.TaskID, i))
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

	func() {
		for _, f := range Files {
			f.Close()
		}
	}()

	for i := range Files {
		err := os.Rename(fmt.Sprintf("%s/temp-mr-map-%s-%d", w.OutputDir, task.TaskID, i), fmt.Sprintf("%s/mr-map-%s-%d", w.OutputDir, task.TaskID, i))
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
2.sorts by key
3.calls the reduce function for each unique key
4.writes the result to an output file
*/

func (w *Worker) ReduceExecution(task Task) {
	// how are we planning to read all the intermediate data?
	// we need a way to decode so we will need gob decoders
	// we will need to get the reducefn key for each of them but how ?
	// see the naming is deterministic yeah ? ->
	decoders := make([]*gob.Decoder, w.NReduce)
	Files := make([]*os.File, w.NReduce)
}
