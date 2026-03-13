package mapreduce


/*
what does the worker need to know:
manager's channels to communicate
a map function
a reduce function
how many reduce partitions to create when running a map task
workerID
*/
type Worker struct{
	ID						string
	ManagerRequestCh		chan TaskRequest
	ManagerCompleteCh		chan TaskCompletion
	NReduce					int
	MapFn					MapFunc
	ReduceFn				ReduceFunc
}

type MapFunc func(key string , value string) []KeyValue
type ReduceFunc func(key string, value string) string
type KeyValue struct{
	Key		string
	Value	string
}

/*
what does the worker loop needs to do:
1.sending a TaskRequest
2.reading the TaskResponse
3.check the task type - map or reduce - and execute the right function
4.send a completion
5.if WaitAndRetry then sleep briefly and loop
6.if JobDone then return
*/
func(w *Worker) Run(){
	workerResponseCh := make(chan TaskResponse)

	newRequest := TaskRequest{
		WorkerID: w.ID,
		ResponseCh: workerResponseCh,
	}

	w.ManagerRequestCh <- newRequest
	newTask := <-workerResponseCh

	if newTask.Task.Type == MapTask {
		w.MapFn()//what to pass here now ?
	} else if newTask.Task.Type == ReduceTask {
		w.ReduceFn()
	}

	TaskCompleted := TaskCompletion{
		TaskID: newTask.Task.TaskID,
		WorkerID: w.ID,
	}
	w.ManagerCompleteCh <- TaskCompleted

}

/*
what does map execution do :
1. call the map func on the contents of the input split
2.get back key-value pairs
3.partitions them by hashing the key
4.writes each partition to an intermediate file with a deterministic name
*/
func MapExecution(){

}

/*
what does reduce execution do:
1.Reads all intermediate files for its partition across all the map tasks
2.sorts by key
3.calls the reduce function for each unique key
4.writes the result to an output file
*/

func ReduceExecution(){

}