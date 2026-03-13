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