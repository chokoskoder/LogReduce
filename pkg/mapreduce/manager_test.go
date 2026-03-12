package mapreduce

import (
	"fmt"
	"testing"
	"time"
)


func TestManager(t *testing.T){
	//we need :
	//1. A worker which will be used to stress test the manager
	//2. A config which will be used to fill the manager

	t.Run("worker requests task", func(t *testing.T) {
		cg := JobConfig{
			NMap: 2,
			NReduce: 1,
			inputSplits: []string{"file1.txt" , "file2.txt"} ,
			Timeout: 2*time.Second,
		}
		ResponseChannel := make(chan TaskResponse)
		//every worker will have its own response channel , thats why we can do it this way `
		m := NewManager(cg)
		newRequest := TaskRequest{
			WorkerID: "worker-1",
			ResponseCh: ResponseChannel,
		}	
		//now all we need to do is send this request to the managers requestchannel after we do go.Run()
		go m.Run()
		m.RequestCh <- newRequest
		task := <-ResponseChannel
		fmt.Printf("recieved task %d" , task.Task)

		if task.Task.Type != MapTask{
			t.Fatal("The assigned task is of the wrong type")
		}
		if task.Task.State != TaskInProgress {
			t.Fatal("The assigned task is in the  wrong state")
		}
		if task.Response != TaskAssigned{
			t.Fatal("The response recieved does not convey properly if the task has been assigned or not")
		}
		close(m.CloseCh)
	})
}