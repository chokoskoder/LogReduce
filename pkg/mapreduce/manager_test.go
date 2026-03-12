package mapreduce

import (
	"testing"
	"time"
)


func TestManager(t *testing.T){

	t.Run("worker requests task", func(t *testing.T) {
		cg := JobConfig{
			NMap: 2,
			NReduce: 1,
			inputSplits: []string{"file1.txt" , "file2.txt"} ,
			Timeout: 2*time.Second,
		}
		ResponseChannel := make(chan TaskResponse)

		m := NewManager(cg)
		newRequest := TaskRequest{
			WorkerID: "worker-1",
			ResponseCh: ResponseChannel,
		}	

		go m.Run()
		m.RequestCh <- newRequest
		task := <-ResponseChannel

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

	t.Run("manager transitions to reduce phase" , func (t *testing.T)  {
		cg := JobConfig{
			NMap: 1,
			NReduce: 1,
			inputSplits: []string{"file1.txt"} ,
			Timeout: 2*time.Second,
		}

		ResponseChannel := make(chan TaskResponse)

		m := NewManager(cg)
		newRequest := TaskRequest{
			WorkerID: "worker-1",
			ResponseCh: ResponseChannel,
		}

		go m.Run()
		m.RequestCh <- newRequest
		task := <-ResponseChannel

		if task.Task.State == TaskInProgress{
			time.Sleep(1000 * time.Millisecond)
			task.Task.State = TaskCompleted
		}
		task_completed := TaskCompletion{
			TaskID: task.Task.TaskID,
			WorkerID: newRequest.WorkerID,
		}
		m.CompleteCh <- task_completed

		m.RequestCh <- newRequest
		task = <-ResponseChannel

		if task.Response == WaitAndRetry{
			for i:=0 ; i<9 ; i++ {
			time.Sleep(100 * time.Millisecond)
			m.RequestCh <- newRequest
			task = <-ResponseChannel
			if 	task.Response == TaskAssigned {
				break
			}
		}
		}
		if task.Task.Type != ReduceTask {
			t.Fatal("The response recieved is not a reduce task")
		}
	})

	t.Run("race condition" , func(t *testing.T) {
		cg := JobConfig{
			NMap: 1,
			NReduce: 1,
			inputSplits: []string{"file1.txt"} ,
			Timeout: 2*time.Second,
		}

		ResponseChannel := make(chan TaskResponse)

		m := NewManager(cg)
		newRequest := TaskRequest{
			WorkerID: "worker-1",
			ResponseCh: ResponseChannel,
		}

		go m.Run()
		m.RequestCh <- newRequest
		task := <-ResponseChannel
		if task.Task.State == TaskInProgress{
			time.Sleep(1000 * time.Millisecond)
			task.Task.State = TaskCompleted
		}

		m.RequestCh <- newRequest
		task = <-ResponseChannel
		count := 0
		if task.Response == WaitAndRetry{
			for i:=0 ; i<9 ; i++ {
			time.Sleep(100 * time.Millisecond)
			m.RequestCh <- newRequest
			count++;
			task = <-ResponseChannel
			if 	task.Response == TaskAssigned {
				break
			}}
		}
		if count != 9 {
			t.Fatal("The response is wait and retry , the manager is doing")
		}
	})

	t.Run("timeout case" , func(t *testing.T) {
		cg := JobConfig{
			NMap: 1,
			NReduce: 1,
			inputSplits: []string{"file1.txt"} ,
			Timeout: 2*time.Second,
		}

		ResponseChannel := make(chan TaskResponse)

		m := NewManager(cg)
		newRequest := TaskRequest{
			WorkerID: "worker-1",
			ResponseCh: ResponseChannel,
		}

		go m.Run()
		m.RequestCh <- newRequest
		task1 := <-ResponseChannel
		if task1.Task.State == TaskInProgress{
			time.Sleep(5 * time.Second)
		}

		m.RequestCh <- newRequest
		task2 := <-ResponseChannel
		if task1.Task.TaskID != task2.Task.TaskID {
			t.Fatal("the recieved task is not the same ID")
		}
	})
}