package mapreduce

import (
	"fmt"
	"log"
	"time"
)

type Manager struct{
	MapTasks 	[]Task
	ReduceTasks	[]Task
	Timeout		time.Duration
	
	//channels
	RequestCh	chan TaskRequest
	CompleteCh	chan TaskCompletion
	DoneCh		chan struct{} //manager close this when the jon is done
	CloseCh		chan struct{}
	
}

type ResponseStatus string
const (
	TaskAssigned	ResponseStatus="TaskAssigned"
	WaitAndRetry	ResponseStatus="WaitAndRetry"
	JobDone			ResponseStatus="JobDone"
)

type TaskResponse struct{
	Response	ResponseStatus
	Task		Task
}

type TaskRequest struct{
	WorkerID	string
	ResponseCh	chan TaskResponse
}

type TaskCompletion struct{
	TaskID		string
	WorkerID	string
}

func NewManager(cg JobConfig ) *Manager {
	m := &Manager{
		MapTasks: 			make([]Task , cg.NMap),
		ReduceTasks: 		make([]Task , cg.NReduce),
		Timeout: 			cg.Timeout,
		RequestCh: 			make(chan TaskRequest),					//unbuffered
		CompleteCh: 		make(chan TaskCompletion , cg.NMap),	//buffered
		DoneCh:				make(chan struct{}),	
		CloseCh: 			make(chan struct{}),				//unbuffered
	}

	//two loops for paritioning the data when sending it to workers
	for i := 0 ; i <cg.NMap ; i++ {
		m.MapTasks[i] = Task{
			TaskID		:	fmt.Sprintf("map-%d" , i),
			Type		:	MapTask,
			State		:	TaskIdle ,
			MaxRetries	:	3 ,
			InputSplit	:	cg.inputSplits[i] ,
		}
	}

	for i := 0; i < cg.NReduce; i++ {
		m.ReduceTasks[i] = Task{
			TaskID		:	fmt.Sprintf("reduce-%d" , i),
			Type		:	ReduceTask ,
			State		:	TaskIdle ,
			MaxRetries	:	3 ,

		}
	}
	return m
}
  
//where do input splits come in here ??

func(m *Manager) Run() error {
	//This is where we will write the logic and make sure it agnostic , meaning it can run any map->reduce flow
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select{
			case req := <-m.RequestCh:
				task := findIdleTasks(m.MapTasks)
				if task == nil && allCompleted(m.MapTasks){
					task = findIdleTasks(m.ReduceTasks)
				}
				if task != nil{
					task.Assign(req.WorkerID)
					req.ResponseCh <- TaskResponse{Response : TaskAssigned , Task: *task}
				} else if allCompleted(m.ReduceTasks){
					req.ResponseCh <- TaskResponse{Response: JobDone}
				} else {
					req.ResponseCh <- TaskResponse{Response: WaitAndRetry}
				}
					
			case report := <-m.CompleteCh:
				found := false
				for i := range m.MapTasks{
					if m.MapTasks[i].TaskID == report.TaskID {
						err := m.MapTasks[i].Complete()
						if err != nil {
  							log.Printf("task %s completion error: %v", m.MapTasks[i].TaskID, err)
						}
						found = true
						break
					}
				}

				if !found {
						for i := range m.ReduceTasks{
							if m.ReduceTasks[i].TaskID == report.TaskID{
								err := m.ReduceTasks[i].Complete()
								if err != nil {
									log.Printf("task %s completion error: %v", m.ReduceTasks[i].TaskID, err)
								}
								break
							}
						}
				}

				if allCompleted(m.MapTasks) && allCompleted(m.ReduceTasks){
					close(m.DoneCh)
					return nil
				}
			case <-ticker.C:
				for i := range m.MapTasks {
					if m.MapTasks[i].IsTimedOut(m.Timeout) {
						m.MapTasks[i].Fail()
					}
				}
				for i := range m.ReduceTasks {
					if m.ReduceTasks[i].IsTimedOut(m.Timeout) {
						m.ReduceTasks[i].Fail()
					}
				}
			case <-m.CloseCh:
				return nil

		}
	}
	return nil
}

func findIdleTasks(tasks []Task) *Task{
	for i := range tasks{
		if tasks[i].State == TaskIdle {
			return &tasks[i]
		}
	}
	return nil
}

func allCompleted(tasks []Task) bool{
	for i := range tasks{
		if tasks[i].State != TaskCompleted{
			return false
		}
	}
	return true
}