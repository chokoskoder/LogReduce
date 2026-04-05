package mapreduce

import (
	"fmt"
	"time"
)

type Task struct {
	TaskID     int
	Type       TaskType  // MapTask or ReduceTask
	State      TaskState // idle , inProgress,Completed,Failed
	AssignedTo string    // worker id
	AssignedAt time.Time // zero value when idle
	RetryCount int
	MaxRetries int    // default 3
	InputSplit string // file path to this tasks input chunk
}

type TaskType string

const (
	MapTask    TaskType = "MapTask"
	ReduceTask TaskType = "ReduceTask"
)

type TaskState string

const (
	TaskIdle       TaskState = "idle"
	TaskInProgress TaskState = "inProgress"
	TaskCompleted  TaskState = "completed"
	TaskFailed     TaskState = "failed"
)

func (t *Task) Assign(workerID string) error {
	if t.State != TaskIdle {
		return fmt.Errorf("cannot assign task: currently in %s state", t.State)
	}

	t.State = TaskInProgress
	t.AssignedTo = workerID
	t.AssignedAt = time.Now()
	t.RetryCount++
	return nil
}

func (t *Task) Complete() error {
	if t.State != TaskInProgress {
		return fmt.Errorf("cannot complete task: task is in %s state, not in progress", t.State)
	}
	t.State = TaskCompleted
	return nil
}

func (t *Task) Fail() error {
	// two error possibilities here
	// 1.if we get an error from the worker
	// 2.if we ran out of time , basically isTimedOut OR we have exhausted our retries ??
	if t.State != TaskInProgress {
		return fmt.Errorf("Task is not in progress , current state : %s}", t.State)
	}
	if t.RetryCount >= t.MaxRetries {
		t.State = TaskFailed
		return nil
	}
	t.State = TaskIdle
	t.AssignedTo = ""
	t.AssignedAt = time.Time{}
	return nil
}

// The reason Failed doesnt set the retyrCount is becauase we should only increment it when it is assigned

func (t *Task) IsTimedOut(timeout time.Duration) bool {
	if t.State != TaskInProgress {
		return false
	}

	return time.Since(t.AssignedAt) > timeout
}			
