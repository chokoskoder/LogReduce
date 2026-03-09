package mapreduce

import (
	"testing"
	"time"
)

func TestTaskTransitions(t *testing.T) {

	// ============================================================
	// VALID TRANSITIONS
	// ============================================================

	t.Run("assign idle task", func(t *testing.T) {
		task := Task{
			TaskID:     "map-0",
			Type:       MapTask,
			State:      TaskIdle,
			MaxRetries: 3,
		}

		err := task.Assign("worker-1")

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if task.State != TaskInProgress {
			t.Errorf("expected state %s, got %s", TaskInProgress, task.State)
		}
		if task.AssignedTo != "worker-1" {
			t.Errorf("expected assignedTo worker-1, got %s", task.AssignedTo)
		}
		if task.RetryCount != 1 {
			t.Errorf("expected retryCount 1, got %d", task.RetryCount)
		}
		if task.AssignedAt.IsZero() {
			t.Errorf("expected assignedAt to be set, got zero time")
		}
	})

	t.Run("complete in-progress task", func(t *testing.T) {
		task := Task{
			TaskID:     "map-1",
			Type:       MapTask,
			State:      TaskIdle,
			MaxRetries: 3,
		}

		// walk to in-progress first
		task.Assign("worker-1")

		err := task.Complete()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if task.State != TaskCompleted {
			t.Errorf("expected state %s, got %s", TaskCompleted, task.State)
		}
		// assignedTo should still be set — we want to know who completed it
		if task.AssignedTo != "worker-1" {
			t.Errorf("expected assignedTo to remain worker-1, got %s", task.AssignedTo)
		}
	})

	t.Run("fail in-progress task returns to idle", func(t *testing.T) {
		task := Task{
			TaskID:     "map-2",
			Type:       MapTask,
			State:      TaskIdle,
			MaxRetries: 3,
		}

		task.Assign("worker-1")

		err := task.Fail()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if task.State != TaskIdle {
			t.Errorf("expected state %s, got %s", TaskIdle, task.State)
		}
		if task.AssignedTo != "" {
			t.Errorf("expected assignedTo to be cleared, got %s", task.AssignedTo)
		}
		if !task.AssignedAt.IsZero() {
			t.Errorf("expected assignedAt to be zero, got %v", task.AssignedAt)
		}
		// retryCount stays at 1 — it was set during Assign()
		if task.RetryCount != 1 {
			t.Errorf("expected retryCount 1, got %d", task.RetryCount)
		}
	})

	t.Run("fail exceeds max retries marks task as failed", func(t *testing.T) {
		task := Task{
			TaskID:     "map-3",
			Type:       MapTask,
			State:      TaskIdle,
			MaxRetries: 2,
		}

		// attempt 1: assign and fail
		task.Assign("worker-1")
		task.Fail()
		// retryCount=1, back to idle

		if task.State != TaskIdle {
			t.Fatalf("expected idle after first failure, got %s", task.State)
		}

		// attempt 2: assign and fail — now retryCount will be 2, which equals maxRetries
		task.Assign("worker-2")
		task.Fail()

		if task.State != TaskFailed {
			t.Errorf("expected state %s, got %s", TaskFailed, task.State)
		}
		if task.RetryCount != 2 {
			t.Errorf("expected retryCount 2, got %d", task.RetryCount)
		}
	})

	// ============================================================
	// INVALID TRANSITIONS
	// ============================================================

	t.Run("cannot complete idle task", func(t *testing.T) {
		task := Task{
			TaskID: "map-4",
			State:  TaskIdle,
		}

		err := task.Complete()

		if err == nil {
			t.Fatalf("expected error but got nil")
		}
		if task.State != TaskIdle {
			t.Errorf("state should still be %s, got %s", TaskIdle, task.State)
		}
	})

	t.Run("cannot fail idle task", func(t *testing.T) {
		task := Task{
			TaskID: "map-5",
			State:  TaskIdle,
		}

		err := task.Fail()

		if err == nil {
			t.Fatalf("expected error but got nil")
		}
		if task.State != TaskIdle {
			t.Errorf("state should still be %s, got %s", TaskIdle, task.State)
		}
	})

	t.Run("cannot assign in-progress task", func(t *testing.T) {
		task := Task{
			TaskID:     "map-6",
			State:      TaskIdle,
			MaxRetries: 3,
		}

		task.Assign("worker-1")
		err := task.Assign("worker-2")

		if err == nil {
			t.Fatalf("expected error but got nil")
		}
		if task.AssignedTo != "worker-1" {
			t.Errorf("assignedTo should still be worker-1, got %s", task.AssignedTo)
		}
	})

	t.Run("cannot assign completed task", func(t *testing.T) {
		task := Task{
			TaskID:     "map-7",
			State:      TaskIdle,
			MaxRetries: 3,
		}

		task.Assign("worker-1")
		task.Complete()

		err := task.Assign("worker-2")

		if err == nil {
			t.Fatalf("expected error but got nil")
		}
		if task.State != TaskCompleted {
			t.Errorf("state should still be %s, got %s", TaskCompleted, task.State)
		}
	})

	t.Run("cannot complete completed task", func(t *testing.T) {
		task := Task{
			TaskID:     "map-8",
			State:      TaskIdle,
			MaxRetries: 3,
		}

		task.Assign("worker-1")
		task.Complete()

		err := task.Complete()

		if err == nil {
			t.Fatalf("expected error but got nil")
		}
	})

	t.Run("cannot fail completed task", func(t *testing.T) {
		task := Task{
			TaskID:     "map-9",
			State:      TaskIdle,
			MaxRetries: 3,
		}

		task.Assign("worker-1")
		task.Complete()

		err := task.Fail()

		if err == nil {
			t.Fatalf("expected error but got nil")
		}
		if task.State != TaskCompleted {
			t.Errorf("state should still be %s, got %s", TaskCompleted, task.State)
		}
	})

	t.Run("cannot assign failed task", func(t *testing.T) {
		task := Task{
			TaskID:     "map-10",
			State:      TaskIdle,
			MaxRetries: 1,
		}

		// one attempt, then fail → permanently failed
		task.Assign("worker-1")
		task.Fail()

		if task.State != TaskFailed {
			t.Fatalf("setup failed: expected Failed state, got %s", task.State)
		}

		err := task.Assign("worker-2")

		if err == nil {
			t.Fatalf("expected error but got nil")
		}
		if task.State != TaskFailed {
			t.Errorf("state should still be %s, got %s", TaskFailed, task.State)
		}
	})

	t.Run("cannot complete failed task", func(t *testing.T) {
		task := Task{
			TaskID:     "map-11",
			State:      TaskIdle,
			MaxRetries: 1,
		}

		task.Assign("worker-1")
		task.Fail()

		err := task.Complete()

		if err == nil {
			t.Fatalf("expected error but got nil")
		}
		if task.State != TaskFailed {
			t.Errorf("state should still be %s, got %s", TaskFailed, task.State)
		}
	})
}

func TestTaskTimeout(t *testing.T) {

	t.Run("not timed out yet", func(t *testing.T) {
		task := Task{
			TaskID:     "map-12",
			State:      TaskIdle,
			MaxRetries: 3,
		}

		task.Assign("worker-1")

		// just assigned — should not be timed out with a 5 second timeout
		if task.IsTimedOut(5 * time.Second) {
			t.Errorf("task should not be timed out immediately after assignment")
		}
	})

	t.Run("timed out", func(t *testing.T) {
		task := Task{
			TaskID:     "map-13",
			State:      TaskIdle,
			MaxRetries: 3,
		}

		task.Assign("worker-1")

		// simulate passage of time by backdating assignedAt
		// this is why we don't use time.Sleep — tests stay fast
		task.AssignedAt = time.Now().Add(-10 * time.Second)

		if !task.IsTimedOut(5 * time.Second) {
			t.Errorf("task should be timed out after 10s with 5s timeout")
		}
	})

	t.Run("idle task is never timed out", func(t *testing.T) {
		task := Task{
			TaskID: "map-14",
			State:  TaskIdle,
		}

		if task.IsTimedOut(5 * time.Second) {
			t.Errorf("idle task should never report as timed out")
		}
	})
}