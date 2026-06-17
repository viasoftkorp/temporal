package matching

import (
	"go.temporal.io/server/common/metrics"
)

// getInvalidTaskTag returns the tasks_expired stage tag for a task being thrown away
// due to in-memory expiry or validation failure.
func getInvalidTaskTag(task *internalTask) metrics.Tag {
	if IsTaskExpired(task.event.AllocatedTaskInfo) {
		return metrics.TaskExpireStageMemoryTag
	}
	return metrics.TaskInvalidTag
}

// getDroppedTaskExpiryReasonTag returns the tasks_dropped reason tag for a task
// being thrown away due to in-memory expiry or validation failure.
func getDroppedTaskExpiryReasonTag(task *internalTask) metrics.Tag {
	if IsTaskExpired(task.event.AllocatedTaskInfo) {
		return metrics.DroppedTaskReasonExpiredMemoryTag
	}
	return metrics.DroppedTaskReasonInvalidTag
}

// recordDroppedTask records the tasks_dropped counter on the given physical-queue
// handler. It is a no-op when reason is nil (i.e. a normal, non-drop completion).
func recordDroppedTask(handler metrics.Handler, reason *metrics.Tag) {
	if reason == nil {
		return
	}
	metrics.DroppedTasksCounter.With(handler).Record(1, *reason)
}
