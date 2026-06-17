package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func backlogTaskWithExpiry(t *testing.T, expiry *timestamppb.Timestamp) *internalTask {
	t.Helper()
	return newInternalTaskFromBacklog(&persistencespb.AllocatedTaskInfo{
		TaskId: 1,
		Data: &persistencespb.TaskInfo{
			CreateTime: timestamppb.Now(),
			ExpiryTime: expiry,
		},
	}, func(*internalTask, taskResponse) {})
}

func TestGetInvalidTaskTag(t *testing.T) {
	t.Run("expired -> memory stage", func(t *testing.T) {
		task := backlogTaskWithExpiry(t, timestamppb.New(time.Now().Add(-time.Minute)))
		require.Equal(t, metrics.TaskExpireStageMemoryTag, getInvalidTaskTag(task))
	})

	t.Run("not expired -> invalid", func(t *testing.T) {
		task := backlogTaskWithExpiry(t, nil)
		require.Equal(t, metrics.TaskInvalidTag, getInvalidTaskTag(task))
	})
}

func TestGetDroppedTaskExpiryReasonTag(t *testing.T) {
	t.Run("expired -> expired_memory", func(t *testing.T) {
		task := backlogTaskWithExpiry(t, timestamppb.New(time.Now().Add(-time.Minute)))
		require.Equal(t, metrics.DroppedTaskReasonExpiredMemoryTag, getDroppedTaskExpiryReasonTag(task))
	})

	t.Run("not expired -> invalid", func(t *testing.T) {
		task := backlogTaskWithExpiry(t, nil)
		require.Equal(t, metrics.DroppedTaskReasonInvalidTag, getDroppedTaskExpiryReasonTag(task))
	})
}

// TestRecordDroppedTask verifies the single tasks_dropped entry point records the counter
// with the given reason, and is a no-op when the reason is nil.
func TestRecordDroppedTask(t *testing.T) {
	capture := metricstest.NewCaptureHandler()
	c := capture.StartCapture()
	defer capture.StopCapture(c)

	// nil reason (normal, non-drop completion): no-op.
	recordDroppedTask(capture, nil)
	require.Empty(t, c.Snapshot()[metrics.DroppedTasksCounter.Name()])

	// non-nil reason: counted.
	recordDroppedTask(capture, &metrics.DroppedTaskReasonNotFoundTag)
	recordings := c.Snapshot()[metrics.DroppedTasksCounter.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, metrics.DroppedTaskReasonNotFoundTag.Value, recordings[0].Tags["reason"])
}
