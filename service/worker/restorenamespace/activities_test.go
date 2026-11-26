package restorenamespace

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

const (
	testNamespaceID   = "ns-id"
	testRestoreID     = "restore-1"
	testActive        = "active"
	testStandby       = "standby"
	testConfigVersion = int64(10)
)

var testInput = RestoreJobInput{NamespaceID: testNamespaceID, RestoreID: testRestoreID, RestoreVersion: testConfigVersion}

type activityTestEnv struct {
	activities *Activities
	registry   *namespace.MockRegistry
	visibility *manager.MockVisibilityManager
	clients    map[string]*adminservicemock.MockAdminServiceClient
	env        *testsuite.TestActivityEnvironment
}

func newActivityTestEnv(t *testing.T, currentCluster string) *activityTestEnv {
	ctrl := gomock.NewController(t)
	te := &activityTestEnv{
		registry:   namespace.NewMockRegistry(ctrl),
		visibility: manager.NewMockVisibilityManager(ctrl),
		clients: map[string]*adminservicemock.MockAdminServiceClient{
			testActive:  adminservicemock.NewMockAdminServiceClient(ctrl),
			testStandby: adminservicemock.NewMockAdminServiceClient(ctrl),
		},
	}
	bean := client.NewMockBean(ctrl)
	bean.EXPECT().GetRemoteAdminClient(gomock.Any()).DoAndReturn(func(cluster string) (adminservice.AdminServiceClient, error) {
		return te.clients[cluster], nil
	}).AnyTimes()
	te.activities = NewActivities(
		te.visibility,
		te.registry,
		bean,
		currentCluster,
		dynamicconfig.GetIntPropertyFn(1000),
		dynamicconfig.GetIntPropertyFn(4),
		metrics.NoopMetricsHandler,
		log.NewNoopLogger(),
		log.NewNoopLogger(),
	)
	te.activities.inlineRetryInterval = time.Millisecond
	te.activities.deferredRetryInterval = time.Millisecond
	te.activities.maxDeferredRetryInterval = time.Millisecond
	te.activities.registryWait = 0

	var suite testsuite.WorkflowTestSuite
	te.env = suite.NewTestActivityEnvironment()
	te.env.RegisterActivityWithOptions(te.activities.RefreshExecutions, activity.RegisterOptions{Name: RefreshExecutionsActivityName})
	te.env.RegisterActivityWithOptions(te.activities.CompleteRestore, activity.RegisterOptions{Name: CompleteRestoreActivityName})
	return te
}

func testNamespace(t *testing.T, configVersion int64, restore *persistencespb.NamespaceRestoreInfo) *namespace.Namespace {
	detail := &persistencespb.NamespaceDetail{
		Info:   &persistencespb.NamespaceInfo{Id: testNamespaceID, Name: "ns", State: enumspb.NAMESPACE_STATE_REGISTERED, RestoreInfo: restore},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: testActive,
			// Standby first, so the test sees the job reorder them.
			Clusters: []string{testStandby, testActive},
		},
		ConfigVersion:   configVersion,
		FailoverVersion: 1,
	}
	ns, err := namespace.FromPersistentState(detail, namespace.NewDefaultReplicationResolverFactory()(detail), namespace.WithGlobalFlag(true))
	require.NoError(t, err)
	return ns
}

func runningRestore() *persistencespb.NamespaceRestoreInfo {
	return &persistencespb.NamespaceRestoreInfo{RestoreId: testRestoreID, HostCluster: testActive, State: enumspb.NAMESPACE_RESTORE_STATE_RUNNING}
}

func executionInfo(workflowID string) *workflowpb.WorkflowExecutionInfo {
	return &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: workflowID + "-run"}}
}

// refreshLog records, per workflow ID, the clusters refreshed in order.
type refreshLog struct {
	sync.Mutex
	calls map[string][]string
}

func (l *refreshLog) expect(t *testing.T, te *activityTestEnv, cluster string, result func(workflowID string, attempt int) error) {
	te.clients[cluster].EXPECT().RefreshWorkflowTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *adminservice.RefreshWorkflowTasksRequest, _ ...grpc.CallOption) (*adminservice.RefreshWorkflowTasksResponse, error) {
			require.Equal(t, testNamespaceID, request.GetNamespaceId())
			require.Equal(t, testConfigVersion, request.GetMinNamespaceConfigVersion())
			require.Equal(t, chasm.WorkflowArchetypeID, request.GetArchetypeId())
			workflowID := request.GetExecution().GetWorkflowId()
			l.Lock()
			if l.calls == nil {
				l.calls = map[string][]string{}
			}
			l.calls[workflowID] = append(l.calls[workflowID], cluster)
			attempt := 0
			for _, c := range l.calls[workflowID] {
				if c == cluster {
					attempt++
				}
			}
			l.Unlock()
			return &adminservice.RefreshWorkflowTasksResponse{}, result(workflowID, attempt)
		}).AnyTimes()
}

func TestRefreshExecutions(t *testing.T) {
	t.Parallel()
	te := newActivityTestEnv(t, testActive)
	te.registry.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(testNamespace(t, testConfigVersion, runningRestore()), nil).AnyTimes()
	te.visibility.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{Count: 4}, nil)
	gomock.InOrder(
		te.visibility.EXPECT().ListWorkflowExecutions(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, request *manager.ListWorkflowExecutionsRequestV2) (*manager.ListWorkflowExecutionsResponse, error) {
				require.Empty(t, request.NextPageToken)
				return &manager.ListWorkflowExecutionsResponse{
					Executions:    []*workflowpb.WorkflowExecutionInfo{executionInfo("refreshed"), executionInfo("gone-on-standby")},
					NextPageToken: []byte("page-2"),
				}, nil
			}),
		te.visibility.EXPECT().ListWorkflowExecutions(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, request *manager.ListWorkflowExecutionsRequestV2) (*manager.ListWorkflowExecutionsResponse, error) {
				require.Equal(t, []byte("page-2"), request.NextPageToken)
				return &manager.ListWorkflowExecutionsResponse{
					Executions: []*workflowpb.WorkflowExecutionInfo{executionInfo("data-loss"), executionInfo("flaky-standby")},
				}, nil
			}),
	)

	var calls refreshLog
	calls.expect(t, te, testActive, func(string, int) error { return nil })
	calls.expect(t, te, testStandby, func(workflowID string, attempt int) error {
		switch workflowID {
		case "gone-on-standby":
			return serviceerror.NewNotFound("gone")
		case "data-loss":
			return serviceerror.NewDataLoss("corrupt")
		case "flaky-standby":
			// Fails every inline attempt, so it is deferred, then succeeds.
			if attempt <= inlineAttempts {
				return serviceerror.NewUnavailable("registry behind")
			}
		default:
		}
		return nil
	})

	result, err := te.env.ExecuteActivity(RefreshExecutionsActivityName, testInput)
	require.NoError(t, err)
	var stats RefreshStats
	require.NoError(t, result.Get(&stats))
	require.Equal(t, int64(4), stats.TotalEstimate)
	require.Equal(t, int64(4), stats.ProcessedCount)
	require.Equal(t, int64(1), stats.DataLossCount)
	require.False(t, stats.RefreshStartTime.IsZero())

	require.Equal(t, []string{testActive, testStandby}, calls.calls["refreshed"])
	require.Equal(t, []string{testActive, testStandby}, calls.calls["gone-on-standby"])
	require.Equal(t, []string{testActive, testStandby}, calls.calls["data-loss"])
	require.Equal(t, []string{testActive, testStandby, testStandby, testStandby, testStandby}, calls.calls["flaky-standby"],
		"the deferred retry resumes on the standby without refreshing the active cluster again")
}

func TestRefreshExecutions_Stops(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name           string
		currentCluster string
		ns             func(t *testing.T) *namespace.Namespace
		expectClose    bool
		wantRetryable  bool
	}{
		{
			name:           "registry behind the restore",
			currentCluster: testActive,
			ns:             func(t *testing.T) *namespace.Namespace { return testNamespace(t, testConfigVersion-1, nil) },
			wantRetryable:  true,
		},
		{
			name:           "superseded",
			currentCluster: testActive,
			ns: func(t *testing.T) *namespace.Namespace {
				restore := runningRestore()
				restore.RestoreId = "restore-2"
				return testNamespace(t, testConfigVersion+1, restore)
			},
		},
		{
			name:           "abandoned by soft delete",
			currentCluster: testActive,
			ns: func(t *testing.T) *namespace.Namespace {
				restore := runningRestore()
				restore.State = enumspb.NAMESPACE_RESTORE_STATE_FAILED
				return testNamespace(t, testConfigVersion+1, restore)
			},
		},
		{
			name:           "failed over",
			currentCluster: testStandby,
			ns:             func(t *testing.T) *namespace.Namespace { return testNamespace(t, testConfigVersion, runningRestore()) },
			expectClose:    true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			te := newActivityTestEnv(t, tc.currentCluster)
			te.registry.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(tc.ns(t), nil)
			if tc.expectClose {
				te.clients[tc.currentCluster].EXPECT().CloseNamespaceRestore(gomock.Any(), &adminservice.CloseNamespaceRestoreRequest{
					NamespaceId: testNamespaceID,
					RestoreId:   testRestoreID,
					State:       enumspb.NAMESPACE_RESTORE_STATE_FAILED,
				}).Return(&adminservice.CloseNamespaceRestoreResponse{}, nil)
			}

			_, err := te.env.ExecuteActivity(RefreshExecutionsActivityName, testInput)
			require.Error(t, err)
			var appErr *temporal.ApplicationError
			require.ErrorAs(t, err, &appErr)
			require.Equal(t, !tc.wantRetryable, appErr.NonRetryable(), err.Error())
		})
	}
}

func TestCompleteRestore(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name          string
		closeErr      error
		wantErr       bool
		wantRetryable bool
	}{
		{name: "completed"},
		{name: "no longer current", closeErr: serviceerror.NewFailedPrecondition("superseded"), wantErr: true},
		{name: "unavailable", closeErr: serviceerror.NewUnavailable("down"), wantErr: true, wantRetryable: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			te := newActivityTestEnv(t, testActive)
			te.clients[testActive].EXPECT().CloseNamespaceRestore(gomock.Any(), &adminservice.CloseNamespaceRestoreRequest{
				NamespaceId: testNamespaceID,
				RestoreId:   testRestoreID,
				State:       enumspb.NAMESPACE_RESTORE_STATE_COMPLETED,
			}).Return(&adminservice.CloseNamespaceRestoreResponse{}, tc.closeErr)

			_, err := te.env.ExecuteActivity(CompleteRestoreActivityName, testInput)
			if !tc.wantErr {
				require.NoError(t, err)
				return
			}
			var appErr *temporal.ApplicationError
			require.ErrorAs(t, err, &appErr, err.Error())
			require.Equal(t, !tc.wantRetryable, appErr.NonRetryable())
		})
	}
}

func TestActiveFirst(t *testing.T) {
	t.Parallel()
	ns := testNamespace(t, testConfigVersion, nil)
	require.Equal(t, []string{testActive, testStandby}, activeFirst(ns, "wf", []string{testStandby, testActive}))
	require.Equal(t, []string{testStandby}, activeFirst(ns, "wf", []string{testStandby}))
}

func TestRefreshExecutions_WaitsForRegistry(t *testing.T) {
	t.Parallel()
	te := newActivityTestEnv(t, testActive)
	te.activities.registryWait = time.Minute
	gomock.InOrder(
		te.registry.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(testNamespace(t, testConfigVersion-1, nil), nil),
		te.registry.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(testNamespace(t, testConfigVersion, runningRestore()), nil).AnyTimes(),
	)
	te.visibility.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{}, nil)
	te.visibility.EXPECT().ListWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.ListWorkflowExecutionsResponse{}, nil)

	_, err := te.env.ExecuteActivity(RefreshExecutionsActivityName, testInput)
	require.NoError(t, err)
}
