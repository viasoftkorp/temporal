package restorenamespace

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/searchattribute/sadefs"
	workercommon "go.temporal.io/server/service/worker/common"
)

const (
	listPageSize = 1000
	// maxDeferred caps the deferred set, since every heartbeat carries it. While it is full, the
	// listing pauses and the deferred set is retried.
	maxDeferred = 1000
	// inlineAttempts is how many times an execution is tried on a cluster before it is deferred.
	inlineAttempts = 3

	// errTypeRestoreStopped marks a refresh that must not be retried: the restore was abandoned,
	// superseded, or failed over.
	errTypeRestoreStopped = "NamespaceRestoreStopped"

	registryPollInterval = 500 * time.Millisecond
)

var errRegistryBehind = errors.New("namespace registry is behind the restore")

type (
	Activities struct {
		visibilityManager manager.VisibilityManager
		namespaceRegistry namespace.Registry
		clientBean        client.Bean
		currentCluster    string
		refreshRPS        dynamicconfig.IntPropertyFn
		concurrency       dynamicconfig.IntPropertyFn
		metricsHandler    metrics.Handler
		logger            log.Logger
		// throttledLogger logs per-execution problems, which can number in the millions.
		throttledLogger log.Logger

		inlineRetryInterval      time.Duration
		deferredRetryInterval    time.Duration
		maxDeferredRetryInterval time.Duration
		heartbeatInterval        time.Duration
		// How long the refresh waits at start for this host's namespace registry to have the
		// restore, which it normally does within one registry refresh interval.
		registryWait time.Duration
	}

	// refreshState is the refresh's progress, shared with the heartbeat goroutine.
	refreshState struct {
		sync.Mutex
		hb RefreshHeartbeat
	}

	executionResult struct {
		execution   *commonpb.WorkflowExecution
		archetypeID chasm.ArchetypeID
		// Clusters not yet refreshed, in the order they were tried. Empty once the execution is done.
		remaining []string
		dataLoss  bool
		attempts  int
	}
)

func NewActivities(
	visibilityManager manager.VisibilityManager,
	namespaceRegistry namespace.Registry,
	clientBean client.Bean,
	currentCluster string,
	refreshRPS dynamicconfig.IntPropertyFn,
	concurrency dynamicconfig.IntPropertyFn,
	metricsHandler metrics.Handler,
	logger log.Logger,
	throttledLogger log.Logger,
) *Activities {
	return &Activities{
		visibilityManager:        visibilityManager,
		namespaceRegistry:        namespaceRegistry,
		clientBean:               clientBean,
		currentCluster:           currentCluster,
		refreshRPS:               refreshRPS,
		concurrency:              concurrency,
		metricsHandler:           metricsHandler,
		logger:                   logger,
		throttledLogger:          throttledLogger,
		inlineRetryInterval:      time.Second,
		deferredRetryInterval:    10 * time.Second,
		maxDeferredRetryInterval: 5 * time.Minute,
		heartbeatInterval:        refreshHeartbeatTimeout / 4,
		registryWait:             30 * time.Second,
	}
}

// RefreshExecutions refreshes every execution of the namespace on every cluster that holds it,
// the execution's active cluster first. It returns once every execution listed is done: refreshed,
// gone, or unreadable. It stops without retrying if the restore is no longer the namespace's
// running restore, or if this cluster is no longer active for the namespace.
func (a *Activities) RefreshExecutions(ctx context.Context, input RestoreJobInput) (RefreshStats, error) {
	logger := log.With(a.logger,
		tag.WorkflowNamespaceID(input.NamespaceID),
		tag.NewStringTag("restore-id", input.RestoreID))

	state := &refreshState{}
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &state.hb); err != nil {
			logger.Warn("Unable to read the restore refresh checkpoint; starting over.", tag.Error(err))
			state.hb = RefreshHeartbeat{}
		}
	}

	ns, err := a.awaitRegistry(ctx, input, state)
	if err != nil {
		return RefreshStats{}, err
	}
	ctx = headers.SetCallerInfo(ctx, headers.NewPreemptableCallerInfo(ns.Name().String()))
	if state.hb.RefreshStartTime.IsZero() {
		resp, err := a.visibilityManager.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{
			NamespaceID: ns.ID(),
			Namespace:   ns.Name(),
			Query:       sadefs.QueryWithAnyNamespaceDivision(""),
		})
		if err != nil {
			return RefreshStats{}, err
		}
		state.hb.TotalEstimate = resp.Count
		state.hb.RefreshStartTime = time.Now().UTC()
	}

	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go a.heartbeat(heartbeatCtx, state)

	limiter := quotas.NewDefaultOutgoingRateLimiter(func() float64 { return float64(a.refreshRPS()) })
	deferredRound := 0
	for {
		listingDone, pageToken, deferredCount := state.progress()
		if listingDone && deferredCount == 0 {
			break
		}
		if ns, err = a.checkRestore(ctx, input); err != nil {
			return RefreshStats{}, err
		}
		// Retry the deferred set once listing is done, or while it is full.
		if listingDone || deferredCount >= maxDeferred {
			deferredRound++
			if err := a.retryDeferredRound(ctx, ns, input, state, limiter, deferredRound); err != nil {
				return RefreshStats{}, err
			}
			continue
		}
		deferredRound = 0
		if err := a.refreshNextPage(ctx, ns, input, state, pageToken, limiter); err != nil {
			return RefreshStats{}, err
		}
	}

	state.Lock()
	defer state.Unlock()
	return state.hb.RefreshStats, nil
}

// refreshNextPage lists the page at pageToken, refreshes it, and advances the checkpoint past it.
func (a *Activities) refreshNextPage(
	ctx context.Context,
	ns *namespace.Namespace,
	input RestoreJobInput,
	state *refreshState,
	pageToken []byte,
	limiter quotas.RateLimiter,
) error {
	resp, err := a.visibilityManager.ListWorkflowExecutions(ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   ns.ID(),
		Namespace:     ns.Name(),
		PageSize:      listPageSize,
		NextPageToken: pageToken,
		Query:         sadefs.QueryWithAnyNamespaceDivision(""),
	})
	if err != nil {
		return err
	}
	results, err := a.refreshPage(ctx, ns, input, resp.Executions, limiter)
	if err != nil {
		return err
	}
	state.resolve(results, func(hb *RefreshHeartbeat) {
		hb.PageToken = resp.NextPageToken
		hb.ListingDone = len(resp.NextPageToken) == 0
	})
	a.recordHeartbeat(ctx, state)
	return nil
}

// retryDeferredRound retries the deferred set once and, if any remain, backs off before the next
// round.
func (a *Activities) retryDeferredRound(
	ctx context.Context,
	ns *namespace.Namespace,
	input RestoreJobInput,
	state *refreshState,
	limiter quotas.RateLimiter,
	round int,
) error {
	if err := a.retryDeferred(ctx, ns, input, state, limiter); err != nil {
		return err
	}
	if _, _, deferredCount := state.progress(); deferredCount == 0 {
		return nil
	}
	return sleep(ctx, a.deferredBackoff(round))
}

// CompleteRestore marks the restore COMPLETED.
func (a *Activities) CompleteRestore(ctx context.Context, input RestoreJobInput) error {
	err := a.closeRestore(ctx, input, enumspb.NAMESPACE_RESTORE_STATE_COMPLETED)
	if failedPrecondition, ok := errors.AsType[*serviceerror.FailedPrecondition](err); ok {
		return temporal.NewNonRetryableApplicationError(failedPrecondition.Message, errTypeRestoreStopped, err)
	}
	return err
}

// awaitRegistry runs checkRestore, waiting up to registryWait for this host's registry to catch up
// with the restore.
func (a *Activities) awaitRegistry(ctx context.Context, input RestoreJobInput, state *refreshState) (*namespace.Namespace, error) {
	deadline := time.Now().Add(a.registryWait)
	for {
		ns, err := a.checkRestore(ctx, input)
		if !errors.Is(err, errRegistryBehind) || !time.Now().Before(deadline) {
			return ns, err
		}
		// Keeps the checkpoint loaded from the previous attempt.
		a.recordHeartbeat(ctx, state)
		if err := sleep(ctx, registryPollInterval); err != nil {
			return nil, err
		}
	}
}

// checkRestore reads the namespace from the registry and checks that this restore is still the
// namespace's running restore, hosted on an active cluster.
func (a *Activities) checkRestore(ctx context.Context, input RestoreJobInput) (*namespace.Namespace, error) {
	ns, err := a.namespaceRegistry.GetNamespaceByID(namespace.ID(input.NamespaceID))
	if err != nil {
		if _, ok := errors.AsType[*serviceerror.NamespaceNotFound](err); ok {
			return nil, temporal.NewNonRetryableApplicationError("namespace not found", errTypeRestoreStopped, err)
		}
		return nil, err
	}
	// The job can start before this host's registry has the restore.
	if ns.ConfigVersion() < input.RestoreVersion {
		return nil, fmt.Errorf("%w: config version %d, restore version %d",
			errRegistryBehind, ns.ConfigVersion(), input.RestoreVersion)
	}
	restore := ns.Info().GetRestoreInfo()
	if restore.GetRestoreId() != input.RestoreID {
		return nil, stopped("restore %s was superseded by restore %s", input.RestoreID, restore.GetRestoreId())
	}
	if restore.GetState() != enumspb.NAMESPACE_RESTORE_STATE_RUNNING {
		return nil, stopped("restore %s is %s", input.RestoreID, restore.GetState())
	}
	//nolint:forbidigo // Namespace-level check: the job covers the whole namespace.
	if !ns.ActiveInCluster(a.currentCluster) {
		err := a.closeRestore(ctx, input, enumspb.NAMESPACE_RESTORE_STATE_FAILED)
		var failedPrecondition *serviceerror.FailedPrecondition
		if err != nil && !errors.As(err, &failedPrecondition) {
			return nil, err
		}
		return nil, stopped("cluster %s is no longer active for namespace %s", a.currentCluster, ns.Name())
	}
	return ns, nil
}

func (a *Activities) closeRestore(ctx context.Context, input RestoreJobInput, state enumspb.NamespaceRestoreState) error {
	adminClient, err := a.clientBean.GetRemoteAdminClient(a.currentCluster)
	if err != nil {
		return err
	}
	_, err = adminClient.CloseNamespaceRestore(ctx, &adminservice.CloseNamespaceRestoreRequest{
		NamespaceId: input.NamespaceID,
		RestoreId:   input.RestoreID,
		State:       state,
	})
	return err
}

// refreshPage refreshes a page of executions concurrently. It returns an error only if ctx is done,
// in which case the page is not resolved.
func (a *Activities) refreshPage(
	ctx context.Context,
	ns *namespace.Namespace,
	input RestoreJobInput,
	executions []*workflowpb.WorkflowExecutionInfo,
	limiter quotas.RateLimiter,
) ([]executionResult, error) {
	results := make([]executionResult, len(executions))
	archetypeIDs := make([]chasm.ArchetypeID, len(executions))
	for i, info := range executions {
		archetypeID, err := workercommon.ArchetypeIDFromExecutionInfo(info)
		if err != nil {
			return nil, fmt.Errorf("execution %s: %w", info.GetExecution().GetWorkflowId(), err)
		}
		archetypeIDs[i] = archetypeID
	}
	a.forEach(ctx, len(executions), func(i int) {
		execution := executions[i].GetExecution()
		results[i] = a.refreshExecution(ctx, input, limiter, execution, archetypeIDs[i],
			activeFirst(ns, execution.GetWorkflowId(), ns.ClusterNames(execution.GetWorkflowId())), inlineAttempts)
		results[i].attempts = 1
	})
	return results, ctx.Err()
}

// retryDeferred tries every deferred execution once on its remaining clusters.
func (a *Activities) retryDeferred(
	ctx context.Context,
	ns *namespace.Namespace,
	input RestoreJobInput,
	state *refreshState,
	limiter quotas.RateLimiter,
) error {
	state.Lock()
	deferred := slices.Clone(state.hb.Deferred)
	state.Unlock()

	results := make([]executionResult, len(deferred))
	a.forEach(ctx, len(deferred), func(i int) {
		d := deferred[i]
		results[i] = a.refreshExecution(ctx, input, limiter, d.Execution, d.ArchetypeID,
			activeFirst(ns, d.Execution.GetWorkflowId(), d.RemainingClusters), 1)
		results[i].attempts = d.Attempts + 1
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	state.resolve(results, func(hb *RefreshHeartbeat) {
		hb.Deferred = hb.Deferred[len(deferred):]
	})
	a.recordHeartbeat(ctx, state)
	return nil
}

// forEach runs fn for 0..n-1, at most the configured concurrency at a time, and waits for them.
func (a *Activities) forEach(ctx context.Context, n int, fn func(i int)) {
	sem := make(chan struct{}, max(1, a.concurrency()))
	var wg sync.WaitGroup
	for i := range n {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return
		}
		wg.Go(func() {
			defer func() { <-sem }()
			fn(i)
		})
	}
	wg.Wait()
}

// refreshExecution refreshes the execution on each cluster in order, stopping at the first cluster
// that still fails after the given attempts.
func (a *Activities) refreshExecution(
	ctx context.Context,
	input RestoreJobInput,
	limiter quotas.RateLimiter,
	execution *commonpb.WorkflowExecution,
	archetypeID chasm.ArchetypeID,
	clusters []string,
	attempts int,
) executionResult {
	result := executionResult{execution: execution, archetypeID: archetypeID, remaining: clusters}
	if err := limiter.Wait(ctx); err != nil {
		return result
	}
	for i, cluster := range clusters {
		dataLoss, err := a.refreshOnCluster(ctx, input, cluster, execution, archetypeID, attempts)
		if err != nil {
			result.remaining = clusters[i:]
			return result
		}
		result.dataLoss = result.dataLoss || dataLoss
	}
	result.remaining = nil
	return result
}

// refreshOnCluster refreshes the execution on one cluster. NotFound means the cluster has no copy,
// usually because the execution was deleted after it was listed, and DataLoss means its mutable
// state can't be read, so it can't make progress either way: both finish the cluster. Every other
// error is retried.
func (a *Activities) refreshOnCluster(
	ctx context.Context,
	input RestoreJobInput,
	cluster string,
	execution *commonpb.WorkflowExecution,
	archetypeID chasm.ArchetypeID,
	attempts int,
) (dataLoss bool, _ error) {
	interval := a.inlineRetryInterval
	for attempt := 1; ; attempt++ {
		err := a.refresh(ctx, input, cluster, execution, archetypeID)
		var notFound *serviceerror.NotFound
		var dataLossErr *serviceerror.DataLoss
		switch {
		case err == nil, errors.As(err, &notFound):
			return false, nil
		case errors.As(err, &dataLossErr):
			metrics.NamespaceRestoreDataLossCount.With(a.metricsHandler).Record(1, metrics.TargetClusterTag(cluster))
			a.throttledLogger.Error("Namespace restore skipped an execution whose mutable state can't be read.",
				tag.WorkflowNamespaceID(input.NamespaceID),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.ClusterName(cluster),
				tag.Error(err))
			return true, nil
		}
		// Counted, not logged: a cluster that is down fails every execution. The executions still
		// failing are in the heartbeat's deferred set.
		metrics.NamespaceRestoreRefreshFailureCount.With(a.metricsHandler).Record(1, metrics.TargetClusterTag(cluster))
		if attempt >= attempts {
			return false, err
		}
		if err := sleep(ctx, interval); err != nil {
			return false, err
		}
		interval *= 2
	}
}

func (a *Activities) refresh(
	ctx context.Context,
	input RestoreJobInput,
	cluster string,
	execution *commonpb.WorkflowExecution,
	archetypeID chasm.ArchetypeID,
) error {
	adminClient, err := a.clientBean.GetRemoteAdminClient(cluster)
	if err != nil {
		return err
	}
	_, err = adminClient.RefreshWorkflowTasks(ctx, &adminservice.RefreshWorkflowTasksRequest{
		NamespaceId:               input.NamespaceID,
		Execution:                 execution,
		ArchetypeId:               archetypeID,
		MinNamespaceConfigVersion: input.RestoreVersion,
	})
	return err
}

func (a *Activities) heartbeat(ctx context.Context, state *refreshState) {
	ticker := time.NewTicker(a.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.recordHeartbeat(ctx, state)
		}
	}
}

func (a *Activities) recordHeartbeat(ctx context.Context, state *refreshState) {
	state.Lock()
	hb := state.hb
	hb.Deferred = slices.Clone(state.hb.Deferred)
	state.Unlock()
	activity.RecordHeartbeat(ctx, hb)
}

func (a *Activities) deferredBackoff(round int) time.Duration {
	return min(a.maxDeferredRetryInterval, a.deferredRetryInterval<<min(round-1, 16))
}

func (s *refreshState) progress() (listingDone bool, pageToken []byte, deferredCount int) {
	s.Lock()
	defer s.Unlock()
	return s.hb.ListingDone, s.hb.PageToken, len(s.hb.Deferred)
}

// resolve records results: finished executions are counted and the rest are deferred. update runs
// under the same lock, so the checkpoint never moves past executions that aren't yet recorded.
func (s *refreshState) resolve(results []executionResult, update func(hb *RefreshHeartbeat)) {
	s.Lock()
	defer s.Unlock()
	update(&s.hb)
	for _, r := range results {
		if len(r.remaining) == 0 {
			s.hb.ProcessedCount++
			if r.dataLoss {
				s.hb.DataLossCount++
			}
			continue
		}
		s.hb.Deferred = append(s.hb.Deferred, DeferredExecution{
			Execution:         r.execution,
			ArchetypeID:       r.archetypeID,
			RemainingClusters: r.remaining,
			Attempts:          r.attempts,
		})
	}
}

// activeFirst orders clusters with the execution's active cluster first, read when the execution is
// processed, so a failover during the restore reorders the remaining refreshes.
func activeFirst(ns *namespace.Namespace, businessID string, clusters []string) []string {
	active := ns.ActiveClusterName(namespace.RoutingKey{ID: businessID})
	ordered := make([]string, 0, len(clusters))
	if slices.Contains(clusters, active) {
		ordered = append(ordered, active)
	}
	for _, c := range clusters {
		if c != active {
			ordered = append(ordered, c)
		}
	}
	return ordered
}

func stopped(format string, args ...any) error {
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf(format, args...), errTypeRestoreStopped, nil)
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
