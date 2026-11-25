package frontend

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var errRestoreSystemNamespace = serviceerror.NewFailedPrecondition("The system namespace can't be restored.")

// restoreHostedElsewhereError is returned when a namespace lifecycle call has to run on the cluster
// hosting the namespace's running restore, so that its writes and the restore job's are ordered by
// one database.
type restoreHostedElsewhereError struct {
	hostCluster string
}

func (e *restoreHostedElsewhereError) Error() string {
	return fmt.Sprintf("the namespace's restore is running on cluster %s", e.hostCluster)
}

// readNamespaceForWrite reads the notification version, which is the lock on the namespace table,
// then the namespace.
func (d *namespaceHandler) readNamespaceForWrite(
	ctx context.Context,
	nsName namespace.Name,
	nsID namespace.ID,
) (*persistence.GetNamespaceResponse, int64, error) {
	metadata, err := d.metadataMgr.GetMetadata(ctx)
	if err != nil {
		return nil, 0, err
	}
	getResponse, err := d.metadataMgr.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		Name: nsName.String(),
		ID:   nsID.String(),
	})
	if err != nil {
		return nil, 0, err
	}
	return getResponse, metadata.NotificationVersion, nil
}

// startRestore moves the namespace to REGISTERED and records a running restore hosted on this
// cluster, in one write. If this cluster's restore is already running, as on a retry, it keeps it
// and replicates the namespace again. validatePeers runs after the local checks and before the
// write. It returns the namespace as written: its ConfigVersion is the restore version.
func (d *namespaceHandler) startRestore(
	ctx context.Context,
	nsName namespace.Name,
	nsID namespace.ID,
	validatePeers func(*persistencespb.NamespaceDetail) error,
) (*persistencespb.NamespaceDetail, error) {
	getResponse, notificationVersion, err := d.readNamespaceForWrite(ctx, nsName, nsID)
	if err != nil {
		return nil, err
	}
	detail := getResponse.Namespace
	isGlobalNamespace := getResponse.IsGlobalNamespace
	if detail.Info.Id == primitives.SystemNamespaceID {
		return nil, errRestoreSystemNamespace
	}
	state := detail.Info.State
	if state != enumspb.NAMESPACE_STATE_REGISTERED && state != enumspb.NAMESPACE_STATE_SOFT_DELETED {
		return nil, serviceerror.NewFailedPreconditionf(
			"Namespace %s is in state %s; only a %s or %s namespace can be restored.",
			detail.Info.Name, state, enumspb.NAMESPACE_STATE_SOFT_DELETED, enumspb.NAMESPACE_STATE_REGISTERED,
		)
	}
	// The job runs here, and stops itself once this cluster isn't active.
	currentCluster := d.clusterMetadata.GetCurrentClusterName()
	ns, err := namespace.FromPersistentState(detail, d.replicationResolverFactory(detail), namespace.WithGlobalFlag(isGlobalNamespace))
	if err != nil {
		return nil, err
	}
	//nolint:forbidigo // Namespace-level check: the restore job covers the whole namespace.
	if !ns.ActiveInCluster(currentCluster) {
		return nil, serviceerror.NewNamespaceNotActive(detail.Info.Name, currentCluster, ns.ActiveClusterName(namespace.RoutingKey{}))
	}
	if err := validatePeers(detail); err != nil {
		return nil, err
	}

	eventBefore := namespaceStateFields(detail, isGlobalNamespace)
	previousReplicationClusters := d.replicationClusters(common.CloneProto(detail))
	restore := detail.Info.GetRestoreInfo()
	retry := state == enumspb.NAMESPACE_STATE_REGISTERED &&
		restore.GetState() == enumspb.NAMESPACE_RESTORE_STATE_RUNNING &&
		restore.GetHostCluster() == currentCluster

	var updateReq *persistence.UpdateNamespaceRequest
	if !retry {
		detail.Info.State = enumspb.NAMESPACE_STATE_REGISTERED
		detail.Info.RestoreInfo = &persistencespb.NamespaceRestoreInfo{
			RestoreId:   uuid.NewString(),
			HostCluster: currentCluster,
			State:       enumspb.NAMESPACE_RESTORE_STATE_RUNNING,
			StartTime:   timestamppb.New(d.timeSource.Now()),
		}
		detail.ConfigVersion++
		updateReq = &persistence.UpdateNamespaceRequest{
			Namespace:           detail,
			IsGlobalNamespace:   isGlobalNamespace,
			NotificationVersion: notificationVersion,
		}
	}
	err = d.persistAndReplicate(ctx, updateReq, detail, isGlobalNamespace, notificationVersion, false, detail.ReplicationConfig.Clusters, previousReplicationClusters, func() {
		d.emitNamespaceUpdated(buildNamespaceUpdatedInput(eventBefore, detail, isGlobalNamespace, false, false, nil))
	})
	if err != nil {
		return nil, err
	}
	return detail, nil
}

// CloseNamespaceRestore moves the namespace's running restore to state, which is COMPLETED or
// FAILED. A repeated call with the same state replicates the namespace again.
func (d *namespaceHandler) CloseNamespaceRestore(
	ctx context.Context,
	nsID namespace.ID,
	restoreID string,
	state enumspb.NamespaceRestoreState,
) error {
	if state != enumspb.NAMESPACE_RESTORE_STATE_COMPLETED && state != enumspb.NAMESPACE_RESTORE_STATE_FAILED {
		return serviceerror.NewInvalidArgumentf("A restore can only be closed as %s or %s.",
			enumspb.NAMESPACE_RESTORE_STATE_COMPLETED, enumspb.NAMESPACE_RESTORE_STATE_FAILED)
	}
	getResponse, notificationVersion, err := d.readNamespaceForWrite(ctx, "", nsID)
	if err != nil {
		return err
	}
	detail := getResponse.Namespace
	isGlobalNamespace := getResponse.IsGlobalNamespace
	restore := detail.Info.GetRestoreInfo()
	if restore.GetRestoreId() != restoreID {
		return serviceerror.NewFailedPreconditionf("Restore %s is not the current restore of namespace %s.", restoreID, detail.Info.Name)
	}

	eventBefore := namespaceStateFields(detail, isGlobalNamespace)
	previousReplicationClusters := d.replicationClusters(common.CloneProto(detail))
	var updateReq *persistence.UpdateNamespaceRequest
	switch restore.GetState() {
	case state:
	case enumspb.NAMESPACE_RESTORE_STATE_RUNNING:
		restore.State = state
		restore.CloseTime = timestamppb.New(d.timeSource.Now())
		detail.ConfigVersion++
		updateReq = &persistence.UpdateNamespaceRequest{
			Namespace:           detail,
			IsGlobalNamespace:   isGlobalNamespace,
			NotificationVersion: notificationVersion,
		}
	default:
		return serviceerror.NewFailedPreconditionf("Restore %s of namespace %s is already %s.", restoreID, detail.Info.Name, restore.GetState())
	}
	return d.persistAndReplicate(ctx, updateReq, detail, isGlobalNamespace, notificationVersion, false, detail.ReplicationConfig.Clusters, previousReplicationClusters, func() {
		d.emitNamespaceUpdated(buildNamespaceUpdatedInput(eventBefore, detail, isGlobalNamespace, false, false, nil))
	})
}
