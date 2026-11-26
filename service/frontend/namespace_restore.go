package frontend

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
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

// startRestore moves the namespace to REGISTERED and records a running restore hosted on this
// cluster, in one UpdateNamespace write. If this cluster's restore is already running, as on a
// retry, it keeps it and replicates the namespace again. validatePeers runs after the local checks
// and before the write. It returns the namespace as written: its ConfigVersion is the restore
// version.
func (d *namespaceHandler) startRestore(
	ctx context.Context,
	nsName namespace.Name,
	nsID namespace.ID,
	validatePeers func(*persistencespb.NamespaceDetail) error,
) (*persistencespb.NamespaceDetail, error) {
	_, detail, err := d.updateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace:  nsName.String(),
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{State: enumspb.NAMESPACE_STATE_REGISTERED},
	}, internalUpdateOptions{
		namespaceID:  nsID,
		allowRestore: true,
		mutate: func(existing *persistence.GetNamespaceResponse) (bool, error) {
			return d.recordRestore(existing, validatePeers)
		},
	})
	return detail, err
}

// recordRestore checks that the namespace can be restored on this cluster and records a running
// restore hosted here. It changes nothing if this cluster's restore is already running.
func (d *namespaceHandler) recordRestore(
	existing *persistence.GetNamespaceResponse,
	validatePeers func(*persistencespb.NamespaceDetail) error,
) (bool, error) {
	detail := existing.Namespace
	if detail.Info.Id == primitives.SystemNamespaceID {
		return false, errRestoreSystemNamespace
	}
	state := detail.Info.State
	if state != enumspb.NAMESPACE_STATE_REGISTERED && state != enumspb.NAMESPACE_STATE_SOFT_DELETED {
		return false, serviceerror.NewFailedPreconditionf(
			"Namespace %s is in state %s; only a %s or %s namespace can be restored.",
			detail.Info.Name, state, enumspb.NAMESPACE_STATE_SOFT_DELETED, enumspb.NAMESPACE_STATE_REGISTERED,
		)
	}
	// The job runs here, and stops itself once this cluster isn't active.
	currentCluster := d.clusterMetadata.GetCurrentClusterName()
	ns, err := namespace.FromPersistentState(detail, d.replicationResolverFactory(detail), namespace.WithGlobalFlag(existing.IsGlobalNamespace))
	if err != nil {
		return false, err
	}
	//nolint:forbidigo // Namespace-level check: the restore job covers the whole namespace.
	if !ns.ActiveInCluster(currentCluster) {
		return false, serviceerror.NewNamespaceNotActive(detail.Info.Name, currentCluster, ns.ActiveClusterName(namespace.RoutingKey{}))
	}
	if err := validatePeers(detail); err != nil {
		return false, err
	}

	restore := detail.Info.GetRestoreInfo()
	if state == enumspb.NAMESPACE_STATE_REGISTERED &&
		restore.GetState() == enumspb.NAMESPACE_RESTORE_STATE_RUNNING &&
		restore.GetHostCluster() == currentCluster {
		return false, nil
	}
	detail.Info.RestoreInfo = &persistencespb.NamespaceRestoreInfo{
		RestoreId:   uuid.NewString(),
		HostCluster: currentCluster,
		State:       enumspb.NAMESPACE_RESTORE_STATE_RUNNING,
		StartTime:   timestamppb.New(d.timeSource.Now()),
	}
	return true, nil
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
	_, _, err := d.updateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{}, internalUpdateOptions{
		namespaceID: nsID,
		mutate: func(existing *persistence.GetNamespaceResponse) (bool, error) {
			restore := existing.Namespace.Info.GetRestoreInfo()
			if restore.GetRestoreId() != restoreID {
				return false, serviceerror.NewFailedPreconditionf("Restore %s is not the current restore of namespace %s.",
					restoreID, existing.Namespace.Info.Name)
			}
			switch restore.GetState() {
			case state:
				return false, nil
			case enumspb.NAMESPACE_RESTORE_STATE_RUNNING:
				restore.State = state
				restore.CloseTime = timestamppb.New(d.timeSource.Now())
				return true, nil
			default:
				return false, serviceerror.NewFailedPreconditionf("Restore %s of namespace %s is already %s.",
					restoreID, existing.Namespace.Info.Name, restore.GetState())
			}
		},
	})
	return err
}

// failRunningRestore marks the namespace's running restore FAILED, for a soft delete that abandons
// it. If the restore is hosted on another cluster, it returns *restoreHostedElsewhereError, so the
// soft delete runs there instead.
func (d *namespaceHandler) failRunningRestore(existing *persistence.GetNamespaceResponse) (bool, error) {
	restore := existing.Namespace.Info.GetRestoreInfo()
	if restore.GetState() != enumspb.NAMESPACE_RESTORE_STATE_RUNNING {
		return false, nil
	}
	if host := restore.GetHostCluster(); host != d.clusterMetadata.GetCurrentClusterName() {
		return false, &restoreHostedElsewhereError{hostCluster: host}
	}
	restore.State = enumspb.NAMESPACE_RESTORE_STATE_FAILED
	restore.CloseTime = timestamppb.New(d.timeSource.Now())
	return true, nil
}
