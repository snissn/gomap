package nativewire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	hraft "github.com/hashicorp/raft"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// One phase's native call survives short control requests. Polls do not create
// another worker, and Close keeps stores alive through its actual return. The
// durable catalog/receiver state, not this process-local result, authorizes work.
type replacementNativeWorkV1 struct {
	operation  string
	phase      string
	cancel     context.CancelFunc
	done       chan struct{}
	seed       *raftcluster.ReplacementSnapshotSeedV1
	err        error
	lease      peerWorkLeaseV1
	cleanupErr error
}

type replacementNativeSlotV1 struct {
	mu            sync.Mutex
	closed        bool
	work          *replacementNativeWorkV1
	begin         raftplacement.ReplicaReplacementBeginV1
	ownerSource   *CollectionVectorPartitionGenerationSourceV1
	ownerDB       *backenddb.DB
	ownerEndpoint *replacementOwnerEndpointV1
}

// Only a freshly fenced committed BEGIN may supersede the process-local slot.
// The previous actual worker and every cleanup owner must have finished first.
func (d *fixedPeerDataV1) authorizeReplacementWorkLockedV1(command raftplacement.ReplicaReplacementBeginV1) error {
	slot := &d.replacementWork
	if slot.closed {
		return raftcluster.ErrAdmissionUnavailable
	}
	previous, _ := raftplacement.EncodeReplicaReplacementBeginV1(slot.begin)
	wanted, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return err
	}
	if string(previous) == string(wanted) {
		return nil
	}
	if slot.begin.OperationID != "" {
		if command.ConfigDigest != slot.begin.ConfigDigest || command.GroupID != slot.begin.GroupID || command.ExpectedEpoch <= slot.begin.ExpectedEpoch {
			return raftplacement.ErrCatalogMetaConflict
		}
	}
	if slot.work != nil {
		if slot.begin.OperationID == "" {
			return raftplacement.ErrCatalogMetaConflict
		}
		select {
		case <-slot.work.done:
		default:
			return raftcluster.ErrAdmissionUnavailable
		}
		if slot.work.cleanupErr != nil {
			return slot.work.cleanupErr
		}
	}
	if d.fsm.SnapshotWorkReleasePendingV1() {
		return raftcluster.ErrAdmissionUnavailable
	}
	slot.work, slot.begin = nil, command
	return nil
}

func (d *fixedPeerDataV1) replacementAuthorizedWorkV1(command raftplacement.ReplicaReplacementBeginV1, phase string, work func(context.Context) (*raftcluster.ReplacementSnapshotSeedV1, error), reply *fixedPeerReplyV1) error {
	d.replacementWork.mu.Lock()
	defer d.replacementWork.mu.Unlock()
	if err := d.authorizeReplacementWorkLockedV1(command); err != nil {
		return err
	}
	return d.replacementWorkLockedV1(command.OperationID, phase, nil, work, reply)
}

func (d *fixedPeerDataV1) replacementAuthorizedSeedWorkV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, preflight func(context.Context) error, work func(context.Context) (*raftcluster.ReplacementSnapshotSeedV1, error), reply *fixedPeerReplyV1) error {
	d.replacementWork.mu.Lock()
	defer d.replacementWork.mu.Unlock()
	if err := d.authorizeReplacementWorkLockedV1(command); err != nil {
		return err
	}
	return d.replacementWorkLockedV1(command.OperationID, "seed", func() error { return preflight(ctx) }, work, reply)
}

func (d *fixedPeerDataV1) replacementWorkLockedV1(operation, phase string, preflight func() error, work func(context.Context) (*raftcluster.ReplacementSnapshotSeedV1, error), reply *fixedPeerReplyV1) error {
	if d.replacementWork.closed {
		return raftcluster.ErrAdmissionUnavailable
	}
	current := d.replacementWork.work
	if current != nil {
		if current.operation != operation {
			return raftplacement.ErrCatalogMetaConflict
		}
		select {
		case <-current.done:
			if current.phase == phase {
				seedRetry := phase == "seed" && errors.Is(current.err, raftcluster.ErrReadBarrierNotSatisfied) && !d.fsm.SnapshotWorkReleasePendingV1()
				installRetry := phase == "install" && errors.Is(current.err, raftcluster.ErrReplacementInstallNotSentV1)
				if current.cleanupErr == nil && (seedRetry || installRetry) {
					// The seed refusal preceded native Create; the install refusal
					// preceded native send. Neither can duplicate an in-flight operation.
					d.replacementWork.work = nil
					if installRetry {
						// Surface this completed refusal to the client before its
						// bounded retry starts another exact-operation worker.
						return current.err
					}
				} else {
					reply.ReplacementSeed = current.seed
					return current.err
				}
			}
			if d.replacementWork.work != nil && (current.phase != "seed" || phase != "install" || current.err != nil) {
				return raftcluster.ErrAdmissionUnavailable
			}
		default:
			if current.phase != phase {
				return raftcluster.ErrAdmissionUnavailable
			}
			reply.ReplacementPending = true
			return nil
		}
	}
	limits, err := d.fsm.SnapshotOperationLimitsV1()
	if err != nil {
		return err
	}
	var lease peerWorkLeaseV1
	if phase == "seed" {
		if transport, ok := d.transport.(*peerRaftTransportV1); ok {
			bytes, err := transport.snapshotBytes(0)
			if err != nil {
				return err
			}
			lease, err = transport.admission.work(transport.scope, peerSnapshotsV1, bytes)
			if err != nil {
				// Saturation is a retryable admission refusal, not this operation's
				// cached terminal result. Polls above never acquire another lease.
				return err
			}
		}
	}
	if preflight != nil {
		if err := preflight(); err != nil {
			lease.release()
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), limits.Lifetime)
	current = &replacementNativeWorkV1{operation: operation, phase: phase, cancel: cancel, done: make(chan struct{}), lease: lease}
	d.replacementWork.work = current
	go func() {
		defer close(current.done)
		defer cancel()
		current.seed, current.err = work(ctx)
		if phase == "seed" {
			if raftcluster.ReplacementSnapshotCleanupRequiredV1(current.err) {
				// Keep the opaque native owner in err and its budget until process
				// restart. Close reports this debt; it cannot prove native cleanup.
				current.cleanupErr = current.err
			} else {
				current.cleanupErr = d.fsm.HandoffSnapshotWorkReleaseV1(current.lease.release)
				current.err = errors.Join(current.err, current.cleanupErr)
			}
		}
	}()
	reply.ReplacementPending = true
	return nil
}

// Check the operation-owned artifact before requiring a newer command. An
// identical BEGIN may have copied its native seed before catalog publication
// became ambiguous; that exact retained seed remains a valid retry source.
func (r *FixedPeerTCPRuntimeV1) replacementSeedPreflightV1(ctx context.Context, state raftplacement.ReplicaReplacementStateV1, d *fixedPeerDataV1) error {
	store, path, err := r.replacementSeedStoreV1(state)
	if err != nil {
		return err
	}
	metas, err := store.List()
	if err != nil {
		return err
	}
	if len(metas) != 0 {
		return nil // the worker verifies exact seed identity and contents
	}
	entries, err := replacementSeedDirectoryEntriesV1(filepath.Join(path, "snapshots"))
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return fmt.Errorf("%w: incomplete retained replacement seed", raftcluster.ErrAdmissionUnavailable)
	}
	return d.provider.ReplacementSnapshotReadyV1(ctx)
}

func (d *fixedPeerDataV1) cancelReplacementWorkV1() <-chan struct{} {
	d.replacementWork.mu.Lock()
	defer d.replacementWork.mu.Unlock()
	d.replacementWork.closed = true
	if d.replacementWork.work == nil {
		return nil
	}
	d.replacementWork.work.cancel()
	return d.replacementWork.work.done
}

// Called after actual worker completion; an unresolved native owner is not
// released by runtime Close or a later poll. Process restart is required.
func (d *fixedPeerDataV1) replacementCleanupErrorV1() error {
	d.replacementWork.mu.Lock()
	defer d.replacementWork.mu.Unlock()
	if work := d.replacementWork.work; work != nil {
		select {
		case <-work.done:
			return work.cleanupErr
		default:
			return raftcluster.ErrAdmissionUnavailable
		}
	}
	return nil
}

// The operation name is a fixed digest of canonical authority, never raw user
// text. At most one operation directory can exist in this group's seed root;
// unresolved artifacts from another operation cannot be aliased or overwritten.
func (r *FixedPeerTCPRuntimeV1) replacementSeedStoreV1(state raftplacement.ReplicaReplacementStateV1) (*hraft.FileSnapshotStore, string, error) {
	command := state.Begin
	group, err := r.replacementGroupV1(state, true)
	if err != nil {
		return nil, "", err
	}
	cfg, err := raftcluster.Validate(raftcluster.Config{Dir: filepath.Join(r.config.DataRoot, string(group.ID)), ClusterDir: r.config.RaftRoot, DisableSideStores: true, NodeID: r.config.NodeID, GroupID: group.ID, Peers: group.Peers, Features: group.Features})
	if err != nil {
		return nil, "", err
	}
	root := filepath.Join(cfg.Layout.GroupDir, "replacement-seed")
	path, err := prepareReplacementSeedDirectoryV1(root, command)
	if err != nil {
		return nil, "", err
	}
	store, err := hraft.NewFileSnapshotStore(path, 1, io.Discard)
	if err != nil {
		return nil, "", err
	}
	for _, dir := range []string{filepath.Join(path, "snapshots"), path, root, cfg.Layout.GroupDir} {
		if err := syncFixedPeerDirectoryV1(dir); err != nil {
			return nil, "", err
		}
	}
	return store, path, nil
}

func (r *FixedPeerTCPRuntimeV1) deriveReplacementSeedV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, d *fixedPeerDataV1) (*raftcluster.ReplacementSnapshotSeedV1, error) {
	state, err := r.replacementStateAuthorityV1(ctx, command)
	if err != nil {
		return nil, err
	}
	if state.Seed != nil && state.Seed.SourceNodeID != r.config.NodeID {
		return nil, raftcluster.ErrRouteTargetUnknown
	}
	store, path, err := r.replacementSeedStoreV1(state)
	if err != nil {
		return nil, err
	}
	limits, err := d.fsm.SnapshotOperationLimitsV1()
	if err != nil {
		return nil, err
	}
	metas, err := store.List()
	if err != nil {
		return nil, err
	}
	if len(metas) > 0 {
		seed, err := raftcluster.InspectReplacementSnapshotSeedV1(ctx, store, command.OldNodeID, command.NewPeer.ID, command.NewPeer.Address, limits.MaxBytes)
		if err != nil {
			return nil, err
		}
		if seed.SourceNodeID != r.config.NodeID || seed.Manifest.GroupID != command.GroupID || state.Seed != nil && !raftcluster.SameReplacementSnapshotSeedV1(seed, *state.Seed) {
			return nil, raftplacement.ErrCatalogMetaConflict
		}
		return &seed, nil
	}
	if state.Phase != raftplacement.ReplicaReplacementBegunV1 || state.Seed != nil {
		return nil, raftcluster.ErrInvalidSnapshotManifest
	}
	// An interrupted native copy remains an explicit unready operation. It is
	// never silently replaced by another seed, and retries cannot accumulate it.
	entries, err := replacementSeedDirectoryEntriesV1(filepath.Join(path, "snapshots"))
	if err != nil {
		return nil, err
	}
	if len(entries) != 0 {
		return nil, fmt.Errorf("%w: incomplete retained replacement seed", raftcluster.ErrAdmissionUnavailable)
	}
	selected, err := d.provider.SnapshotForReplacementV1(ctx)
	if err != nil {
		return nil, err
	}
	if err := d.fsm.AdmitSnapshotRetainedCopyV1(path, selected.SizeBytes); err != nil {
		return nil, err
	}
	seed, err := d.provider.RetainReplacementSnapshotSeedV1(ctx, selected, store, d.transport, command.OldNodeID, command.NewPeer.ID, command.NewPeer.Address, limits.MaxBytes)
	if err != nil {
		return nil, err
	}
	return &seed, nil
}

func (r *FixedPeerTCPRuntimeV1) runReplacementInstallV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, d *fixedPeerDataV1) (*raftcluster.ReplacementSnapshotSeedV1, error) {
	state, err := r.replacementStateAuthorityV1(ctx, command)
	if err != nil {
		return nil, err
	}
	if state.Phase != raftplacement.ReplicaReplacementSeededV1 || state.Seed == nil || state.Seed.SourceNodeID != r.config.NodeID {
		return nil, raftcluster.ErrAdmissionUnavailable
	}
	store, _, err := r.replacementSeedStoreV1(state)
	if err != nil {
		return nil, err
	}
	err = d.provider.InstallReplacementSeedV1(ctx, *state.Seed, store, d.transport, command.OldNodeID, command.NewPeer)
	return state.Seed, err
}

func (r *FixedPeerTCPRuntimeV1) replacementReceiverStatusV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, reply *fixedPeerReplyV1) error {
	state, err := r.replacementStateAuthorityV1(ctx, command)
	if err != nil {
		return err
	}
	d := r.localDataV1(command.GroupID)
	if d == nil || d.prejoin == nil || d.replacementReceiver == nil || state.Seed == nil || command.NewPeer.ID != r.config.NodeID || !raftcluster.SameReplacementSnapshotSeedV1(d.prejoin.seed, *state.Seed) {
		return raftcluster.ErrAdmissionUnavailable
	}
	d.prejoin.mu.Lock()
	phase, verified, inflight := d.prejoin.phase, d.prejoin.verified, d.prejoin.inflight
	d.prejoin.mu.Unlock()
	if inflight {
		reply.ReplacementPending = true
		return nil
	}
	if phase == replacementReceiverPreparedV1 {
		return nil
	}
	if !verified && phase != replacementReceiverAddIntentV1 {
		if err := d.replacementAuthorizedWorkV1(command, "verify", func(context.Context) (*raftcluster.ReplacementSnapshotSeedV1, error) {
			return state.Seed, d.prejoin.reconcileInstalled()
		}, reply); err != nil {
			return err
		}
		if reply.ReplacementPending {
			return nil
		}
	}
	if err := r.verifyReplacementHostedOwnerV1(ctx, state, state.Seed.Manifest.LastIncludedIndex); err != nil {
		return err
	}
	reply.ReplacementInstalled = true
	reply.ReplacementSeed = state.Seed
	return nil
}

// Bound malformed/unresolved directory inspection before native List can scan.
func replacementSeedDirectoryEntriesV1(path string) ([]os.DirEntry, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, raftcluster.ErrInvalidConfig
	}
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	entries, err := dir.ReadDir(2)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if len(entries) > 1 {
		return nil, raftcluster.ErrAdmissionUnavailable
	}
	return entries, nil
}
