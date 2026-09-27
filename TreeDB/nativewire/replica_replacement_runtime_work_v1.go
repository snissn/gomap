package nativewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// One phase's native call survives short control requests. Polls do not create
// another worker, and Close keeps stores alive through its actual return. The
// durable catalog/receiver state, not this process-local result, authorizes work.
type replacementNativeWorkV1 struct {
	operation string
	phase     string
	cancel    context.CancelFunc
	done      chan struct{}
	seed      *raftcluster.ReplacementSnapshotSeedV1
	err       error
}

type replacementNativeSlotV1 struct {
	mu     sync.Mutex
	closed bool
	work   *replacementNativeWorkV1
}

func (d *fixedPeerDataV1) replacementWorkV1(operation, phase string, work func(context.Context) (*raftcluster.ReplacementSnapshotSeedV1, error), reply *fixedPeerReplyV1) error {
	d.replacementWork.mu.Lock()
	defer d.replacementWork.mu.Unlock()
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
				reply.ReplacementSeed = current.seed
				return current.err
			}
			if current.phase != "seed" || phase != "install" || current.err != nil {
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
	ctx, cancel := context.WithTimeout(context.Background(), limits.Lifetime)
	current = &replacementNativeWorkV1{operation: operation, phase: phase, cancel: cancel, done: make(chan struct{})}
	d.replacementWork.work = current
	go func() {
		defer close(current.done)
		defer cancel()
		current.seed, current.err = work(ctx)
	}()
	reply.ReplacementPending = true
	return nil
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

// The operation name is a fixed digest of canonical authority, never raw user
// text. At most one operation directory can exist in this group's seed root;
// unresolved artifacts from another operation cannot be aliased or overwritten.
func (r *FixedPeerTCPRuntimeV1) replacementSeedStoreV1(command raftplacement.ReplicaReplacementBeginV1) (*hraft.FileSnapshotStore, string, error) {
	group, err := r.validateReplacementBeginV1(command)
	if err != nil {
		return nil, "", err
	}
	cfg, err := raftcluster.Validate(raftcluster.Config{Dir: filepath.Join(r.config.DataRoot, string(group.ID)), ClusterDir: r.config.RaftRoot, DisableSideStores: true, NodeID: r.config.NodeID, GroupID: group.ID, Peers: group.Peers, Features: group.Features})
	if err != nil {
		return nil, "", err
	}
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(raw)
	name := hex.EncodeToString(sum[:])
	root := filepath.Join(cfg.Layout.GroupDir, "replacement-seed")
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, "", err
	}
	entries, err := replacementSeedDirectoryEntriesV1(root)
	if err != nil {
		return nil, "", err
	}
	if len(entries) > 1 || len(entries) == 1 && (entries[0].Name() != name || !entries[0].IsDir() || entries[0].Type()&os.ModeSymlink != 0) {
		return nil, "", raftplacement.ErrCatalogMetaConflict
	}
	path := filepath.Join(root, name)
	if _, err := os.Lstat(path); err == nil {
		if _, err := replacementSeedDirectoryEntriesV1(filepath.Join(path, "snapshots")); err != nil {
			return nil, "", err
		}
	} else if !os.IsNotExist(err) {
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
	store, path, err := r.replacementSeedStoreV1(command)
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
	selected, err := d.provider.Snapshot(ctx)
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
	store, _, err := r.replacementSeedStoreV1(command)
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
		if err := d.replacementWorkV1(command.OperationID, "verify", func(context.Context) (*raftcluster.ReplacementSnapshotSeedV1, error) {
			return state.Seed, d.prejoin.reconcileInstalled()
		}, reply); err != nil {
			return err
		}
		if reply.ReplacementPending {
			return nil
		}
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
