package nativewire

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// An existing receiver record is the only restart authority. A request cannot
// turn an unrelated persisted group into a fresh seed target.
func (r *FixedPeerTCPRuntimeV1) openAuthorizedReplacementReceiverV1(cfg raftcluster.ResolvedConfig, state raftplacement.ReplicaReplacementStateV1) (*replacementReceiverOwnerV1, error) {
	if state.Seed == nil || state.Begin.NewPeer.ID != r.config.NodeID || state.Begin.GroupID != cfg.GroupID || state.Phase == raftplacement.ReplicaReplacementBegunV1 {
		return nil, raftcluster.ErrAdmissionUnavailable
	}
	recordPath := filepath.Join(cfg.Layout.GroupDir, replacementReceiverRecordNameV1)
	info, err := os.Lstat(recordPath)
	if err == nil {
		if !info.Mode().IsRegular() {
			return nil, raftcluster.ErrInvalidConfig
		}
	} else if errors.Is(err, os.ErrNotExist) {
		// No owner means both storage roots must be fresh, not just missing a
		// native snapshot. Logs-only join is deliberately not a seed receipt.
		for _, dir := range []string{cfg.Dir, cfg.Layout.GroupDir} {
			entries, readErr := os.ReadDir(dir)
			if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
				return nil, readErr
			}
			if len(entries) != 0 {
				return nil, raftcluster.ErrInvalidConfig
			}
		}
	} else {
		return nil, err
	}
	if err := os.MkdirAll(cfg.Layout.GroupDir, 0700); err != nil {
		return nil, err
	}
	if err := syncFixedPeerDirectoryV1(filepath.Dir(cfg.Layout.GroupDir)); err != nil {
		return nil, err
	}
	return openReplacementReceiverOwnerV1(cfg.Layout.GroupDir, state.Begin, *state.Seed)
}

// Pure status for an authenticated original group member. It cannot reverify,
// install, or advance a phase, so enrollment can independently check the
// durable cutoff without granting mutation authority to a data-only node.
func (r *FixedPeerTCPRuntimeV1) replacementReceiverCutoffV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, reply *fixedPeerReplyV1) error {
	return r.replacementReceiverCutoffAtIndexV1(ctx, command, reply, 0)
}

func (r *FixedPeerTCPRuntimeV1) replacementReceiverCutoffAtIndexV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, reply *fixedPeerReplyV1, minimumIndex uint64) error {
	state, err := r.replacementStateAuthorityV1(ctx, command)
	if err != nil {
		return err
	}
	d := r.localDataV1(command.GroupID)
	if !replacementHasEnrollmentV1(state.Phase) || state.Seed == nil || command.NewPeer.ID != r.config.NodeID || d == nil || d.replacementID != command.OperationID || d.prejoin == nil || d.replacementReceiver == nil {
		return raftcluster.ErrAdmissionUnavailable
	}
	if err := r.verifyReplacementHostedOwnerV1(ctx, state, max(minimumIndex, state.Seed.Manifest.LastIncludedIndex)); err != nil {
		return err
	}
	owner := d.replacementReceiver
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.parent == nil || owner.poison != nil || owner.record.Phase != replacementReceiverAddIntentV1 || !raftcluster.SameReplacementSnapshotSeedV1(owner.record.Seed, *state.Seed) {
		return raftcluster.ErrAdmissionUnavailable
	}
	wanted, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return err
	}
	actual, err := raftplacement.EncodeReplicaReplacementBeginV1(owner.record.Begin)
	if err != nil || string(actual) != string(wanted) {
		return raftplacement.ErrCatalogMetaConflict
	}
	reply.ReplacementSeed = state.Seed
	return nil
}

var errReplacementReceiverUnopenedV1 = errors.New("nativewire: prior replacement receiver requires current membership")

// Startup inspects one bounded record per configured group, without opening a
// provider or treating the historical receiver as current membership authority.
// Retain the marker in the existing data inventory so retirement cannot turn a
// restarted former replica into an apparently fresh routing-only gateway.
func (r *FixedPeerTCPRuntimeV1) captureUnopenedReplacementReceiverV1(group raftcluster.GroupID) (*fixedPeerDataV1, error) {
	// This is the deterministic identity layout used by raftcluster.storageLayout;
	// all path components came from the validated immutable fixed configuration.
	dir := filepath.Join(r.config.RaftRoot, "nodes", string(r.config.NodeID), "groups", string(group))
	if _, err := os.Lstat(filepath.Join(dir, replacementReceiverRecordNameV1)); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	record, err := r.readCurrentReplacementReceiverV1(dir, group)
	if err != nil {
		return nil, err
	}
	return &fixedPeerDataV1{startErr: errReplacementReceiverUnopenedV1, replacementID: record.Begin.OperationID}, nil
}

func (r *FixedPeerTCPRuntimeV1) readCurrentReplacementReceiverV1(dir string, group raftcluster.GroupID) (replacementReceiverRecordV1, error) {
	parent, err := rootpublication.OpenStableParent(dir)
	if err != nil {
		return replacementReceiverRecordV1{}, err
	}
	file, err := rootpublication.OpenStableChildFile(parent, replacementReceiverRecordNameV1, os.O_RDONLY, 0)
	if err != nil {
		return replacementReceiverRecordV1{}, errors.Join(err, parent.Close())
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, replacementReceiverRecordMaxBytesV1+1))
	if err := errors.Join(readErr, file.Close(), parent.Close()); err != nil {
		return replacementReceiverRecordV1{}, err
	}
	record, err := decodeReplacementReceiverRecordV1(raw)
	if err != nil {
		return record, err
	}
	if record.Begin.ConfigDigest != r.client.digest || record.Begin.GroupID != group || record.Begin.NewPeer.ID != r.config.NodeID {
		return record, raftcluster.ErrInvalidConfig
	}
	return record, nil
}

// Current membership, fenced by the catalog caller, permits reopening a prior
// receiver after later operations compact its BEGIN. Its exact durable record
// still supplies the permanent seed floor; a request cannot replace that record.
func (r *FixedPeerTCPRuntimeV1) openCurrentReplacementReceiverV1(cfg raftcluster.ResolvedConfig) (*replacementReceiverOwnerV1, error) {
	record, err := r.readCurrentReplacementReceiverV1(cfg.Layout.GroupDir, cfg.GroupID)
	if err != nil {
		return nil, err
	}
	if record.Phase != replacementReceiverAddIntentV1 {
		return nil, raftcluster.ErrAdmissionUnavailable
	}
	return openReplacementReceiverOwnerV1(cfg.Layout.GroupDir, record.Begin, record.Seed)
}
