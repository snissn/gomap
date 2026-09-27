package nativewire

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
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
	state, err := r.replacementStateAuthorityV1(ctx, command)
	if err != nil {
		return err
	}
	d := r.localDataV1(command.GroupID)
	if !replacementHasEnrollmentV1(state.Phase) || state.Seed == nil || command.NewPeer.ID != r.config.NodeID || d == nil || d.replacementID != command.OperationID || d.prejoin == nil || d.replacementReceiver == nil {
		return raftcluster.ErrAdmissionUnavailable
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
