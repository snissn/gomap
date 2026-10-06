package main

import (
	"context"
	"errors"
	"fmt"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

const compactCommandWALSettleEndpoint = "compact-command-wal-settle-v1"

type compactCheckpointRefresh struct {
	Basis               *treedbdb.StateToken       `json:"basis"`
	Result              *treedbdb.StateToken       `json:"result"`
	NextLSNBefore       uint64                     `json:"next_lsn_before"`
	NextLSNAfter        uint64                     `json:"next_lsn_after"`
	RootsBefore         []treedbdb.RecoverableRoot `json:"roots_before"`
	RootsAfter          []treedbdb.RecoverableRoot `json:"roots_after"`
	Status              string                     `json:"status"`
	CheckpointStatus    string                     `json:"checkpoint_status"`
	SummaryBeforeStatus string                     `json:"summary_before_status"`
	SummaryAfterStatus  string                     `json:"summary_after_status"`
}

type compactEndpointGC struct {
	Status string                         `json:"status"`
	Stats  treedbdb.LeafGenerationGCStats `json:"stats"`
}

// Reports retain the original applied report and final dry-run audit. Completed
// means the fixed operations and cleanup succeeded; convergence is separate.
type compactEndpointReceipt struct {
	Schema        int                          `json:"schema"`
	Endpoint      string                       `json:"endpoint"`
	Status        string                       `json:"status"`
	Reports       []treedb.CompactStorageStats `json:"reports"`
	InitialStatus string                       `json:"initial_status"`
	Refresh       compactCheckpointRefresh     `json:"refresh"`
	LeafGC        compactEndpointGC            `json:"leaf_gc"`
	AuditStatus   string                       `json:"audit_status"`
	CleanupStatus string                       `json:"cleanup_status"`
	Error         string                       `json:"error,omitempty"`
}

func compactRootSummary(ctx context.Context, backend *treedbdb.DB) ([]treedbdb.RecoverableRoot, error) {
	roots, err := backend.CaptureRecoverableRootSetForInspection(ctx)
	if err != nil {
		return nil, err
	}
	defer roots.Release()
	return roots.Roots(), roots.Revalidate()
}

// Captures release their leases before every mutation. The existing refresh
// can lawfully do nothing or advance one durable slot without a new WAL frame.
// Its API has no context argument; cancellation is checked at its boundaries.
func refreshCompactCheckpoint(ctx context.Context, backend *treedbdb.DB, refresh *compactCheckpointRefresh) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	basis, ok := backend.StateToken()
	if !ok {
		return errors.New("missing state before checkpoint refresh")
	}
	refresh.Basis = &basis
	refresh.NextLSNBefore = backend.CommandWALNextLSN()
	refresh.SummaryBeforeStatus = "failed"
	var err error
	refresh.RootsBefore, err = compactRootSummary(ctx, backend)
	if err != nil {
		return err
	}
	refresh.SummaryBeforeStatus = "succeeded"
	if err := ctx.Err(); err != nil {
		return err
	}
	refresh.Status = "failed"
	if err := backend.RefreshCommandWALCheckpointFallback(); err != nil {
		return err
	}
	refresh.Status = "succeeded"
	if err := ctx.Err(); err != nil {
		return err
	}
	refresh.CheckpointStatus = "failed"
	if err := backend.Checkpoint(); err != nil {
		return err
	}
	refresh.CheckpointStatus = "succeeded"
	if err := ctx.Err(); err != nil {
		return err
	}
	result, ok := backend.StateToken()
	if !ok {
		return errors.New("missing state after checkpoint refresh")
	}
	refresh.Result = &result
	refresh.NextLSNAfter = backend.CommandWALNextLSN()
	if (result.CommitSeq != basis.CommitSeq && (basis.CommitSeq == ^uint64(0) || result.CommitSeq != basis.CommitSeq+1)) ||
		result.RootPageID != basis.RootPageID || result.SystemRootPageID != basis.SystemRootPageID ||
		result.AppliedCommandLSN != basis.AppliedCommandLSN || result.MaxEntryRevision != basis.MaxEntryRevision ||
		refresh.NextLSNBefore == 0 || refresh.NextLSNAfter != refresh.NextLSNBefore {
		return errors.New("checkpoint refresh changed root, RID or WAL authority")
	}
	refresh.SummaryAfterStatus = "failed"
	refresh.RootsAfter, err = compactRootSummary(ctx, backend)
	if err == nil {
		refresh.SummaryAfterStatus = "succeeded"
	}
	return err
}

// Exactly one applying compact, existing logged checkpoint fallback refresh,
// checkpoint, leaf GC, and dedicated dry-run plan. External wall/RSS includes
// Open and cleanup as well as all of these operations.
func runCompactCommandWALSettle(ctx context.Context, opts treedb.Options, compactOpts treedb.CompactStorageOptions) (receipt compactEndpointReceipt, err error) {
	receipt = compactEndpointReceipt{Schema: 1, Endpoint: compactCommandWALSettleEndpoint, Status: "failed",
		InitialStatus: "not_started", AuditStatus: "not_started", CleanupStatus: "not_started",
		LeafGC:  compactEndpointGC{Status: "not_started"},
		Refresh: compactCheckpointRefresh{Status: "not_started", CheckpointStatus: "not_started", SummaryBeforeStatus: "not_started", SummaryAfterStatus: "not_started"}}
	defer func() {
		if err != nil {
			receipt.Error = err.Error()
		} else {
			receipt.Status = "completed"
		}
	}()
	if err = ctx.Err(); err != nil {
		return receipt, err
	}
	if compactOpts.DryRun || (compactOpts.Mode != treedb.CompactStorageFull && compactOpts.Mode != treedb.CompactStorageExhaustive) {
		return receipt, errors.New("command-WAL settle requires applied full or exhaustive compaction")
	}
	// Actual persisted admission authority is checked before writable Open/replay.
	// No profile/default override may activate this endpoint on an unlogged store.
	mainDir := resolveMainDBDir(opts.Dir)
	required, gateErr := treedbdb.CommandWALRequiredFeatureEnabled(mainDir)
	if gateErr != nil {
		return receipt, gateErr
	}
	profile, present, profileErr := treedbdb.LoadPersistedDurabilityProfile(mainDir)
	if profileErr != nil {
		return receipt, profileErr
	}
	if !required || !present || (profile != treedbdb.ProfileCommandWALDurable && profile != treedbdb.ProfileCommandWALRelaxed) ||
		(opts.ResolvedProfile != "" && opts.ResolvedProfile != profile) {
		return receipt, fmt.Errorf("%w: command-WAL settle requires an existing command-WAL feature and compatible persisted profile", treedb.ErrCommandWALUnsupported)
	}
	if err = ctx.Err(); err != nil {
		return receipt, err
	}
	backend, cleanup, openErr := treedb.OpenBackend(opts)
	if openErr != nil {
		return receipt, openErr
	}
	defer func() {
		receipt.CleanupStatus = "failed"
		cleanupErr := cleanup()
		if cleanupErr == nil {
			receipt.CleanupStatus = "succeeded"
		}
		err = errors.Join(err, cleanupErr)
	}()
	if !backend.CommandWALEnabled() {
		return receipt, treedb.ErrCommandWALUnsupported
	}
	receipt.InitialStatus = "failed"
	initial, compactErr := backend.CompactStorage(ctx, compactOpts)
	receipt.Reports = append(receipt.Reports, initial)
	if compactErr != nil {
		return receipt, compactErr
	}
	receipt.InitialStatus = "succeeded"
	if err = refreshCompactCheckpoint(ctx, backend, &receipt.Refresh); err != nil {
		return receipt, err
	}
	if err = ctx.Err(); err != nil {
		return receipt, err
	}
	receipt.LeafGC.Status = "failed"
	receipt.LeafGC.Stats, err = backend.LeafGenerationGC(ctx, treedbdb.LeafGenerationGCOptions{})
	if err != nil {
		return receipt, err
	}
	receipt.LeafGC.Status = "succeeded"
	if err = ctx.Err(); err != nil {
		return receipt, err
	}
	receipt.AuditStatus = "failed"
	final, auditErr := backend.CompactStoragePlan(ctx, compactOpts)
	receipt.Reports = append(receipt.Reports, final)
	if auditErr != nil {
		return receipt, auditErr
	}
	receipt.AuditStatus = "succeeded"
	return receipt, ctx.Err()
}
