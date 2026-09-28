package raftfsm

import (
	"bytes"
	"context"

	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// CurrentCatalogVersion follows the FSM-owned DB across snapshot replacement.
// The caller's original DB handle is closed when a snapshot is installed.
func (f *FSM) CurrentCatalogVersion(ctx context.Context) (uint64, bool, error) {
	ctx = readBarrierContext(ctx)
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if f == nil {
		return 0, false, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if f.closed || f.db == nil {
		return 0, false, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	state := f.db.State()
	if state == nil {
		return 0, false, nil
	}
	return state.CommitSeq, true, nil
}

// AppliedIdempotencyGuardV1 returns the original deterministic guard and digest
// only from a durable, locally covered applied result. The FSM lock follows
// snapshot replacement, so the lookup cannot use a detached result store.
func (f *FSM) AppliedIdempotencyGuardV1(ctx context.Context, key []byte) (uint64, raftentry.CommandDigestV1, bool, error) {
	ctx = readBarrierContext(ctx)
	if err := ctx.Err(); err != nil {
		return 0, raftentry.CommandDigestV1{}, false, err
	}
	if f == nil {
		return 0, raftentry.CommandDigestV1{}, false, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return 0, raftentry.CommandDigestV1{}, false, err
	}
	if f.closed || f.db == nil || f.results == nil {
		return 0, raftentry.CommandDigestV1{}, false, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM result store is not open")
	}
	record, found, err := f.results.LookupApplyResultByIdempotencyKey(key)
	if err != nil || !found {
		return 0, raftentry.CommandDigestV1{}, false, err
	}
	state, ok := f.db.StateToken()
	if !ok || !record.HasExpectedCatalogVersion || record.AppliedCommandLSN == 0 || state.AppliedCommandLSN < record.AppliedCommandLSN ||
		(record.Result.Status != raftentry.ApplyStatusApplied && record.Result.Status != raftentry.ApplyStatusAlreadyApplied) ||
		record.Result.CommandDigest != record.CommandDigest {
		return 0, raftentry.CommandDigestV1{}, false, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "applied idempotency result has no covered original guard")
	}
	return record.ExpectedCatalogVersion, record.CommandDigest, true, nil
}

// PreflightCommandEntryV1 adapts the raftcluster pre-commit deterministic
// preflight request into the local FSM apply preflight shape.
func (f *FSM) PreflightCommandEntryV1(ctx context.Context, req raftcluster.CommandEntryPreflightRequestV1) (raftcluster.CommandEntryPreflightResultV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return raftcluster.CommandEntryPreflightResultV1{}, ctx.Err()
	default:
	}
	if f == nil {
		return raftcluster.CommandEntryPreflightResultV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.closed {
		return raftcluster.CommandEntryPreflightResultV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is closed")
	}
	if f.db == nil {
		return raftcluster.CommandEntryPreflightResultV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	if req.GroupID != "" && req.GroupID != f.cluster.GroupID {
		return raftcluster.CommandEntryPreflightResultV1{}, codedError(raftentry.ErrorRejectedConflictV1, "preflight group %q does not match local group %q", req.GroupID, f.cluster.GroupID)
	}
	if req.NodeID != "" && req.NodeID != f.cluster.NodeID {
		return raftcluster.CommandEntryPreflightResultV1{}, codedError(raftentry.ErrorRejectedConflictV1, "preflight node %q does not match local node %q", req.NodeID, f.cluster.NodeID)
	}
	if len(req.EntryBytes) == 0 {
		return raftcluster.CommandEntryPreflightResultV1{}, codedError(raftentry.ErrorMalformedEntryV1, "empty command entry")
	}
	meta := raftapply.ApplyMetadataV1{
		LocalDurabilityBoundary:  raftapply.LocalDurabilityCommandWALV1,
		SyncLocalCommandWAL:      req.SyncLocalCommandWAL,
		CurrentCatalogVersion:    req.CurrentCatalogVersion,
		HasCurrentCatalogVersion: req.HasCurrentCatalogVersion,
		ScopeRule:                f.scopeRule,
		DatabaseScope:            f.database,
		CatalogScope:             f.catalog,
		RequestMetadata:          cloneRequestMetadataV1(req.RequestMetadata),
		ExpectedTarget:           cloneExpectedTargetV1(req.ExpectedTarget),
	}
	opts := raftapply.Options{DecodeLimits: f.decodeLimits, ResultStore: f.results}
	var result raftapply.PreflightResultV1
	var err error
	if len(req.DecodedEntry.Bytes) != 0 {
		if !bytes.Equal(req.DecodedEntry.Bytes, req.EntryBytes) {
			return raftcluster.CommandEntryPreflightResultV1{}, codedError(raftentry.ErrorMalformedEntryV1, "decoded command entry does not match raw entry bytes")
		}
		result, err = raftapply.PreflightDecodedCommandEntryV1(f.db, req.DecodedEntry, meta, opts)
	} else {
		result, err = raftapply.PreflightCommandEntryV1(f.db, req.EntryBytes, meta, opts)
	}
	if err != nil {
		return raftcluster.CommandEntryPreflightResultV1{}, err
	}
	return raftcluster.CommandEntryPreflightResultV1{KnownIdempotencyReplay: result.KnownIdempotencyReplay}, nil
}

// ApplyCommittedCommandEntryV1 adapts the raftcluster provider-delivered
// committed command shape into the local FSM committed-entry shape.
func (f *FSM) ApplyCommittedCommandEntryV1(ctx context.Context, entry raftcluster.CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	return f.ApplyCommittedEntryV1(CommittedEntryFromClusterV1(entry))
}

func CommittedEntryFromClusterV1(entry raftcluster.CommittedCommandEntryV1) CommittedEntryV1 {
	return CommittedEntryV1{
		Type:                     EntryTypeCommandEntryV1,
		Term:                     entry.Term,
		Index:                    entry.Index,
		Bytes:                    append([]byte(nil), entry.Bytes...),
		CurrentCatalogVersion:    entry.CurrentCatalogVersion,
		HasCurrentCatalogVersion: entry.HasCurrentCatalogVersion,
		SyncLocalCommandWAL:      entry.SyncLocalCommandWAL,
		RequestMetadata:          cloneRequestMetadataV1(entry.RequestMetadata),
		ExpectedTarget:           cloneExpectedTargetV1(entry.ExpectedTarget),
	}
}
