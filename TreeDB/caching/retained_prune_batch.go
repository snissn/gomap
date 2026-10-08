package caching

import (
	"context"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/pager"
)

type retainedPruneCandidate struct {
	path     string
	size     int64
	id       uint32
	hasID    bool
	observed bool
}

type retainedPruneMembershipCounters struct {
	CertifiedRoots, UncoveredRoots, FullRootScans, Records, PointerProjections, PhysicalBytes uint64
	BackendRecords, CacheRecords, GCRecords                                                   uint64
	GCCalls, GCCaptures, Requested, Eligible, Marked, Pending, Deleted                        uint64
	FallbackReason                                                                            string
	ProofStage                                                                                string
}

func (db *DB) observeRetainedPruneMembership(out retainedValueLogPruneStats) {
	a, b := out.ScanStats.Membership, out.GCStats.Membership
	last := retainedPruneMembershipCounters{
		CertifiedRoots: a.CertifiedRoots + b.CertifiedRoots, UncoveredRoots: a.UncoveredRoots + b.UncoveredRoots,
		FullRootScans: a.FullRootScans + b.FullRootScans, Records: a.RecordsScanned + b.RecordsScanned,
		PointerProjections: a.PointerProjections + b.PointerProjections, PhysicalBytes: a.PhysicalBytesRead + b.PhysicalBytesRead,
		GCCalls: out.GCCalls, GCCaptures: out.GCStats.RecoverableCaptures,
		Requested: uint64(len(out.GCStats.RequestedFileIDs)), Eligible: uint64(len(out.GCStats.EligibleFileIDs)),
		Marked: uint64(len(out.GCStats.ZombieMarkedFileIDs)), Pending: uint64(len(out.PendingFileIDs)), Deleted: uint64(len(out.DeletedFileIDs)),
		FallbackReason: a.LastFallbackReason,
		BackendRecords: a.RecordsScanned, GCRecords: b.RecordsScanned,
		ProofStage: out.ScanStats.ProofStage,
	}
	// In the certified path Records contains only detached-cache projection.
	// Legacy full scans retain their existing record counter without claiming
	// that all of their work belongs to this boundary.
	if out.Mode == retainedPruneModeCertifiedMembership {
		last.CacheRecords = uint64(out.ScanStats.Records)
	}
	if last.ProofStage == "" {
		last.ProofStage = "not_started"
	}
	if b.LastFallbackReason != "" {
		last.FallbackReason = b.LastFallbackReason
	}
	db.retainedPruneMembershipMu.Lock()
	defer db.retainedPruneMembershipMu.Unlock()
	db.retainedPruneMembershipLast = last
	t := &db.retainedPruneMembershipTotals
	t.CertifiedRoots += last.CertifiedRoots
	t.UncoveredRoots += last.UncoveredRoots
	t.FullRootScans += last.FullRootScans
	t.Records += last.Records
	t.BackendRecords += last.BackendRecords
	t.CacheRecords += last.CacheRecords
	t.GCRecords += last.GCRecords
	t.PointerProjections += last.PointerProjections
	t.PhysicalBytes += last.PhysicalBytes
	t.GCCalls += last.GCCalls
	t.GCCaptures += last.GCCaptures
	t.Requested += last.Requested
	t.Eligible += last.Eligible
	t.Marked += last.Marked
	t.Pending += last.Pending
	t.Deleted += last.Deleted
	t.FallbackReason = last.FallbackReason
}

func (c retainedPruneMembershipCounters) appendStats(stats map[string]string, prefix string) {
	for key, value := range map[string]uint64{
		"certified_roots": c.CertifiedRoots, "uncovered_roots": c.UncoveredRoots, "full_root_scans": c.FullRootScans,
		"fallback_records": c.Records, "pointer_projections": c.PointerProjections, "physical_bytes_read": c.PhysicalBytes,
		"backend_records": c.BackendRecords, "cache_records": c.CacheRecords, "gc_records": c.GCRecords,
		"gc_calls": c.GCCalls, "gc_captures": c.GCCaptures, "requested_ids": c.Requested, "eligible_ids": c.Eligible,
		"marked_ids": c.Marked, "pending_ids": c.Pending, "deleted_ids": c.Deleted,
	} {
		stats["treedb.cache.vlog_retained_prune.membership."+prefix+key] = fmt.Sprint(value)
	}
	stats["treedb.cache.vlog_retained_prune.membership."+prefix+"fallback_reason"] = c.FallbackReason
	if prefix == "last_" {
		stage := c.ProofStage
		if stage == "" {
			stage = "not_started"
		}
		stats["treedb.cache.vlog_retained_prune.membership.last_proof_stage"] = stage
	}
}

// collectRetainedPruneProtectedValueLogIDs returns conservative physical
// presence, including older selectable roots and pending recovery resources.
// Unlike the exact visible-live collector, it may include raw leaf identities;
// only main-domain retained candidates are compared against it. Detached cache
// roots still require exact projection even when their numeric IDs match a
// certified backend root. Destructive GC recaptures and revalidates authority.
func (db *DB) collectRetainedPruneProtectedValueLogIDs(ctx context.Context, lastWrite int64, scanStats *valueLogLiveIDScanStats) (map[uint32]struct{}, error) {
	if scanStats != nil {
		scanStats.ProofStage = "backend_membership"
	}
	if db == nil || !db.valueLogEnabled() {
		return make(map[uint32]struct{}), nil
	}
	if err := retainedPruneContextErr(ctx); err != nil {
		return nil, err
	}
	if refresher, ok := db.backend.(valueLogSetRefresher); ok {
		if err := refresher.RefreshValueLogSet(); err != nil {
			return nil, err
		}
	}
	ctx, cancel := db.retainedPruneLiveIDScanContext(ctx, lastWrite, 0)
	defer cancel()
	snapper, ok := db.backend.(interface{ AcquireSnapshot() *backenddb.Snapshot })
	if !ok {
		return nil, backenddb.ErrRecoverableRootSetStale
	}
	snap := snapper.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrRecoverableRootSetStale
	}
	defer snap.Close()
	state, p := snap.State(), snap.Pager()
	if state == nil || p == nil {
		return nil, backenddb.ErrRecoverableRootSetStale
	}
	protected, work, err := snap.RecoverableValueLogMembership(ctx)
	if scanStats != nil {
		scanStats.Membership = work
	}
	if err != nil {
		return nil, retainedPruneContextCause(ctx, err)
	}
	db.mu.RLock()
	publishedRoots := clonePublishedRootSet(db.rootPublishedSet)
	db.mu.RUnlock()
	reader := newCachedLiveScanReader(valueReaderForBackendState(state), db.valueLogReader)
	if scanStats != nil {
		scanStats.ProofStage = "cached_projection"
	}
	if err := db.collectPublishedRootValueLogLiveIDsUntil(ctx, p, reader, publishedRoots, 0, 0, nil, protected, lastWrite, scanStats); err != nil {
		return nil, retainedPruneContextCause(ctx, err)
	}
	if err := retainedPruneContextErr(ctx); err != nil {
		return nil, err
	}
	if db.foregroundWritesResumedSince(lastWrite) {
		return nil, errForegroundWritesResumed
	}
	if err := snap.Close(); err != nil {
		return nil, err
	}
	return protected, nil
}

// Use the existing concrete snapshot and GC seams. Mock/iterator-only backends
// retain their complete iterator path and its cancellation/budget controls.
func (db *DB) pruneRetainedValueLogsBatch(ctx context.Context, candidates []retainedPruneCandidate, force bool, observed map[uint32]struct{}, opts retainedValueLogPruneRunOptions, out *retainedValueLogPruneStats) bool {
	snapper, ok := db.backend.(interface{ AcquireSnapshot() *backenddb.Snapshot })
	if !ok {
		return false
	}
	gc, ok := db.backend.(backendValueLogGCer)
	if !ok {
		return false
	}
	out.Mode = retainedPruneModeCertifiedMembership
	out.ScanStats.ProofStage = "backend_membership"
	lastWrite := db.lastForegroundWriteUnixNano.Load()
	domain := db.rootDomainVersion.Load()
	ctx, cancel := db.retainedPruneLiveIDScanContext(ctx, lastWrite, opts.fullLiveIDScanBudget)
	defer cancel()
	abort := func(err error) {
		err = retainedPruneContextCause(ctx, err)
		if !db.observeRetainedPruneAbortError(out, err) {
			out.ScanError = true
			db.reportError(fmt.Errorf("cachingdb: retained value-log batch prune: %w", err))
		}
	}
	snap := snapper.AcquireSnapshot()
	if snap == nil {
		abort(backenddb.ErrRecoverableRootSetStale)
		return true
	}
	token, bound := snap.StateToken()
	p := snap.Pager()
	_ = snap.Close()
	if !bound || p == nil {
		abort(backenddb.ErrRecoverableRootSetStale)
		return true
	}
	protected, err := db.collectRetainedPruneProtectedValueLogIDs(ctx, lastWrite, &out.ScanStats)
	if err != nil {
		abort(err)
		return true
	}
	selected := make(map[uint32]struct{})
	byID := make(map[uint32]retainedPruneCandidate)
	for _, candidate := range candidates {
		if force && len(observed) > 0 && !candidate.observed {
			continue
		}
		if !candidate.hasID {
			out.ParseSkippedSegments++
			out.ParseSkippedBytes += candidate.size
			if candidate.observed {
				out.ObservedSourceParseSkippedSegments++
				out.ObservedSourceParseSkippedBytes += candidate.size
			}
			continue
		}
		if _, present := protected[candidate.id]; present {
			out.LiveSkippedSegments++
			out.LiveSkippedBytes += candidate.size
			if candidate.observed {
				out.ObservedSourceLiveSkippedSegments++
				out.ObservedSourceLiveSkippedBytes += candidate.size
			}
			continue
		}
		selected[candidate.id] = struct{}{}
		byID[candidate.id] = candidate
	}
	if len(selected) == 0 {
		out.ScanStats.ProofStage = "no_selection"
		return true
	}
	gcOpts := db.valueLogGCOptions(false)
	// Drop only selected retention. Mutable/queued/in-use protection survives.
	gcOpts.ProtectedRetainedPaths = filterObservedValueLogPaths(gcOpts.ProtectedRetainedPaths, selected)
	gcOpts.ProtectedPaths = mergeUniqueNonEmptyStrings(gcOpts.ProtectedRetainedPaths, gcOpts.ProtectedInUsePaths)
	gcOpts.ObservedSourcesOnly = true
	for id := range selected {
		gcOpts.ObservedSourceFileIDs = append(gcOpts.ObservedSourceFileIDs, id)
	}
	if (force && len(observed) > 0) || opts.reclaimClosedCandidates {
		// These IDs already passed complete recovery and detached-cache proof.
		// The late writer fence below proves they remain closed and unused;
		// it never authorizes reclaiming an actual writable lane head. Ordinary
		// scheduled pruning, including pressure-forced admission, keeps the
		// backend's additional active/recent heuristic.
		gcOpts.ObservedSourceAssumeUnreferenced = true
		gcOpts.ObservedSourceReclaimActive = true
	}
	gcOpts.BeforeMutation = func(ctx context.Context, current backenddb.StateToken, currentPager *pager.Pager) (func(), error) {
		out.ScanStats.ProofStage = "mutation_fence"
		if err := retainedPruneContextErr(ctx); err != nil {
			return nil, err
		}
		// Contention means a foreground writer has resumed. Never wait past the
		// finite quiet-time budget, and acquire cache writeMu before publication.
		if !db.writeMu.TryLock() {
			return nil, errForegroundWritesResumed
		}
		release := db.writeMu.Unlock
		if err := retainedPruneContextErr(ctx); err != nil {
			return release, err
		}
		if db.rootDomainVersion.Load() != domain || db.foregroundWritesResumedSince(lastWrite) {
			return release, errForegroundWritesResumed
		}
		if current != token || currentPager != p {
			return release, backenddb.ErrRecoverableRootSetStale
		}
		for _, path := range db.valueOnlyLogPaths(db.valueLogInUsePaths()) {
			if id, ok := valueLogFileIDForPath(path); ok {
				if _, chosen := selected[id]; chosen {
					return release, errForegroundWritesResumed
				}
			}
		}
		return release, nil
	}
	out.ScanStats.ProofStage = "gc_recovery"
	if err := db.retainedPruneScanHook(ctx, "before_batch_gc"); err != nil {
		abort(err)
		return true
	}
	out.GCCalls++
	out.GCStats, err = gc.ValueLogGC(ctx, gcOpts)
	// An error may follow successful earlier marks. Only the backend's actual
	// mutation identities authorize forgetting retention and reader eviction.
	for _, id := range out.GCStats.ZombieMarkedFileIDs {
		candidate, ok := byID[id]
		if !ok {
			continue
		}
		if db.valueLogReader != nil {
			_ = db.valueLogReader.EvictSegment(id)
		}
		db.forgetValueLogRetain(candidate.path)
		if db.cleanupMissingRetainedValueLog(candidate.path) {
			out.DeletedFileIDs = append(out.DeletedFileIDs, id)
			out.RemovedSegments++
			out.RemovedBytes += candidate.size
			if candidate.observed {
				out.ObservedSourceRemovedSegments++
				out.ObservedSourceRemovedBytes += candidate.size
			}
		} else {
			out.PendingFileIDs = append(out.PendingFileIDs, id)
			// Legacy action counters are disjoint: removed or marked pending,
			// never both for one segment. GCStats retains every actual mark.
			out.ZombieMarkedSegments++
			out.ZombieMarkedBytes += candidate.size
			if candidate.observed {
				out.ObservedSourceZombieMarkedSegments++
				out.ObservedSourceZombieMarkedBytes += candidate.size
			}
		}
	}
	if err != nil {
		abort(err)
	} else {
		out.ScanStats.ProofStage = "completed"
	}
	return true
}
