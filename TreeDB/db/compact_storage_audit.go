package db

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type compactStorageAuditKey struct {
	CommitSeq                  uint64
	RootPageID                 uint64
	SystemRootPageID           uint64
	LeafGenerationStateVersion uint64
	ValueLogSetIdentity        *valuelog.Set
	ProtectedRootSetHash       [32]byte
	ProtectedPathSetHash       [32]byte
}

type compactStorageAuditCounters = CompactStorageAuditStats

type compactStorageAuditRaw struct {
	valueLogRefCounts          map[uint32]uint64
	valueLogReferencedSegments map[uint32]struct{}
	valueLogLiveBytesBySegment map[uint32]int64
	leafGenerationLive         leafGenerationLiveScanStats
	counters                   compactStorageAuditCounters
}

var errCompactStorageAuditProtectedBasisDrift = errors.New("compact storage protected basis drift")

type compactStorageProtectedBasis struct {
	paths               []string
	rootIDs             []uint64
	systemRootIDs       []uint64
	rootHash            [32]byte
	pathHash            [32]byte
	leafPageLogVersion  uint64
	rootProviderVersion uint64
}

func (b compactStorageProtectedBasis) equal(other compactStorageProtectedBasis) bool {
	return b.rootHash == other.rootHash && b.pathHash == other.pathHash
}

type compactStorageAuditInput struct {
	db                     *DB
	snap                   *Snapshot
	manifest               *leafGenerationManifest
	protectedPaths         []string
	protectedRootIDs       []uint64
	protectedSystemRootIDs []uint64
	protectedBasis         compactStorageProtectedBasis
	key                    compactStorageAuditKey
}

func (in *compactStorageAuditInput) close() {
	if in == nil || in.snap == nil {
		return
	}
	_ = in.snap.Close()
	in.snap = nil
}

type compactStorageAuditSession struct {
	valid bool
	key   compactStorageAuditKey
	raw   compactStorageAuditRaw
}

func (s *compactStorageAuditSession) close() {}

var compactStorageSharedAuditScanHook struct {
	mu sync.Mutex
	fn func(compactStorageAuditCounters)
}

func registerCompactStorageSharedAuditScanHook(hook func(compactStorageAuditCounters)) func() {
	compactStorageSharedAuditScanHook.mu.Lock()
	prev := compactStorageSharedAuditScanHook.fn
	compactStorageSharedAuditScanHook.fn = hook
	compactStorageSharedAuditScanHook.mu.Unlock()
	return func() {
		compactStorageSharedAuditScanHook.mu.Lock()
		compactStorageSharedAuditScanHook.fn = prev
		compactStorageSharedAuditScanHook.mu.Unlock()
	}
}

func runCompactStorageSharedAuditScanHook(counters compactStorageAuditCounters) {
	compactStorageSharedAuditScanHook.mu.Lock()
	hook := compactStorageSharedAuditScanHook.fn
	compactStorageSharedAuditScanHook.mu.Unlock()
	if hook != nil {
		hook(counters)
	}
}

func (db *DB) acquireCompactStorageAuditInput(opts CompactStorageOptions) (*compactStorageAuditInput, error) {
	return db.acquireCompactStorageAuditInputAttempt(opts, 0)
}

func (db *DB) acquireCompactStorageAuditInputAttempt(opts CompactStorageOptions, attempt int) (*compactStorageAuditInput, error) {
	if db == nil {
		return nil, fmt.Errorf("missing db")
	}
	if err := db.prepareCompactStorageAuditTopology(); err != nil {
		return nil, err
	}
	protectedBefore := db.captureCompactStorageProtectedBasis(opts)
	db.runCompactStorageAuditProtectedBasisHook("acquire-after-first-protected-basis", attempt)

	db.writeMu.RLock()
	if db.leafPageLogVersion != protectedBefore.leafPageLogVersion {
		db.writeMu.RUnlock()
		return nil, errCompactStorageAuditProtectedBasisDrift
	}
	manifest := db.leafGenerationManifest.clone()
	snap := db.AcquireSnapshot()
	db.writeMu.RUnlock()
	db.runCompactStorageAuditProtectedBasisHook("acquire-after-state", attempt)
	if snap == nil || snap.state == nil || snap.idx == nil {
		if snap != nil {
			_ = snap.Close()
		}
		return nil, ErrClosed
	}
	protectedAfter := db.captureCompactStorageProtectedBasis(opts)
	if !protectedBefore.equal(protectedAfter) {
		_ = snap.Close()
		return nil, errCompactStorageAuditProtectedBasisDrift
	}
	state := snap.state
	return &compactStorageAuditInput{
		db:                     db,
		snap:                   snap,
		manifest:               manifest,
		protectedPaths:         protectedAfter.paths,
		protectedRootIDs:       protectedAfter.rootIDs,
		protectedSystemRootIDs: protectedAfter.systemRootIDs,
		protectedBasis:         protectedAfter,
		key: compactStorageAuditKey{
			CommitSeq:                  state.CommitSeq,
			RootPageID:                 state.RootPageID,
			SystemRootPageID:           state.SystemRootPageID,
			LeafGenerationStateVersion: state.LeafGenerationStateVersion,
			ValueLogSetIdentity:        state.ValueLogSet,
			ProtectedRootSetHash:       protectedAfter.rootHash,
			ProtectedPathSetHash:       protectedAfter.pathHash,
		},
	}, nil
}

func (db *DB) captureCompactStorageProtectedBasis(opts CompactStorageOptions) compactStorageProtectedBasis {
	paths := canonicalCompactStorageProtectedPaths(compactStorageFencedValueLogProtectedPaths(opts))
	rootIDs, systemRootIDs := compactStorageOptionProtectedRootIDPair(opts)

	db.writeMu.RLock()
	leafPageLog := db.leafPageLog
	leafPageLogVersion := db.leafPageLogVersion
	db.writeMu.RUnlock()
	providerRootIDs, providerSystemRootIDs, providerVersion := protectedLeafGenerationRootIDPairSnapshot(leafPageLog)
	rootIDs = appendCompactStorageProtectedRootIDs(rootIDs, providerRootIDs)
	systemRootIDs = appendCompactStorageProtectedRootIDs(systemRootIDs, providerSystemRootIDs)
	rootIDs = canonicalCompactStorageRootIDs(rootIDs)
	systemRootIDs = canonicalCompactStorageRootIDs(systemRootIDs)

	return compactStorageProtectedBasis{
		paths:               paths,
		rootIDs:             rootIDs,
		systemRootIDs:       systemRootIDs,
		rootHash:            hashCompactStorageProtectedRoots(rootIDs, systemRootIDs, leafPageLogVersion, providerVersion),
		pathHash:            hashCompactStorageProtectedPaths(paths),
		leafPageLogVersion:  leafPageLogVersion,
		rootProviderVersion: providerVersion,
	}
}

func protectedLeafGenerationRootIDPairSnapshot(log LeafPageLog) ([]uint64, []uint64, uint64) {
	if log == nil {
		return nil, nil, 0
	}
	if provider, ok := log.(leafPageLogProtectedRootPairSnapshotProvider); ok {
		return provider.ProtectedLeafGenerationRootIDPairSnapshot()
	}
	if provider, ok := log.(leafPageLogProtectedRootPairProvider); ok {
		rootIDs, systemRootIDs := provider.ProtectedLeafGenerationRootIDPair()
		return rootIDs, systemRootIDs, 0
	}
	var rootIDs []uint64
	if provider, ok := log.(leafPageLogProtectedRootProvider); ok {
		rootIDs = provider.ProtectedLeafGenerationRootIDs()
	}
	var systemRootIDs []uint64
	if provider, ok := log.(leafPageLogProtectedSystemRootProvider); ok {
		systemRootIDs = provider.ProtectedLeafGenerationSystemRootIDs()
	}
	return rootIDs, systemRootIDs, 0
}

func (db *DB) runCompactStorageAuditProtectedBasisHook(stage string, attempt int) {
	if db != nil && db.compactStorageAuditProtectedBasisHook != nil {
		db.compactStorageAuditProtectedBasisHook(stage, attempt)
	}
}

func (db *DB) prepareCompactStorageAuditTopology() error {
	if db == nil {
		return nil
	}
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := db.checkWriteAdmissionLocked(); err != nil {
		return err
	}
	if db.valueLogManager == nil {
		return nil
	}
	refreshed := false
	if db.indexOuterLeavesInValueLog {
		if err := db.valueLogManager.Refresh(); err != nil {
			return err
		}
		refreshed = true
		commitSeq := uint64(1)
		if state := db.state.Load(); state != nil && state.CommitSeq != 0 {
			commitSeq = state.CommitSeq
		}
		if _, err := db.reconcileLeafGenerationManifestWithDirLocked(commitSeq); err != nil {
			return err
		}
	}
	managerSet := db.valueLogManager.CurrentSetNoRefresh()
	if managerSet == nil {
		return nil
	}
	if len(managerSet.Files) == 0 && !refreshed {
		_ = db.valueLogManager.Release(managerSet)
		if err := db.valueLogManager.Refresh(); err != nil {
			return err
		}
		managerSet = db.valueLogManager.CurrentSetNoRefresh()
		if managerSet == nil {
			return nil
		}
	}
	state := db.state.Load()
	currentSet := (*valuelog.Set)(nil)
	if state != nil {
		currentSet = state.ValueLogSet
	}
	same := compactStorageValueLogSetsMatch(currentSet, managerSet)
	if same {
		return db.valueLogManager.Release(managerSet)
	}
	if compactStorageValueLogSetAddsOnlyZeroByteFiles(currentSet, managerSet) {
		return db.valueLogManager.Release(managerSet)
	}
	return db.publishCompactStorageValueLogSet(managerSet)
}

func compactStorageValueLogSetAddsOnlyZeroByteFiles(current, candidate *valuelog.Set) bool {
	if candidate == nil {
		return false
	}
	if current != nil {
		for id, file := range current.Files {
			if candidate.Files[id] != file {
				return false
			}
		}
	}
	added := false
	for id, file := range candidate.Files {
		if current != nil && current.Files[id] == file {
			continue
		}
		if file == nil || file.File == nil {
			return false
		}
		info, err := file.File.Stat()
		if err != nil || info.Size() != 0 {
			return false
		}
		added = true
	}
	return added
}

// publishCompactStorageValueLogSet consumes a retained manager set and
// publishes that exact topology. In particular, an empty set that has already
// been refreshed must not enter publishValueLogSetNoRefresh's discovery
// fallback and resurrect topology that the manager has retired.
func (db *DB) publishCompactStorageValueLogSet(valueLogSet *valuelog.Set) error {
	if db == nil || db.valueLogManager == nil || valueLogSet == nil {
		return nil
	}
	db.mu.Lock()
	oldState := db.state.Load()
	if oldState == nil || compactStorageValueLogSetsMatch(oldState.ValueLogSet, valueLogSet) {
		db.mu.Unlock()
		return db.valueLogManager.Release(valueLogSet)
	}
	newState := &DBState{
		CommitSeq:                  oldState.CommitSeq,
		RootPageID:                 oldState.RootPageID,
		SystemRootPageID:           oldState.SystemRootPageID,
		AppliedCommandLSN:          oldState.AppliedCommandLSN,
		MaxEntryRevision:           oldState.MaxEntryRevision,
		ValueLogSet:                valueLogSet,
		LeafGenerations:            oldState.LeafGenerations,
		LeafGenerationStateVersion: oldState.LeafGenerationStateVersion,
	}
	db.state.Store(newState)
	db.publishSnapshotView(db.idx.Load(), newState, db.valueLogManager)
	db.mu.Unlock()
	if oldState.ValueLogSet != nil {
		return db.valueLogManager.Release(oldState.ValueLogSet)
	}
	return nil
}

func compactStorageValueLogSetsMatch(a, b *valuelog.Set) bool {
	if a == nil || b == nil {
		return a == b || (a == nil && len(b.Files) == 0) || (b == nil && len(a.Files) == 0)
	}
	if len(a.Files) != len(b.Files) {
		return false
	}
	for id, file := range a.Files {
		if b.Files[id] != file {
			return false
		}
	}
	return true
}

func canonicalCompactStorageProtectedPaths(paths []string) []string {
	if len(paths) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(paths))
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		out = append(out, path)
	}
	sort.Strings(out)
	return out
}

func canonicalCompactStorageRootIDs(rootIDs []uint64) []uint64 {
	if len(rootIDs) == 0 {
		return nil
	}
	seen := make(map[uint64]struct{}, len(rootIDs))
	out := make([]uint64, 0, len(rootIDs))
	for _, rootID := range rootIDs {
		if rootID == 0 {
			continue
		}
		if _, ok := seen[rootID]; ok {
			continue
		}
		seen[rootID] = struct{}{}
		out = append(out, rootID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func hashCompactStorageProtectedRoots(rootIDs, systemRootIDs []uint64, leafPageLogVersion, providerVersion uint64) [32]byte {
	h := sha256.New()
	var buf [8]byte
	for _, version := range []uint64{leafPageLogVersion, providerVersion} {
		binary.LittleEndian.PutUint64(buf[:], version)
		_, _ = h.Write(buf[:])
	}
	for _, roots := range [][]uint64{rootIDs, systemRootIDs} {
		binary.LittleEndian.PutUint64(buf[:], uint64(len(roots)))
		_, _ = h.Write(buf[:])
		for _, rootID := range roots {
			binary.LittleEndian.PutUint64(buf[:], rootID)
			_, _ = h.Write(buf[:])
		}
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func hashCompactStorageProtectedPaths(paths []string) [32]byte {
	h := sha256.New()
	var buf [8]byte
	for _, path := range paths {
		binary.LittleEndian.PutUint64(buf[:], uint64(len(path)))
		_, _ = h.Write(buf[:])
		_, _ = h.Write([]byte(path))
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func compactStorageAuditKeyMissReason(previous, current compactStorageAuditKey) string {
	switch {
	case previous.CommitSeq != current.CommitSeq:
		return "commit_seq"
	case previous.RootPageID != current.RootPageID:
		return "root_page_id"
	case previous.SystemRootPageID != current.SystemRootPageID:
		return "system_root_page_id"
	case previous.LeafGenerationStateVersion != current.LeafGenerationStateVersion:
		return "leaf_generation_state_version"
	case previous.ValueLogSetIdentity != current.ValueLogSetIdentity:
		return "value_log_set_identity"
	case previous.ProtectedRootSetHash != current.ProtectedRootSetHash:
		return "protected_root_set"
	case previous.ProtectedPathSetHash != current.ProtectedPathSetHash:
		return "protected_path_set"
	default:
		return "cold"
	}
}

func (db *DB) scanCompactStorageAudit(ctx context.Context, in *compactStorageAuditInput) (compactStorageAuditRaw, error) {
	if in == nil {
		return compactStorageAuditRaw{}, fmt.Errorf("compact storage audit: missing input")
	}
	result, err := db.maintenanceReachabilityScan(ctx, in.snap, maintenanceReachabilityScanOptions{
		Collectors: maintenanceReachabilityValueLogRefCounts |
			maintenanceReachabilityValueLogLiveBytes |
			maintenanceReachabilityLeafGenerationTotals,
		ProtectedRootIDs:       in.protectedRootIDs,
		ProtectedSystemRootIDs: in.protectedSystemRootIDs,
	})
	raw := compactStorageAuditRaw{
		valueLogRefCounts:          result.valueLogRefCounts,
		valueLogReferencedSegments: result.valueLogReferencedSegments,
		valueLogLiveBytesBySegment: result.valueLogLiveBytesBySegment,
		leafGenerationLive:         result.leafGenerationLive,
		counters:                   result.counters,
	}
	if err == nil {
		runCompactStorageSharedAuditScanHook(raw.counters)
	}
	return raw, err
}

func (db *DB) compactStorageAuditRevalidate(in *compactStorageAuditInput, opts CompactStorageOptions, raw compactStorageAuditRaw, attempt int) (bool, error) {
	if db.compactStorageAuditBeforeRevalidate != nil {
		db.compactStorageAuditBeforeRevalidate(attempt)
	}
	protectedBefore := db.captureCompactStorageProtectedBasis(opts)
	db.runCompactStorageAuditProtectedBasisHook("revalidate-after-first-protected-basis", attempt)
	if !protectedBefore.equal(in.protectedBasis) {
		return false, nil
	}

	db.writeMu.RLock()
	stateBefore := db.state.Load()
	stateBeforeValid := compactStorageAuditStateMatches(stateBefore, in.key) &&
		db.leafPageLogVersion == protectedBefore.leafPageLogVersion
	db.writeMu.RUnlock()
	if !stateBeforeValid {
		return false, nil
	}
	db.runCompactStorageAuditProtectedBasisHook("revalidate-after-state", attempt)
	protectedAfter := db.captureCompactStorageProtectedBasis(opts)
	if !protectedBefore.equal(protectedAfter) || !protectedAfter.equal(in.protectedBasis) {
		return false, nil
	}

	// The protected basis is stable on both sides of an exact backend-state
	// check. Versioned providers make same-ID ABA visible. For legacy callbacks,
	// equal canonical IDs are observationally the same basis: retiring or reusing
	// one of those pages necessarily changes the commit/root state checked here.
	db.writeMu.Lock()
	db.mu.Lock()
	state := db.state.Load()
	valid := compactStorageAuditStateMatches(state, in.key) &&
		db.leafPageLogVersion == protectedAfter.leafPageLogVersion
	if valid && db.valueLogRefTracker != nil {
		db.valueLogRefTracker.replace(raw.valueLogRefCounts, in.key.CommitSeq, true)
	}
	db.mu.Unlock()
	db.writeMu.Unlock()
	return valid, nil
}

func compactStorageAuditStateMatches(state *DBState, key compactStorageAuditKey) bool {
	return state != nil &&
		state.CommitSeq == key.CommitSeq &&
		state.RootPageID == key.RootPageID &&
		state.SystemRootPageID == key.SystemRootPageID &&
		state.LeafGenerationStateVersion == key.LeafGenerationStateVersion &&
		state.ValueLogSet == key.ValueLogSetIdentity
}

func (db *DB) compactStorageSharedAudit(ctx context.Context, opts CompactStorageOptions, session *compactStorageAuditSession) (*compactStorageAuditInput, compactStorageAuditRaw, CompactStorageAuditStats, error) {
	var aggregate CompactStorageAuditStats
	for attempt := 0; attempt < 2; attempt++ {
		in, err := db.acquireCompactStorageAuditInputAttempt(opts, attempt)
		if err != nil {
			if errors.Is(err, errCompactStorageAuditProtectedBasisDrift) {
				if attempt == 0 {
					aggregate.RevalidationRetries++
					continue
				}
				return nil, compactStorageAuditRaw{}, aggregate, ErrCompactStorageAuditStale
			}
			return nil, compactStorageAuditRaw{}, aggregate, err
		}
		var raw compactStorageAuditRaw
		if session != nil && session.valid && session.key == in.key {
			raw = session.raw
			aggregate.StructuralReuseHits++
		} else {
			raw, err = db.scanCompactStorageAudit(ctx, in)
			if err != nil {
				in.close()
				return nil, compactStorageAuditRaw{}, aggregate, err
			}
			addCompactStorageAuditStats(&aggregate, raw.counters)
			aggregate.StructuralReuseMisses++
			if session != nil && session.valid {
				aggregate.LastStructuralReuseMissReason = compactStorageAuditKeyMissReason(session.key, in.key)
			} else {
				aggregate.LastStructuralReuseMissReason = "cold"
			}
		}
		if len(in.snap.leafGenerationIDs) > 0 {
			in.snap.releaseLeafGenerationPins()
		}
		valid, err := db.compactStorageAuditRevalidate(in, opts, raw, attempt)
		if err != nil {
			in.close()
			return nil, compactStorageAuditRaw{}, aggregate, err
		}
		if valid {
			if session != nil {
				session.valid = true
				session.key = in.key
				session.raw = raw
			}
			return in, raw, aggregate, nil
		}
		in.close()
		if attempt == 0 {
			aggregate.RevalidationRetries++
		}
	}
	return nil, compactStorageAuditRaw{}, aggregate, ErrCompactStorageAuditStale
}

func addCompactStorageAuditStats(dst *CompactStorageAuditStats, src CompactStorageAuditStats) {
	if dst == nil {
		return
	}
	dst.SharedScans += src.SharedScans
	dst.StructuralReuseHits += src.StructuralReuseHits
	dst.StructuralReuseMisses += src.StructuralReuseMisses
	dst.RevalidationRetries += src.RevalidationRetries
	dst.RootSets += src.RootSets
	dst.PagesVisited += src.PagesVisited
	dst.MemoHits += src.MemoHits
	dst.PointerProjections += src.PointerProjections
	dst.GroupedRecordDedupeHits += src.GroupedRecordDedupeHits
	dst.PhysicalBytesRead += src.PhysicalBytesRead
	if src.LastStructuralReuseMissReason != "" {
		dst.LastStructuralReuseMissReason = src.LastStructuralReuseMissReason
	}
}

func (db *DB) compactStorageRewritePlanFromAudit(opts ValueLogRewriteOnlineOptions, set *valuelog.Set, liveByID map[uint32]int64) ValueLogRewritePlan {
	var plan ValueLogRewritePlan
	if set == nil {
		return plan
	}
	files := db.valueOnlyValueLogFiles(set.Files)
	if len(files) == 0 {
		return plan
	}
	plan.SegmentsTotal = len(files)
	for _, f := range files {
		plan.BytesTotal += fileSize(f)
	}
	if !rewritePlanNeedsLiveEstimate(opts) {
		liveByID = nil
	}

	sourceIDs := map[uint32]struct{}(nil)
	var selectionStats rewriteSourceSelectionStats
	if hasOnlyExplicitRewriteSources(opts) {
		sourceIDs = selectExplicitRewriteSourceIDs(opts.SourceFileIDs, files)
	} else if hasRewriteSourceSelection(opts) {
		active := currentValueLogIDs(&valuelog.Set{Files: files})
		sourceIDs, selectionStats = selectRewriteSourceSegmentsWithStats(opts, files, active, liveByID)
	}
	plan.AgeBlockedSegments = selectionStats.ageBlockedSegments
	plan.AgeBlockedBytesTotal = selectionStats.ageBlockedBytesTotal
	plan.AgeBlockedBytesLive = selectionStats.ageBlockedBytesLive
	plan.AgeBlockedBytesStale = selectionStats.ageBlockedBytesStale
	plan.AgeBlockedMinRemainingAge = selectionStats.ageBlockedMinRemainingAge

	if liveByID != nil {
		for id, f := range files {
			size := fileSize(f)
			if size <= 0 {
				continue
			}
			live := liveByID[id]
			if live < 0 {
				live = 0
			}
			if live > size {
				live = size
			}
			plan.BytesLive += live
			plan.BytesStale += size - live
		}
	}

	if len(sourceIDs) == 0 {
		return plan
	}
	plan.SourceFileIDs = make([]uint32, 0, len(sourceIDs))
	for id := range sourceIDs {
		plan.SourceFileIDs = append(plan.SourceFileIDs, id)
	}
	sort.Slice(plan.SourceFileIDs, func(i, j int) bool { return plan.SourceFileIDs[i] < plan.SourceFileIDs[j] })
	plan.SegmentsSelected = len(plan.SourceFileIDs)
	if liveByID != nil {
		plan.SelectedSegments = make([]ValueLogRewritePlanSegment, 0, len(plan.SourceFileIDs))
	}
	for _, id := range plan.SourceFileIDs {
		f := files[id]
		if f == nil {
			continue
		}
		size := fileSize(f)
		if size <= 0 {
			continue
		}
		plan.SelectedBytesTotal += size
		if liveByID == nil {
			continue
		}
		live := liveByID[id]
		if live < 0 {
			live = 0
		}
		if live > size {
			live = size
		}
		plan.SelectedBytesLive += live
		stale := size - live
		plan.SelectedBytesStale += stale
		staleRatio := float64(0)
		if size > 0 && stale > 0 {
			staleRatio = float64(stale) / float64(size)
		}
		plan.SelectedSegments = append(plan.SelectedSegments, ValueLogRewritePlanSegment{
			FileID:     id,
			BytesTotal: size,
			BytesLive:  live,
			BytesStale: stale,
			StaleRatio: staleRatio,
		})
	}
	return plan
}

func (db *DB) compactStorageValueLogGCFromAudit(ctx context.Context, set *valuelog.Set, referenced map[uint32]struct{}, protected []string) (ValueLogGCStats, error) {
	var stats ValueLogGCStats
	if set == nil {
		return stats, nil
	}
	files := db.valueOnlyValueLogFiles(set.Files)
	valueSet := &valuelog.Set{Files: files}
	keptIDs := currentValueLogIDs(valueSet)
	if recent := recentValueLogIDsForProtectedPaths(valueSet, valueLogKeepRecentSegmentsPerLane, protected); len(recent) > 0 {
		keptIDs = recent
		for id := range currentValueLogIDs(valueSet) {
			lane, _ := valuelog.DecodeFileID(id)
			if lane == 0 {
				keptIDs[id] = struct{}{}
			}
		}
	}
	for id := range db.pendingValueLogAppendFileIDs() {
		keptIDs[id] = struct{}{}
	}
	protectedSet := make(map[string]struct{}, len(protected))
	for _, path := range protected {
		if path != "" {
			protectedSet[path] = struct{}{}
		}
	}
	for id, f := range files {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		size := fileSize(f)
		stats.SegmentsTotal++
		stats.BytesTotal += size
		switch {
		case containsValueLogFileID(referenced, id):
			stats.SegmentsReferenced++
			stats.BytesReferenced += size
		case containsValueLogFileID(keptIDs, id):
			stats.SegmentsActive++
			stats.BytesActive += size
		case containsValueLogPath(protectedSet, f):
			stats.SegmentsProtected++
			stats.BytesProtected += size
			stats.SegmentsProtectedOther++
			stats.BytesProtectedOther += size
		default:
			stats.SegmentsEligible++
			stats.BytesEligible += size
			stats.SegmentsPending++
			stats.BytesPending += size
		}
	}
	return stats, nil
}

func containsValueLogFileID(set map[uint32]struct{}, id uint32) bool {
	_, ok := set[id]
	return ok
}

func containsValueLogPath(set map[string]struct{}, f *valuelog.File) bool {
	if f == nil {
		return false
	}
	_, ok := set[f.Path]
	return ok
}

func (db *DB) compactStorageFencedUnreferencedFromAudit(opts CompactStorageOptions, set *valuelog.Set, referenced map[uint32]struct{}, protectedPaths []string) ([]uint32, int64) {
	if !opts.UnsafeValueLogReclaimFencedUnreferenced || set == nil {
		return nil, 0
	}
	if db.compactStorageFencedValueLogRefHook != nil {
		db.compactStorageFencedValueLogRefHook(compactStorageFencedValueLogRefEvent{
			Source:             valueLogRefResolutionSourceValidationScan,
			ReferencedSegments: len(referenced),
		})
	}
	protected := compactStorageProtectedPathSet(protectedPaths)
	protectedFileIDs := compactStorageProtectedFileIDSet(protectedPaths, nil)
	pendingFileIDs := db.pendingValueLogAppendFileIDs()
	files := db.valueOnlyValueLogFiles(set.Files)
	ids := make([]uint32, 0, len(files))
	var bytes int64
	for id, f := range files {
		if _, ok := referenced[id]; ok || f == nil {
			continue
		}
		if _, ok := protected[filepath.Clean(f.Path)]; ok {
			continue
		}
		if _, ok := protectedFileIDs[id]; ok {
			continue
		}
		if _, ok := pendingFileIDs[id]; ok {
			continue
		}
		size := fileSize(f)
		if size <= 0 {
			continue
		}
		ids = append(ids, id)
		bytes += size
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, bytes
}

func (db *DB) compactStorageLeafGenerationPlanFromAudit(opts LeafGenerationPlanOptions, in *compactStorageAuditInput, liveScan leafGenerationLiveScanStats) LeafGenerationPlan {
	var plan LeafGenerationPlan
	if !db.indexOuterLeavesInValueLog {
		plan.Admission = leafGenerationPlanAdmissionDisabled
		return plan
	}
	if in == nil || in.manifest == nil || in.snap == nil || in.snap.state == nil {
		plan.Admission = leafGenerationPlanAdmissionNoCandidates
		return plan
	}
	manifest := in.manifest
	plan.CurrentCommitSeq = in.snap.state.CommitSeq
	plan.CurrentGenerationID = manifest.CurrentGenerationID
	set := in.snap.state.ValueLogSet
	plan.Generations = make([]LeafGenerationPlanGeneration, 0, len(manifest.Generations))
	plan.Candidates = make([]LeafGenerationPlanGeneration, 0, len(manifest.Generations))
	for _, gen := range manifest.Generations {
		entry := LeafGenerationPlanGeneration{
			GenerationID: gen.GenerationID,
			State:        gen.State,
			FileIDs:      append([]uint32(nil), gen.FileIDs...),
			FileCount:    len(gen.FileIDs),
			PinnedCount:  db.leafGenerationPins.count(gen.GenerationID),
		}
		for _, rawFileID := range gen.FileIDs {
			entry.BytesTotal += leafGenerationRawFilePhysicalSize(db.dir, set, rawFileID)
		}
		if live := liveScan.Generations[gen.GenerationID]; live.LiveBytes > 0 || live.LivePages > 0 {
			entry.BytesLive = live.LiveBytes
			entry.LivePages = live.LivePages
		}
		if entry.BytesLive > entry.BytesTotal && entry.BytesTotal > 0 {
			entry.BytesLive = entry.BytesTotal
		}
		entry.BytesDead = entry.BytesTotal - entry.BytesLive
		if entry.BytesDead < 0 {
			entry.BytesDead = 0
		}
		entry.BytesToCopy = entry.BytesLive
		if plan.CurrentCommitSeq > gen.PublishedCommitSeq {
			entry.AgeCommits = plan.CurrentCommitSeq - gen.PublishedCommitSeq
		}
		entry.DeadRatioPPM = ratioPPM(entry.BytesDead, entry.BytesTotal)
		entry.LiveRatioPPM = ratioPPM(entry.BytesLive, entry.BytesTotal)
		entry.WholeGenerationGCEligible = leafGenerationWholeGenerationGCEligible(entry)
		entry.Eligible, entry.SkipReason = leafGenerationPlanEligibility(entry, opts)
		plan.Generations = append(plan.Generations, entry)
		if entry.Eligible {
			plan.Candidates = append(plan.Candidates, entry)
		}
	}
	rankLeafGenerationPlanCandidates(plan.Candidates)
	plan.CandidateGenerationIDs = make([]uint64, 0, len(plan.Candidates))
	for _, gen := range plan.Candidates {
		plan.CandidateGenerationIDs = append(plan.CandidateGenerationIDs, gen.GenerationID)
		plan.CandidateBytesTotal += gen.BytesTotal
		plan.CandidateBytesLive += gen.BytesLive
		plan.CandidateBytesDead += gen.BytesDead
		plan.CandidateBytesToCopy += gen.BytesToCopy
		plan.CandidateLivePages += gen.LivePages
	}
	plan.ExpectedReclaimBytes = plan.CandidateBytesDead
	plan.ExpectedReclaimRatioPPM = ratioPPM(plan.CandidateBytesDead, plan.CandidateBytesTotal)
	plan.ExpectedReclaimPerByteCopiedPPM = ratioPPM(plan.CandidateBytesDead, plan.CandidateBytesToCopy)
	plan.Admission = leafGenerationPlanAdmission(opts, plan)
	return plan
}

func (db *DB) compactStorageLeafGenerationGCFromAudit(ctx context.Context, in *compactStorageAuditInput, liveScan leafGenerationLiveScanStats) (LeafGenerationGCStats, error) {
	var stats LeafGenerationGCStats
	if !db.indexOuterLeavesInValueLog || in == nil || in.manifest == nil || in.snap == nil || in.snap.state == nil || in.snap.state.LeafGenerations == nil {
		return stats, nil
	}
	currentLeafLogRawFileIDs, err := db.currentLeafPageLogRawFileIDSet()
	if err != nil {
		return stats, err
	}
	filePaths := make(map[uint32]string)
	set := in.snap.state.ValueLogSet
	for _, gen := range in.manifest.Generations {
		for _, fileID := range gen.FileIDs {
			if set != nil {
				if f := set.Files[page.ValueLogFileID(fileID)]; f != nil && f.Path != "" {
					filePaths[fileID] = f.Path
					continue
				}
			}
			filePaths[fileID] = leafGenerationFallbackPath(db.dir, fileID)
		}
	}
	liveGenerations := make(map[uint64]struct{}, len(liveScan.Generations))
	for generationID, totals := range liveScan.Generations {
		if generationID != 0 && (totals.LiveBytes > 0 || totals.LivePages > 0) {
			liveGenerations[generationID] = struct{}{}
		}
	}
	for _, gen := range in.manifest.Generations {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		stats.GenerationsTotal++
		switch {
		case gen.State == leafGenerationStateDeleted:
			continue
		case gen.State == leafGenerationStateWritable:
			stats.GenerationsWritable++
			continue
		case leafGenerationRecordIntersectsFileIDSet(gen, currentLeafLogRawFileIDs):
			stats.GenerationsLive++
			continue
		}
		if _, ok := liveGenerations[gen.GenerationID]; ok {
			stats.GenerationsLive++
			continue
		}
		if db.leafGenerationPins.count(gen.GenerationID) > 0 {
			stats.GenerationsRetiring++
			continue
		}
		stats.GenerationsEligible++
		stats.BytesEligible += leafGenerationRecordBytesTotal(gen, filePaths, db.reportError)
	}
	return stats, nil
}
