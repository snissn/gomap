package db

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"sort"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	leafGenerationPlanAdmissionDisabled             = "disabled"
	leafGenerationPlanAdmissionNoCandidates         = "no_candidates"
	leafGenerationPlanAdmissionTooFewGenerations    = "too_few_generations"
	leafGenerationPlanAdmissionReclaimTooSmall      = "reclaim_too_small"
	leafGenerationPlanAdmissionReclaimTooLow        = "reclaim_ratio_too_low"
	leafGenerationPlanAdmissionReclaimPerCopyTooLow = "reclaim_per_copy_too_low"
	leafGenerationPlanAdmissionEligible             = "eligible"

	leafGenerationPlanSkipWritableGeneration = "writable_generation"
	leafGenerationPlanSkipRetiringGeneration = "retiring_generation"
	leafGenerationPlanSkipDeletedGeneration  = "deleted_generation"
	leafGenerationPlanSkipWholeGenerationGC  = "whole_generation_gc_candidate"
	leafGenerationPlanSkipFreshGeneration    = "fresh_generation"
	leafGenerationPlanSkipNoDeadBytes        = "no_dead_bytes"

	leafGenerationGroupedFrameScanCacheEntries = 2048
)

type LeafGenerationPlanOptions struct {
	MinPublishedAgeCommits     uint64
	MinCandidateGenerations    int
	MinExpectedReclaimBytes    int64
	MinExpectedReclaimRatioPPM int
	MinReclaimPerByteCopiedPPM int
	Force                      bool
	// ProtectedRootIDs are additional ordinary root page IDs whose leaf-log
	// children must be treated as live while planning maintenance. Empty uses the
	// existing cached live-scan path when no protected system roots are present.
	ProtectedRootIDs []uint64
	// ProtectedSystemRootIDs are system-root page IDs whose collection root
	// descriptors should be expanded into additional protected roots.
	ProtectedSystemRootIDs []uint64
}

type LeafGenerationPlanGeneration struct {
	GenerationID uint64
	State        string
	FileIDs      []uint32
	FileCount    int

	BytesTotal  int64
	BytesLive   int64
	BytesDead   int64
	BytesToCopy int64
	LivePages   int

	AgeCommits                uint64
	PinnedCount               uint64
	DeadRatioPPM              int
	LiveRatioPPM              int
	WholeGenerationGCEligible bool
	Eligible                  bool
	SkipReason                string
}

type LeafGenerationPlan struct {
	CurrentCommitSeq       uint64
	CurrentGenerationID    uint64
	Generations            []LeafGenerationPlanGeneration
	Candidates             []LeafGenerationPlanGeneration
	CandidateGenerationIDs []uint64

	CandidateBytesTotal  int64
	CandidateBytesLive   int64
	CandidateBytesDead   int64
	CandidateBytesToCopy int64
	CandidateLivePages   int

	ExpectedReclaimBytes            int64
	ExpectedReclaimRatioPPM         int
	ExpectedReclaimPerByteCopiedPPM int
	Admission                       string

	stateKey  treeReachabilityCacheKey
	liveStats leafGenerationLiveScanStats
}

type leafGenerationLiveScanStats struct {
	Generations map[uint64]leafGenerationLiveTotals
}

type leafGenerationLiveTotals struct {
	LivePages int
	LiveBytes int64
}

type leafGenerationSubtreeStats map[uint64]leafGenerationLiveTotals

type leafGenerationScanFileState struct {
	fileID     uint32
	genID      uint64
	persist    bool
	idx        *leafGenerationRecordLengthIndex
	lookupHint int
}

type leafGenerationScanContext struct {
	snap          *Snapshot
	fileStateByID map[uint32]*leafGenerationScanFileState
	lastFileID    uint32
	lastFileState *leafGenerationScanFileState
	groupedFrames leafGenerationGroupedFrameScanCache
}

type leafGenerationLiveStatsScanOptions struct {
	DisableCache           bool
	ProtectedRootIDs       []uint64
	ProtectedSystemRootIDs []uint64
}

var leafGenerationLiveScanHook struct {
	mu sync.Mutex
	fn func()
}

var leafGenerationGCExclusivePhaseHook struct {
	mu sync.Mutex
	fn func(bool)
}

var leafGenerationPlanCallHook struct {
	mu sync.Mutex
	fn func()
}

var leafGenerationSubtreeCacheMissHook struct {
	mu sync.Mutex
	fn func(uint64)
}

func registerLeafGenerationPlanCallHook(hook func()) func() {
	leafGenerationPlanCallHook.mu.Lock()
	prev := leafGenerationPlanCallHook.fn
	leafGenerationPlanCallHook.fn = hook
	leafGenerationPlanCallHook.mu.Unlock()
	return func() {
		leafGenerationPlanCallHook.mu.Lock()
		leafGenerationPlanCallHook.fn = prev
		leafGenerationPlanCallHook.mu.Unlock()
	}
}

func runLeafGenerationPlanCallHook() {
	leafGenerationPlanCallHook.mu.Lock()
	hook := leafGenerationPlanCallHook.fn
	leafGenerationPlanCallHook.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func registerLeafGenerationLiveScanHook(hook func()) func() {
	leafGenerationLiveScanHook.mu.Lock()
	prev := leafGenerationLiveScanHook.fn
	leafGenerationLiveScanHook.fn = hook
	leafGenerationLiveScanHook.mu.Unlock()
	return func() {
		leafGenerationLiveScanHook.mu.Lock()
		leafGenerationLiveScanHook.fn = prev
		leafGenerationLiveScanHook.mu.Unlock()
	}
}

func runLeafGenerationLiveScanHook() {
	leafGenerationLiveScanHook.mu.Lock()
	hook := leafGenerationLiveScanHook.fn
	leafGenerationLiveScanHook.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func registerLeafGenerationGCExclusivePhaseHook(hook func(bool)) func() {
	leafGenerationGCExclusivePhaseHook.mu.Lock()
	prev := leafGenerationGCExclusivePhaseHook.fn
	leafGenerationGCExclusivePhaseHook.fn = hook
	leafGenerationGCExclusivePhaseHook.mu.Unlock()
	return func() {
		leafGenerationGCExclusivePhaseHook.mu.Lock()
		leafGenerationGCExclusivePhaseHook.fn = prev
		leafGenerationGCExclusivePhaseHook.mu.Unlock()
	}
}

func runLeafGenerationGCExclusivePhaseHook(entering bool) {
	leafGenerationGCExclusivePhaseHook.mu.Lock()
	hook := leafGenerationGCExclusivePhaseHook.fn
	leafGenerationGCExclusivePhaseHook.mu.Unlock()
	if hook != nil {
		hook(entering)
	}
}

// LeafGenerationPlan estimates reclaim opportunities for sealed leaf generations
// by scanning the current live tree once and attributing reachable leaf-log
// pages back to manifest generations.
func (db *DB) LeafGenerationPlan(ctx context.Context, opts LeafGenerationPlanOptions) (LeafGenerationPlan, error) {
	var plan LeafGenerationPlan
	if db == nil {
		return plan, fmt.Errorf("missing db")
	}
	runLeafGenerationPlanCallHook()
	if !db.indexOuterLeavesInValueLog {
		plan.Admission = leafGenerationPlanAdmissionDisabled
		return plan, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	db.writeMu.RLock()
	manifest := db.leafGenerationManifest.clone()
	snap := db.AcquireSnapshot()
	db.writeMu.RUnlock()
	if snap == nil {
		return plan, ErrClosed
	}
	if len(snap.leafGenerationIDs) > 0 {
		snap.releaseLeafGenerationPins()
	}
	defer func() { _ = snap.Close() }()

	if manifest == nil || snap.state == nil {
		plan.Admission = leafGenerationPlanAdmissionNoCandidates
		return plan, nil
	}
	plan.CurrentCommitSeq = snap.state.CommitSeq
	plan.CurrentGenerationID = manifest.CurrentGenerationID

	liveScan, err := db.leafGenerationLiveStatsForSnapshotWithProtectedRoots(ctx, snap, opts.ProtectedRootIDs, opts.ProtectedSystemRootIDs)
	if err != nil {
		return plan, err
	}
	plan = db.leafGenerationPlanFromLiveStats(opts, snap.state, manifest, liveScan)
	if key, ok := leafGenerationPlanStateKey(snap.state, opts); ok {
		plan.stateKey = key
	}
	plan.liveStats = cloneLeafGenerationLiveScanStats(liveScan)
	return plan, nil
}

func leafGenerationPlanStateKey(state *DBState, opts LeafGenerationPlanOptions) (treeReachabilityCacheKey, bool) {
	key, ok := leafGenerationLiveStatsKeyForState(state)
	if !ok {
		return treeReachabilityCacheKey{}, false
	}
	if len(opts.ProtectedRootIDs) > 0 || len(opts.ProtectedSystemRootIDs) > 0 {
		key.protectedRoots = leafGenerationProtectedRootsHash(opts.ProtectedRootIDs, opts.ProtectedSystemRootIDs)
	}
	return key, true
}

func (db *DB) leafGenerationPlanFromLiveStats(opts LeafGenerationPlanOptions, state *DBState, manifest *leafGenerationManifest, liveScan leafGenerationLiveScanStats) LeafGenerationPlan {
	var plan LeafGenerationPlan
	if state == nil || manifest == nil {
		plan.Admission = leafGenerationPlanAdmissionNoCandidates
		return plan
	}
	plan.CurrentCommitSeq = state.CommitSeq
	plan.CurrentGenerationID = manifest.CurrentGenerationID

	set := state.ValueLogSet
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

func leafGenerationPlanAdmission(opts LeafGenerationPlanOptions, plan LeafGenerationPlan) string {
	if len(plan.Candidates) == 0 {
		return leafGenerationPlanAdmissionNoCandidates
	}
	if opts.Force {
		return leafGenerationPlanAdmissionEligible
	}
	if opts.MinCandidateGenerations > 0 && len(plan.Candidates) < opts.MinCandidateGenerations {
		return leafGenerationPlanAdmissionTooFewGenerations
	}
	if opts.MinExpectedReclaimBytes > 0 && plan.ExpectedReclaimBytes < opts.MinExpectedReclaimBytes {
		return leafGenerationPlanAdmissionReclaimTooSmall
	}
	if opts.MinExpectedReclaimRatioPPM > 0 && plan.ExpectedReclaimRatioPPM < opts.MinExpectedReclaimRatioPPM {
		return leafGenerationPlanAdmissionReclaimTooLow
	}
	if opts.MinReclaimPerByteCopiedPPM > 0 && plan.ExpectedReclaimPerByteCopiedPPM < opts.MinReclaimPerByteCopiedPPM {
		return leafGenerationPlanAdmissionReclaimPerCopyTooLow
	}
	return leafGenerationPlanAdmissionEligible
}

func leafGenerationWholeGenerationGCEligible(gen LeafGenerationPlanGeneration) bool {
	if gen.PinnedCount > 0 {
		return false
	}
	switch gen.State {
	case leafGenerationStateWritable, leafGenerationStateRetiring, leafGenerationStateDeleted:
		return false
	}
	return gen.BytesDead > 0 && gen.BytesLive <= 0
}

func leafGenerationPlanEligibility(gen LeafGenerationPlanGeneration, opts LeafGenerationPlanOptions) (bool, string) {
	switch gen.State {
	case leafGenerationStateWritable:
		return false, leafGenerationPlanSkipWritableGeneration
	case leafGenerationStateRetiring:
		return false, leafGenerationPlanSkipRetiringGeneration
	case leafGenerationStateDeleted:
		return false, leafGenerationPlanSkipDeletedGeneration
	}
	if gen.BytesDead <= 0 {
		if opts.Force && gen.BytesLive > 0 {
			return true, ""
		}
		return false, leafGenerationPlanSkipNoDeadBytes
	}
	if gen.BytesLive <= 0 {
		return false, leafGenerationPlanSkipWholeGenerationGC
	}
	if !opts.Force && opts.MinPublishedAgeCommits > 0 && gen.AgeCommits < opts.MinPublishedAgeCommits {
		return false, leafGenerationPlanSkipFreshGeneration
	}
	return true, ""
}

func rankLeafGenerationPlanCandidates(gens []LeafGenerationPlanGeneration) {
	sort.SliceStable(gens, func(i, j int) bool {
		a := gens[i]
		b := gens[j]
		cmp := compareDeadPerLive(a.BytesDead, a.BytesLive, b.BytesDead, b.BytesLive)
		if cmp != 0 {
			return cmp > 0
		}
		if a.BytesDead != b.BytesDead {
			return a.BytesDead > b.BytesDead
		}
		if a.BytesLive != b.BytesLive {
			return a.BytesLive < b.BytesLive
		}
		return a.GenerationID < b.GenerationID
	})
}

func compareDeadPerLive(aDead, aLive, bDead, bLive int64) int {
	if aDead < 0 {
		aDead = 0
	}
	if aLive < 0 {
		aLive = 0
	}
	if bDead < 0 {
		bDead = 0
	}
	if bLive < 0 {
		bLive = 0
	}
	aInf := aDead > 0 && aLive == 0
	bInf := bDead > 0 && bLive == 0
	if aInf != bInf {
		if aInf {
			return 1
		}
		return -1
	}
	if aDead == 0 && bDead == 0 {
		return 0
	}
	if aLive == 0 {
		aLive = 1
	}
	if bLive == 0 {
		bLive = 1
	}
	leftHi, leftLo := bits.Mul64(uint64(aDead), uint64(bLive))
	rightHi, rightLo := bits.Mul64(uint64(bDead), uint64(aLive))
	if leftHi > rightHi {
		return 1
	}
	if leftHi < rightHi {
		return -1
	}
	if leftLo > rightLo {
		return 1
	}
	if leftLo < rightLo {
		return -1
	}
	return 0
}

func ratioPPM(num, den int64) int {
	if num <= 0 || den <= 0 {
		return 0
	}
	if num >= den {
		return 1_000_000
	}
	return int((num * 1_000_000) / den)
}

func cloneLeafGenerationLiveTotalsMap(src map[uint64]leafGenerationLiveTotals) map[uint64]leafGenerationLiveTotals {
	if len(src) == 0 {
		return map[uint64]leafGenerationLiveTotals{}
	}
	dst := make(map[uint64]leafGenerationLiveTotals, len(src))
	for id, totals := range src {
		dst[id] = totals
	}
	return dst
}

func cloneLeafGenerationLiveScanStats(src leafGenerationLiveScanStats) leafGenerationLiveScanStats {
	return leafGenerationLiveScanStats{
		Generations: cloneLeafGenerationLiveTotalsMap(src.Generations),
	}
}

func registerLeafGenerationSubtreeCacheMissHook(hook func(uint64)) func() {
	leafGenerationSubtreeCacheMissHook.mu.Lock()
	prev := leafGenerationSubtreeCacheMissHook.fn
	leafGenerationSubtreeCacheMissHook.fn = hook
	leafGenerationSubtreeCacheMissHook.mu.Unlock()
	return func() {
		leafGenerationSubtreeCacheMissHook.mu.Lock()
		leafGenerationSubtreeCacheMissHook.fn = prev
		leafGenerationSubtreeCacheMissHook.mu.Unlock()
	}
}

func runLeafGenerationSubtreeCacheMissHook(pageID uint64) {
	leafGenerationSubtreeCacheMissHook.mu.Lock()
	hook := leafGenerationSubtreeCacheMissHook.fn
	leafGenerationSubtreeCacheMissHook.mu.Unlock()
	if hook != nil {
		hook(pageID)
	}
}

func mergeLeafGenerationTotals(dst map[uint64]leafGenerationLiveTotals, src map[uint64]leafGenerationLiveTotals) map[uint64]leafGenerationLiveTotals {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = make(map[uint64]leafGenerationLiveTotals, len(src))
	}
	for genID, totals := range src {
		merged := dst[genID]
		merged.LivePages += totals.LivePages
		merged.LiveBytes += totals.LiveBytes
		dst[genID] = merged
	}
	return dst
}

func (db *DB) loadLeafGenerationSubtreeStats(pageID uint64) (leafGenerationSubtreeStats, bool) {
	if db == nil {
		return nil, false
	}
	db.leafGenerationSubtreeStatsMu.RLock()
	stats, ok := db.leafGenerationSubtreeStatsByPage[pageID]
	db.leafGenerationSubtreeStatsMu.RUnlock()
	return stats, ok
}

func (db *DB) storeLeafGenerationSubtreeStats(pageID uint64, stats leafGenerationSubtreeStats) {
	if db == nil {
		return
	}
	db.leafGenerationSubtreeStatsMu.Lock()
	if db.leafGenerationSubtreeStatsByPage == nil {
		db.leafGenerationSubtreeStatsByPage = make(map[uint64]leafGenerationSubtreeStats)
	}
	db.leafGenerationSubtreeStatsByPage[pageID] = stats
	db.leafGenerationSubtreeStatsMu.Unlock()
}

func (db *DB) invalidateLeafGenerationSubtreeStats(pageIDs []uint64) {
	if db == nil || len(pageIDs) == 0 {
		return
	}
	db.leafGenerationSubtreeStatsMu.Lock()
	for _, pageID := range pageIDs {
		delete(db.leafGenerationSubtreeStatsByPage, pageID)
	}
	db.leafGenerationSubtreeStatsMu.Unlock()
}

func (db *DB) clearLeafGenerationReachabilityCaches() {
	if db == nil {
		return
	}
	db.leafGenerationLiveStatsMu.Lock()
	db.leafGenerationLiveStatsCache = leafGenerationLiveStatsCache{}
	db.leafGenerationLiveStatsMu.Unlock()

	db.leafGenerationSubtreeStatsMu.Lock()
	clear(db.leafGenerationSubtreeStatsByPage)
	db.leafGenerationSubtreeStatsMu.Unlock()
}

func leafGenerationLiveStatsKeyForState(state *DBState) (treeReachabilityCacheKey, bool) {
	if state == nil {
		return treeReachabilityCacheKey{}, false
	}
	key := treeReachabilityCacheKey{
		commitSeq:           state.CommitSeq,
		rootID:              state.RootPageID,
		systemRoot:          state.SystemRootPageID,
		leafGenerationStamp: state.LeafGenerationStateVersion,
	}
	return key, true
}

func (db *DB) loadCachedLeafGenerationLiveStats(key treeReachabilityCacheKey) (leafGenerationLiveScanStats, bool) {
	if db == nil {
		return leafGenerationLiveScanStats{}, false
	}
	db.leafGenerationLiveStatsMu.RLock()
	cache := db.leafGenerationLiveStatsCache
	db.leafGenerationLiveStatsMu.RUnlock()
	if !cache.ok || cache.key != key {
		return leafGenerationLiveScanStats{}, false
	}
	return cloneLeafGenerationLiveScanStats(cache.stats), true
}

func (db *DB) storeCachedLeafGenerationLiveStats(key treeReachabilityCacheKey, stats leafGenerationLiveScanStats) {
	if db == nil {
		return
	}
	cloned := cloneLeafGenerationLiveScanStats(stats)
	db.leafGenerationLiveStatsMu.Lock()
	db.leafGenerationLiveStatsCache = leafGenerationLiveStatsCache{
		key:   key,
		stats: cloned,
		ok:    true,
	}
	db.leafGenerationLiveStatsMu.Unlock()
}

func (db *DB) leafGenerationLiveStatsForSnapshot(ctx context.Context, snap *Snapshot) (leafGenerationLiveScanStats, error) {
	if snap == nil || snap.state == nil {
		runLeafGenerationLiveScanHook()
		return db.scanLeafGenerationLiveStats(ctx, snap)
	}
	cacheKey, cacheable := leafGenerationLiveStatsKeyForState(snap.state)
	if cacheable {
		if stats, ok := db.loadCachedLeafGenerationLiveStats(cacheKey); ok {
			return stats, nil
		}
	}
	runLeafGenerationLiveScanHook()
	stats, err := db.scanLeafGenerationLiveStats(ctx, snap)
	if err != nil {
		return leafGenerationLiveScanStats{}, err
	}
	if cacheable {
		db.storeCachedLeafGenerationLiveStats(cacheKey, stats)
	}
	return stats, nil
}

func (db *DB) leafGenerationLiveStatsForSnapshotWithProtectedRoots(ctx context.Context, snap *Snapshot, protectedRootIDs, protectedSystemRootIDs []uint64) (leafGenerationLiveScanStats, error) {
	if len(protectedRootIDs) == 0 && len(protectedSystemRootIDs) == 0 {
		return db.leafGenerationLiveStatsForSnapshot(ctx, snap)
	}
	if snap == nil || snap.state == nil {
		return db.leafGenerationLiveStatsForSnapshotUncachedWithProtectedRoots(ctx, snap, protectedRootIDs, protectedSystemRootIDs)
	}
	cacheKey, cacheable := leafGenerationLiveStatsKeyForState(snap.state)
	if cacheable {
		cacheKey.protectedRoots = leafGenerationProtectedRootsHash(protectedRootIDs, protectedSystemRootIDs)
		if stats, ok := db.loadCachedLeafGenerationLiveStats(cacheKey); ok {
			return stats, nil
		}
	}
	runLeafGenerationLiveScanHook()
	stats, err := db.scanLeafGenerationLiveStatsWithOptions(ctx, snap, leafGenerationLiveStatsScanOptions{
		DisableCache:           true,
		ProtectedRootIDs:       protectedRootIDs,
		ProtectedSystemRootIDs: protectedSystemRootIDs,
	})
	if err != nil {
		return leafGenerationLiveScanStats{}, err
	}
	if cacheable {
		db.storeCachedLeafGenerationLiveStats(cacheKey, stats)
	}
	return stats, nil
}

// leafGenerationProtectedRootsHash makes cache identity independent of caller
// ordering and duplicates while retaining the user/system root distinction.
func leafGenerationProtectedRootsHash(rootIDs, systemRootIDs []uint64) [32]byte {
	canonicalize := func(ids []uint64) []uint64 {
		if len(ids) == 0 {
			return nil
		}
		seen := make(map[uint64]struct{}, len(ids))
		out := make([]uint64, 0, len(ids))
		for _, id := range ids {
			if id == 0 {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return out
	}
	h := sha256.New()
	var buf [8]byte
	for _, roots := range [][]uint64{canonicalize(rootIDs), canonicalize(systemRootIDs)} {
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

func leafGenerationProtectedRootsOverlapMaintenance(ctx context.Context, snap *Snapshot, rootIDs, systemRootIDs []uint64) (bool, error) {
	maintenanceRoots, err := maintenanceRootsForSnapshotWithContext(ctx, snap)
	if err != nil {
		return false, err
	}
	maintenanceRootIDs := make(map[uint64]struct{}, len(maintenanceRoots))
	for _, root := range maintenanceRoots {
		if root.rootID != 0 {
			maintenanceRootIDs[root.rootID] = struct{}{}
		}
	}
	for _, rootID := range rootIDs {
		if _, ok := maintenanceRootIDs[rootID]; ok && rootID != 0 {
			return true, nil
		}
	}
	for _, systemRootID := range systemRootIDs {
		if systemRootID == 0 {
			continue
		}
		protectedRoots, err := collectMaintenanceRootsForSystemRootWithContext(ctx, snap.idx.pager, &snap.reader, systemRootID)
		if err != nil {
			return false, err
		}
		for _, root := range protectedRoots {
			if _, ok := maintenanceRootIDs[root.rootID]; ok && root.rootID != 0 {
				return true, nil
			}
		}
	}
	return false, nil
}

func (db *DB) leafGenerationLiveStatsForSnapshotUncached(ctx context.Context, snap *Snapshot) (leafGenerationLiveScanStats, error) {
	return db.leafGenerationLiveStatsForSnapshotUncachedWithProtectedRoots(ctx, snap, nil, nil)
}

func (db *DB) leafGenerationLiveStatsForSnapshotUncachedWithProtectedRoots(ctx context.Context, snap *Snapshot, protectedRootIDs, protectedSystemRootIDs []uint64) (leafGenerationLiveScanStats, error) {
	runLeafGenerationLiveScanHook()
	return db.scanLeafGenerationLiveStatsWithOptions(ctx, snap, leafGenerationLiveStatsScanOptions{
		DisableCache:           true,
		ProtectedRootIDs:       protectedRootIDs,
		ProtectedSystemRootIDs: protectedSystemRootIDs,
	})
}

type leafGenerationGroupedFrameInfo struct {
	recordLen uint32
	k         int
	rawLen    uint32
	offsets   []uint32
}

type leafGenerationGroupedFrameScanCache struct {
	max    int
	frames map[groupedRecordKey]leafGenerationGroupedFrameInfo
	order  []groupedRecordKey
	next   int
}

func newLeafGenerationGroupedFrameScanCache(max int) leafGenerationGroupedFrameScanCache {
	return leafGenerationGroupedFrameScanCache{max: max}
}

func (cache *leafGenerationGroupedFrameScanCache) get(key groupedRecordKey) (leafGenerationGroupedFrameInfo, bool) {
	if cache == nil || cache.frames == nil {
		return leafGenerationGroupedFrameInfo{}, false
	}
	info, ok := cache.frames[key]
	return info, ok
}

func (cache *leafGenerationGroupedFrameScanCache) store(key groupedRecordKey, info leafGenerationGroupedFrameInfo) {
	if cache == nil || cache.max <= 0 {
		return
	}
	if cache.frames == nil {
		cache.frames = make(map[groupedRecordKey]leafGenerationGroupedFrameInfo, min(cache.max, 64))
	}
	if _, ok := cache.frames[key]; ok {
		cache.frames[key] = info
		return
	}
	if len(cache.frames) < cache.max {
		cache.frames[key] = info
		cache.order = append(cache.order, key)
		return
	}
	if cache.next < 0 || cache.next >= len(cache.order) {
		cache.next = 0
	}
	evict := cache.order[cache.next]
	delete(cache.frames, evict)
	cache.order[cache.next] = key
	cache.next = (cache.next + 1) % len(cache.order)
	cache.frames[key] = info
}

func (cache *leafGenerationGroupedFrameScanCache) len() int {
	if cache == nil {
		return 0
	}
	return len(cache.frames)
}

func (info leafGenerationGroupedFrameInfo) liveByteContribution(subIndex uint16) (uint32, bool) {
	if info.k <= 1 {
		if info.recordLen == 0 {
			return 0, false
		}
		return info.recordLen, true
	}
	idx := int(subIndex)
	if idx < 0 || idx >= info.k || info.recordLen == 0 {
		return 0, false
	}
	if info.rawLen == 0 {
		start := (uint64(info.recordLen) * uint64(idx)) / uint64(info.k)
		end := (uint64(info.recordLen) * uint64(idx+1)) / uint64(info.k)
		contribution := uint32(end - start)
		if contribution == 0 {
			contribution = 1
		}
		return contribution, true
	}
	if idx+1 >= len(info.offsets) {
		return 0, false
	}
	startRaw := info.offsets[idx]
	endRaw := info.offsets[idx+1]
	if endRaw < startRaw || endRaw > info.rawLen {
		return 0, false
	}
	start := (uint64(info.recordLen) * uint64(startRaw)) / uint64(info.rawLen)
	end := (uint64(info.recordLen) * uint64(endRaw)) / uint64(info.rawLen)
	contribution := uint32(end - start)
	if contribution == 0 {
		contribution = 1
	}
	return contribution, true
}

func (db *DB) leafGenerationGroupedFrameInfo(scan *leafGenerationScanContext, ptr page.LeafLogPtr, recordLen uint32) (leafGenerationGroupedFrameInfo, bool, error) {
	if scan == nil || scan.snap == nil || scan.snap.state == nil || scan.snap.state.ValueLogSet == nil || ptr.Offset < 4 {
		return leafGenerationGroupedFrameInfo{}, false, nil
	}
	vptr := ptr.ValuePtr()
	key, err := groupedRecordKeyForPtr(vptr)
	if err != nil {
		return leafGenerationGroupedFrameInfo{}, false, err
	}
	if info, ok := scan.groupedFrames.get(key); ok {
		return info, true, nil
	}
	f := scan.snap.state.ValueLogSet.Files[vptr.FileID]
	if f == nil || f.File == nil {
		return leafGenerationGroupedFrameInfo{}, false, nil
	}
	if err := f.EnsureCurrentWritableReadableFor(vptr); err != nil {
		return leafGenerationGroupedFrameInfo{}, false, err
	}
	start := int64(ptr.Offset - 4)
	var header [valuelog.HeaderSize]byte
	if _, err := f.File.ReadAt(header[:], start); err != nil {
		return leafGenerationGroupedFrameInfo{}, false, err
	}
	if header[4] != valuelog.Version {
		return leafGenerationGroupedFrameInfo{}, false, valuelog.ErrCorrupt
	}
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	expectedLen := uint32(valuelog.HeaderSize-4) + valueLen
	if recordLen != 0 && recordLen != expectedLen {
		return leafGenerationGroupedFrameInfo{}, false, valuelog.ErrCorrupt
	}
	physicalLen := expectedLen
	if physicalLen <= ^uint32(0)-4 {
		physicalLen += 4
	}
	if header[5]&1 == 0 {
		return leafGenerationGroupedFrameInfo{recordLen: physicalLen, k: 1}, true, nil
	}
	if valueLen < valuelog.FrameHeaderSize {
		return leafGenerationGroupedFrameInfo{}, false, valuelog.ErrCorrupt
	}

	const maxPrefixLen = valuelog.FrameHeaderSize + (valuelog.MaxFrameK * 8) + ((valuelog.MaxFrameK + 1) * 4)
	var prefix [maxPrefixLen]byte
	frameOff := start + valuelog.HeaderSize
	if _, err := f.File.ReadAt(prefix[:valuelog.FrameHeaderSize], frameOff); err != nil {
		return leafGenerationGroupedFrameInfo{}, false, err
	}
	if prefix[0] != valuelog.FrameVersion {
		return leafGenerationGroupedFrameInfo{}, false, valuelog.ErrCorrupt
	}
	k := int(prefix[2])
	if k <= 0 || k > valuelog.MaxFrameK {
		return leafGenerationGroupedFrameInfo{}, false, valuelog.ErrCorrupt
	}
	prefixLen := valuelog.FrameHeaderSize + (k * 8) + ((k + 1) * 4)
	if int(valueLen) < prefixLen {
		return leafGenerationGroupedFrameInfo{}, false, valuelog.ErrCorrupt
	}
	if _, err := f.File.ReadAt(prefix[valuelog.FrameHeaderSize:prefixLen], frameOff+valuelog.FrameHeaderSize); err != nil {
		return leafGenerationGroupedFrameInfo{}, false, err
	}

	info := leafGenerationGroupedFrameInfo{
		recordLen: physicalLen,
		k:         k,
		offsets:   make([]uint32, k+1),
	}
	ridOff := valuelog.FrameHeaderSize
	for i := 0; i < k; i++ {
		if binary.LittleEndian.Uint64(prefix[ridOff:ridOff+8]) == 0 {
			return leafGenerationGroupedFrameInfo{}, false, valuelog.ErrCorrupt
		}
		ridOff += 8
	}
	off := valuelog.FrameHeaderSize + (k * 8)
	prev := uint32(0)
	for i := 0; i < k+1; i++ {
		cur := binary.LittleEndian.Uint32(prefix[off : off+4])
		if cur < prev {
			return leafGenerationGroupedFrameInfo{}, false, valuelog.ErrCorrupt
		}
		info.offsets[i] = cur
		prev = cur
		off += 4
	}
	info.rawLen = info.offsets[k]
	scan.groupedFrames.store(key, info)
	return info, true, nil
}

func (db *DB) newLeafGenerationScanContext(snap *Snapshot) (*leafGenerationScanContext, error) {
	if snap == nil || snap.state == nil || snap.state.LeafGenerations == nil {
		return nil, nil
	}
	view := snap.state.LeafGenerations
	fileStateByID := make(map[uint32]*leafGenerationScanFileState, len(view.FileToGeneration))
	fileStates := make([]leafGenerationScanFileState, 0, len(view.FileToGeneration))
	for fileID, genID := range view.FileToGeneration {
		gen, ok := view.Generations[genID]
		if !ok {
			return nil, fmt.Errorf("maintenance reachability: missing generation for leaf file %d", fileID)
		}
		persist := gen.State == leafGenerationStateSealed
		idx, err := db.loadOrBuildLeafGenerationRecordLengthIndex(fileID, snap.state.ValueLogSet, persist)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		fileStates = append(fileStates, leafGenerationScanFileState{
			fileID:  fileID,
			genID:   genID,
			persist: persist,
			idx:     idx,
		})
		fileStateByID[fileID] = &fileStates[len(fileStates)-1]
	}
	return &leafGenerationScanContext{
		snap:          snap,
		fileStateByID: fileStateByID,
		groupedFrames: newLeafGenerationGroupedFrameScanCache(leafGenerationGroupedFrameScanCacheEntries),
	}, nil
}

func (db *DB) scanLeafGenerationPtrTotals(scan *leafGenerationScanContext, dst leafGenerationSubtreeStats, ptr page.LeafLogPtr) (leafGenerationSubtreeStats, error) {
	if scan == nil {
		return dst, fmt.Errorf("leaf generation plan: missing scan context")
	}
	fileState := scan.lastFileState
	if fileState == nil || scan.lastFileID != ptr.FileID {
		var ok bool
		fileState, ok = scan.fileStateByID[ptr.FileID]
		if !ok {
			return dst, fmt.Errorf("leaf generation plan: missing generation for leaf file %d", ptr.FileID)
		}
		scan.lastFileID = ptr.FileID
		scan.lastFileState = fileState
	}
	recordLen := ptr.RecordLength()
	ok := recordLen != 0
	if !ok {
		var hint int
		recordLen, hint, ok = fileState.idx.lookupWithHint(ptr.Offset, fileState.lookupHint)
		fileState.lookupHint = hint
	}
	if !ok {
		if !fileState.persist {
			idx, err := db.buildLeafGenerationRecordLengthIndex(ptr.FileID, scan.snap.state.ValueLogSet)
			if err != nil {
				return dst, err
			}
			db.storeLeafGenerationRecordLengthIndex(ptr.FileID, idx)
			fileState.idx = idx
			var hint int
			recordLen, hint, ok = idx.lookupWithHint(ptr.Offset, fileState.lookupHint)
			fileState.lookupHint = hint
		}
		if !ok {
			if fileState.persist {
				return dst, fmt.Errorf("leaf generation plan: missing record length for file=%d offset=%d", ptr.FileID, ptr.Offset)
			}
			var err error
			recordLen, err = db.valueLogRecordLengthForRewriteInSet(ptr.ValuePtr(), scan.snap.state.ValueLogSet)
			if err != nil {
				return dst, err
			}
		}
	}
	liveBytes := recordLen
	if liveBytes <= ^uint32(0)-4 {
		liveBytes += 4
	}
	if ptr.IsGrouped() {
		if info, grouped, err := db.leafGenerationGroupedFrameInfo(scan, ptr, recordLen); err != nil {
			return dst, err
		} else if grouped {
			if contribution, ok := info.liveByteContribution(ptr.SubIndex); ok {
				liveBytes = contribution
			}
		}
	}
	if dst == nil {
		dst = make(leafGenerationSubtreeStats, 1)
	}
	totals := dst[fileState.genID]
	totals.LivePages++
	totals.LiveBytes += int64(liveBytes)
	dst[fileState.genID] = totals
	return dst, nil
}

func (db *DB) scanLeafGenerationLiveStats(ctx context.Context, snap *Snapshot) (leafGenerationLiveScanStats, error) {
	return db.scanLeafGenerationLiveStatsWithOptions(ctx, snap, leafGenerationLiveStatsScanOptions{})
}

func (db *DB) scanLeafGenerationLiveStatsWithOptions(ctx context.Context, snap *Snapshot, opts leafGenerationLiveStatsScanOptions) (leafGenerationLiveScanStats, error) {
	stats := leafGenerationLiveScanStats{
		Generations: make(map[uint64]leafGenerationLiveTotals),
	}
	if snap == nil || snap.state == nil || snap.state.LeafGenerations == nil || snap.idx == nil || snap.idx.pager == nil {
		return stats, nil
	}
	result, err := db.maintenanceReachabilityScan(ctx, snap, maintenanceReachabilityScanOptions{
		Collectors:              maintenanceReachabilityLeafGenerationTotals,
		ProtectedRootIDs:        opts.ProtectedRootIDs,
		ProtectedSystemRootIDs:  opts.ProtectedSystemRootIDs,
		DisableLeafSubtreeCache: opts.DisableCache,
	})
	if err != nil {
		return leafGenerationLiveScanStats{}, err
	}
	return result.leafGenerationLive, nil
}
