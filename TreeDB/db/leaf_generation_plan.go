package db

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/node"
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
)

type LeafGenerationPlanOptions struct {
	MinPublishedAgeCommits     uint64
	MinCandidateGenerations    int
	MinExpectedReclaimBytes    int64
	MinExpectedReclaimRatioPPM int
	MinReclaimPerByteCopiedPPM int
	Force                      bool
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
}

type leafGenerationLiveScanStats struct {
	Generations map[uint64]leafGenerationLiveTotals
	Files       map[uint32]leafGenerationLiveTotals
}

type leafGenerationLiveTotals struct {
	LivePages int
	LiveBytes int64
}

var leafGenerationLiveScanHook struct {
	mu sync.Mutex
	fn func()
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

// LeafGenerationPlan estimates reclaim opportunities for sealed leaf generations
// by scanning the current live tree once and attributing reachable LeafRef pages
// back to manifest generations.
func (db *DB) LeafGenerationPlan(ctx context.Context, opts LeafGenerationPlanOptions) (LeafGenerationPlan, error) {
	var plan LeafGenerationPlan
	if db == nil {
		return plan, fmt.Errorf("missing db")
	}
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
		db.unpinLeafGenerationIDs(snap.leafGenerationIDs)
		snap.leafGenerationIDs = snap.leafGenerationIDs[:0]
	}
	defer func() { _ = snap.Close() }()

	if manifest == nil || snap.state == nil {
		plan.Admission = leafGenerationPlanAdmissionNoCandidates
		return plan, nil
	}
	plan.CurrentCommitSeq = snap.state.CommitSeq
	plan.CurrentGenerationID = manifest.CurrentGenerationID

	liveScan, err := db.leafGenerationLiveStatsForSnapshot(ctx, snap)
	if err != nil {
		return plan, err
	}

	set := snap.state.ValueLogSet
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
			entry.BytesTotal += leafGenerationRawFileSize(db.dir, set, rawFileID)
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
	return plan, nil
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
	left := aDead * bLive
	right := bDead * aLive
	if left > right {
		return 1
	}
	if left < right {
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

func cloneLeafGenerationFileTotalsMap(src map[uint32]leafGenerationLiveTotals) map[uint32]leafGenerationLiveTotals {
	if len(src) == 0 {
		return map[uint32]leafGenerationLiveTotals{}
	}
	dst := make(map[uint32]leafGenerationLiveTotals, len(src))
	for id, totals := range src {
		dst[id] = totals
	}
	return dst
}

func cloneLeafGenerationLiveScanStats(src leafGenerationLiveScanStats) leafGenerationLiveScanStats {
	return leafGenerationLiveScanStats{
		Generations: cloneLeafGenerationLiveTotalsMap(src.Generations),
		Files:       cloneLeafGenerationFileTotalsMap(src.Files),
	}
}

func (db *DB) loadCachedLeafGenerationLiveStats(key valueLogRewriteLiveBytesKey) (leafGenerationLiveScanStats, bool) {
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

func (db *DB) storeCachedLeafGenerationLiveStats(key valueLogRewriteLiveBytesKey, stats leafGenerationLiveScanStats) {
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
	cacheKey, cacheable := rewritePlanLiveBytesKeyForState(snap.state)
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

func (db *DB) scanLeafGenerationLiveStats(ctx context.Context, snap *Snapshot) (leafGenerationLiveScanStats, error) {
	stats := leafGenerationLiveScanStats{
		Generations: make(map[uint64]leafGenerationLiveTotals),
		Files:       make(map[uint32]leafGenerationLiveTotals),
	}
	if snap == nil || snap.state == nil || snap.state.LeafGenerations == nil || snap.idx == nil || snap.idx.pager == nil {
		return stats, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	view := snap.state.LeafGenerations
	type leafGenerationScanFileState struct {
		fileID   uint32
		genIndex int
		persist  bool
		idx      *leafGenerationRecordLengthIndex
	}
	genStates := make([]uint64, 0, len(view.GenerationOrder))
	genIndexByID := make(map[uint64]int, len(view.GenerationOrder))
	for _, genID := range view.GenerationOrder {
		if _, ok := view.Generations[genID]; !ok {
			continue
		}
		genIndexByID[genID] = len(genStates)
		genStates = append(genStates, genID)
	}
	fileStates := make([]leafGenerationScanFileState, 0, len(view.FileToGeneration))
	fileIndexByID := make(map[uint32]int, len(view.FileToGeneration))
	for fileID, genID := range view.FileToGeneration {
		gen, ok := view.Generations[genID]
		if !ok {
			return leafGenerationLiveScanStats{}, fmt.Errorf("leaf generation plan: missing generation for leaf file %d", fileID)
		}
		genIndex, ok := genIndexByID[genID]
		if !ok {
			return leafGenerationLiveScanStats{}, fmt.Errorf("leaf generation plan: missing generation order entry for leaf generation %d", genID)
		}
		persist := gen.State == leafGenerationStateSealed
		idx, err := db.loadOrBuildLeafGenerationRecordLengthIndex(fileID, snap.state.ValueLogSet, persist)
		if err != nil {
			return leafGenerationLiveScanStats{}, err
		}
		fileIndexByID[fileID] = len(fileStates)
		fileStates = append(fileStates, leafGenerationScanFileState{
			fileID:   fileID,
			genIndex: genIndex,
			persist:  persist,
			idx:      idx,
		})
	}
	fileTotals := make([]leafGenerationLiveTotals, len(fileStates))
	genTotals := make([]leafGenerationLiveTotals, len(genStates))
	verify := func(pageID uint64, n node.Node) error {
		if !n.VerifyChecksum() {
			return fmt.Errorf("leaf generation plan: checksum mismatch on page %d", pageID)
		}
		return nil
	}
	lastFileID := uint32(0)
	lastFileIndex := -1
	visit := func(ptr page.LeafLogPtr) error {
		// Published B-tree state should not reference the same external leaf page
		// from multiple parents; leafrefscan.Walk already deduplicates pager page
		// IDs. Keep per-file planner state local to the scan so the hot path only
		// resolves file identity once per contiguous run.
		fileIndex := lastFileIndex
		if fileIndex < 0 || lastFileID != ptr.FileID {
			var ok bool
			fileIndex, ok = fileIndexByID[ptr.FileID]
			if !ok {
				return fmt.Errorf("leaf generation plan: missing generation for leaf file %d", ptr.FileID)
			}
			lastFileID = ptr.FileID
			lastFileIndex = fileIndex
		}
		fileState := &fileStates[fileIndex]
		recordLen, ok := fileState.idx.lookup(ptr.Offset)
		if !ok {
			if !fileState.persist {
				idx, err := db.buildLeafGenerationRecordLengthIndex(ptr.FileID, snap.state.ValueLogSet)
				if err != nil {
					return err
				}
				db.storeLeafGenerationRecordLengthIndex(ptr.FileID, idx)
				fileState.idx = idx
				recordLen, ok = idx.lookup(ptr.Offset)
			}
			if !ok {
				if fileState.persist {
					return fmt.Errorf("leaf generation plan: missing record length for file=%d offset=%d", ptr.FileID, ptr.Offset)
				}
				var err error
				recordLen, err = db.valueLogRecordLengthForRewriteInSet(ptr.ValuePtr(), snap.state.ValueLogSet)
				if err != nil {
					return err
				}
			}
		}
		fileTotals[fileIndex].LivePages++
		fileTotals[fileIndex].LiveBytes += int64(recordLen)
		genTotals[fileState.genIndex].LivePages++
		genTotals[fileState.genIndex].LiveBytes += int64(recordLen)
		return nil
	}
	if err := leafrefscan.WalkRoots(ctx, []uint64{snap.state.RootPageID, snap.state.SystemRootPageID}, snap.idx.pager.Get, verify, visit); err != nil {
		return leafGenerationLiveScanStats{}, err
	}
	for i, totals := range fileTotals {
		if totals.LivePages == 0 && totals.LiveBytes == 0 {
			continue
		}
		stats.Files[fileStates[i].fileID] = totals
	}
	for i, totals := range genTotals {
		if totals.LivePages == 0 && totals.LiveBytes == 0 {
			continue
		}
		stats.Generations[genStates[i]] = totals
	}
	return stats, nil
}
