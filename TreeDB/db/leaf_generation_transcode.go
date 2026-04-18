package db

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	leafGenerationTranscodePlanAdmissionDisabled           = "disabled"
	leafGenerationTranscodePlanAdmissionNoCandidates       = "no_candidates"
	leafGenerationTranscodePlanAdmissionNoEstimatedSavings = "no_estimated_savings"
	leafGenerationTranscodePlanAdmissionTooFewGenerations  = "too_few_generations"
	leafGenerationTranscodePlanAdmissionSavedTooSmall      = "saved_too_small"
	leafGenerationTranscodePlanAdmissionSavedRatioTooLow   = "saved_ratio_too_low"
	leafGenerationTranscodePlanAdmissionSavedPerCopyTooLow = "saved_per_copy_too_low"
	leafGenerationTranscodePlanAdmissionEligible           = "eligible"
	leafGenerationTranscodeSkipNoLiveBytes                 = "no_live_bytes"
	leafGenerationTranscodeSkipNoLivePages                 = "no_live_pages"
	leafGenerationTranscodeSkipNoEstimatedSavings          = "no_estimated_savings"
	leafGenerationTranscodeDictStrategyReuseCurrent        = "reuse_current"
	leafGenerationTranscodeDictStrategyReuseClustered      = "reuse_clustered"
	leafGenerationTranscodeDictStrategyFreshSingle         = "fresh_single"
	leafGenerationTranscodeDefaultMinSavedPerByteCopiedPPM = 10_000
	leafGenerationTranscodeDefaultSamplePagesPerGeneration = 64
)

var errLeafGenerationTranscodeEnoughSamples = errors.New("leaf generation transcode: enough samples")

var leafGenerationTranscodePlanPreparedHook = func(db *DB, ctx context.Context, opts LeafGenerationTranscodeOptions) (LeafGenerationTranscodePlan, leafGenerationTranscodePreparedDictSet, error) {
	return db.leafGenerationTranscodePlanPrepared(ctx, opts)
}

var leafGenerationTranscodePackSelectedHook = func(db *DB, ctx context.Context, opts LeafGenerationPackOptions, selectedPlan LeafGenerationPlan) (LeafGenerationPackStats, error) {
	return db.leafGenerationPackSelected(ctx, opts, selectedPlan)
}

var leafGenerationTranscodeTrainFreshDictHook = func(d *DB, state *DBState) ([]byte, error) {
	dict, err := trainRewriteLeafDictFromLiveLeafRefs(d, state, compression.TrainConfig{})
	if err != nil || len(dict) > 0 {
		return dict, err
	}
	return trainLeafGenerationTranscodeFreshDictFallback(d, state)
}

type LeafGenerationTranscodeOptions struct {
	Sync                     bool
	Force                    bool
	IncludeWritableCurrent   bool
	DictStrategy             string
	MinPublishedAgeCommits   uint64
	MinCandidateGenerations  int
	MinExpectedSavedBytes    int64
	MinExpectedSavedRatioPPM int
	MinSavedPerByteCopiedPPM int
	MaxGenerations           int
	MaxBytesToCopy           int64
	SamplePagesPerGeneration int
	ReserveRIDs              func(count int) (start uint64, err error)
}

type LeafGenerationTranscodePlanGeneration struct {
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

	SamplePages                   int
	SampleCurrentBytes            int64
	SampleEstimatedBytesAfter     int64
	EstimatedBytesAfter           int64
	ExpectedBytesSaved            int64
	ExpectedSavedRatioPPM         int
	ExpectedSavedPerByteCopiedPPM int
	SelectedLeafDictID            uint64
	SelectedLeafDictUseRawPages   bool

	Eligible   bool
	SkipReason string
}

type LeafGenerationTranscodePlan struct {
	CurrentCommitSeq    uint64
	CurrentGenerationID uint64

	LeafDictID          uint64
	LeafDictIDs         []uint64
	LeafDictUseRawPages bool
	LeafDictStrategy    string

	Generations            []LeafGenerationTranscodePlanGeneration
	Candidates             []LeafGenerationTranscodePlanGeneration
	CandidateGenerationIDs []uint64

	CandidateBytesTotal          int64
	CandidateBytesLive           int64
	CandidateBytesDead           int64
	CandidateBytesToCopy         int64
	CandidateLivePages           int
	CandidateSamplePages         int
	CandidateEstimatedBytesAfter int64
	CandidateBytesSaved          int64

	ExpectedBytesSaved                 int64
	ExpectedBytesSavedRatioPPM         int
	ExpectedBytesSavedPerByteCopiedPPM int
	Admission                          string
}

type LeafGenerationTranscodeRunOnceStats struct {
	Plan       LeafGenerationTranscodePlan
	Selection  LeafGenerationTranscodeSelection
	Pack       LeafGenerationPackStats
	Ran        bool
	SkipReason string
}

type leafGenerationTranscodePreparedDict struct {
	id          uint64
	bytes       []byte
	useRawPages bool
}

type leafGenerationTranscodePreparedDictSet struct {
	choices []leafGenerationTranscodePreparedDict
	byID    map[uint64]leafGenerationTranscodePreparedDict
}

type leafGenerationTranscodeEstimate struct {
	SamplePages                 int
	SampleCurrentBytes          int64
	SampleEstimatedBytesAfter   int64
	EstimatedBytesAfter         int64
	ExpectedBytesSaved          int64
	SelectedLeafDictID          uint64
	SelectedLeafDictUseRawPages bool
}

// LeafGenerationTranscodePlan estimates the size win from rewriting sealed live
// generations through the outer-leaf dict pipeline, independent of dead-byte
// reclaim economics.
func (db *DB) LeafGenerationTranscodePlan(ctx context.Context, opts LeafGenerationTranscodeOptions) (LeafGenerationTranscodePlan, error) {
	plan, _, err := db.leafGenerationTranscodePlanPrepared(ctx, opts)
	return plan, err
}

// LeafGenerationTranscodeRunOnce computes the current transcode plan, selects a
// bounded subset, and rewrites those sealed generations with the prepared
// outer-leaf dict mode.
func (db *DB) LeafGenerationTranscodeRunOnce(ctx context.Context, opts LeafGenerationTranscodeOptions) (LeafGenerationTranscodeRunOnceStats, error) {
	var stats LeafGenerationTranscodeRunOnceStats
	plan, preparedDicts, err := leafGenerationTranscodePlanPreparedHook(db, ctx, opts)
	if err != nil {
		return stats, err
	}
	stats.Plan = plan
	if plan.Admission != leafGenerationTranscodePlanAdmissionEligible {
		stats.SkipReason = fmt.Sprintf("plan_admission:%s", plan.Admission)
		return stats, nil
	}
	selection, err := SelectLeafGenerationTranscodeCandidates(plan, LeafGenerationTranscodeSelectOptions{
		Force:                    opts.Force,
		MinExpectedSavedBytes:    opts.MinExpectedSavedBytes,
		MinExpectedSavedRatioPPM: opts.MinExpectedSavedRatioPPM,
		MaxGenerations:           opts.MaxGenerations,
		MaxBytesToCopy:           opts.MaxBytesToCopy,
		MinSavedPerByteCopiedPPM: opts.MinSavedPerByteCopiedPPM,
	})
	if err != nil {
		stats.SkipReason = fmt.Sprintf("selection:%v", err)
		return stats, nil
	}
	stats.Selection = selection
	groups, err := groupLeafGenerationTranscodeSelectionByDict(selection, preparedDicts)
	if err != nil {
		return stats, err
	}
	for _, group := range groups {
		packStats, err := leafGenerationTranscodePackSelectedHook(db, ctx, LeafGenerationPackOptions{
			GenerationIDs:        append([]uint64(nil), group.selection.GenerationIDs...),
			Sync:                 opts.Sync,
			ReserveRIDs:          opts.ReserveRIDs,
			Force:                true,
			leafDictID:           group.dict.id,
			leafDictBytes:        append([]byte(nil), group.dict.bytes...),
			leafDictUseRawPages:  group.dict.useRawPages,
			allowWritableCurrent: opts.IncludeWritableCurrent,
		}, selectedLeafGenerationTranscodePlan(group.selection))
		if err != nil {
			return stats, err
		}
		mergeLeafGenerationPackStats(&stats.Pack, packStats)
	}
	stats.Ran = true
	return stats, nil
}

func normalizeLeafGenerationTranscodeOptions(opts LeafGenerationTranscodeOptions) LeafGenerationTranscodeOptions {
	if opts.SamplePagesPerGeneration <= 0 {
		opts.SamplePagesPerGeneration = leafGenerationTranscodeDefaultSamplePagesPerGeneration
	}
	switch opts.DictStrategy {
	case "":
		opts.DictStrategy = leafGenerationTranscodeDictStrategyReuseCurrent
	case leafGenerationTranscodeDictStrategyReuseCurrent, leafGenerationTranscodeDictStrategyReuseClustered, leafGenerationTranscodeDictStrategyFreshSingle:
	}
	if !opts.Force && opts.MinExpectedSavedBytes == 0 && opts.MinExpectedSavedRatioPPM == 0 && opts.MinSavedPerByteCopiedPPM == 0 {
		opts.MinSavedPerByteCopiedPPM = leafGenerationTranscodeDefaultMinSavedPerByteCopiedPPM
	}
	return opts
}

func (db *DB) leafGenerationTranscodePlanPrepared(ctx context.Context, opts LeafGenerationTranscodeOptions) (LeafGenerationTranscodePlan, leafGenerationTranscodePreparedDictSet, error) {
	var (
		plan         LeafGenerationTranscodePlan
		preparedDict leafGenerationTranscodePreparedDictSet
	)
	if db == nil {
		return plan, preparedDict, fmt.Errorf("missing db")
	}
	if !db.indexOuterLeavesInValueLog {
		plan.Admission = leafGenerationTranscodePlanAdmissionDisabled
		return plan, preparedDict, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	opts = normalizeLeafGenerationTranscodeOptions(opts)

	basePlan, err := db.LeafGenerationPlan(ctx, LeafGenerationPlanOptions{Force: true})
	if err != nil {
		return plan, preparedDict, err
	}
	plan.CurrentCommitSeq = basePlan.CurrentCommitSeq
	plan.CurrentGenerationID = basePlan.CurrentGenerationID
	plan.Generations = make([]LeafGenerationTranscodePlanGeneration, 0, len(basePlan.Generations))

	candidateIndexByID := make(map[uint64]int)
	candidateGens := make([]LeafGenerationTranscodePlanGeneration, 0, len(basePlan.Generations))
	for _, gen := range basePlan.Generations {
		entry := leafGenerationTranscodePlanGenerationFromLeafGeneration(gen)
		entry.Eligible, entry.SkipReason = leafGenerationTranscodeEligibility(gen, basePlan.CurrentGenerationID, opts)
		if entry.Eligible {
			candidateIndexByID[entry.GenerationID] = len(candidateGens)
			candidateGens = append(candidateGens, entry)
		}
		plan.Generations = append(plan.Generations, entry)
	}
	if len(candidateGens) == 0 {
		plan.Admission = leafGenerationTranscodePlanAdmissionNoCandidates
		return plan, preparedDict, nil
	}

	snap := db.AcquireSnapshot()
	if snap == nil || snap.state == nil || snap.state.ValueLogSet == nil || snap.idx == nil {
		if snap != nil {
			_ = snap.Close()
		}
		return plan, preparedDict, fmt.Errorf("leaf generation transcode: snapshot unavailable")
	}
	defer func() { _ = snap.Close() }()

	preparedDict, err = prepareLeafGenerationTranscodeDictSet(
		db,
		snap.state,
		candidateGens,
		opts.DictStrategy,
	)
	if err != nil {
		return plan, preparedDict, fmt.Errorf("leaf generation transcode: prepare leaf dict: %w", err)
	}
	if len(preparedDict.choices) == 0 {
		plan.Admission = leafGenerationTranscodePlanAdmissionNoCandidates
		return plan, preparedDict, nil
	}
	plan.LeafDictID = preparedDict.choices[0].id
	plan.LeafDictUseRawPages = preparedDict.choices[0].useRawPages
	plan.LeafDictStrategy = opts.DictStrategy
	for _, choice := range preparedDict.choices {
		plan.LeafDictIDs = append(plan.LeafDictIDs, choice.id)
	}

	estimates, err := db.estimateLeafGenerationTranscodeSavings(ctx, snap, candidateGens, preparedDict, opts)
	if err != nil {
		return plan, preparedDict, err
	}

	for i := range plan.Generations {
		estimate, ok := estimates[plan.Generations[i].GenerationID]
		if !ok {
			if plan.Generations[i].Eligible {
				plan.Generations[i].Eligible = false
				plan.Generations[i].SkipReason = leafGenerationTranscodeSkipNoEstimatedSavings
			}
			continue
		}
		applyLeafGenerationTranscodeEstimate(&plan.Generations[i], estimate)
	}

	plan.Candidates = plan.Candidates[:0]
	for _, gen := range plan.Generations {
		if !gen.Eligible || gen.ExpectedBytesSaved <= 0 {
			continue
		}
		plan.Candidates = append(plan.Candidates, gen)
	}
	if len(plan.Candidates) == 0 {
		plan.Admission = leafGenerationTranscodePlanAdmissionNoEstimatedSavings
		return plan, preparedDict, nil
	}
	rankLeafGenerationTranscodeCandidates(plan.Candidates)
	for _, gen := range plan.Candidates {
		plan.CandidateGenerationIDs = append(plan.CandidateGenerationIDs, gen.GenerationID)
		plan.CandidateBytesTotal += gen.BytesTotal
		plan.CandidateBytesLive += gen.BytesLive
		plan.CandidateBytesDead += gen.BytesDead
		plan.CandidateBytesToCopy += gen.BytesToCopy
		plan.CandidateLivePages += gen.LivePages
		plan.CandidateSamplePages += gen.SamplePages
		plan.CandidateEstimatedBytesAfter += gen.EstimatedBytesAfter
		plan.CandidateBytesSaved += gen.ExpectedBytesSaved
	}
	plan.ExpectedBytesSaved = plan.CandidateBytesSaved
	plan.ExpectedBytesSavedRatioPPM = ratioPPM(plan.CandidateBytesSaved, plan.CandidateBytesToCopy)
	plan.ExpectedBytesSavedPerByteCopiedPPM = ratioPPM(plan.CandidateBytesSaved, plan.CandidateBytesToCopy)
	plan.Admission = leafGenerationTranscodePlanAdmission(opts, plan)

	for _, gen := range plan.Candidates {
		if idx, ok := candidateIndexByID[gen.GenerationID]; ok {
			candidateGens[idx] = gen
		}
	}
	return plan, preparedDict, nil
}

func leafGenerationTranscodePlanGenerationFromLeafGeneration(gen LeafGenerationPlanGeneration) LeafGenerationTranscodePlanGeneration {
	return LeafGenerationTranscodePlanGeneration{
		GenerationID:              gen.GenerationID,
		State:                     gen.State,
		FileIDs:                   append([]uint32(nil), gen.FileIDs...),
		FileCount:                 gen.FileCount,
		BytesTotal:                gen.BytesTotal,
		BytesLive:                 gen.BytesLive,
		BytesDead:                 gen.BytesDead,
		BytesToCopy:               gen.BytesToCopy,
		LivePages:                 gen.LivePages,
		AgeCommits:                gen.AgeCommits,
		PinnedCount:               gen.PinnedCount,
		DeadRatioPPM:              gen.DeadRatioPPM,
		LiveRatioPPM:              gen.LiveRatioPPM,
		WholeGenerationGCEligible: gen.WholeGenerationGCEligible,
	}
}

func leafGenerationTranscodeEligibility(gen LeafGenerationPlanGeneration, currentGenerationID uint64, opts LeafGenerationTranscodeOptions) (bool, string) {
	switch gen.State {
	case leafGenerationStateWritable:
		if opts.IncludeWritableCurrent && gen.GenerationID == currentGenerationID {
			break
		}
		return false, leafGenerationPlanSkipWritableGeneration
	case leafGenerationStateRetiring:
		return false, leafGenerationPlanSkipRetiringGeneration
	case leafGenerationStateDeleted:
		return false, leafGenerationPlanSkipDeletedGeneration
	}
	if gen.BytesLive <= 0 {
		return false, leafGenerationTranscodeSkipNoLiveBytes
	}
	if gen.LivePages <= 0 {
		return false, leafGenerationTranscodeSkipNoLivePages
	}
	if !opts.Force && opts.MinPublishedAgeCommits > 0 && gen.AgeCommits < opts.MinPublishedAgeCommits {
		return false, leafGenerationPlanSkipFreshGeneration
	}
	return true, ""
}

func prepareLeafGenerationTranscodeDict(d *DB, state *DBState, strategy string) (uint64, []byte, bool, error) {
	switch strategy {
	case "", leafGenerationTranscodeDictStrategyReuseCurrent:
		return prepareRewriteLeafDict(
			d,
			state,
			d.valueLogDictCurrentForClass,
			d.valueLogDictLeafPayloadMode,
			d.valueLogDictLookup,
			d.valueLogDictPut,
			d.valueLogDictSetCurrentForClass,
			d.valueLogDictSetLeafPayloadMode,
			compression.TrainConfig{},
		)
	case leafGenerationTranscodeDictStrategyFreshSingle:
		if d == nil || state == nil {
			return 0, nil, false, nil
		}
		if d.valueLogDictPut == nil {
			return 0, nil, false, nil
		}
		dictBytes, err := leafGenerationTranscodeTrainFreshDictHook(d, state)
		if err != nil || len(dictBytes) == 0 {
			return 0, nil, false, err
		}
		dictID, err := d.valueLogDictPut(context.Background(), dictBytes)
		if err != nil {
			return 0, nil, false, err
		}
		if d.valueLogDictSetCurrentForClass != nil {
			if err := d.valueLogDictSetCurrentForClass(context.Background(), "outer_leaf", dictID); err != nil {
				return 0, nil, false, err
			}
		}
		if d.valueLogDictSetLeafPayloadMode != nil {
			if err := d.valueLogDictSetLeafPayloadMode(context.Background(), dictID, false); err != nil {
				return 0, nil, false, err
			}
		}
		return dictID, dictBytes, false, nil
	default:
		return 0, nil, false, fmt.Errorf("leaf generation transcode: unsupported dict strategy %q", strategy)
	}
}

func prepareLeafGenerationTranscodeDictSet(d *DB, state *DBState, candidates []LeafGenerationTranscodePlanGeneration, strategy string) (leafGenerationTranscodePreparedDictSet, error) {
	var out leafGenerationTranscodePreparedDictSet
	add := func(dictID uint64, dictBytes []byte, useRawPages bool) {
		if dictID == 0 || len(dictBytes) == 0 {
			return
		}
		if out.byID == nil {
			out.byID = make(map[uint64]leafGenerationTranscodePreparedDict)
		}
		if _, exists := out.byID[dictID]; exists {
			return
		}
		entry := leafGenerationTranscodePreparedDict{
			id:          dictID,
			bytes:       append([]byte(nil), dictBytes...),
			useRawPages: useRawPages,
		}
		out.byID[dictID] = entry
		out.choices = append(out.choices, entry)
	}

	switch strategy {
	case "", leafGenerationTranscodeDictStrategyReuseCurrent, leafGenerationTranscodeDictStrategyFreshSingle:
		dictID, dictBytes, useRawPages, err := prepareLeafGenerationTranscodeDict(d, state, strategy)
		if err != nil {
			return out, err
		}
		add(dictID, dictBytes, useRawPages)
		return out, nil
	case leafGenerationTranscodeDictStrategyReuseClustered:
		if d == nil || state == nil {
			return out, nil
		}
		if d.valueLogDictCurrentForClass != nil && d.valueLogDictLookup != nil {
			dictID, err := d.valueLogDictCurrentForClass(context.Background(), "outer_leaf")
			if err != nil {
				return out, err
			}
			if dictID != 0 {
				if dictBytes, err := d.valueLogDictLookup(dictID); err == nil && len(dictBytes) > 0 {
					useRawPages, err := resolveRewriteLeafDictUseRawPages(d.valueLogDictLeafPayloadMode, dictID, false)
					if err != nil {
						return out, err
					}
					add(dictID, dictBytes, useRawPages)
				}
			}
		}
		segmentDictIDs, err := collectLeafGenerationTranscodeSegmentDictIDs(state, candidates)
		if err != nil {
			return out, err
		}
		for _, dictID := range segmentDictIDs {
			if d.valueLogDictLookup == nil {
				break
			}
			dictBytes, err := d.valueLogDictLookup(dictID)
			if err != nil || len(dictBytes) == 0 {
				continue
			}
			useRawPages, err := resolveRewriteLeafDictUseRawPages(d.valueLogDictLeafPayloadMode, dictID, true)
			if err != nil {
				return out, err
			}
			add(dictID, dictBytes, useRawPages)
		}
		if len(out.choices) == 0 {
			dictID, dictBytes, useRawPages, err := prepareLeafGenerationTranscodeDict(d, state, leafGenerationTranscodeDictStrategyReuseCurrent)
			if err != nil {
				return out, err
			}
			add(dictID, dictBytes, useRawPages)
		}
		return out, nil
	default:
		return out, fmt.Errorf("leaf generation transcode: unsupported dict strategy %q", strategy)
	}
}

func collectLeafGenerationTranscodeSegmentDictIDs(state *DBState, candidates []LeafGenerationTranscodePlanGeneration) ([]uint64, error) {
	if state == nil || state.ValueLogSet == nil || len(candidates) == 0 {
		return nil, nil
	}
	seenFiles := make(map[uint32]struct{}, 64)
	seenDicts := make(map[uint64]struct{}, 8)
	out := make([]uint64, 0, 8)
	for _, gen := range candidates {
		for _, rawID := range gen.FileIDs {
			fileID := page.ValueLogFileID(rawID)
			if _, ok := seenFiles[fileID]; ok {
				continue
			}
			seenFiles[fileID] = struct{}{}
			seg := state.ValueLogSet.Files[fileID]
			if seg == nil {
				continue
			}
			dictID, err := scanValueLogSegmentPreferredDictID(seg)
			if err != nil {
				return nil, err
			}
			if dictID == 0 {
				continue
			}
			if _, ok := seenDicts[dictID]; ok {
				continue
			}
			seenDicts[dictID] = struct{}{}
			out = append(out, dictID)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func trainLeafGenerationTranscodeFreshDictFallback(d *DB, state *DBState) ([]byte, error) {
	if d == nil || state == nil {
		return nil, nil
	}
	roots := make([]uint64, 0, 2)
	if state.RootPageID != 0 {
		roots = append(roots, state.RootPageID)
	}
	if state.SystemRootPageID != 0 {
		roots = append(roots, state.SystemRootPageID)
	}
	if len(roots) == 0 {
		return nil, nil
	}
	const (
		targetBytes = 4 << 20
		minSamples  = 64
		maxSamples  = 4096
	)
	seen := make(map[page.ValuePtr]struct{}, 1024)
	samples := make([][]byte, 0, minSamples)
	totalBytes := 0
	scratch := make([]byte, 0, page.PageSize)
	visit := func(ptr page.LeafLogPtr) error {
		valuePtr := ptr.ValuePtr()
		if _, ok := seen[valuePtr]; ok {
			return nil
		}
		seen[valuePtr] = struct{}{}
		leafPage, usedScratch, err := d.valueLogManager.ReadUnsafeTo(valuePtr, scratch[:0])
		if err != nil {
			return err
		}
		if usedScratch {
			if cap(leafPage) > rewriteReadScratchMaxCap {
				scratch = nil
			} else {
				scratch = leafPage[:0]
			}
		}
		payload, _, err := valuelog.MaybeCompactLeafLogPayload(leafPage)
		if err != nil {
			return err
		}
		samples = append(samples, append([]byte(nil), payload...))
		totalBytes += len(payload)
		if len(samples) >= maxSamples || (len(samples) >= minSamples && totalBytes >= targetBytes) {
			return errLeafGenerationTranscodeEnoughSamples
		}
		return nil
	}
	if err := leafrefscan.WalkRoots(context.Background(), roots, d.Pager().Get, nil, visit); err != nil && !errors.Is(err, errLeafGenerationTranscodeEnoughSamples) {
		return nil, err
	}
	if len(samples) < minSamples {
		return nil, nil
	}
	return buildRewriteLeafDict(samples, rewriteLeafDictBytes(compression.TrainConfig{}))
}

func applyLeafGenerationTranscodeEstimate(gen *LeafGenerationTranscodePlanGeneration, estimate leafGenerationTranscodeEstimate) {
	if gen == nil {
		return
	}
	gen.SamplePages = estimate.SamplePages
	gen.SampleCurrentBytes = estimate.SampleCurrentBytes
	gen.SampleEstimatedBytesAfter = estimate.SampleEstimatedBytesAfter
	gen.EstimatedBytesAfter = estimate.EstimatedBytesAfter
	gen.ExpectedBytesSaved = estimate.ExpectedBytesSaved
	gen.ExpectedSavedRatioPPM = ratioPPM(gen.ExpectedBytesSaved, gen.BytesToCopy)
	gen.ExpectedSavedPerByteCopiedPPM = ratioPPM(gen.ExpectedBytesSaved, gen.BytesToCopy)
	gen.SelectedLeafDictID = estimate.SelectedLeafDictID
	gen.SelectedLeafDictUseRawPages = estimate.SelectedLeafDictUseRawPages
	if gen.ExpectedBytesSaved <= 0 {
		gen.Eligible = false
		gen.SkipReason = leafGenerationTranscodeSkipNoEstimatedSavings
	}
}

func leafGenerationTranscodePlanAdmission(opts LeafGenerationTranscodeOptions, plan LeafGenerationTranscodePlan) string {
	if len(plan.Candidates) == 0 {
		if plan.Admission == leafGenerationTranscodePlanAdmissionNoEstimatedSavings {
			return plan.Admission
		}
		return leafGenerationTranscodePlanAdmissionNoCandidates
	}
	if opts.Force {
		return leafGenerationTranscodePlanAdmissionEligible
	}
	if opts.MinCandidateGenerations > 0 && len(plan.Candidates) < opts.MinCandidateGenerations {
		return leafGenerationTranscodePlanAdmissionTooFewGenerations
	}
	if opts.MinExpectedSavedBytes > 0 && plan.ExpectedBytesSaved < opts.MinExpectedSavedBytes {
		return leafGenerationTranscodePlanAdmissionSavedTooSmall
	}
	if opts.MinExpectedSavedRatioPPM > 0 && plan.ExpectedBytesSavedRatioPPM < opts.MinExpectedSavedRatioPPM {
		return leafGenerationTranscodePlanAdmissionSavedRatioTooLow
	}
	if opts.MinSavedPerByteCopiedPPM > 0 && plan.ExpectedBytesSavedPerByteCopiedPPM < opts.MinSavedPerByteCopiedPPM {
		return leafGenerationTranscodePlanAdmissionSavedPerCopyTooLow
	}
	return leafGenerationTranscodePlanAdmissionEligible
}

func rankLeafGenerationTranscodeCandidates(gens []LeafGenerationTranscodePlanGeneration) {
	sort.SliceStable(gens, func(i, j int) bool {
		a := gens[i]
		b := gens[j]
		if a.ExpectedSavedPerByteCopiedPPM != b.ExpectedSavedPerByteCopiedPPM {
			return a.ExpectedSavedPerByteCopiedPPM > b.ExpectedSavedPerByteCopiedPPM
		}
		if a.ExpectedBytesSaved != b.ExpectedBytesSaved {
			return a.ExpectedBytesSaved > b.ExpectedBytesSaved
		}
		if a.BytesToCopy != b.BytesToCopy {
			return a.BytesToCopy < b.BytesToCopy
		}
		return a.GenerationID < b.GenerationID
	})
}

func selectedLeafGenerationTranscodePlan(selection LeafGenerationTranscodeSelection) LeafGenerationPlan {
	gens := make([]LeafGenerationPlanGeneration, 0, len(selection.Generations))
	for _, gen := range selection.Generations {
		gens = append(gens, LeafGenerationPlanGeneration{
			GenerationID: gen.GenerationID,
			State:        gen.State,
			FileIDs:      append([]uint32(nil), gen.FileIDs...),
			FileCount:    gen.FileCount,
			BytesTotal:   gen.BytesLive,
			BytesLive:    gen.EstimatedBytesAfter,
			BytesDead:    gen.ExpectedBytesSaved,
			BytesToCopy:  gen.BytesToCopy,
			LivePages:    gen.LivePages,
			AgeCommits:   gen.AgeCommits,
			PinnedCount:  gen.PinnedCount,
			DeadRatioPPM: ratioPPM(gen.ExpectedBytesSaved, gen.BytesLive),
			LiveRatioPPM: ratioPPM(gen.EstimatedBytesAfter, gen.BytesLive),
			Eligible:     true,
		})
	}
	return LeafGenerationPlan{
		Admission:                       leafGenerationPlanAdmissionEligible,
		Generations:                     append([]LeafGenerationPlanGeneration(nil), gens...),
		Candidates:                      append([]LeafGenerationPlanGeneration(nil), gens...),
		CandidateGenerationIDs:          append([]uint64(nil), selection.GenerationIDs...),
		CandidateBytesTotal:             selection.BytesLive,
		CandidateBytesLive:              selection.EstimatedBytesAfter,
		CandidateBytesDead:              selection.ExpectedBytesSaved,
		CandidateBytesToCopy:            selection.BytesToCopy,
		CandidateLivePages:              selection.LivePages,
		ExpectedReclaimBytes:            selection.ExpectedBytesSaved,
		ExpectedReclaimRatioPPM:         ratioPPM(selection.ExpectedBytesSaved, selection.BytesLive),
		ExpectedReclaimPerByteCopiedPPM: ratioPPM(selection.ExpectedBytesSaved, selection.BytesToCopy),
	}
}

func (db *DB) estimateLeafGenerationTranscodeSavings(ctx context.Context, snap *Snapshot, candidates []LeafGenerationTranscodePlanGeneration, preparedDicts leafGenerationTranscodePreparedDictSet, opts LeafGenerationTranscodeOptions) (map[uint64]leafGenerationTranscodeEstimate, error) {
	if db == nil || snap == nil || snap.state == nil || snap.state.LeafGenerations == nil || snap.state.ValueLogSet == nil || snap.idx == nil {
		return nil, fmt.Errorf("leaf generation transcode: snapshot state unavailable")
	}
	if len(preparedDicts.choices) == 0 {
		return map[uint64]leafGenerationTranscodeEstimate{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	quotas := make(map[uint64]int, len(candidates))
	candidateByID := make(map[uint64]LeafGenerationTranscodePlanGeneration, len(candidates))
	remaining := 0
	for _, gen := range candidates {
		quota := gen.LivePages
		if quota > opts.SamplePagesPerGeneration {
			quota = opts.SamplePagesPerGeneration
		}
		if quota <= 0 {
			continue
		}
		candidateByID[gen.GenerationID] = gen
		quotas[gen.GenerationID] = quota
		remaining += quota
	}
	if remaining == 0 {
		return map[uint64]leafGenerationTranscodeEstimate{}, nil
	}

	verifyAlways := snap.idx.pager.VerifyOnRead()
	verify := func(pageID uint64, n node.Node) error {
		if verifyAlways || !snap.idx.pager.IsVerified(pageID) {
			if !n.VerifyChecksum() {
				return fmt.Errorf("leaf generation transcode: checksum mismatch on page %d", pageID)
			}
			if !verifyAlways {
				snap.idx.pager.MarkVerified(pageID)
			}
		}
		return nil
	}
	roots := make([]uint64, 0, 2)
	if snap.state.RootPageID != 0 {
		roots = append(roots, snap.state.RootPageID)
	}
	if snap.state.SystemRootPageID != 0 {
		roots = append(roots, snap.state.SystemRootPageID)
	}
	if len(roots) == 0 {
		return map[uint64]leafGenerationTranscodeEstimate{}, nil
	}

	preparer := valuelog.NewFramePreparer()
	records := [1]valuelog.Record{{RID: 1}}
	bodyScratch := make([]byte, 0, page.PageSize)
	leafScratch := make([]byte, 0, page.PageSize)
	estimates := make(map[uint64]leafGenerationTranscodeEstimate, len(candidates))
	sampleEstimatedByDict := make(map[uint64][]int64, len(candidates))
	view := snap.state.LeafGenerations

	visit := func(ptr page.LeafLogPtr) error {
		genID, ok := view.FileToGeneration[ptr.FileID]
		if !ok {
			return nil
		}
		quota := quotas[genID]
		if quota <= 0 {
			return nil
		}
		currentLen, _, err := db.leafGenerationRecordLengthForPlan(ptr, snap.state.ValueLogSet, view)
		if err != nil {
			return err
		}
		leafPage, usedScratch, err := snap.state.ValueLogSet.ReadUnsafeTo(ptr.ValuePtr(), leafScratch[:0])
		if err != nil {
			return err
		}
		if usedScratch {
			if cap(leafPage) > rewriteReadScratchMaxCap {
				leafScratch = nil
			} else {
				leafScratch = leafPage[:0]
			}
		}
		var compactPayload []byte
		haveCompact := false
		perDictEstimated, ok := sampleEstimatedByDict[genID]
		if !ok {
			perDictEstimated = make([]int64, len(preparedDicts.choices))
			sampleEstimatedByDict[genID] = perDictEstimated
		}
		for idx, preparedDict := range preparedDicts.choices {
			payload := leafPage
			if !preparedDict.useRawPages {
				if !haveCompact {
					compactPayload, _, err = valuelog.MaybeCompactLeafLogPayload(leafPage)
					if err != nil {
						return err
					}
					haveCompact = true
				}
				payload = compactPayload
			}
			records[0].Value = payload
			body, _, err := preparer.PrepareFrameInto(bodyScratch[:0], preparedDict.id, preparedDict.bytes, records[:])
			if err != nil {
				return err
			}
			perDictEstimated[idx] += int64((valuelog.HeaderSize - 4) + len(body))
			if cap(body) > rewriteReadScratchMaxCap {
				bodyScratch = nil
			} else {
				bodyScratch = body[:0]
			}
		}
		estimate := estimates[genID]
		estimate.SamplePages++
		estimate.SampleCurrentBytes += int64(currentLen)
		estimates[genID] = estimate
		quotas[genID] = quota - 1
		remaining--
		if remaining <= 0 {
			return errLeafGenerationTranscodeEnoughSamples
		}
		return nil
	}
	if err := leafrefscan.WalkRoots(ctx, roots, snap.idx.pager.Get, verify, visit); err != nil && !errors.Is(err, errLeafGenerationTranscodeEnoughSamples) {
		return nil, err
	}

	for genID, estimate := range estimates {
		gen := candidateByID[genID]
		if estimate.SampleCurrentBytes <= 0 || gen.BytesLive <= 0 {
			delete(estimates, genID)
			continue
		}
		bestIdx, bestSampleEstimatedAfter := pickLeafGenerationTranscodeBestDict(sampleEstimatedByDict[genID])
		if bestIdx < 0 {
			delete(estimates, genID)
			continue
		}
		estimate.SampleEstimatedBytesAfter = bestSampleEstimatedAfter
		estimate.SelectedLeafDictID = preparedDicts.choices[bestIdx].id
		estimate.SelectedLeafDictUseRawPages = preparedDicts.choices[bestIdx].useRawPages
		estimatedAfter := (gen.BytesLive * estimate.SampleEstimatedBytesAfter) / estimate.SampleCurrentBytes
		if estimatedAfter < 0 {
			estimatedAfter = 0
		}
		if estimatedAfter > gen.BytesLive {
			estimatedAfter = gen.BytesLive
		}
		estimate.EstimatedBytesAfter = estimatedAfter
		estimate.ExpectedBytesSaved = gen.BytesLive - estimatedAfter
		if estimate.ExpectedBytesSaved <= 0 {
			delete(estimates, genID)
			continue
		}
		estimates[genID] = estimate
	}
	return estimates, nil
}

func pickLeafGenerationTranscodeBestDict(sampleEstimatedBytesAfter []int64) (int, int64) {
	bestIdx := -1
	bestBytes := int64(0)
	for idx, bytesAfter := range sampleEstimatedBytesAfter {
		if bytesAfter <= 0 {
			continue
		}
		if bestIdx < 0 || bytesAfter < bestBytes {
			bestIdx = idx
			bestBytes = bytesAfter
		}
	}
	return bestIdx, bestBytes
}

type leafGenerationTranscodeSelectionGroup struct {
	dict      leafGenerationTranscodePreparedDict
	selection LeafGenerationTranscodeSelection
}

func groupLeafGenerationTranscodeSelectionByDict(selection LeafGenerationTranscodeSelection, preparedDicts leafGenerationTranscodePreparedDictSet) ([]leafGenerationTranscodeSelectionGroup, error) {
	if len(selection.Generations) == 0 {
		return nil, nil
	}
	if len(preparedDicts.choices) == 0 {
		return nil, fmt.Errorf("leaf generation transcode: missing prepared dicts")
	}
	defaultDict := preparedDicts.choices[0]
	lookup := preparedDicts.byID
	groupsByID := make(map[uint64]*leafGenerationTranscodeSelectionGroup, len(preparedDicts.choices))
	order := make([]uint64, 0, len(preparedDicts.choices))
	for _, gen := range selection.Generations {
		dictID := gen.SelectedLeafDictID
		if dictID == 0 {
			dictID = defaultDict.id
		}
		dict, ok := lookup[dictID]
		if !ok {
			if dictID == defaultDict.id {
				dict = defaultDict
			} else {
				return nil, fmt.Errorf("leaf generation transcode: missing prepared dict %d for generation %d", dictID, gen.GenerationID)
			}
		}
		group := groupsByID[dict.id]
		if group == nil {
			group = &leafGenerationTranscodeSelectionGroup{dict: dict}
			groupsByID[dict.id] = group
			order = append(order, dict.id)
		}
		appendLeafGenerationTranscodeSelection(&group.selection, gen)
	}
	out := make([]leafGenerationTranscodeSelectionGroup, 0, len(order))
	for _, dictID := range order {
		group := groupsByID[dictID]
		group.selection.ExpectedBytesSavedRatioPPM = ratioPPM(group.selection.ExpectedBytesSaved, group.selection.BytesToCopy)
		group.selection.ExpectedBytesSavedPerByteCopiedPPM = ratioPPM(group.selection.ExpectedBytesSaved, group.selection.BytesToCopy)
		out = append(out, *group)
	}
	return out, nil
}

func mergeLeafGenerationPackStats(dst *LeafGenerationPackStats, src LeafGenerationPackStats) {
	if dst == nil {
		return
	}
	dst.GenerationsRequested += src.GenerationsRequested
	dst.GenerationsMatched += src.GenerationsMatched
	dst.SourceGenerationIDs = append(dst.SourceGenerationIDs, src.SourceGenerationIDs...)
	dst.SourceFilesRequested += src.SourceFilesRequested
	dst.SourceFileIDs = append(dst.SourceFileIDs, src.SourceFileIDs...)
	dst.SourceBytesTotal += src.SourceBytesTotal
	dst.SourceBytesLive += src.SourceBytesLive
	dst.SourceBytesDead += src.SourceBytesDead
	dst.SourceBytesToCopy += src.SourceBytesToCopy
	dst.ExpectedReclaimBytes += src.ExpectedReclaimBytes
	dst.LeafPagesCopied += src.LeafPagesCopied
	dst.BytesCopied += src.BytesCopied
	dst.InternalPagesVisited += src.InternalPagesVisited
	dst.SubtreesPruned += src.SubtreesPruned
	dst.CreatedFileIDs = append(dst.CreatedFileIDs, src.CreatedFileIDs...)
	dst.WallTimeNanos += src.WallTimeNanos
	dst.ExpectedReclaimRatioPPM = ratioPPM(dst.ExpectedReclaimBytes, dst.SourceBytesLive)
	dst.ExpectedReclaimPerByteCopiedPPM = ratioPPM(dst.ExpectedReclaimBytes, dst.SourceBytesToCopy)
}
