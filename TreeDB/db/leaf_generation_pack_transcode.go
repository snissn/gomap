package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	leafGenerationPackSelectionModeReclaim   = "reclaim"
	leafGenerationPackSelectionModeTranscode = "transcode"

	leafGenerationPackDefaultMinTranscodePerByteCopiedPPM = 50_000
	leafGenerationPackTranscodeSamplePagesPerGeneration   = 16
)

var errLeafGenerationPackEnoughTranscodeSamples = errors.New("leaf generation pack: enough transcode samples")

var leafGenerationPackSelectTranscode = func(db *DB, ctx context.Context, plan LeafGenerationPlan, opts LeafGenerationPackFromPlanOptions) (LeafGenerationPackSelection, error) {
	return db.selectLeafGenerationPackTranscodeCandidates(ctx, plan, opts)
}

var leafGenerationPackPrepareLeafDict = prepareRewriteLeafDict

type leafGenerationPackTranscodeEstimate struct {
	SamplePages               int
	SampleCurrentBytes        int64
	SampleEstimatedBytesAfter int64
	EstimatedBytesAfter       int64
	ExpectedSavedBytes        int64
}

func (db *DB) selectLeafGenerationPackTranscodeCandidates(ctx context.Context, plan LeafGenerationPlan, opts LeafGenerationPackFromPlanOptions) (LeafGenerationPackSelection, error) {
	candidates := filterLeafGenerationPackTranscodeCandidates(plan.Generations, opts)
	if len(candidates) == 0 {
		return LeafGenerationPackSelection{}, fmt.Errorf("leaf generation pack transcode selection: no sealed live generations are eligible")
	}
	snap := db.AcquireSnapshot()
	if snap == nil || snap.state == nil || snap.idx == nil {
		return LeafGenerationPackSelection{}, fmt.Errorf("leaf generation pack transcode selection: snapshot unavailable")
	}
	defer func() { _ = snap.Close() }()

	dictID, dictBytes, useRawPages, err := leafGenerationPackPrepareLeafDict(
		db,
		snap.state,
		db.valueLogDictCurrentForClass,
		db.valueLogDictLeafPayloadMode,
		db.valueLogDictLookup,
		db.valueLogDictPut,
		db.valueLogDictSetCurrentForClass,
		db.valueLogDictSetLeafPayloadMode,
		compression.TrainConfig{},
	)
	if err != nil {
		return LeafGenerationPackSelection{}, fmt.Errorf("leaf generation pack transcode selection: prepare leaf dict: %w", err)
	}
	if dictID == 0 || len(dictBytes) == 0 {
		return LeafGenerationPackSelection{}, fmt.Errorf("leaf generation pack transcode selection: outer-leaf dict unavailable")
	}

	estimates, err := db.estimateLeafGenerationPackTranscodeSavings(ctx, snap, candidates, dictID, dictBytes, useRawPages)
	if err != nil {
		return LeafGenerationPackSelection{}, err
	}
	transcodePlan, err := buildLeafGenerationPackTranscodePlan(candidates, estimates, opts)
	if err != nil {
		return LeafGenerationPackSelection{}, err
	}

	minTranscodePerCopyPPM := opts.MinTranscodePerByteCopiedPPM
	if minTranscodePerCopyPPM <= 0 {
		minTranscodePerCopyPPM = leafGenerationPackDefaultMinTranscodePerByteCopiedPPM
	}
	selection, err := SelectLeafGenerationPackCandidates(transcodePlan, LeafGenerationPackSelectOptions{
		Force:                      opts.Force,
		MinExpectedReclaimBytes:    opts.MinExpectedReclaimBytes,
		MinExpectedReclaimRatioPPM: opts.MinExpectedReclaimRatioPPM,
		MaxGenerations:             opts.MaxGenerations,
		MaxBytesToCopy:             opts.MaxBytesToCopy,
		MinReclaimPerByteCopiedPPM: minTranscodePerCopyPPM,
	})
	if err != nil {
		return LeafGenerationPackSelection{}, fmt.Errorf("leaf generation pack transcode selection: %w", err)
	}
	selection.Mode = leafGenerationPackSelectionModeTranscode
	return selection, nil
}

func filterLeafGenerationPackTranscodeCandidates(gens []LeafGenerationPlanGeneration, opts LeafGenerationPackFromPlanOptions) []LeafGenerationPlanGeneration {
	out := make([]LeafGenerationPlanGeneration, 0, len(gens))
	for _, gen := range gens {
		switch gen.State {
		case leafGenerationStateWritable, leafGenerationStateRetiring, leafGenerationStateDeleted:
			continue
		}
		if gen.BytesLive <= 0 || gen.LivePages <= 0 {
			continue
		}
		if !opts.Force && opts.MinPublishedAgeCommits > 0 && gen.AgeCommits < opts.MinPublishedAgeCommits {
			continue
		}
		out = append(out, gen)
	}
	return out
}

func buildLeafGenerationPackTranscodePlan(gens []LeafGenerationPlanGeneration, estimates map[uint64]leafGenerationPackTranscodeEstimate, opts LeafGenerationPackFromPlanOptions) (LeafGenerationPlan, error) {
	plan := LeafGenerationPlan{Admission: leafGenerationPlanAdmissionEligible}
	for _, gen := range gens {
		estimate, ok := estimates[gen.GenerationID]
		if !ok || estimate.ExpectedSavedBytes <= 0 {
			continue
		}
		candidate := gen
		candidate.BytesTotal = gen.BytesLive
		candidate.BytesLive = estimate.EstimatedBytesAfter
		candidate.BytesDead = estimate.ExpectedSavedBytes
		candidate.BytesToCopy = gen.BytesLive
		candidate.Eligible = true
		candidate.SkipReason = ""
		plan.Candidates = append(plan.Candidates, candidate)
	}
	if len(plan.Candidates) == 0 {
		return plan, fmt.Errorf("leaf generation pack transcode selection: sampled generations did not predict any size win")
	}
	if !opts.Force && opts.MinCandidateGenerations > 0 && len(plan.Candidates) < opts.MinCandidateGenerations {
		return plan, fmt.Errorf("leaf generation pack transcode selection: only %d candidate generations satisfy transcode economics (need >= %d)", len(plan.Candidates), opts.MinCandidateGenerations)
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
	return plan, nil
}

func (db *DB) estimateLeafGenerationPackTranscodeSavings(ctx context.Context, snap *Snapshot, candidates []LeafGenerationPlanGeneration, dictID uint64, dict []byte, useRawPages bool) (map[uint64]leafGenerationPackTranscodeEstimate, error) {
	if db == nil || snap == nil || snap.state == nil || snap.state.LeafGenerations == nil || snap.state.ValueLogSet == nil {
		return nil, fmt.Errorf("leaf generation pack transcode selection: snapshot state unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	quotas := make(map[uint64]int, len(candidates))
	candidateByID := make(map[uint64]LeafGenerationPlanGeneration, len(candidates))
	remaining := 0
	for _, gen := range candidates {
		quota := gen.LivePages
		if quota > leafGenerationPackTranscodeSamplePagesPerGeneration {
			quota = leafGenerationPackTranscodeSamplePagesPerGeneration
		}
		if quota <= 0 {
			continue
		}
		candidateByID[gen.GenerationID] = gen
		quotas[gen.GenerationID] = quota
		remaining += quota
	}
	if remaining == 0 {
		return map[uint64]leafGenerationPackTranscodeEstimate{}, nil
	}

	verifyAlways := snap.idx.pager.VerifyOnRead()
	verify := func(pageID uint64, n node.Node) error {
		if verifyAlways || !snap.idx.pager.IsVerified(pageID) {
			if !n.VerifyChecksum() {
				return fmt.Errorf("leaf generation pack transcode selection: checksum mismatch on page %d", pageID)
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
		return map[uint64]leafGenerationPackTranscodeEstimate{}, nil
	}

	preparer := valuelog.NewFramePreparer()
	records := [1]valuelog.Record{{RID: 1}}
	bodyScratch := make([]byte, 0, page.PageSize)
	leafScratch := make([]byte, 0, page.PageSize)
	estimates := make(map[uint64]leafGenerationPackTranscodeEstimate, len(candidates))
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
		payload := leafPage
		if !useRawPages {
			payload, _, err = valuelog.MaybeCompactLeafLogPayload(leafPage)
			if err != nil {
				return err
			}
		}
		records[0].Value = payload
		body, _, err := preparer.PrepareFrameInto(bodyScratch[:0], dictID, dict, records[:])
		if err != nil {
			return err
		}
		if cap(body) > rewriteReadScratchMaxCap {
			bodyScratch = nil
		} else {
			bodyScratch = body[:0]
		}
		estimate := estimates[genID]
		estimate.SamplePages++
		estimate.SampleCurrentBytes += int64(currentLen)
		estimate.SampleEstimatedBytesAfter += int64((valuelog.HeaderSize - 4) + len(body))
		estimates[genID] = estimate
		quota--
		quotas[genID] = quota
		remaining--
		if remaining <= 0 {
			return errLeafGenerationPackEnoughTranscodeSamples
		}
		return nil
	}
	if err := leafrefscan.WalkRoots(ctx, roots, snap.idx.pager.Get, verify, visit); err != nil && !errors.Is(err, errLeafGenerationPackEnoughTranscodeSamples) {
		return nil, err
	}

	for genID, estimate := range estimates {
		gen := candidateByID[genID]
		if estimate.SampleCurrentBytes <= 0 || gen.BytesLive <= 0 {
			delete(estimates, genID)
			continue
		}
		estimatedAfter := (gen.BytesLive * estimate.SampleEstimatedBytesAfter) / estimate.SampleCurrentBytes
		if estimatedAfter < 0 {
			estimatedAfter = 0
		}
		if estimatedAfter > gen.BytesLive {
			estimatedAfter = gen.BytesLive
		}
		estimate.EstimatedBytesAfter = estimatedAfter
		estimate.ExpectedSavedBytes = gen.BytesLive - estimatedAfter
		if estimate.ExpectedSavedBytes <= 0 {
			delete(estimates, genID)
			continue
		}
		estimates[genID] = estimate
	}
	return estimates, nil
}
