package db

import "fmt"

// LeafGenerationPackSelectOptions bounds a selected ranked subset of plan
// candidates. Selected generations preserve plan rank order, but oversized
// candidates may be skipped so later ranked generations can still fit a bounded
// online maintenance window.
type LeafGenerationPackSelectOptions struct {
	MaxGenerations             int
	MaxBytesToCopy             int64
	MinReclaimPerByteCopiedPPM int
}

// LeafGenerationPackSelection summarizes a bounded subset of pack candidates.
type LeafGenerationPackSelection struct {
	GenerationIDs                   []uint64
	Generations                     []LeafGenerationPlanGeneration
	BytesTotal                      int64
	BytesLive                       int64
	BytesDead                       int64
	BytesToCopy                     int64
	LivePages                       int
	ExpectedReclaimBytes            int64
	ExpectedReclaimRatioPPM         int
	ExpectedReclaimPerByteCopiedPPM int
}

// SelectLeafGenerationPackCandidates selects a bounded ranked subset from an
// eligible leaf-generation plan. The selected set never exceeds the requested
// bytes-to-copy cap and may stop early when adding the next ranked candidate
// would violate the requested reclaim-per-copy floor.
func SelectLeafGenerationPackCandidates(plan LeafGenerationPlan, opts LeafGenerationPackSelectOptions) (LeafGenerationPackSelection, error) {
	var out LeafGenerationPackSelection
	if plan.Admission != leafGenerationPlanAdmissionEligible {
		return out, fmt.Errorf("leaf generation pack selection: plan admission=%s", plan.Admission)
	}
	if len(plan.Candidates) == 0 {
		return out, fmt.Errorf("leaf generation pack selection: plan produced no candidate generations")
	}
	rejectedOversize := false
	rejectedLowYield := false
	for _, gen := range plan.Candidates {
		if opts.MaxGenerations > 0 && len(out.GenerationIDs) >= opts.MaxGenerations {
			break
		}
		if opts.MaxBytesToCopy > 0 && gen.BytesToCopy > 0 && out.BytesToCopy+gen.BytesToCopy > opts.MaxBytesToCopy {
			rejectedOversize = true
			continue
		}
		if opts.MinReclaimPerByteCopiedPPM > 0 {
			tentativeDead := out.BytesDead + gen.BytesDead
			tentativeCopy := out.BytesToCopy + gen.BytesToCopy
			if tentativeCopy > 0 && ratioPPM(tentativeDead, tentativeCopy) < opts.MinReclaimPerByteCopiedPPM {
				rejectedLowYield = true
				break
			}
		}
		out.GenerationIDs = append(out.GenerationIDs, gen.GenerationID)
		out.Generations = append(out.Generations, gen)
		out.BytesTotal += gen.BytesTotal
		out.BytesLive += gen.BytesLive
		out.BytesDead += gen.BytesDead
		out.BytesToCopy += gen.BytesToCopy
		out.LivePages += gen.LivePages
	}
	if len(out.GenerationIDs) == 0 {
		if opts.MinReclaimPerByteCopiedPPM > 0 && rejectedLowYield {
			return out, fmt.Errorf("leaf generation pack selection: no candidate generations satisfy min-reclaim-per-byte-copied-ppm=%d", opts.MinReclaimPerByteCopiedPPM)
		}
		if opts.MaxBytesToCopy > 0 && rejectedOversize {
			return out, fmt.Errorf("leaf generation pack selection: no candidate generations fit max-bytes-to-copy=%d", opts.MaxBytesToCopy)
		}
		if opts.MinReclaimPerByteCopiedPPM > 0 {
			return out, fmt.Errorf("leaf generation pack selection: no candidate generations satisfy min-reclaim-per-byte-copied-ppm=%d", opts.MinReclaimPerByteCopiedPPM)
		}
		return out, fmt.Errorf("leaf generation pack selection: plan produced no candidate generations within limits")
	}
	out.ExpectedReclaimBytes = out.BytesDead
	out.ExpectedReclaimRatioPPM = ratioPPM(out.BytesDead, out.BytesTotal)
	out.ExpectedReclaimPerByteCopiedPPM = ratioPPM(out.BytesDead, out.BytesToCopy)
	return out, nil
}
