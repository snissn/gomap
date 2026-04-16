package db

import "fmt"

// LeafGenerationPackSelectOptions bounds a selected prefix of plan candidates.
type LeafGenerationPackSelectOptions struct {
	MaxGenerations int
	MaxBytesToCopy int64
}

// LeafGenerationPackSelection summarizes a bounded prefix of pack candidates.
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

// SelectLeafGenerationPackCandidates selects a bounded prefix from an eligible
// leaf-generation plan. The selected prefix preserves candidate rank order and
// never silently exceeds the requested bytes-to-copy cap.
func SelectLeafGenerationPackCandidates(plan LeafGenerationPlan, opts LeafGenerationPackSelectOptions) (LeafGenerationPackSelection, error) {
	var out LeafGenerationPackSelection
	if plan.Admission != leafGenerationPlanAdmissionEligible {
		return out, fmt.Errorf("leaf generation pack selection: plan admission=%s", plan.Admission)
	}
	if len(plan.Candidates) == 0 {
		return out, fmt.Errorf("leaf generation pack selection: plan produced no candidate generations")
	}
	for _, gen := range plan.Candidates {
		if opts.MaxGenerations > 0 && len(out.GenerationIDs) >= opts.MaxGenerations {
			break
		}
		if opts.MaxBytesToCopy > 0 && gen.BytesToCopy > 0 && out.BytesToCopy+gen.BytesToCopy > opts.MaxBytesToCopy {
			if len(out.GenerationIDs) == 0 {
				return out, fmt.Errorf("leaf generation pack selection: first candidate bytes_to_copy=%d exceeds max-bytes-to-copy=%d", gen.BytesToCopy, opts.MaxBytesToCopy)
			}
			break
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
		return out, fmt.Errorf("leaf generation pack selection: plan produced no candidate generations within limits")
	}
	out.ExpectedReclaimBytes = out.BytesDead
	out.ExpectedReclaimRatioPPM = ratioPPM(out.BytesDead, out.BytesTotal)
	out.ExpectedReclaimPerByteCopiedPPM = ratioPPM(out.BytesDead, out.BytesToCopy)
	return out, nil
}
