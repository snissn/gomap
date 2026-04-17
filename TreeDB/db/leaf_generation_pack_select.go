package db

import (
	"fmt"
	"sort"
)

// LeafGenerationPackSelectOptions bounds a selected ranked subset of plan
// candidates.
type LeafGenerationPackSelectOptions struct {
	Force                      bool
	MinExpectedReclaimBytes    int64
	MinExpectedReclaimRatioPPM int
	MaxGenerations             int
	MaxBytesToCopy             int64
	MinReclaimPerByteCopiedPPM int
}

// LeafGenerationPackSelection summarizes a bounded subset of pack candidates.
type LeafGenerationPackSelection struct {
	Mode                               string
	GenerationIDs                      []uint64
	Generations                        []LeafGenerationPlanGeneration
	BytesTotal                         int64
	BytesLive                          int64
	BytesDead                          int64
	BytesToCopy                        int64
	LivePages                          int
	ExpectedReclaimBytes               int64
	ExpectedReclaimRatioPPM            int
	ExpectedReclaimPerByteCopiedPPM    int
	ExpectedBytesAfter                 int64
	ExpectedBytesSaved                 int64
	ExpectedBytesSavedRatioPPM         int
	ExpectedBytesSavedPerByteCopiedPPM int
}

type leafGenerationPackSelectionState struct {
	indices     []int
	bytesDead   int64
	bytesToCopy int64
}

// SelectLeafGenerationPackCandidates selects a bounded subset from an eligible
// leaf-generation plan. For bounded windows it maximizes reclaimable bytes
// within the requested generation/copy limits, then emits the chosen
// generations in their original plan order.
func SelectLeafGenerationPackCandidates(plan LeafGenerationPlan, opts LeafGenerationPackSelectOptions) (LeafGenerationPackSelection, error) {
	if plan.Admission != leafGenerationPlanAdmissionEligible {
		return LeafGenerationPackSelection{}, fmt.Errorf("leaf generation pack selection: plan admission=%s", plan.Admission)
	}
	if len(plan.Candidates) == 0 {
		return LeafGenerationPackSelection{}, fmt.Errorf("leaf generation pack selection: plan produced no candidate generations")
	}
	if opts.MaxGenerations <= 0 {
		return selectLeafGenerationPackCandidatesGreedy(plan, opts)
	}
	return selectLeafGenerationPackCandidatesBounded(plan, opts)
}

func selectLeafGenerationPackCandidatesGreedy(plan LeafGenerationPlan, opts LeafGenerationPackSelectOptions) (LeafGenerationPackSelection, error) {
	var out LeafGenerationPackSelection
	rejectedOversize := false
	var rejected leafGenerationPackSelectionThresholdRejections
	for _, gen := range plan.Candidates {
		if opts.MaxGenerations > 0 && len(out.GenerationIDs) >= opts.MaxGenerations {
			break
		}
		if opts.MaxBytesToCopy > 0 && gen.BytesToCopy > 0 && out.BytesToCopy+gen.BytesToCopy > opts.MaxBytesToCopy {
			rejectedOversize = true
			continue
		}
		if !opts.Force && opts.MinReclaimPerByteCopiedPPM > 0 {
			tentativeDead := out.BytesDead + gen.BytesDead
			tentativeCopy := out.BytesToCopy + gen.BytesToCopy
			if tentativeCopy > 0 && ratioPPM(tentativeDead, tentativeCopy) < opts.MinReclaimPerByteCopiedPPM {
				rejected.minPerCopy = true
				break
			}
		}
		appendLeafGenerationPackSelection(&out, gen)
	}
	return finalizeLeafGenerationPackSelection(out, opts, rejectedOversize, rejected)
}

func selectLeafGenerationPackCandidatesBounded(plan LeafGenerationPlan, opts LeafGenerationPackSelectOptions) (LeafGenerationPackSelection, error) {
	maxGenerations := opts.MaxGenerations
	if maxGenerations <= 0 || maxGenerations > len(plan.Candidates) {
		maxGenerations = len(plan.Candidates)
	}
	frontiers := make([][]leafGenerationPackSelectionState, maxGenerations+1)
	frontiers[0] = []leafGenerationPackSelectionState{{}}
	rejectedOversize := false
	for idx, gen := range plan.Candidates {
		for selected := maxGenerations - 1; selected >= 0; selected-- {
			if len(frontiers[selected]) == 0 {
				continue
			}
			next := append([]leafGenerationPackSelectionState(nil), frontiers[selected+1]...)
			for _, state := range frontiers[selected] {
				tentativeCopy := state.bytesToCopy + gen.BytesToCopy
				if opts.MaxBytesToCopy > 0 && tentativeCopy > opts.MaxBytesToCopy {
					rejectedOversize = true
					continue
				}
				indices := append(append([]int(nil), state.indices...), idx)
				next = append(next, leafGenerationPackSelectionState{
					indices:     indices,
					bytesDead:   state.bytesDead + gen.BytesDead,
					bytesToCopy: tentativeCopy,
				})
			}
			frontiers[selected+1] = pruneLeafGenerationPackSelectionStates(next)
		}
	}

	var (
		bestState leafGenerationPackSelectionState
		haveBest  bool
		rejected  leafGenerationPackSelectionThresholdRejections
	)
	for selected := 1; selected <= maxGenerations; selected++ {
		for _, state := range frontiers[selected] {
			if ok, flags := leafGenerationPackSelectionThresholdsOK(state.bytesDead, state.bytesToCopy, opts); !ok {
				rejected.merge(flags)
				continue
			}
			if !haveBest || betterLeafGenerationPackSelectionState(state, bestState) {
				bestState = cloneLeafGenerationPackSelectionState(state)
				haveBest = true
			}
		}
	}
	if !haveBest {
		return finalizeLeafGenerationPackSelection(LeafGenerationPackSelection{}, opts, rejectedOversize, rejected)
	}
	var out LeafGenerationPackSelection
	for _, idx := range bestState.indices {
		appendLeafGenerationPackSelection(&out, plan.Candidates[idx])
	}
	return finalizeLeafGenerationPackSelection(out, opts, rejectedOversize, rejected)
}

type leafGenerationPackSelectionThresholdRejections struct {
	minBytes   bool
	minRatio   bool
	minPerCopy bool
}

func (r *leafGenerationPackSelectionThresholdRejections) merge(other leafGenerationPackSelectionThresholdRejections) {
	r.minBytes = r.minBytes || other.minBytes
	r.minRatio = r.minRatio || other.minRatio
	r.minPerCopy = r.minPerCopy || other.minPerCopy
}

func leafGenerationPackSelectionThresholdsOK(bytesDead, bytesToCopy int64, opts LeafGenerationPackSelectOptions) (bool, leafGenerationPackSelectionThresholdRejections) {
	var rejected leafGenerationPackSelectionThresholdRejections
	if opts.Force {
		return true, rejected
	}
	if opts.MinExpectedReclaimBytes > 0 && bytesDead < opts.MinExpectedReclaimBytes {
		rejected.minBytes = true
	}
	if opts.MinExpectedReclaimRatioPPM > 0 && ratioPPM(bytesDead, bytesDead+bytesToCopy) < opts.MinExpectedReclaimRatioPPM {
		rejected.minRatio = true
	}
	if opts.MinReclaimPerByteCopiedPPM > 0 && ratioPPM(bytesDead, bytesToCopy) < opts.MinReclaimPerByteCopiedPPM {
		rejected.minPerCopy = true
	}
	return !(rejected.minBytes || rejected.minRatio || rejected.minPerCopy), rejected
}

func leafGenerationPackSelectionThresholdError(opts LeafGenerationPackSelectOptions, rejected leafGenerationPackSelectionThresholdRejections) error {
	if opts.Force {
		return nil
	}
	switch {
	case opts.MinExpectedReclaimBytes > 0 && rejected.minBytes:
		return fmt.Errorf("leaf generation pack selection: no candidate generations satisfy min-expected-reclaim-bytes=%d", opts.MinExpectedReclaimBytes)
	case opts.MinExpectedReclaimRatioPPM > 0 && rejected.minRatio:
		return fmt.Errorf("leaf generation pack selection: no candidate generations satisfy min-expected-reclaim-ratio-ppm=%d", opts.MinExpectedReclaimRatioPPM)
	case opts.MinReclaimPerByteCopiedPPM > 0 && rejected.minPerCopy:
		return fmt.Errorf("leaf generation pack selection: no candidate generations satisfy min-reclaim-per-byte-copied-ppm=%d", opts.MinReclaimPerByteCopiedPPM)
	default:
		return nil
	}
}

func finalizeLeafGenerationPackSelection(out LeafGenerationPackSelection, opts LeafGenerationPackSelectOptions, rejectedOversize bool, rejected leafGenerationPackSelectionThresholdRejections) (LeafGenerationPackSelection, error) {
	if len(out.GenerationIDs) == 0 {
		if err := leafGenerationPackSelectionThresholdError(opts, rejected); err != nil {
			return out, err
		}
		if opts.MaxBytesToCopy > 0 && rejectedOversize {
			return out, fmt.Errorf("leaf generation pack selection: no candidate generations fit max-bytes-to-copy=%d", opts.MaxBytesToCopy)
		}
		return out, fmt.Errorf("leaf generation pack selection: plan produced no candidate generations within limits")
	}
	if ok, thresholdRejected := leafGenerationPackSelectionThresholdsOK(out.BytesDead, out.BytesToCopy, opts); !ok {
		return out, leafGenerationPackSelectionThresholdError(opts, thresholdRejected)
	}
	out.ExpectedReclaimBytes = out.BytesDead
	out.ExpectedReclaimRatioPPM = ratioPPM(out.BytesDead, out.BytesTotal)
	out.ExpectedReclaimPerByteCopiedPPM = ratioPPM(out.BytesDead, out.BytesToCopy)
	if out.Mode == "" {
		out.Mode = leafGenerationPackSelectionModeReclaim
	}
	out.ExpectedBytesAfter = out.BytesLive
	out.ExpectedBytesSaved = out.ExpectedReclaimBytes
	out.ExpectedBytesSavedRatioPPM = out.ExpectedReclaimRatioPPM
	out.ExpectedBytesSavedPerByteCopiedPPM = out.ExpectedReclaimPerByteCopiedPPM
	return out, nil
}

func appendLeafGenerationPackSelection(out *LeafGenerationPackSelection, gen LeafGenerationPlanGeneration) {
	out.GenerationIDs = append(out.GenerationIDs, gen.GenerationID)
	out.Generations = append(out.Generations, gen)
	out.BytesTotal += gen.BytesTotal
	out.BytesLive += gen.BytesLive
	out.BytesDead += gen.BytesDead
	out.BytesToCopy += gen.BytesToCopy
	out.LivePages += gen.LivePages
}

func pruneLeafGenerationPackSelectionStates(states []leafGenerationPackSelectionState) []leafGenerationPackSelectionState {
	if len(states) <= 1 {
		return states
	}
	sort.SliceStable(states, func(i, j int) bool {
		a := states[i]
		b := states[j]
		if a.bytesToCopy != b.bytesToCopy {
			return a.bytesToCopy < b.bytesToCopy
		}
		if a.bytesDead != b.bytesDead {
			return a.bytesDead > b.bytesDead
		}
		return compareLeafGenerationPackSelectionIndices(a.indices, b.indices) < 0
	})
	pruned := states[:0]
	bestDead := int64(-1)
	for _, state := range states {
		if state.bytesDead <= bestDead {
			continue
		}
		pruned = append(pruned, state)
		bestDead = state.bytesDead
	}
	return pruned
}

func betterLeafGenerationPackSelectionState(a, b leafGenerationPackSelectionState) bool {
	if a.bytesDead != b.bytesDead {
		return a.bytesDead > b.bytesDead
	}
	aRatio := ratioPPM(a.bytesDead, a.bytesToCopy)
	bRatio := ratioPPM(b.bytesDead, b.bytesToCopy)
	if aRatio != bRatio {
		return aRatio > bRatio
	}
	if a.bytesToCopy != b.bytesToCopy {
		return a.bytesToCopy < b.bytesToCopy
	}
	if len(a.indices) != len(b.indices) {
		return len(a.indices) < len(b.indices)
	}
	return compareLeafGenerationPackSelectionIndices(a.indices, b.indices) < 0
}

func compareLeafGenerationPackSelectionIndices(a, b []int) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}

func cloneLeafGenerationPackSelectionState(state leafGenerationPackSelectionState) leafGenerationPackSelectionState {
	cloned := state
	cloned.indices = append([]int(nil), state.indices...)
	return cloned
}
