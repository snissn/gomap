package db

import (
	"fmt"
	"sort"
)

// LeafGenerationTranscodeSelectOptions bounds a selected ranked subset of
// estimated sealed-generation transcode candidates.
type LeafGenerationTranscodeSelectOptions struct {
	Force                    bool
	MinExpectedSavedBytes    int64
	MinExpectedSavedRatioPPM int
	MaxGenerations           int
	MaxBytesToCopy           int64
	MinSavedPerByteCopiedPPM int
}

// LeafGenerationTranscodeSelection summarizes a bounded subset of transcode
// candidates selected from a plan.
type LeafGenerationTranscodeSelection struct {
	GenerationIDs                      []uint64
	Generations                        []LeafGenerationTranscodePlanGeneration
	BytesTotal                         int64
	BytesLive                          int64
	BytesDead                          int64
	BytesToCopy                        int64
	LivePages                          int
	SamplePages                        int
	EstimatedBytesAfter                int64
	ExpectedBytesSaved                 int64
	ExpectedBytesSavedRatioPPM         int
	ExpectedBytesSavedPerByteCopiedPPM int
}

type leafGenerationTranscodeSelectionState struct {
	indices     []int
	bytesSaved  int64
	bytesToCopy int64
}

// SelectLeafGenerationTranscodeCandidates selects a bounded subset from an
// eligible transcode plan. For bounded windows it maximizes estimated bytes
// saved within the requested generation/copy limits, then emits the chosen
// generations in their original plan order.
func SelectLeafGenerationTranscodeCandidates(plan LeafGenerationTranscodePlan, opts LeafGenerationTranscodeSelectOptions) (LeafGenerationTranscodeSelection, error) {
	if plan.Admission != leafGenerationTranscodePlanAdmissionEligible {
		return LeafGenerationTranscodeSelection{}, fmt.Errorf("leaf generation transcode selection: plan admission=%s", plan.Admission)
	}
	if len(plan.Candidates) == 0 {
		return LeafGenerationTranscodeSelection{}, fmt.Errorf("leaf generation transcode selection: plan produced no candidate generations")
	}
	if opts.MaxGenerations <= 0 {
		return selectLeafGenerationTranscodeCandidatesGreedy(plan, opts)
	}
	return selectLeafGenerationTranscodeCandidatesBounded(plan, opts)
}

func selectLeafGenerationTranscodeCandidatesGreedy(plan LeafGenerationTranscodePlan, opts LeafGenerationTranscodeSelectOptions) (LeafGenerationTranscodeSelection, error) {
	var out LeafGenerationTranscodeSelection
	rejectedOversize := false
	var rejected leafGenerationTranscodeSelectionThresholdRejections
	for _, gen := range plan.Candidates {
		if opts.MaxGenerations > 0 && len(out.GenerationIDs) >= opts.MaxGenerations {
			break
		}
		if opts.MaxBytesToCopy > 0 && gen.BytesToCopy > 0 && out.BytesToCopy+gen.BytesToCopy > opts.MaxBytesToCopy {
			rejectedOversize = true
			continue
		}
		if !opts.Force && opts.MinSavedPerByteCopiedPPM > 0 {
			tentativeSaved := out.ExpectedBytesSaved + gen.ExpectedBytesSaved
			tentativeCopy := out.BytesToCopy + gen.BytesToCopy
			if tentativeCopy > 0 && ratioPPM(tentativeSaved, tentativeCopy) < opts.MinSavedPerByteCopiedPPM {
				rejected.minPerCopy = true
				break
			}
		}
		appendLeafGenerationTranscodeSelection(&out, gen)
	}
	return finalizeLeafGenerationTranscodeSelection(out, opts, rejectedOversize, rejected)
}

func selectLeafGenerationTranscodeCandidatesBounded(plan LeafGenerationTranscodePlan, opts LeafGenerationTranscodeSelectOptions) (LeafGenerationTranscodeSelection, error) {
	maxGenerations := opts.MaxGenerations
	if maxGenerations <= 0 || maxGenerations > len(plan.Candidates) {
		maxGenerations = len(plan.Candidates)
	}
	frontiers := make([][]leafGenerationTranscodeSelectionState, maxGenerations+1)
	frontiers[0] = []leafGenerationTranscodeSelectionState{{}}
	rejectedOversize := false
	for idx, gen := range plan.Candidates {
		for selected := maxGenerations - 1; selected >= 0; selected-- {
			if len(frontiers[selected]) == 0 {
				continue
			}
			next := append([]leafGenerationTranscodeSelectionState(nil), frontiers[selected+1]...)
			for _, state := range frontiers[selected] {
				tentativeCopy := state.bytesToCopy + gen.BytesToCopy
				if opts.MaxBytesToCopy > 0 && tentativeCopy > opts.MaxBytesToCopy {
					rejectedOversize = true
					continue
				}
				indices := append(append([]int(nil), state.indices...), idx)
				next = append(next, leafGenerationTranscodeSelectionState{
					indices:     indices,
					bytesSaved:  state.bytesSaved + gen.ExpectedBytesSaved,
					bytesToCopy: tentativeCopy,
				})
			}
			frontiers[selected+1] = pruneLeafGenerationTranscodeSelectionStates(next)
		}
	}

	var (
		bestState leafGenerationTranscodeSelectionState
		haveBest  bool
		rejected  leafGenerationTranscodeSelectionThresholdRejections
	)
	for selected := 1; selected <= maxGenerations; selected++ {
		for _, state := range frontiers[selected] {
			if ok, flags := leafGenerationTranscodeSelectionThresholdsOK(state.bytesSaved, state.bytesToCopy, opts); !ok {
				rejected.merge(flags)
				continue
			}
			if !haveBest || betterLeafGenerationTranscodeSelectionState(state, bestState) {
				bestState = cloneLeafGenerationTranscodeSelectionState(state)
				haveBest = true
			}
		}
	}
	if !haveBest {
		return finalizeLeafGenerationTranscodeSelection(LeafGenerationTranscodeSelection{}, opts, rejectedOversize, rejected)
	}
	var out LeafGenerationTranscodeSelection
	for _, idx := range bestState.indices {
		appendLeafGenerationTranscodeSelection(&out, plan.Candidates[idx])
	}
	return finalizeLeafGenerationTranscodeSelection(out, opts, rejectedOversize, rejected)
}

type leafGenerationTranscodeSelectionThresholdRejections struct {
	minBytes   bool
	minRatio   bool
	minPerCopy bool
}

func (r *leafGenerationTranscodeSelectionThresholdRejections) merge(other leafGenerationTranscodeSelectionThresholdRejections) {
	r.minBytes = r.minBytes || other.minBytes
	r.minRatio = r.minRatio || other.minRatio
	r.minPerCopy = r.minPerCopy || other.minPerCopy
}

func leafGenerationTranscodeSelectionThresholdsOK(bytesSaved, bytesToCopy int64, opts LeafGenerationTranscodeSelectOptions) (bool, leafGenerationTranscodeSelectionThresholdRejections) {
	var rejected leafGenerationTranscodeSelectionThresholdRejections
	if opts.Force {
		return true, rejected
	}
	if opts.MinExpectedSavedBytes > 0 && bytesSaved < opts.MinExpectedSavedBytes {
		rejected.minBytes = true
	}
	if opts.MinExpectedSavedRatioPPM > 0 && ratioPPM(bytesSaved, bytesToCopy) < opts.MinExpectedSavedRatioPPM {
		rejected.minRatio = true
	}
	if opts.MinSavedPerByteCopiedPPM > 0 && ratioPPM(bytesSaved, bytesToCopy) < opts.MinSavedPerByteCopiedPPM {
		rejected.minPerCopy = true
	}
	return !(rejected.minBytes || rejected.minRatio || rejected.minPerCopy), rejected
}

func leafGenerationTranscodeSelectionThresholdError(opts LeafGenerationTranscodeSelectOptions, rejected leafGenerationTranscodeSelectionThresholdRejections) error {
	if opts.Force {
		return nil
	}
	switch {
	case opts.MinExpectedSavedBytes > 0 && rejected.minBytes:
		return fmt.Errorf("leaf generation transcode selection: no candidate generations satisfy min-expected-saved-bytes=%d", opts.MinExpectedSavedBytes)
	case opts.MinExpectedSavedRatioPPM > 0 && rejected.minRatio:
		return fmt.Errorf("leaf generation transcode selection: no candidate generations satisfy min-expected-saved-ratio-ppm=%d", opts.MinExpectedSavedRatioPPM)
	case opts.MinSavedPerByteCopiedPPM > 0 && rejected.minPerCopy:
		return fmt.Errorf("leaf generation transcode selection: no candidate generations satisfy min-saved-per-byte-copied-ppm=%d", opts.MinSavedPerByteCopiedPPM)
	default:
		return nil
	}
}

func finalizeLeafGenerationTranscodeSelection(out LeafGenerationTranscodeSelection, opts LeafGenerationTranscodeSelectOptions, rejectedOversize bool, rejected leafGenerationTranscodeSelectionThresholdRejections) (LeafGenerationTranscodeSelection, error) {
	if len(out.GenerationIDs) == 0 {
		if err := leafGenerationTranscodeSelectionThresholdError(opts, rejected); err != nil {
			return out, err
		}
		if opts.MaxBytesToCopy > 0 && rejectedOversize {
			return out, fmt.Errorf("leaf generation transcode selection: no candidate generations fit max-bytes-to-copy=%d", opts.MaxBytesToCopy)
		}
		return out, fmt.Errorf("leaf generation transcode selection: plan produced no candidate generations within limits")
	}
	if ok, thresholdRejected := leafGenerationTranscodeSelectionThresholdsOK(out.ExpectedBytesSaved, out.BytesToCopy, opts); !ok {
		return out, leafGenerationTranscodeSelectionThresholdError(opts, thresholdRejected)
	}
	out.ExpectedBytesSavedRatioPPM = ratioPPM(out.ExpectedBytesSaved, out.BytesToCopy)
	out.ExpectedBytesSavedPerByteCopiedPPM = ratioPPM(out.ExpectedBytesSaved, out.BytesToCopy)
	return out, nil
}

func appendLeafGenerationTranscodeSelection(out *LeafGenerationTranscodeSelection, gen LeafGenerationTranscodePlanGeneration) {
	out.GenerationIDs = append(out.GenerationIDs, gen.GenerationID)
	out.Generations = append(out.Generations, gen)
	out.BytesTotal += gen.BytesTotal
	out.BytesLive += gen.BytesLive
	out.BytesDead += gen.BytesDead
	out.BytesToCopy += gen.BytesToCopy
	out.LivePages += gen.LivePages
	out.SamplePages += gen.SamplePages
	out.EstimatedBytesAfter += gen.EstimatedBytesAfter
	out.ExpectedBytesSaved += gen.ExpectedBytesSaved
}

func pruneLeafGenerationTranscodeSelectionStates(states []leafGenerationTranscodeSelectionState) []leafGenerationTranscodeSelectionState {
	if len(states) <= 1 {
		return states
	}
	sort.SliceStable(states, func(i, j int) bool {
		a := states[i]
		b := states[j]
		if a.bytesToCopy != b.bytesToCopy {
			return a.bytesToCopy < b.bytesToCopy
		}
		if a.bytesSaved != b.bytesSaved {
			return a.bytesSaved > b.bytesSaved
		}
		return compareLeafGenerationPackSelectionIndices(a.indices, b.indices) < 0
	})
	pruned := states[:0]
	bestSaved := int64(-1)
	for _, state := range states {
		if state.bytesSaved <= bestSaved {
			continue
		}
		pruned = append(pruned, state)
		bestSaved = state.bytesSaved
	}
	return pruned
}

func betterLeafGenerationTranscodeSelectionState(a, b leafGenerationTranscodeSelectionState) bool {
	if a.bytesSaved != b.bytesSaved {
		return a.bytesSaved > b.bytesSaved
	}
	aRatio := ratioPPM(a.bytesSaved, a.bytesToCopy)
	bRatio := ratioPPM(b.bytesSaved, b.bytesToCopy)
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

func cloneLeafGenerationTranscodeSelectionState(state leafGenerationTranscodeSelectionState) leafGenerationTranscodeSelectionState {
	return leafGenerationTranscodeSelectionState{
		indices:     append([]int(nil), state.indices...),
		bytesSaved:  state.bytesSaved,
		bytesToCopy: state.bytesToCopy,
	}
}
