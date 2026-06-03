package db

import "context"

// LeafGenerationPackFromPlanOptions combines planner thresholds, bounded
// selection limits, and pack execution settings for the manual from-plan path.
type LeafGenerationPackFromPlanOptions struct {
	Sync                       bool
	Force                      bool
	MinPublishedAgeCommits     uint64
	MinCandidateGenerations    int
	MinExpectedReclaimBytes    int64
	MinExpectedReclaimRatioPPM int
	MinReclaimPerByteCopiedPPM int
	MaxGenerations             int
	MaxBytesToCopy             int64
	ReserveRIDs                func(count int) (start uint64, err error)
	LeafFrameK                 int
	ProtectedRootIDs           []uint64
	ProtectedSystemRootIDs     []uint64
}

func leafGenerationPackFromPlanPlanOptions(opts LeafGenerationPackFromPlanOptions) LeafGenerationPlanOptions {
	return LeafGenerationPlanOptions{
		MinPublishedAgeCommits:  opts.MinPublishedAgeCommits,
		MinCandidateGenerations: opts.MinCandidateGenerations,
		MinExpectedReclaimBytes: opts.MinExpectedReclaimBytes,
		Force:                   opts.Force,
		ProtectedRootIDs:        opts.ProtectedRootIDs,
		ProtectedSystemRootIDs:  opts.ProtectedSystemRootIDs,
	}
}

func leafGenerationPackFromPlanSelectOptions(opts LeafGenerationPackFromPlanOptions) LeafGenerationPackSelectOptions {
	return LeafGenerationPackSelectOptions{
		Force:                      opts.Force,
		MinExpectedReclaimBytes:    opts.MinExpectedReclaimBytes,
		MinExpectedReclaimRatioPPM: opts.MinExpectedReclaimRatioPPM,
		MaxGenerations:             opts.MaxGenerations,
		MaxBytesToCopy:             opts.MaxBytesToCopy,
		MinReclaimPerByteCopiedPPM: opts.MinReclaimPerByteCopiedPPM,
	}
}

func leafGenerationPackFromPlanPackOptions(opts LeafGenerationPackFromPlanOptions, generationIDs []uint64) LeafGenerationPackOptions {
	return LeafGenerationPackOptions{
		GenerationIDs:              generationIDs,
		Sync:                       opts.Sync,
		MinPublishedAgeCommits:     opts.MinPublishedAgeCommits,
		MinExpectedReclaimBytes:    opts.MinExpectedReclaimBytes,
		MinExpectedReclaimRatioPPM: opts.MinExpectedReclaimRatioPPM,
		MinReclaimPerByteCopiedPPM: opts.MinReclaimPerByteCopiedPPM,
		ReserveRIDs:                opts.ReserveRIDs,
		Force:                      opts.Force,
		LeafFrameK:                 opts.LeafFrameK,
		ProtectedRootIDs:           opts.ProtectedRootIDs,
		ProtectedSystemRootIDs:     opts.ProtectedSystemRootIDs,
	}
}

// LeafGenerationPackFromPlan computes the current plan, selects a bounded
// candidate prefix, then packs those sealed generations.
func (db *DB) LeafGenerationPackFromPlan(ctx context.Context, opts LeafGenerationPackFromPlanOptions) (LeafGenerationPackStats, error) {
	plan, err := db.LeafGenerationPlan(ctx, leafGenerationPackFromPlanPlanOptions(opts))
	if err != nil {
		return LeafGenerationPackStats{}, err
	}
	selection, err := SelectLeafGenerationPackCandidates(plan, leafGenerationPackFromPlanSelectOptions(opts))
	if err != nil {
		return LeafGenerationPackStats{}, err
	}
	return db.leafGenerationPackSelected(ctx, leafGenerationPackFromPlanPackOptions(opts, selection.GenerationIDs), selectedLeafGenerationPackPlan(selection), true)
}
