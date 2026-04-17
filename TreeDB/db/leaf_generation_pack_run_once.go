package db

import (
	"context"
	"fmt"
)

// LeafGenerationPackRunOnceStats describes one bounded admission/evaluation pass
// for leaf-generation packing.
type LeafGenerationPackRunOnceStats struct {
	Plan       LeafGenerationPlan
	Selection  LeafGenerationPackSelection
	Pack       LeafGenerationPackStats
	Ran        bool
	SkipReason string
}

// LeafGenerationPackRunOnce computes the current plan, applies bounded
// selection, and either runs one pack pass or reports why it skipped.
func (db *DB) LeafGenerationPackRunOnce(ctx context.Context, opts LeafGenerationPackFromPlanOptions) (LeafGenerationPackRunOnceStats, error) {
	var stats LeafGenerationPackRunOnceStats
	plan, err := db.LeafGenerationPlan(ctx, leafGenerationPackFromPlanPlanOptions(opts))
	if err != nil {
		return stats, err
	}
	stats.Plan = plan
	var selection LeafGenerationPackSelection
	baseSkipReason := ""
	if plan.Admission == leafGenerationPlanAdmissionEligible {
		selection, err = SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{
			MinExpectedReclaimBytes:    opts.MinExpectedReclaimBytes,
			MinExpectedReclaimRatioPPM: opts.MinExpectedReclaimRatioPPM,
			MaxGenerations:             opts.MaxGenerations,
			MaxBytesToCopy:             opts.MaxBytesToCopy,
			MinReclaimPerByteCopiedPPM: opts.MinReclaimPerByteCopiedPPM,
		})
		if err == nil {
			stats.Selection = selection
		} else {
			baseSkipReason = fmt.Sprintf("selection:%v", err)
		}
	} else {
		baseSkipReason = fmt.Sprintf("plan_admission:%s", plan.Admission)
	}
	if len(stats.Selection.GenerationIDs) == 0 {
		transcodeSelection, transcodeErr := leafGenerationPackSelectTranscode(db, ctx, plan, opts)
		if transcodeErr != nil {
			if baseSkipReason != "" {
				stats.SkipReason = baseSkipReason
			} else {
				stats.SkipReason = fmt.Sprintf("transcode:%v", transcodeErr)
			}
			return stats, nil
		}
		stats.Selection = transcodeSelection
	}
	packStats, err := db.leafGenerationPackSelected(ctx, leafGenerationPackFromPlanPackOptions(opts, stats.Selection.GenerationIDs), selectedLeafGenerationPackPlan(stats.Selection))
	if err != nil {
		return stats, err
	}
	stats.Pack = packStats
	stats.Ran = true
	return stats, nil
}
