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
	return db.leafGenerationPackRunOnce(ctx, opts, true)
}

func (db *DB) leafGenerationPackRunOnce(ctx context.Context, opts LeafGenerationPackFromPlanOptions, lockMaintenance bool) (LeafGenerationPackRunOnceStats, error) {
	var stats LeafGenerationPackRunOnceStats
	plan, err := db.LeafGenerationPlan(ctx, leafGenerationPackFromPlanPlanOptions(opts))
	if err != nil {
		return stats, err
	}
	stats.Plan = plan
	if plan.Admission != leafGenerationPlanAdmissionEligible {
		stats.SkipReason = fmt.Sprintf("plan_admission:%s", plan.Admission)
		return stats, nil
	}
	selection, err := SelectLeafGenerationPackCandidates(plan, leafGenerationPackFromPlanSelectOptions(opts))
	if err != nil {
		stats.SkipReason = fmt.Sprintf("selection:%v", err)
		return stats, nil
	}
	stats.Selection = selection
	packStats, err := db.leafGenerationPackSelected(ctx, leafGenerationPackFromPlanPackOptions(opts, selection.GenerationIDs), selectedLeafGenerationPackPlan(selection), lockMaintenance)
	if err != nil {
		return stats, err
	}
	stats.Pack = packStats
	stats.Ran = true
	return stats, nil
}
