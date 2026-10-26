package db

import (
	"context"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
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

// LeafGenerationPackRunOnceForPrivateCOW runs only this equivalent physical
// rewrite for the cache's originally bound owner. The caller keeps its flush
// lease through basis refresh; this method grants no callback a publisher role.
func (db *DB) LeafGenerationPackRunOnceForPrivateCOW(owner *transactionowner.Owner, ctx context.Context, opts LeafGenerationPackFromPlanOptions) (LeafGenerationPackRunOnceStats, error) {
	if db == nil || owner == nil || owner != db.privateCOWOwner {
		return LeafGenerationPackRunOnceStats{}, ErrNativeSemanticUnsupported
	}
	if err := db.CheckStorageMaintenanceReady(); err != nil {
		return LeafGenerationPackRunOnceStats{}, err
	}
	// Activation must not cross a preexisting unowned pass. Active private COW
	// uses its original owner; no logical mutation or command coverage is granted.
	tracked := false
	if !db.nativeSemanticActive.Load() {
		var err error
		tracked, err = db.beginSemanticBackendOperation(nil, false)
		if err != nil {
			return LeafGenerationPackRunOnceStats{}, err
		}
	}
	defer db.endSemanticBackendOperation(tracked)
	return db.leafGenerationPackRunOnceWithOwner(ctx, opts, true, owner)
}

func (db *DB) leafGenerationPackRunOnce(ctx context.Context, opts LeafGenerationPackFromPlanOptions, lockMaintenance bool) (LeafGenerationPackRunOnceStats, error) {
	return db.leafGenerationPackRunOnceWithOwner(ctx, opts, lockMaintenance, nil)
}

func (db *DB) leafGenerationPackRunOnceWithOwner(ctx context.Context, opts LeafGenerationPackFromPlanOptions, lockMaintenance bool, owner *transactionowner.Owner) (LeafGenerationPackRunOnceStats, error) {
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
	packOpts := leafGenerationPackFromPlanPackOptions(opts, selection.GenerationIDs)
	packOpts.privateCOWOwner = owner
	packStats, err := db.leafGenerationPackSelected(ctx, packOpts, selectedLeafGenerationPackPlan(selection), lockMaintenance)
	if err != nil {
		return stats, err
	}
	stats.Pack = packStats
	stats.Ran = true
	return stats, nil
}
