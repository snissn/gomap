package collections

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// ColumnAssetGCOptions controls safe M15B column asset segment reclamation.
type ColumnAssetGCOptions struct {
	DryRun bool
	// Detailed keeps detailed ref and segment entries in the returned plan.
	Detailed bool
	// SegmentDetails keeps segment-level entries in the returned plan without
	// retaining per-ref entries.
	SegmentDetails bool
	CandidateRefs  []ColumnAssetRef
	PendingRefs    []ColumnAssetRef
	PreparedRefs   []ColumnAssetRef
	PinnedRefs     []ColumnAssetRef
}

// ColumnAssetGCStats summarizes safe whole-segment column asset reclamation.
type ColumnAssetGCStats struct {
	DryRun bool

	Plan ColumnAssetReachabilityPlan

	SegmentsEligible int
	SegmentsDeleted  int
	SegmentsRetained int
	BytesEligible    int64
	BytesDeleted     int64
	BytesRetained    int64
}

var (
	columnAssetGCTestHookMu             sync.RWMutex
	removeColumnAssetGCSegment          = os.Remove
	syncColumnAssetGCDeletedSegmentsDir = syncColumnAssetDir
)

// ColumnAssetGC reclaims only complete, canonical column asset segments that
// M15A reachability proves wholly reclaimable. Mixed segments remain rewrite
// debt for M15C; incomplete reachability fails closed before deletion.
func (c *Collection) ColumnAssetGC(ctx context.Context, opts ColumnAssetGCOptions) (ColumnAssetGCStats, error) {
	var stats ColumnAssetGCStats
	if c == nil {
		return stats, errCollectionNil
	}
	if c.db == nil {
		return stats, errCollectionDBNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	if !opts.DryRun {
		if err := c.db.CheckStorageMaintenanceReady(); err != nil {
			return stats, err
		}
		// V1 destructive GC keeps planning under the mutation lock so the
		// candidate/protection view cannot race with collection writes. A later
		// planner snapshot handoff can narrow this lock once benchmark evidence
		// justifies the extra complexity.
		unlock := c.lockMutation()
		defer unlock.Unlock()
	}
	return c.columnAssetGC(ctx, opts)
}

func (c *Collection) columnAssetGC(ctx context.Context, opts ColumnAssetGCOptions) (stats ColumnAssetGCStats, err error) {
	defer func() {
		stats.Plan = columnAssetGCPlanForDetail(stats.Plan, opts.Detailed, opts.SegmentDetails)
	}()
	plan, err := c.PlanColumnAssetReachability(ctx, ColumnAssetReachabilityOptions{
		Detailed:                              opts.Detailed,
		SegmentDetails:                        opts.Detailed || opts.SegmentDetails || !opts.DryRun,
		ProtectCandidateRefsForOlderSnapshots: true,
		CandidateRefs:                         opts.CandidateRefs,
		PendingRefs:                           opts.PendingRefs,
		PreparedRefs:                          opts.PreparedRefs,
		PinnedRefs:                            opts.PinnedRefs,
	})
	stats = ColumnAssetGCStats{
		DryRun:           opts.DryRun,
		Plan:             plan,
		SegmentsRetained: plan.Segments.Total,
		BytesRetained:    plan.Segments.BytesTotal,
	}
	if err != nil {
		return stats, c.columnAssetGCNormalizeMaintenanceRaceError(err)
	}
	if !plan.Complete {
		if opts.DryRun {
			return stats, nil
		}
		return stats, fmt.Errorf("%w: collection=%q namespace=%q uncertain_refs=%d unknown_segments=%d missing_segments=%d out_of_bounds_refs=%d",
			ErrColumnAssetReachabilityIncomplete,
			plan.Collection,
			plan.Namespace,
			plan.Refs.Uncertain,
			plan.Segments.Unknown,
			plan.Segments.Missing,
			plan.Segments.OutOfBoundsRefs,
		)
	}

	if opts.DryRun {
		if len(plan.SegmentEntries) == 0 {
			// Non-detailed dry-runs keep the allocation profile bounded by using
			// the canonical segment summary instead of materializing entries.
			stats.SegmentsEligible = plan.Segments.Reclaimable
			stats.BytesEligible = plan.Segments.BytesWholeReclaimable
			return stats, nil
		}
		namespace, err := columnAssetManagerNamespaceForRoot(c.db.ColumnAssetRootDir(), plan.Namespace)
		if err != nil {
			return stats, err
		}
		for _, entry := range plan.SegmentEntries {
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			if !columnAssetGCSegmentEligibleForDelete(namespace.SegmentDir, entry) {
				continue
			}
			stats.SegmentsEligible++
			stats.BytesEligible += entry.Bytes
		}
		return stats, nil
	}

	namespace, err := columnAssetManagerNamespaceForRoot(c.db.ColumnAssetRootDir(), plan.Namespace)
	if err != nil {
		return stats, err
	}
	eligible := make([]ColumnAssetReachabilitySegmentEntry, 0, plan.Segments.Reclaimable)
	for _, entry := range plan.SegmentEntries {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if !columnAssetGCSegmentEligibleForDelete(namespace.SegmentDir, entry) {
			continue
		}
		stats.SegmentsEligible++
		stats.BytesEligible = addColumnAssetReachabilityBytes(stats.BytesEligible, entry.Bytes)
		eligible = append(eligible, entry)
	}
	// Re-check after planning while still under the mutation lock so destructive
	// deletion cannot proceed if the handle became closed, read-only, or
	// command-WAL-poisoned while reachability was being computed.
	if err := c.db.CheckStorageMaintenanceReady(); err != nil {
		return stats, err
	}
	if len(eligible) == 0 {
		return stats, nil
	}
	removeSegment := removeColumnAssetGCSegmentFunc()
	syncDeletedSegmentsDirFn := syncColumnAssetGCDeletedSegmentsDirFunc()
	syncDeletedSegmentsDir := func(retErr error) error {
		if stats.SegmentsDeleted == 0 {
			return retErr
		}
		syncErr := syncDeletedSegmentsDirFn(namespace.SegmentDir)
		if retErr != nil {
			return errors.Join(retErr, syncErr)
		}
		return syncErr
	}

	for _, entry := range eligible {
		if err := ctx.Err(); err != nil {
			return stats, syncDeletedSegmentsDir(err)
		}
		if err := removeSegment(entry.Path); err != nil {
			return stats, syncDeletedSegmentsDir(err)
		}
		stats.SegmentsDeleted++
		stats.BytesDeleted = addColumnAssetReachabilityBytes(stats.BytesDeleted, entry.Bytes)
		stats.SegmentsRetained--
		stats.BytesRetained = subColumnAssetReachabilityBytesFloor(stats.BytesRetained, entry.Bytes)
	}
	if err := syncDeletedSegmentsDir(nil); err != nil {
		return stats, err
	}
	return stats, nil
}

func columnAssetGCSegmentEligibleForDelete(segmentDir string, entry ColumnAssetReachabilitySegmentEntry) bool {
	if entry.Status != ColumnAssetReachabilitySegmentReclaimable ||
		entry.FileID == 0 ||
		entry.Path == "" ||
		entry.Bytes <= 0 ||
		entry.RefCount == 0 ||
		entry.ProtectedBytes != 0 ||
		entry.UnknownBytes != 0 ||
		entry.ReclaimableBytes != entry.Bytes {
		return false
	}
	expected := filepath.Join(segmentDir, columnAssetSegmentFileName(entry.FileID))
	if entry.Path != expected {
		return false
	}
	return true
}

func (c *Collection) columnAssetGCNormalizeMaintenanceRaceError(err error) error {
	if err == nil || c == nil || c.db == nil {
		return err
	}
	maintenanceErr := c.db.CheckStorageMaintenanceReady()
	if errors.Is(maintenanceErr, backenddb.ErrClosed) {
		return maintenanceErr
	}
	return err
}

func columnAssetGCPlanForDetail(plan ColumnAssetReachabilityPlan, detailed, segmentDetails bool) ColumnAssetReachabilityPlan {
	if detailed {
		return plan
	}
	plan.Entries = nil
	if !segmentDetails {
		plan.SegmentEntries = nil
	}
	return plan
}

func removeColumnAssetGCSegmentFunc() func(string) error {
	columnAssetGCTestHookMu.RLock()
	fn := removeColumnAssetGCSegment
	columnAssetGCTestHookMu.RUnlock()
	return fn
}

func syncColumnAssetGCDeletedSegmentsDirFunc() func(string) error {
	columnAssetGCTestHookMu.RLock()
	fn := syncColumnAssetGCDeletedSegmentsDir
	columnAssetGCTestHookMu.RUnlock()
	return fn
}

func setColumnAssetGCTestHooks(remove func(string) error, syncDeletedDir func(string) error) func() {
	columnAssetGCTestHookMu.Lock()
	prevRemove := removeColumnAssetGCSegment
	prevSync := syncColumnAssetGCDeletedSegmentsDir
	if remove != nil {
		removeColumnAssetGCSegment = remove
	}
	if syncDeletedDir != nil {
		syncColumnAssetGCDeletedSegmentsDir = syncDeletedDir
	}
	columnAssetGCTestHookMu.Unlock()
	return func() {
		columnAssetGCTestHookMu.Lock()
		removeColumnAssetGCSegment = prevRemove
		syncColumnAssetGCDeletedSegmentsDir = prevSync
		columnAssetGCTestHookMu.Unlock()
	}
}

func subColumnAssetReachabilityBytesFloor(total, delta int64) int64 {
	if delta <= 0 {
		return total
	}
	if total <= delta {
		return 0
	}
	return total - delta
}
