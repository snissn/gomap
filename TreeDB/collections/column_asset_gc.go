package collections

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ColumnAssetGCOptions controls safe M15B column asset segment reclamation.
type ColumnAssetGCOptions struct {
	DryRun bool
	// Detailed keeps detailed ref and segment entries in the returned plan.
	Detailed      bool
	CandidateRefs []ColumnAssetRef
	PendingRefs   []ColumnAssetRef
	PreparedRefs  []ColumnAssetRef
	PinnedRefs    []ColumnAssetRef
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
		unlock := c.lockMutation()
		defer unlock.Unlock()
	}
	return c.columnAssetGC(ctx, opts)
}

func (c *Collection) columnAssetGC(ctx context.Context, opts ColumnAssetGCOptions) (ColumnAssetGCStats, error) {
	plan, err := c.PlanColumnAssetReachability(ctx, ColumnAssetReachabilityOptions{
		Detailed:       opts.Detailed,
		SegmentDetails: true,
		CandidateRefs:  opts.CandidateRefs,
		PendingRefs:    opts.PendingRefs,
		PreparedRefs:   opts.PreparedRefs,
		PinnedRefs:     opts.PinnedRefs,
	})
	stats := ColumnAssetGCStats{
		DryRun:           opts.DryRun,
		Plan:             plan,
		SegmentsRetained: plan.Segments.Total,
		BytesRetained:    plan.Segments.BytesTotal,
	}
	if err != nil {
		stats.Plan = columnAssetGCPlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}
	if !plan.Complete {
		stats.Plan = columnAssetGCPlanForDetail(stats.Plan, opts.Detailed)
		if opts.DryRun {
			return stats, nil
		}
		return stats, fmt.Errorf("%w: collection=%q namespace=%q unknown_segments=%d missing_segments=%d out_of_bounds_refs=%d",
			ErrColumnAssetReachabilityIncomplete,
			plan.Collection,
			plan.Namespace,
			plan.Segments.Unknown,
			plan.Segments.Missing,
			plan.Segments.OutOfBoundsRefs,
		)
	}

	namespace, err := columnAssetManagerNamespaceForRoot(c.db.ColumnAssetRootDir(), plan.Namespace)
	if err != nil {
		stats.Plan = columnAssetGCPlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}
	eligible := make([]ColumnAssetReachabilitySegmentEntry, 0, plan.Segments.Reclaimable)
	for _, entry := range plan.SegmentEntries {
		if err := ctx.Err(); err != nil {
			stats.Plan = columnAssetGCPlanForDetail(stats.Plan, opts.Detailed)
			return stats, err
		}
		if !columnAssetGCSegmentEligibleForDelete(namespace.SegmentDir, entry) {
			continue
		}
		stats.SegmentsEligible++
		stats.BytesEligible += entry.Bytes
		eligible = append(eligible, entry)
	}
	if opts.DryRun || len(eligible) == 0 {
		stats.Plan = columnAssetGCPlanForDetail(stats.Plan, opts.Detailed)
		return stats, nil
	}
	if err := c.db.CheckStorageMaintenanceReady(); err != nil {
		stats.Plan = columnAssetGCPlanForDetail(stats.Plan, opts.Detailed)
		return stats, err
	}

	for _, entry := range eligible {
		if err := ctx.Err(); err != nil {
			stats.Plan = columnAssetGCPlanForDetail(stats.Plan, opts.Detailed)
			return stats, err
		}
		if err := os.Remove(entry.Path); err != nil {
			stats.Plan = columnAssetGCPlanForDetail(stats.Plan, opts.Detailed)
			return stats, err
		}
		stats.SegmentsDeleted++
		stats.BytesDeleted += entry.Bytes
	}
	if stats.SegmentsDeleted != 0 {
		if err := syncColumnAssetDir(namespace.SegmentDir); err != nil {
			stats.Plan = columnAssetGCPlanForDetail(stats.Plan, opts.Detailed)
			return stats, err
		}
	}
	stats.SegmentsRetained -= stats.SegmentsDeleted
	stats.BytesRetained -= stats.BytesDeleted
	stats.Plan = columnAssetGCPlanForDetail(stats.Plan, opts.Detailed)
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
	if filepath.Base(entry.Path) != columnAssetSegmentFileName(entry.FileID) {
		return false
	}
	cleanSegmentDir := filepath.Clean(segmentDir)
	cleanPath := filepath.Clean(entry.Path)
	rel, err := filepath.Rel(cleanSegmentDir, cleanPath)
	if err != nil || rel == "." || rel == "" || filepath.IsAbs(rel) || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false
	}
	return true
}

func columnAssetGCPlanForDetail(plan ColumnAssetReachabilityPlan, detailed bool) ColumnAssetReachabilityPlan {
	if detailed {
		return plan
	}
	plan.Entries = nil
	plan.SegmentEntries = nil
	return plan
}
