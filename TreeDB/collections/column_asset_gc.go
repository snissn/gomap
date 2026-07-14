package collections

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// ColumnAssetGCOptions controls safe M15B column asset segment reclamation.
type ColumnAssetGCOptions struct {
	DryRun bool
	// Detailed keeps detailed ref and segment entries in the returned plan.
	Detailed bool
	// SegmentDetails keeps segment-level entries in the returned plan without
	// retaining per-ref entries.
	SegmentDetails     bool
	CandidateRefs      []ColumnAssetRef
	PendingRefs        []ColumnAssetRef
	PreparedRefs       []ColumnAssetRef
	PreparedQueryRefs  []ColumnAssetRef
	QuarantineRefs     []ColumnAssetRef
	QuarantineSegments []ColumnAssetQuarantineSegment
	PinnedRefs         []ColumnAssetRef
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
	removeColumnAssetGCSegment          func(string) error
	syncColumnAssetGCDeletedSegmentsDir func(string) error
	columnAssetStableDeleteBeforeUnlink func()
)

type columnAssetStableSegmentDeleter struct {
	segmentDir string
	parent     *os.File
	registry   *rootpublication.IdentityPinRegistry
	leases     []*rootpublication.IdentityDeleteLease
	removed    bool
}

func newColumnAssetStableSegmentDeleter(segmentDir string, registry *rootpublication.IdentityPinRegistry) (*columnAssetStableSegmentDeleter, error) {
	if registry == nil {
		return nil, errors.New("collections: stable column segment delete requires identity pin registry")
	}
	parent, err := os.Open(segmentDir)
	if err != nil {
		return nil, err
	}
	return &columnAssetStableSegmentDeleter{segmentDir: segmentDir, parent: parent, registry: registry}, nil
}

func (deleter *columnAssetStableSegmentDeleter) delete(fileID uint32) (bool, error) {
	if deleter == nil || deleter.parent == nil || fileID == 0 {
		return false, errors.New("collections: incomplete stable column segment delete")
	}
	name := columnAssetSegmentFileName(fileID)
	resource, err := rootpublication.OpenStableChildFile(deleter.parent, name, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer resource.Close()
	identity, err := rootpublication.StableIdentityFromFile(resource)
	if err != nil {
		return false, err
	}
	namespace := filepath.ToSlash(filepath.Join(filepath.Clean(deleter.segmentDir), name))
	lease, err := deleter.registry.BeginDeleteAt(identity, namespace)
	if err != nil {
		return false, err
	}
	abort := true
	defer func() {
		if abort {
			lease.Abort()
		}
	}()
	removeHook, _, beforeUnlink := columnAssetStableDeleteHooks()
	if beforeUnlink != nil {
		beforeUnlink()
	}
	if err := rootpublication.ValidateStableChildLink(deleter.parent, resource, name); err != nil {
		return false, err
	}
	if removeHook != nil {
		err = removeHook(filepath.Join(deleter.segmentDir, name))
	} else {
		err = rootpublication.RemoveStableChildFile(deleter.parent, name)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := deleter.registry.ForgetStableNamespaceLink(deleter.parent, resource, name); err != nil {
		return false, err
	}
	abort = false
	deleter.leases = append(deleter.leases, lease)
	deleter.removed = true
	return true, nil
}

func (deleter *columnAssetStableSegmentDeleter) finish(cause error) error {
	if deleter == nil {
		return cause
	}
	var syncErr error
	if deleter.removed {
		_, syncHook, _ := columnAssetStableDeleteHooks()
		if syncHook != nil {
			syncErr = syncHook(deleter.segmentDir)
		} else if deleter.parent != nil {
			syncErr = deleter.parent.Sync()
		}
	}
	for _, lease := range deleter.leases {
		lease.Commit()
	}
	deleter.leases = nil
	var closeErr error
	if deleter.parent != nil {
		closeErr = deleter.parent.Close()
		deleter.parent = nil
	}
	return errors.Join(cause, syncErr, closeErr)
}

func deleteColumnAssetSegmentWithStableLease(segmentDir string, fileID uint32, registry *rootpublication.IdentityPinRegistry) (bool, error) {
	deleter, err := newColumnAssetStableSegmentDeleter(segmentDir, registry)
	if err != nil {
		return false, err
	}
	deleted, err := deleter.delete(fileID)
	return deleted, deleter.finish(err)
}

func columnAssetStableDeleteHooks() (func(string) error, func(string) error, func()) {
	columnAssetGCTestHookMu.RLock()
	defer columnAssetGCTestHookMu.RUnlock()
	return removeColumnAssetGCSegment, syncColumnAssetGCDeletedSegmentsDir, columnAssetStableDeleteBeforeUnlink
}

func setColumnAssetStableDeleteBeforeUnlinkTestHook(hook func()) func() {
	columnAssetGCTestHookMu.Lock()
	previous := columnAssetStableDeleteBeforeUnlink
	columnAssetStableDeleteBeforeUnlink = hook
	columnAssetGCTestHookMu.Unlock()
	return func() {
		columnAssetGCTestHookMu.Lock()
		columnAssetStableDeleteBeforeUnlink = previous
		columnAssetGCTestHookMu.Unlock()
	}
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
	needSegmentEntries := !opts.DryRun || opts.Detailed || opts.SegmentDetails
	planOpts := c.columnAssetLifecycleAugmentReachabilityOptions(ColumnAssetReachabilityOptions{
		Detailed:                              opts.Detailed,
		SegmentDetails:                        needSegmentEntries,
		ProtectCandidateRefsForOlderSnapshots: true,
		CandidateRefs:                         opts.CandidateRefs,
		PendingRefs:                           opts.PendingRefs,
		PreparedRefs:                          opts.PreparedRefs,
		PreparedQueryRefs:                     opts.PreparedQueryRefs,
		QuarantineRefs:                        opts.QuarantineRefs,
		QuarantineSegments:                    opts.QuarantineSegments,
		PinnedRefs:                            opts.PinnedRefs,
	})
	plan, err := c.PlanColumnAssetReachability(ctx, planOpts)
	stats = ColumnAssetGCStats{
		DryRun:           opts.DryRun,
		Plan:             plan,
		SegmentsRetained: columnAssetGCExistingSegmentCount(plan),
		BytesRetained:    plan.Segments.BytesTotal,
	}
	if err != nil {
		return stats, c.columnAssetGCNormalizeMaintenanceRaceError(err)
	}
	if !plan.Complete {
		if opts.DryRun {
			return stats, nil
		}
		return stats, fmt.Errorf("%w: collection=%q namespace=%q uncertain_refs=%d unknown_segments=%d missing_segments=%d out_of_bounds_refs=%d quarantine_segment_mismatches=%d unconvertible_pins=%d",
			ErrColumnAssetReachabilityIncomplete,
			plan.Collection,
			plan.Namespace,
			plan.Refs.Uncertain,
			plan.Segments.Unknown,
			plan.Segments.Missing,
			plan.Segments.OutOfBoundsRefs,
			plan.Segments.QuarantineSegmentMismatches,
			plan.MappedResources.UnconvertiblePins,
		)
	}
	if opts.DryRun && !needSegmentEntries {
		stats.SegmentsEligible = plan.Segments.Reclaimable
		stats.BytesEligible = plan.Segments.BytesWholeReclaimable
		return stats, nil
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
		stats.BytesEligible = addColumnAssetReachabilityBytes(stats.BytesEligible, entry.ReclaimableBytes)
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
	deleter, err := newColumnAssetStableSegmentDeleter(namespace.SegmentDir, c.db.StableResourceIdentityPinRegistry())
	if err != nil {
		return stats, err
	}

	for _, entry := range eligible {
		if err := ctx.Err(); err != nil {
			return stats, deleter.finish(err)
		}
		deleted, err := deleter.delete(entry.FileID)
		if err != nil {
			// A publication owner can pin an otherwise unreachable segment
			// between planning and deletion. Retain it for a later GC pass.
			if errors.Is(err, rootpublication.ErrResourcePinned) {
				continue
			}
			return stats, deleter.finish(err)
		}
		if !deleted {
			continue
		}
		stats.SegmentsDeleted++
		stats.BytesDeleted = addColumnAssetReachabilityBytes(stats.BytesDeleted, entry.ReclaimableBytes)
		stats.SegmentsRetained--
		stats.BytesRetained = subColumnAssetReachabilityBytesFloor(stats.BytesRetained, entry.ReclaimableBytes)
	}
	if err := deleter.finish(nil); err != nil {
		return stats, err
	}
	return stats, nil
}

func columnAssetGCExistingSegmentCount(plan ColumnAssetReachabilityPlan) int {
	segments := plan.Segments.Total - plan.Segments.Missing
	if segments < 0 {
		return 0
	}
	return segments
}

func columnAssetGCSegmentEligibleForDelete(segmentDir string, entry ColumnAssetReachabilitySegmentEntry) bool {
	if entry.Status != ColumnAssetReachabilitySegmentReclaimable ||
		entry.FileID == 0 ||
		entry.Path == "" ||
		entry.Bytes <= 0 ||
		entry.ProtectedBytes != 0 ||
		entry.UnknownBytes != 0 ||
		entry.ReclaimableBytes != entry.Bytes {
		return false
	}
	expected := columnAssetReachabilitySegmentPath(segmentDir, columnAssetSegmentFileName(entry.FileID))
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
	if maintenanceErr != nil {
		return errors.Join(err, maintenanceErr)
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
