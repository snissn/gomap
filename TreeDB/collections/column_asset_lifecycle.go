package collections

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
)

// ColumnAssetLifecycleIncompleteReason is a stable report label describing why
// the slice-1 lifecycle report is not complete enough for destructive consumers.
type ColumnAssetLifecycleIncompleteReason string

const (
	ColumnAssetLifecycleIncompleteReachabilityPlan         ColumnAssetLifecycleIncompleteReason = "reachability_plan_incomplete"
	ColumnAssetLifecycleIncompleteUnknownSegments          ColumnAssetLifecycleIncompleteReason = "unknown_segments"
	ColumnAssetLifecycleIncompleteMissingSegments          ColumnAssetLifecycleIncompleteReason = "missing_segments"
	ColumnAssetLifecycleIncompleteOutOfBoundsRefs          ColumnAssetLifecycleIncompleteReason = "out_of_bounds_refs"
	ColumnAssetLifecycleIncompleteMappedResourcePins       ColumnAssetLifecycleIncompleteReason = "mappedresource_unconvertible_pins"
	ColumnAssetLifecycleIncompletePendingPublishRegistry   ColumnAssetLifecycleIncompleteReason = "pending_publish_registry_unavailable"
	ColumnAssetLifecycleIncompletePreparedAssetRegistry    ColumnAssetLifecycleIncompleteReason = "prepared_asset_registry_unavailable"
	ColumnAssetLifecycleIncompleteQuarantineRegistry       ColumnAssetLifecycleIncompleteReason = "quarantine_registry_unavailable"
	ColumnAssetLifecycleIncompletePinnedSnapshotExactRoots ColumnAssetLifecycleIncompleteReason = "pinned_snapshot_exact_roots_unavailable"
)

// ColumnAssetLifecyclePinSource identifies the logical owner of an explicit
// process-local lifecycle pin set. Slice 1 reports these pins but does not wire
// them into GC/rewrite deletion policy.
type ColumnAssetLifecyclePinSource string

const (
	ColumnAssetLifecyclePinSourcePreparedQuery  ColumnAssetLifecyclePinSource = "prepared_query"
	ColumnAssetLifecyclePinSourcePreparedAsset  ColumnAssetLifecyclePinSource = "prepared_asset"
	ColumnAssetLifecyclePinSourcePendingPublish ColumnAssetLifecyclePinSource = "pending_publish"
	ColumnAssetLifecyclePinSourcePinnedSnapshot ColumnAssetLifecyclePinSource = "pinned_snapshot"
)

// ColumnAssetLifecyclePinSetOptions describes an explicit process-local pin set
// lease. It is intentionally in-memory/report-only in slice 1; callers must not
// assume this changes destructive maintenance behavior yet.
type ColumnAssetLifecyclePinSetOptions struct {
	Source ColumnAssetLifecyclePinSource `json:"source"`
	Owner  string                        `json:"owner"`
	Reason string                        `json:"reason,omitempty"`
	Refs   []ColumnAssetRef              `json:"refs,omitempty"`
}

// ColumnAssetLifecyclePinSet is a process-local lease over a caller-supplied set
// of column asset refs. Close releases the lease from lifecycle reports.
type ColumnAssetLifecyclePinSet struct {
	mu         sync.Mutex
	collection *Collection
	id         uint64
	source     ColumnAssetLifecyclePinSource
	owner      string
	reason     string
	refs       []ColumnAssetRef
	closed     bool
}

type columnAssetLifecyclePinSetRecord struct {
	ID     uint64
	Source ColumnAssetLifecyclePinSource
	Owner  string
	Reason string
	Refs   []ColumnAssetRef
	Bytes  int64
}

// ID returns the collection-local process lease identifier.
func (p *ColumnAssetLifecyclePinSet) ID() uint64 {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.id
}

// Source returns the logical pin source supplied at acquisition time.
func (p *ColumnAssetLifecyclePinSet) Source() ColumnAssetLifecyclePinSource {
	if p == nil {
		return ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.source
}

// Refs returns a defensive copy of the pinned refs.
func (p *ColumnAssetLifecyclePinSet) Refs() []ColumnAssetRef {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ColumnAssetRef(nil), p.refs...)
}

// Close releases the process-local lifecycle pin set. It is idempotent.
func (p *ColumnAssetLifecyclePinSet) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	collection := p.collection
	id := p.id
	p.collection = nil
	p.mu.Unlock()
	if collection == nil {
		return nil
	}
	collection.columnLifecycleMu.Lock()
	if collection.columnLifecyclePins != nil {
		delete(collection.columnLifecyclePins, id)
	}
	collection.columnLifecycleMu.Unlock()
	return nil
}

// AcquireColumnAssetLifecyclePinSet registers a report-visible process-local pin
// set. Slice 1 deliberately does not make GC/rewrite consume this registry.
func (c *Collection) AcquireColumnAssetLifecyclePinSet(opts ColumnAssetLifecyclePinSetOptions) (*ColumnAssetLifecyclePinSet, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if err := validateColumnAssetLifecyclePinSource(opts.Source); err != nil {
		return nil, err
	}
	if opts.Owner == "" {
		return nil, errors.New("collections: column asset lifecycle pin set owner is required")
	}
	refs := append([]ColumnAssetRef(nil), opts.Refs...)
	var bytes int64
	for _, ref := range refs {
		if err := validateColumnAssetRefForPlan(ref); err != nil {
			return nil, fmt.Errorf("collections: column asset lifecycle pin set ref: %w", err)
		}
		bytes = addColumnAssetReachabilityBytes(bytes, positiveColumnAssetReachabilityLength(ref.Length))
	}
	c.columnLifecycleMu.Lock()
	defer c.columnLifecycleMu.Unlock()
	c.columnLifecycleNextPinID++
	id := c.columnLifecycleNextPinID
	if c.columnLifecyclePins == nil {
		c.columnLifecyclePins = make(map[uint64]columnAssetLifecyclePinSetRecord)
	}
	record := columnAssetLifecyclePinSetRecord{
		ID:     id,
		Source: opts.Source,
		Owner:  opts.Owner,
		Reason: opts.Reason,
		Refs:   refs,
		Bytes:  bytes,
	}
	c.columnLifecyclePins[id] = record
	return &ColumnAssetLifecyclePinSet{
		collection: c,
		id:         id,
		source:     opts.Source,
		owner:      opts.Owner,
		reason:     opts.Reason,
		refs:       append([]ColumnAssetRef(nil), refs...),
	}, nil
}

func validateColumnAssetLifecyclePinSource(source ColumnAssetLifecyclePinSource) error {
	switch source {
	case ColumnAssetLifecyclePinSourcePreparedQuery,
		ColumnAssetLifecyclePinSourcePreparedAsset,
		ColumnAssetLifecyclePinSourcePendingPublish,
		ColumnAssetLifecyclePinSourcePinnedSnapshot:
		return nil
	case "":
		return errors.New("collections: column asset lifecycle pin set source is required")
	default:
		return fmt.Errorf("collections: unsupported column asset lifecycle pin set source %q", source)
	}
}

// ColumnAssetLifecycleOptions controls the report-only collection asset
// lifecycle planner. CandidateRefs and SupersededRefs are both supplied to the
// underlying reachability plan as reclaimable candidates, but are counted
// separately in the report.
type ColumnAssetLifecycleOptions struct {
	Detailed          bool             `json:"detailed,omitempty"`
	SegmentDetails    bool             `json:"segment_details,omitempty"`
	CandidateRefs     []ColumnAssetRef `json:"candidate_refs,omitempty"`
	SupersededRefs    []ColumnAssetRef `json:"superseded_refs,omitempty"`
	PendingRefs       []ColumnAssetRef `json:"pending_refs,omitempty"`
	PreparedRefs      []ColumnAssetRef `json:"prepared_refs,omitempty"`
	PreparedQueryRefs []ColumnAssetRef `json:"prepared_query_refs,omitempty"`
	PinnedRefs        []ColumnAssetRef `json:"pinned_refs,omitempty"`
}

// ColumnAssetLifecycleReport is the stable slice-1 JSON/report shape for
// collection-level lifecycle roots. It is report-only and fail-closed; Complete
// remains false while durable prepared/quarantine registries and exact pinned
// snapshot roots are not available to the planner.
type ColumnAssetLifecycleReport struct {
	DryRun               bool                                       `json:"dry_run"`
	ReportOnly           bool                                       `json:"report_only"`
	Complete             bool                                       `json:"complete"`
	ReachabilityComplete bool                                       `json:"reachability_complete"`
	IncompleteReasons    []ColumnAssetLifecycleIncompleteReason     `json:"incomplete_reasons,omitempty"`
	Identity             ColumnAssetLifecycleIdentity               `json:"identity"`
	Roots                ColumnAssetLifecycleRootCounts             `json:"roots"`
	SnapshotFence        ColumnAssetLifecycleSnapshotFence          `json:"snapshot_fence"`
	MappedResources      ColumnAssetReachabilityMappedResourceStats `json:"mappedresource"`
	PreparedPins         ColumnAssetLifecyclePinSummary             `json:"prepared_pins"`
	Quarantine           ColumnAssetLifecycleQuarantineSummary      `json:"quarantine"`
	Actions              ColumnAssetLifecycleActionSummary          `json:"actions"`
	Reachability         ColumnAssetReachabilityPlan                `json:"reachability"`
}

// ColumnAssetLifecycleIdentity records the system/catalog/manifest identity used
// to build a lifecycle report.
type ColumnAssetLifecycleIdentity struct {
	Collection                 string `json:"collection,omitempty"`
	Namespace                  string `json:"namespace,omitempty"`
	ManifestRootName           string `json:"manifest_root_name,omitempty"`
	ManifestRootID             uint64 `json:"manifest_root_id,omitempty"`
	SystemRoot                 uint64 `json:"system_root,omitempty"`
	PlanCommitSeq              uint64 `json:"plan_commit_seq,omitempty"`
	LifecycleRunID             string `json:"lifecycle_run_id,omitempty"`
	ActiveManifestGeneration   uint64 `json:"active_manifest_generation,omitempty"`
	ActiveManifestChecksum     uint64 `json:"active_manifest_checksum,omitempty"`
	RecoveryManifestGeneration uint64 `json:"recovery_manifest_generation,omitempty"`
	RecoveryManifestChecksum   uint64 `json:"recovery_manifest_checksum,omitempty"`
}

// ColumnAssetLifecycleRootCounts summarizes refs by lifecycle root source.
type ColumnAssetLifecycleRootCounts struct {
	ManifestRoots        int `json:"manifest_roots"`
	ManifestRecords      int `json:"manifest_records"`
	ActiveManifestRefs   int `json:"active_manifest_refs"`
	RecoveryManifestRefs int `json:"recovery_manifest_refs"`
	CandidateRefs        int `json:"candidate_refs"`
	SupersededRefs       int `json:"superseded_refs"`
	PendingPublishRefs   int `json:"pending_publish_refs"`
	PreparedAssetRefs    int `json:"prepared_asset_refs"`
	PreparedQueryRefs    int `json:"prepared_query_refs"`
	PinnedSnapshotRefs   int `json:"pinned_snapshot_refs"`
	MappedResourcePins   int `json:"mappedresource_pins"`
	LifecyclePinSets     int `json:"lifecycle_pin_sets"`
	LifecyclePinnedRefs  int `json:"lifecycle_pinned_refs"`
	QuarantineRefs       int `json:"quarantine_refs"`
	QuarantineSegments   int `json:"quarantine_segments"`
}

// ColumnAssetLifecycleSnapshotFence reports the process-local pinned snapshot
// fence available today. Exact historical manifest roots are not available in
// slice 1, so older pinned snapshots make the lifecycle report incomplete.
type ColumnAssetLifecycleSnapshotFence struct {
	PlanCommitSeq               uint64 `json:"plan_commit_seq,omitempty"`
	MinPinnedSnapshotCommitSeq  uint64 `json:"min_pinned_snapshot_commit_seq,omitempty"`
	OlderSnapshotPinned         bool   `json:"older_snapshot_pinned"`
	ExactSnapshotRootsAvailable bool   `json:"exact_snapshot_roots_available"`
}

// ColumnAssetLifecyclePinSummary summarizes explicit process-local lifecycle
// pin sets. The close/leak/expiry counters are scaffolding slots for later
// durable registries and remain zero in slice 1.
type ColumnAssetLifecyclePinSummary struct {
	OpenSessions  int                                    `json:"open_sessions"`
	Refs          int                                    `json:"refs"`
	Bytes         int64                                  `json:"bytes"`
	CloseErrors   int                                    `json:"close_errors"`
	LeakedLeases  int                                    `json:"leaked_leases"`
	ExpiredLeases int                                    `json:"expired_leases"`
	Sources       []ColumnAssetLifecyclePinSourceSummary `json:"sources,omitempty"`
}

// ColumnAssetLifecyclePinSourceSummary is a per-source rollup for lifecycle pin
// sets.
type ColumnAssetLifecyclePinSourceSummary struct {
	Source       ColumnAssetLifecyclePinSource `json:"source"`
	OpenSessions int                           `json:"open_sessions"`
	Refs         int                           `json:"refs"`
	Bytes        int64                         `json:"bytes"`
}

// ColumnAssetLifecycleQuarantineSummary is report-only scaffolding for the
// future durable quarantine registry.
type ColumnAssetLifecycleQuarantineSummary struct {
	RegistryAvailable bool `json:"registry_available"`
	Refs              int  `json:"refs"`
	Segments          int  `json:"segments"`
}

// ColumnAssetLifecycleActionSummary is intentionally non-destructive in slice 1.
type ColumnAssetLifecycleActionSummary struct {
	DestructiveActionsEnabled bool  `json:"destructive_actions_enabled"`
	GCEligibleSegments        int   `json:"gc_eligible_segments"`
	GCEligibleBytes           int64 `json:"gc_eligible_bytes"`
	RewriteDebtBytes          int64 `json:"rewrite_debt_bytes"`
}

// PlanColumnAssetLifecycle builds a report-only lifecycle inventory for the
// collection's column assets. It never deletes, quarantines, moves, rewrites, or
// changes query semantics.
func (c *Collection) PlanColumnAssetLifecycle(ctx context.Context, opts ColumnAssetLifecycleOptions) (ColumnAssetLifecycleReport, error) {
	if c == nil {
		return ColumnAssetLifecycleReport{}, errCollectionNil
	}
	if c.db == nil {
		return ColumnAssetLifecycleReport{}, errCollectionDBNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ColumnAssetLifecycleReport{}, err
	}
	pins := c.columnAssetLifecyclePinSetSnapshot()
	refs := columnAssetLifecycleReachabilityRefs(opts, pins)
	plan, err := c.PlanColumnAssetReachability(ctx, ColumnAssetReachabilityOptions{
		Detailed:                              opts.Detailed,
		SegmentDetails:                        opts.SegmentDetails,
		ProtectCandidateRefsForOlderSnapshots: true,
		CandidateRefs:                         refs.candidate,
		PendingRefs:                           refs.pending,
		PreparedRefs:                          refs.prepared,
		PreparedQueryRefs:                     refs.preparedQuery,
		PinnedRefs:                            refs.pinned,
	})
	pinSummary := summarizeColumnAssetLifecyclePins(pins)
	fence := c.columnAssetLifecycleSnapshotFence(plan.PlanCommitSeq)
	report := ColumnAssetLifecycleReport{
		DryRun:               true,
		ReportOnly:           true,
		Complete:             plan.Complete,
		ReachabilityComplete: plan.Complete,
		Identity:             columnAssetLifecycleIdentityFromPlan(plan),
		Roots:                columnAssetLifecycleRootCounts(plan, opts, pinSummary),
		SnapshotFence:        fence,
		MappedResources:      plan.MappedResources,
		PreparedPins:         pinSummary,
		Quarantine:           ColumnAssetLifecycleQuarantineSummary{RegistryAvailable: false},
		Actions: ColumnAssetLifecycleActionSummary{
			DestructiveActionsEnabled: false,
			GCEligibleSegments:        plan.Segments.Reclaimable,
			GCEligibleBytes:           plan.Segments.BytesWholeReclaimable,
			RewriteDebtBytes:          plan.RewriteDebtBytes,
		},
		Reachability: plan,
	}
	report.IncompleteReasons = columnAssetLifecycleIncompleteReasons(plan, fence)
	if len(report.IncompleteReasons) != 0 {
		report.Complete = false
	}
	return report, err
}

type columnAssetLifecycleReachabilityRefSets struct {
	candidate     []ColumnAssetRef
	pending       []ColumnAssetRef
	prepared      []ColumnAssetRef
	preparedQuery []ColumnAssetRef
	pinned        []ColumnAssetRef
}

func columnAssetLifecycleReachabilityRefs(opts ColumnAssetLifecycleOptions, pins []columnAssetLifecyclePinSetRecord) columnAssetLifecycleReachabilityRefSets {
	refs := columnAssetLifecycleReachabilityRefSets{
		candidate:     append(append([]ColumnAssetRef(nil), opts.CandidateRefs...), opts.SupersededRefs...),
		pending:       append([]ColumnAssetRef(nil), opts.PendingRefs...),
		prepared:      append([]ColumnAssetRef(nil), opts.PreparedRefs...),
		preparedQuery: append([]ColumnAssetRef(nil), opts.PreparedQueryRefs...),
		pinned:        append([]ColumnAssetRef(nil), opts.PinnedRefs...),
	}
	for _, pin := range pins {
		switch pin.Source {
		case ColumnAssetLifecyclePinSourcePreparedQuery:
			refs.preparedQuery = append(refs.preparedQuery, pin.Refs...)
		case ColumnAssetLifecyclePinSourcePreparedAsset:
			refs.prepared = append(refs.prepared, pin.Refs...)
		case ColumnAssetLifecyclePinSourcePendingPublish:
			refs.pending = append(refs.pending, pin.Refs...)
		case ColumnAssetLifecyclePinSourcePinnedSnapshot:
			refs.pinned = append(refs.pinned, pin.Refs...)
		}
	}
	return refs
}

func (c *Collection) columnAssetLifecyclePinSetSnapshot() []columnAssetLifecyclePinSetRecord {
	if c == nil {
		return nil
	}
	c.columnLifecycleMu.Lock()
	defer c.columnLifecycleMu.Unlock()
	if len(c.columnLifecyclePins) == 0 {
		return nil
	}
	out := make([]columnAssetLifecyclePinSetRecord, 0, len(c.columnLifecyclePins))
	for _, record := range c.columnLifecyclePins {
		record.Refs = append([]ColumnAssetRef(nil), record.Refs...)
		out = append(out, record)
	}
	return out
}

func summarizeColumnAssetLifecyclePins(pins []columnAssetLifecyclePinSetRecord) ColumnAssetLifecyclePinSummary {
	if len(pins) == 0 {
		return ColumnAssetLifecyclePinSummary{}
	}
	summary := ColumnAssetLifecyclePinSummary{OpenSessions: len(pins)}
	bySource := make(map[ColumnAssetLifecyclePinSource]int, 4)
	for _, pin := range pins {
		summary.Refs += len(pin.Refs)
		summary.Bytes = addColumnAssetReachabilityBytes(summary.Bytes, pin.Bytes)
		idx, ok := bySource[pin.Source]
		if !ok {
			idx = len(summary.Sources)
			bySource[pin.Source] = idx
			summary.Sources = append(summary.Sources, ColumnAssetLifecyclePinSourceSummary{Source: pin.Source})
		}
		summary.Sources[idx].OpenSessions++
		summary.Sources[idx].Refs += len(pin.Refs)
		summary.Sources[idx].Bytes = addColumnAssetReachabilityBytes(summary.Sources[idx].Bytes, pin.Bytes)
	}
	return summary
}

func (c *Collection) columnAssetLifecycleSnapshotFence(planCommitSeq uint64) ColumnAssetLifecycleSnapshotFence {
	minPinned := uint64(math.MaxUint64)
	if c != nil && c.db != nil {
		minPinned = c.db.MinPinnedSnapshotCommitSeq()
	}
	return ColumnAssetLifecycleSnapshotFence{
		PlanCommitSeq:               planCommitSeq,
		MinPinnedSnapshotCommitSeq:  minPinned,
		OlderSnapshotPinned:         planCommitSeq != 0 && minPinned < planCommitSeq,
		ExactSnapshotRootsAvailable: false,
	}
}

func columnAssetLifecycleIdentityFromPlan(plan ColumnAssetReachabilityPlan) ColumnAssetLifecycleIdentity {
	return ColumnAssetLifecycleIdentity{
		Collection:                 plan.Collection,
		Namespace:                  plan.Namespace,
		ManifestRootName:           plan.ManifestRootName,
		ManifestRootID:             plan.ManifestRootID,
		SystemRoot:                 plan.SystemRoot,
		PlanCommitSeq:              plan.PlanCommitSeq,
		LifecycleRunID:             fmt.Sprintf("%s/%s@%d:%d", plan.Collection, plan.Namespace, plan.SystemRoot, plan.PlanCommitSeq),
		ActiveManifestGeneration:   plan.ActiveManifestGeneration,
		ActiveManifestChecksum:     plan.ActiveManifestChecksum,
		RecoveryManifestGeneration: plan.RecoveryManifestGeneration,
		RecoveryManifestChecksum:   plan.RecoveryManifestChecksum,
	}
}

func columnAssetLifecycleRootCounts(plan ColumnAssetReachabilityPlan, opts ColumnAssetLifecycleOptions, pins ColumnAssetLifecyclePinSummary) ColumnAssetLifecycleRootCounts {
	return ColumnAssetLifecycleRootCounts{
		ManifestRoots:        plan.Sources.ManifestRoots,
		ManifestRecords:      plan.Sources.ManifestRecords,
		ActiveManifestRefs:   plan.Sources.ActiveManifestRefs,
		RecoveryManifestRefs: plan.Sources.RecoveryManifestRefs,
		CandidateRefs:        len(opts.CandidateRefs),
		SupersededRefs:       len(opts.SupersededRefs),
		PendingPublishRefs:   plan.Sources.PendingRefs,
		PreparedAssetRefs:    plan.Sources.PreparedRefs,
		PreparedQueryRefs:    plan.Sources.PreparedQueryRefs,
		PinnedSnapshotRefs:   plan.Sources.PinnedRefs,
		MappedResourcePins:   plan.Sources.MappedResourcePins,
		LifecyclePinSets:     pins.OpenSessions,
		LifecyclePinnedRefs:  pins.Refs,
	}
}

func columnAssetLifecycleIncompleteReasons(plan ColumnAssetReachabilityPlan, fence ColumnAssetLifecycleSnapshotFence) []ColumnAssetLifecycleIncompleteReason {
	var reasons []ColumnAssetLifecycleIncompleteReason
	add := func(reason ColumnAssetLifecycleIncompleteReason) {
		for _, existing := range reasons {
			if existing == reason {
				return
			}
		}
		reasons = append(reasons, reason)
	}
	if !plan.Complete {
		add(ColumnAssetLifecycleIncompleteReachabilityPlan)
	}
	if plan.Segments.Unknown != 0 {
		add(ColumnAssetLifecycleIncompleteUnknownSegments)
	}
	if plan.Segments.Missing != 0 {
		add(ColumnAssetLifecycleIncompleteMissingSegments)
	}
	if plan.Segments.OutOfBoundsRefs != 0 {
		add(ColumnAssetLifecycleIncompleteOutOfBoundsRefs)
	}
	if plan.MappedResources.UnconvertiblePins != 0 {
		add(ColumnAssetLifecycleIncompleteMappedResourcePins)
	}
	// Slice 1 has no durable registries for these root classes. Keep the report
	// fail-closed for future destructive consumers even when callers supply an
	// advisory ref set.
	add(ColumnAssetLifecycleIncompletePendingPublishRegistry)
	add(ColumnAssetLifecycleIncompletePreparedAssetRegistry)
	add(ColumnAssetLifecycleIncompleteQuarantineRegistry)
	if fence.OlderSnapshotPinned && !fence.ExactSnapshotRootsAvailable {
		add(ColumnAssetLifecycleIncompletePinnedSnapshotExactRoots)
	}
	return reasons
}
