package collections

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// ColumnAssetLifecycleIncompleteReason is a stable report label describing why
// the lifecycle report is not complete enough for destructive consumers.
type ColumnAssetLifecycleIncompleteReason string

const (
	ColumnAssetLifecycleIncompleteReachabilityPlan               ColumnAssetLifecycleIncompleteReason = "reachability_plan_incomplete"
	ColumnAssetLifecycleIncompleteUnknownSegments                ColumnAssetLifecycleIncompleteReason = "unknown_segments"
	ColumnAssetLifecycleIncompleteMissingSegments                ColumnAssetLifecycleIncompleteReason = "missing_segments"
	ColumnAssetLifecycleIncompleteOutOfBoundsRefs                ColumnAssetLifecycleIncompleteReason = "out_of_bounds_refs"
	ColumnAssetLifecycleIncompleteMappedResourcePins             ColumnAssetLifecycleIncompleteReason = "mappedresource_unconvertible_pins"
	ColumnAssetLifecycleIncompletePendingPublishRegistry         ColumnAssetLifecycleIncompleteReason = "pending_publish_registry_unavailable"
	ColumnAssetLifecycleIncompletePreparedAssetRegistry          ColumnAssetLifecycleIncompleteReason = "prepared_asset_registry_unavailable"
	ColumnAssetLifecycleIncompleteQuarantineRegistry             ColumnAssetLifecycleIncompleteReason = "quarantine_registry_unavailable"
	ColumnAssetLifecycleIncompletePendingPublishProcessLocalOnly ColumnAssetLifecycleIncompleteReason = "pending_publish_registry_process_local_only"
	ColumnAssetLifecycleIncompletePreparedAssetProcessLocalOnly  ColumnAssetLifecycleIncompleteReason = "prepared_asset_registry_process_local_only"
	ColumnAssetLifecycleIncompleteQuarantineProcessLocalOnly     ColumnAssetLifecycleIncompleteReason = "quarantine_registry_process_local_only"
	ColumnAssetLifecycleIncompletePinnedSnapshotExactRoots       ColumnAssetLifecycleIncompleteReason = "pinned_snapshot_exact_roots_unavailable"
	ColumnAssetLifecycleIncompleteQuarantineSegmentUncertain     ColumnAssetLifecycleIncompleteReason = "quarantine_segment_uncertain"
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
	mu     sync.Mutex
	id     uint64
	source ColumnAssetLifecyclePinSource
	owner  string
	reason string
	refs   []ColumnAssetRef
	closed bool
}

type columnAssetLifecyclePinScope struct {
	dbID       uint64
	collection string
	namespace  string
}

type columnAssetLifecyclePinSetRecord struct {
	ID         uint64
	Scope      columnAssetLifecyclePinScope
	Collection string
	Namespace  string
	Source     ColumnAssetLifecyclePinSource
	Owner      string
	Reason     string
	Refs       []ColumnAssetRef
	Bytes      int64
}

var columnAssetLifecycleProcessPins = struct {
	sync.Mutex
	nextID   uint64
	nextDBID uint64
	pins     map[uint64]columnAssetLifecyclePinSetRecord
	dbIDs    map[*backenddb.DB]uint64
}{}

// ID returns the process-local lease identifier.
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
	id := p.id
	p.id = 0
	p.mu.Unlock()
	if id == 0 {
		return nil
	}
	columnAssetLifecycleProcessPins.Lock()
	if columnAssetLifecycleProcessPins.pins != nil {
		delete(columnAssetLifecycleProcessPins.pins, id)
	}
	columnAssetLifecycleProcessPins.Unlock()
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
	collectionNamespace := columnAssetLifecycleNamespace(c)
	if collectionNamespace == "" {
		return nil, errors.New("collections: column asset lifecycle pin set requires collection asset namespace")
	}
	var bytes int64
	for _, ref := range refs {
		if err := validateColumnAssetRefForPlan(ref); err != nil {
			return nil, fmt.Errorf("collections: column asset lifecycle pin set ref: %w", err)
		}
		if collectionNamespace != "" && ref.Namespace != "" && ref.Namespace != collectionNamespace {
			return nil, fmt.Errorf("collections: column asset lifecycle pin set ref namespace %q does not match collection namespace %q", ref.Namespace, collectionNamespace)
		}
		bytes = addColumnAssetReachabilityBytes(bytes, positiveColumnAssetReachabilityLength(ref.Length))
	}
	scope := columnAssetLifecyclePinScope{collection: c.meta.Name, namespace: collectionNamespace}
	record := columnAssetLifecyclePinSetRecord{
		Scope:      scope,
		Collection: scope.collection,
		Namespace:  scope.namespace,
		Source:     opts.Source,
		Owner:      opts.Owner,
		Reason:     opts.Reason,
		Refs:       append([]ColumnAssetRef(nil), refs...),
		Bytes:      bytes,
	}
	id, err := columnAssetLifecycleRegisterProcessPin(c.db, record)
	if err != nil {
		return nil, err
	}
	return &ColumnAssetLifecyclePinSet{
		id:     id,
		source: opts.Source,
		owner:  opts.Owner,
		reason: opts.Reason,
		refs:   append([]ColumnAssetRef(nil), refs...),
	}, nil
}

func columnAssetLifecycleRegisterProcessPin(db *backenddb.DB, record columnAssetLifecyclePinSetRecord) (uint64, error) {
	if db == nil {
		return 0, errCollectionDBNil
	}
	for {
		columnAssetLifecycleProcessPins.Lock()
		if dbID, ok := columnAssetLifecycleProcessPins.dbIDs[db]; ok {
			record.Scope.dbID = dbID
			id := columnAssetLifecycleStoreProcessPinLocked(record)
			columnAssetLifecycleProcessPins.Unlock()
			return id, nil
		}
		columnAssetLifecycleProcessPins.Unlock()

		registeredDB := db
		var registeredDBID uint64
		_, ok := registeredDB.RegisterCloseHookIfOpenAfter(func() bool {
			columnAssetLifecycleProcessPins.Lock()
			defer columnAssetLifecycleProcessPins.Unlock()
			if columnAssetLifecycleProcessPins.dbIDs == nil {
				columnAssetLifecycleProcessPins.dbIDs = make(map[*backenddb.DB]uint64)
			}
			if existingDBID, ok := columnAssetLifecycleProcessPins.dbIDs[registeredDB]; ok {
				registeredDBID = existingDBID
				return false
			}
			columnAssetLifecycleProcessPins.nextDBID++
			registeredDBID = columnAssetLifecycleProcessPins.nextDBID
			columnAssetLifecycleProcessPins.dbIDs[registeredDB] = registeredDBID
			return true
		}, func() error {
			columnAssetLifecycleReleaseProcessPinsForDB(registeredDB, registeredDBID)
			return nil
		})
		if !ok {
			return 0, errors.New("collections: column asset lifecycle pin set requires an open backend DB")
		}
	}
}

func columnAssetLifecycleStoreProcessPinLocked(record columnAssetLifecyclePinSetRecord) uint64 {
	columnAssetLifecycleProcessPins.nextID++
	record.ID = columnAssetLifecycleProcessPins.nextID
	record.Refs = append([]ColumnAssetRef(nil), record.Refs...)
	if columnAssetLifecycleProcessPins.pins == nil {
		columnAssetLifecycleProcessPins.pins = make(map[uint64]columnAssetLifecyclePinSetRecord)
	}
	columnAssetLifecycleProcessPins.pins[record.ID] = record
	return record.ID
}

func columnAssetLifecycleProcessDBID(db *backenddb.DB) uint64 {
	if db == nil {
		return 0
	}
	columnAssetLifecycleProcessPins.Lock()
	defer columnAssetLifecycleProcessPins.Unlock()
	return columnAssetLifecycleProcessPins.dbIDs[db]
}

func columnAssetLifecycleReleaseProcessPinsForDB(db *backenddb.DB, dbID uint64) {
	if db == nil || dbID == 0 {
		return
	}
	columnAssetLifecycleProcessPins.Lock()
	defer columnAssetLifecycleProcessPins.Unlock()
	for id, record := range columnAssetLifecycleProcessPins.pins {
		if record.Scope.dbID == dbID {
			delete(columnAssetLifecycleProcessPins.pins, id)
		}
	}
	if columnAssetLifecycleProcessPins.dbIDs != nil && columnAssetLifecycleProcessPins.dbIDs[db] == dbID {
		delete(columnAssetLifecycleProcessPins.dbIDs, db)
	}
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
	Detailed           bool                           `json:"detailed,omitempty"`
	SegmentDetails     bool                           `json:"segment_details,omitempty"`
	CandidateRefs      []ColumnAssetRef               `json:"candidate_refs,omitempty"`
	SupersededRefs     []ColumnAssetRef               `json:"superseded_refs,omitempty"`
	PendingRefs        []ColumnAssetRef               `json:"pending_refs,omitempty"`
	PreparedRefs       []ColumnAssetRef               `json:"prepared_refs,omitempty"`
	PreparedQueryRefs  []ColumnAssetRef               `json:"prepared_query_refs,omitempty"`
	QuarantineRefs     []ColumnAssetRef               `json:"quarantine_refs,omitempty"`
	QuarantineSegments []ColumnAssetQuarantineSegment `json:"quarantine_segments,omitempty"`
	PinnedRefs         []ColumnAssetRef               `json:"pinned_refs,omitempty"`
}

// ColumnAssetLifecycleReport is the stable JSON/report shape for collection-
// level lifecycle roots. It is report-only and fail-closed; Complete remains
// false while registries are process-local/non-durable or exact pinned snapshot
// roots are not available to the planner.
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
	PinSets              ColumnAssetLifecyclePinSummary             `json:"pin_sets"`
	PendingPublish       ColumnAssetLifecycleRegistrySummary        `json:"pending_publish"`
	PreparedAssets       ColumnAssetLifecycleRegistrySummary        `json:"prepared_assets"`
	Quarantine           ColumnAssetLifecycleQuarantineSummary      `json:"quarantine"`
	Bytes                ColumnAssetLifecycleByteClasses            `json:"bytes"`
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
	ManifestRoots                 int `json:"manifest_roots"`
	ManifestRecords               int `json:"manifest_records"`
	ActiveManifestRefs            int `json:"active_manifest_refs"`
	RecoveryManifestRefs          int `json:"recovery_manifest_refs"`
	CandidateRefs                 int `json:"candidate_refs"`
	SupersededRefs                int `json:"superseded_refs"`
	PendingPublishRefs            int `json:"pending_publish_refs"`
	PreparedAssetRefs             int `json:"prepared_asset_refs"`
	PreparedQueryRefs             int `json:"prepared_query_refs"`
	PinnedSnapshotRefs            int `json:"pinned_snapshot_refs"`
	MappedResourcePins            int `json:"mappedresource_pins"`
	LifecyclePinSets              int `json:"lifecycle_pin_sets"`
	LifecyclePinnedRefs           int `json:"lifecycle_pinned_refs"`
	PendingPublishRegistryRecords int `json:"pending_publish_registry_records"`
	PreparedAssetRegistryRecords  int `json:"prepared_asset_registry_records"`
	QuarantineRegistryRecords     int `json:"quarantine_registry_records"`
	QuarantineRefs                int `json:"quarantine_refs"`
	QuarantineSegments            int `json:"quarantine_segments"`
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
// pin sets across all lifecycle pin sources. The close/leak/expiry counters are
// scaffolding slots for later durable registries and remain zero in slice 1.
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

// ColumnAssetLifecycleRegistrySummary summarizes process-local explicit
// registry records that feed lifecycle reports. Durable remains false until a
// future slice defines on-disk registry storage/recovery semantics.
type ColumnAssetLifecycleRegistrySummary struct {
	RegistryAvailable bool                                        `json:"registry_available"`
	Durable           bool                                        `json:"durable"`
	ProcessLocal      bool                                        `json:"process_local"`
	OpenRecords       int                                         `json:"open_records"`
	Refs              int                                         `json:"refs"`
	Bytes             int64                                       `json:"bytes"`
	Sources           []ColumnAssetLifecycleRegistrySourceSummary `json:"sources,omitempty"`
}

// ColumnAssetLifecycleRegistrySourceSummary is a per-source-label rollup for
// pending publish, prepared asset, and quarantine registry records.
type ColumnAssetLifecycleRegistrySourceSummary struct {
	Source       string `json:"source"`
	OpenRecords  int    `json:"open_records"`
	Refs         int    `json:"refs"`
	Bytes        int64  `json:"bytes"`
	Segments     int    `json:"segments,omitempty"`
	SegmentBytes int64  `json:"segment_bytes,omitempty"`
}

// ColumnAssetLifecycleQuarantineSummary is report-only scaffolding for the
// future durable quarantine registry. Slice 2 records are process-local logical
// records only; no asset is moved, renamed, deleted, or truncated.
type ColumnAssetLifecycleQuarantineSummary struct {
	RegistryAvailable bool                                        `json:"registry_available"`
	Durable           bool                                        `json:"durable"`
	ProcessLocal      bool                                        `json:"process_local"`
	OpenRecords       int                                         `json:"open_records"`
	Refs              int                                         `json:"refs"`
	Bytes             int64                                       `json:"bytes"`
	Segments          int                                         `json:"segments"`
	SegmentBytes      int64                                       `json:"segment_bytes"`
	Sources           []ColumnAssetLifecycleRegistrySourceSummary `json:"sources,omitempty"`
}

// ColumnAssetLifecycleByteClasses keeps manifest/catalog bytes separate from
// referenced asset/section bytes and exposes overlapping safety classes used by
// JSONBench lifecycle diagnostics.
type ColumnAssetLifecycleByteClasses struct {
	ManifestCatalogBytes      int64 `json:"manifest_catalog_bytes"`
	ReferencedAssetBytes      int64 `json:"referenced_asset_bytes"`
	LiveBytes                 int64 `json:"live_bytes"`
	StaleBytes                int64 `json:"stale_bytes"`
	ProtectedBytes            int64 `json:"protected_bytes"`
	RewriteDebtBytes          int64 `json:"rewrite_debt_bytes"`
	ReclaimableBytes          int64 `json:"reclaimable_bytes"`
	ActivePinBytes            int64 `json:"active_pin_bytes"`
	PreparedAssetBytes        int64 `json:"prepared_asset_bytes"`
	PreparedQueryBytes        int64 `json:"prepared_query_bytes"`
	PendingPublishBytes       int64 `json:"pending_publish_bytes"`
	SnapshotPinnedBytes       int64 `json:"snapshot_pinned_bytes"`
	MappedResourcePinnedBytes int64 `json:"mappedresource_pinned_bytes"`
	QuarantineBytes           int64 `json:"quarantine_bytes"`
}

// ColumnAssetLifecycleActionSummary is intentionally non-destructive.
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
	registryRecords := c.columnAssetLifecycleRegistrySnapshot()
	registrySummary := summarizeColumnAssetLifecycleRegistries(registryRecords)
	refs := columnAssetLifecycleReachabilityRefs(opts, pins, registryRecords)
	plan, err := c.PlanColumnAssetReachability(ctx, ColumnAssetReachabilityOptions{
		Detailed:                              opts.Detailed,
		SegmentDetails:                        opts.SegmentDetails,
		ProtectCandidateRefsForOlderSnapshots: true,
		CandidateRefs:                         refs.candidate,
		PendingRefs:                           refs.pending,
		PreparedRefs:                          refs.prepared,
		PreparedQueryRefs:                     refs.preparedQuery,
		QuarantineRefs:                        refs.quarantine,
		QuarantineSegments:                    refs.quarantineSegments,
		PinnedRefs:                            refs.pinned,
	})
	if err != nil {
		return ColumnAssetLifecycleReport{}, err
	}
	pinSummary := summarizeColumnAssetLifecyclePins(pins)
	fence := c.columnAssetLifecycleSnapshotFence(plan.PlanCommitSeq)
	report := ColumnAssetLifecycleReport{
		DryRun:               true,
		ReportOnly:           true,
		Complete:             plan.Complete,
		ReachabilityComplete: plan.Complete,
		Identity:             columnAssetLifecycleIdentityFromPlan(plan),
		Roots:                columnAssetLifecycleRootCounts(plan, opts, pinSummary, registrySummary),
		SnapshotFence:        fence,
		MappedResources:      plan.MappedResources,
		PinSets:              pinSummary,
		PendingPublish:       registrySummary.PendingPublish,
		PreparedAssets:       registrySummary.PreparedAssets,
		Quarantine:           registrySummary.Quarantine,
		Bytes:                columnAssetLifecycleByteClasses(plan, pinSummary, registrySummary),
		Actions: ColumnAssetLifecycleActionSummary{
			DestructiveActionsEnabled: false,
			GCEligibleSegments:        plan.Segments.Reclaimable,
			GCEligibleBytes:           plan.Segments.BytesWholeReclaimable,
			RewriteDebtBytes:          plan.RewriteDebtBytes,
		},
		Reachability: plan,
	}
	report.IncompleteReasons = columnAssetLifecycleIncompleteReasons(plan, fence, registrySummary)
	if len(report.IncompleteReasons) != 0 {
		report.Complete = false
	}
	return report, nil
}

type columnAssetLifecycleReachabilityRefSets struct {
	candidate          []ColumnAssetRef
	pending            []ColumnAssetRef
	prepared           []ColumnAssetRef
	preparedQuery      []ColumnAssetRef
	quarantine         []ColumnAssetRef
	quarantineSegments []ColumnAssetQuarantineSegment
	pinned             []ColumnAssetRef
}

func columnAssetLifecycleNamespace(c *Collection) string {
	if c != nil {
		if cfg := c.meta.Options.ColumnStore; cfg != nil && cfg.AssetManager != nil && cfg.AssetManager.Namespace != "" {
			return cfg.AssetManager.Namespace
		}
	}
	return ""
}

func (c *Collection) columnAssetLifecycleAugmentReachabilityOptions(opts ColumnAssetReachabilityOptions) (ColumnAssetReachabilityOptions, error) {
	prepared, pinned, err := c.vectorPartitionReachabilityRefsV1(opts.releaseVectorPartitionReclaimIDs)
	if err != nil {
		return opts, err
	}
	opts.PreparedRefs = append(opts.PreparedRefs, prepared...)
	opts.PinnedRefs = append(opts.PinnedRefs, pinned...)
	pins := c.columnAssetLifecyclePinSetSnapshot()
	registryRecords := c.columnAssetLifecycleRegistrySnapshot()
	refs := columnAssetLifecycleReachabilityRefs(ColumnAssetLifecycleOptions{
		CandidateRefs:      opts.CandidateRefs,
		PendingRefs:        opts.PendingRefs,
		PreparedRefs:       opts.PreparedRefs,
		PreparedQueryRefs:  opts.PreparedQueryRefs,
		QuarantineRefs:     opts.QuarantineRefs,
		QuarantineSegments: opts.QuarantineSegments,
		PinnedRefs:         opts.PinnedRefs,
	}, pins, registryRecords)
	opts.CandidateRefs = refs.candidate
	opts.PendingRefs = refs.pending
	opts.PreparedRefs = refs.prepared
	opts.PreparedQueryRefs = refs.preparedQuery
	opts.QuarantineRefs = refs.quarantine
	opts.QuarantineSegments = refs.quarantineSegments
	opts.PinnedRefs = refs.pinned
	return opts, nil
}

func columnAssetLifecycleReachabilityRefs(opts ColumnAssetLifecycleOptions, pins []columnAssetLifecyclePinSetRecord, registryRecords []columnAssetLifecycleRegistryRecord) columnAssetLifecycleReachabilityRefSets {
	refs := columnAssetLifecycleReachabilityRefSets{
		candidate:          append(append([]ColumnAssetRef(nil), opts.CandidateRefs...), opts.SupersededRefs...),
		pending:            append([]ColumnAssetRef(nil), opts.PendingRefs...),
		prepared:           append([]ColumnAssetRef(nil), opts.PreparedRefs...),
		preparedQuery:      append([]ColumnAssetRef(nil), opts.PreparedQueryRefs...),
		quarantine:         append([]ColumnAssetRef(nil), opts.QuarantineRefs...),
		quarantineSegments: append([]ColumnAssetQuarantineSegment(nil), opts.QuarantineSegments...),
		pinned:             append([]ColumnAssetRef(nil), opts.PinnedRefs...),
	}
	for _, record := range registryRecords {
		switch record.Class {
		case ColumnAssetLifecycleRegistryPendingPublish:
			refs.pending = append(refs.pending, record.Refs...)
		case ColumnAssetLifecycleRegistryPreparedAsset:
			refs.prepared = append(refs.prepared, record.Refs...)
		case ColumnAssetLifecycleRegistryQuarantine:
			refs.quarantine = append(refs.quarantine, record.Refs...)
			refs.quarantineSegments = append(refs.quarantineSegments, record.Segments...)
		}
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
	if c == nil || c.db == nil {
		return nil
	}
	dbID := columnAssetLifecycleProcessDBID(c.db)
	if dbID == 0 {
		return nil
	}
	scope := columnAssetLifecyclePinScope{dbID: dbID, collection: c.meta.Name, namespace: columnAssetLifecycleNamespace(c)}
	columnAssetLifecycleProcessPins.Lock()
	defer columnAssetLifecycleProcessPins.Unlock()
	if len(columnAssetLifecycleProcessPins.pins) == 0 {
		return nil
	}
	out := make([]columnAssetLifecyclePinSetRecord, 0, len(columnAssetLifecycleProcessPins.pins))
	for _, record := range columnAssetLifecycleProcessPins.pins {
		if record.Scope != scope {
			continue
		}
		record.Refs = append([]ColumnAssetRef(nil), record.Refs...)
		out = append(out, record)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
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

func columnAssetLifecycleByteClasses(plan ColumnAssetReachabilityPlan, pins ColumnAssetLifecyclePinSummary, registries columnAssetLifecycleRegistrySummaries) ColumnAssetLifecycleByteClasses {
	activePinBytes := addColumnAssetReachabilityBytes(pins.Bytes, plan.MappedResources.PinnedBytes)
	quarantineBytes := addColumnAssetReachabilityBytes(plan.Sources.QuarantineBytes, registries.Quarantine.SegmentBytes)
	return ColumnAssetLifecycleByteClasses{
		ManifestCatalogBytes:      plan.ManifestCatalogBytes,
		ReferencedAssetBytes:      plan.Refs.BytesTotal,
		LiveBytes:                 plan.Sources.ActiveManifestBytes,
		StaleBytes:                plan.Sources.CandidateBytes,
		ProtectedBytes:            plan.Refs.BytesProtected,
		RewriteDebtBytes:          plan.RewriteDebtBytes,
		ReclaimableBytes:          plan.Segments.BytesWholeReclaimable,
		ActivePinBytes:            activePinBytes,
		PreparedAssetBytes:        plan.Sources.PreparedBytes,
		PreparedQueryBytes:        plan.Sources.PreparedQueryBytes,
		PendingPublishBytes:       plan.Sources.PendingBytes,
		SnapshotPinnedBytes:       plan.Sources.PinnedBytes,
		MappedResourcePinnedBytes: plan.MappedResources.PinnedBytes,
		QuarantineBytes:           quarantineBytes,
	}
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

func columnAssetLifecycleRootCounts(plan ColumnAssetReachabilityPlan, opts ColumnAssetLifecycleOptions, pins ColumnAssetLifecyclePinSummary, registries columnAssetLifecycleRegistrySummaries) ColumnAssetLifecycleRootCounts {
	return ColumnAssetLifecycleRootCounts{
		ManifestRoots:                 plan.Sources.ManifestRoots,
		ManifestRecords:               plan.Sources.ManifestRecords,
		ActiveManifestRefs:            plan.Sources.ActiveManifestRefs,
		RecoveryManifestRefs:          plan.Sources.RecoveryManifestRefs,
		CandidateRefs:                 len(opts.CandidateRefs),
		SupersededRefs:                len(opts.SupersededRefs),
		PendingPublishRefs:            plan.Sources.PendingRefs,
		PreparedAssetRefs:             plan.Sources.PreparedRefs,
		PreparedQueryRefs:             plan.Sources.PreparedQueryRefs,
		PinnedSnapshotRefs:            plan.Sources.PinnedRefs,
		MappedResourcePins:            plan.Sources.MappedResourcePins,
		LifecyclePinSets:              pins.OpenSessions,
		LifecyclePinnedRefs:           pins.Refs,
		PendingPublishRegistryRecords: registries.PendingPublish.OpenRecords,
		PreparedAssetRegistryRecords:  registries.PreparedAssets.OpenRecords,
		QuarantineRegistryRecords:     registries.Quarantine.OpenRecords,
		QuarantineRefs:                plan.Sources.QuarantineRefs,
		QuarantineSegments:            registries.Quarantine.Segments + len(opts.QuarantineSegments),
	}
}

func columnAssetLifecycleIncompleteReasons(plan ColumnAssetReachabilityPlan, fence ColumnAssetLifecycleSnapshotFence, registries columnAssetLifecycleRegistrySummaries) []ColumnAssetLifecycleIncompleteReason {
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
	if plan.Segments.QuarantineSegmentMismatches != 0 {
		add(ColumnAssetLifecycleIncompleteQuarantineSegmentUncertain)
	}
	if plan.MappedResources.UnconvertiblePins != 0 {
		add(ColumnAssetLifecycleIncompleteMappedResourcePins)
	}
	// Slice 2 registries are explicit and process-local only. Keep the report
	// fail-closed for future destructive consumers until a durable registry/root
	// recovery contract exists.
	if registries.PendingPublish.RegistryAvailable {
		if registries.PendingPublish.ProcessLocal && !registries.PendingPublish.Durable {
			add(ColumnAssetLifecycleIncompletePendingPublishProcessLocalOnly)
		}
	} else {
		add(ColumnAssetLifecycleIncompletePendingPublishRegistry)
	}
	if registries.PreparedAssets.RegistryAvailable {
		if registries.PreparedAssets.ProcessLocal && !registries.PreparedAssets.Durable {
			add(ColumnAssetLifecycleIncompletePreparedAssetProcessLocalOnly)
		}
	} else {
		add(ColumnAssetLifecycleIncompletePreparedAssetRegistry)
	}
	if registries.Quarantine.RegistryAvailable {
		if registries.Quarantine.ProcessLocal && !registries.Quarantine.Durable {
			add(ColumnAssetLifecycleIncompleteQuarantineProcessLocalOnly)
		}
	} else {
		add(ColumnAssetLifecycleIncompleteQuarantineRegistry)
	}
	if fence.OlderSnapshotPinned && !fence.ExactSnapshotRootsAvailable {
		add(ColumnAssetLifecycleIncompletePinnedSnapshotExactRoots)
	}
	return reasons
}
