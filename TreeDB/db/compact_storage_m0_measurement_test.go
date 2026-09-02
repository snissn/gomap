package db

import (
	"sort"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

const compactStorageMeasurementSchemaVersion = 3

type compactStorageMeasurementFixture struct {
	Name                      string `json:"name"`
	Seed                      uint64 `json:"seed"`
	KeyDistribution           string `json:"key_distribution"`
	ValueDistribution         string `json:"value_distribution"`
	ValueLogPointerThreshold  int    `json:"value_log_pointer_threshold"`
	WALMode                   string `json:"wal_mode"`
	DatabaseBytes             int64  `json:"database_bytes"`
	GenerationCount           int    `json:"generation_count"`
	PageCount                 int    `json:"page_count"`
	LiveDebtBytes             int64  `json:"live_debt_bytes"`
	DeadDebtBytes             int64  `json:"dead_debt_bytes"`
	ExpectedMaintenanceWork   string `json:"expected_maintenance_work"`
	DatabaseBytesAvailability string `json:"database_bytes_availability"`
	PageCountAvailability     string `json:"page_count_availability"`
	DebtAvailability          string `json:"debt_availability"`
}

type compactStorageMeasurementAvailability string

const (
	compactStorageMeasurementObserved    compactStorageMeasurementAvailability = "observed"
	compactStorageMeasurementUnavailable compactStorageMeasurementAvailability = "unavailable"
)

type compactStorageMeasurementPhase struct {
	Name          string                    `json:"name"`
	Status        CompactStoragePhaseStatus `json:"status"`
	Required      bool                      `json:"required"`
	Reason        string                    `json:"reason"`
	Skipped       bool                      `json:"skipped"`
	SkipReason    string                    `json:"skip_reason"`
	WallTimeNanos int64                     `json:"wall_time_nanos"`
}

type compactStorageMeasurementStableCall struct {
	Phase             string `json:"phase"`
	Resource          string `json:"resource"`
	CallType          string `json:"call_type"`
	Count             uint64 `json:"count"`
	WallTimeNanos     int64  `json:"wall_time_nanos"`
	UnmatchedStarts   uint64 `json:"unmatched_starts"`
	UnmatchedFinishes uint64 `json:"unmatched_finishes"`
}

type compactStorageMeasurementLeafPack struct {
	Attempts                 int   `json:"attempts"`
	Runs                     int   `json:"runs"`
	GenerationsPlanned       int   `json:"generations_planned"`
	GenerationsCopied        int   `json:"generations_copied"`
	GenerationsPublished     int   `json:"generations_published"`
	PagesCopied              int   `json:"pages_copied"`
	BytesPlanned             int64 `json:"bytes_planned"`
	BytesCopied              int64 `json:"bytes_copied"`
	RecordsCopied            int   `json:"records_copied"`
	CopyTimeNanos            int64 `json:"copy_time_nanos"`
	PublishWaitNanos         int64 `json:"publish_wait_nanos"`
	PublishHoldNanos         int64 `json:"publish_hold_nanos"`
	TreeRewriteTimeNanos     int64 `json:"tree_rewrite_time_nanos"`
	RelocationPlanTimeNanos  int64 `json:"relocation_plan_time_nanos"`
	PageSyncTimeNanos        int64 `json:"page_sync_time_nanos"`
	CopiedLeafSyncTimeNanos  int64 `json:"copied_leaf_sync_time_nanos"`
	DirectorySyncTimeNanos   int64 `json:"directory_sync_time_nanos"`
	DirectorySyncWaitNanos   int64 `json:"directory_sync_wait_nanos"`
	FinalizeTimeNanos        int64 `json:"finalize_time_nanos"`
	PublicationPostWorkNanos int64 `json:"publication_post_work_nanos"`
	ReclaimedGenerations     int   `json:"reclaimed_generations"`
	ReclaimedBytes           int64 `json:"reclaimed_bytes"`
	RemainingGenerationDebt  int   `json:"remaining_generation_debt"`
	RemainingGenerationBytes int64 `json:"remaining_generation_bytes"`
}

type compactStorageMeasurementValueLog struct {
	PlanSegments          int   `json:"plan_segments"`
	PlanBytesTotal        int64 `json:"plan_bytes_total"`
	PlanBytesLive         int64 `json:"plan_bytes_live"`
	PlanBytesStale        int64 `json:"plan_bytes_stale"`
	SourceSegments        int   `json:"source_segments"`
	SourceBytes           int64 `json:"source_bytes"`
	RecordsCopied         int   `json:"records_copied"`
	ValueBytesCopied      int64 `json:"value_bytes_copied"`
	GCSegmentsEligible    int   `json:"gc_segments_eligible"`
	GCSegmentsDeleted     int   `json:"gc_segments_deleted"`
	GCBytesEligible       int64 `json:"gc_bytes_eligible"`
	GCBytesDeleted        int64 `json:"gc_bytes_deleted"`
	RemainingRewriteBytes int64 `json:"remaining_rewrite_bytes"`
	RemainingGCBytes      int64 `json:"remaining_gc_bytes"`
}

type compactStorageMeasurementVacuum struct {
	Availability             compactStorageMeasurementAvailability `json:"availability"`
	Status                   CompactStoragePhaseStatus             `json:"status"`
	Required                 bool                                  `json:"required"`
	Attempted                bool                                  `json:"attempted"`
	Ran                      bool                                  `json:"ran"`
	PlanReason               string                                `json:"plan_reason"`
	SkipReason               string                                `json:"skip_reason"`
	ClonePages               uint64                                `json:"clone_pages"`
	RewritePages             uint64                                `json:"rewrite_pages"`
	CloneBytes               int64                                 `json:"clone_bytes"`
	RewriteBytes             int64                                 `json:"rewrite_bytes"`
	ReclaimedPages           int                                   `json:"reclaimed_pages"`
	ReclaimedBytes           int64                                 `json:"reclaimed_bytes"`
	StableCalls              uint64                                `json:"stable_calls"`
	StableCallCounter        compactStorageMeasurementAvailability `json:"stable_call_counter"`
	TotalWallTimeNanos       int64                                 `json:"total_wall_time_nanos"`
	UserTreeTimeNanos        int64                                 `json:"user_tree_time_nanos"`
	SystemReserveTimeNanos   int64                                 `json:"system_reserve_time_nanos"`
	CollectionBasisTimeNanos int64                                 `json:"collection_basis_time_nanos"`
	PreflushTimeNanos        int64                                 `json:"preflush_time_nanos"`
	CutoverTimeNanos         int64                                 `json:"cutover_time_nanos"`
	SystemTreeTimeNanos      int64                                 `json:"system_tree_time_nanos"`
	FinalPagerSyncTimeNanos  int64                                 `json:"final_pager_sync_time_nanos"`
	SwapPublishTimeNanos     int64                                 `json:"swap_publish_time_nanos"`
	MaxWriterPauseNanos      int64                                 `json:"max_writer_pause_nanos"`
}

type compactStorageMeasurementCheckpoint struct {
	Phase                 string                                `json:"phase"`
	Availability          compactStorageMeasurementAvailability `json:"availability"`
	CoverageReason        string                                `json:"coverage_reason"`
	ExactCoverageBefore   bool                                  `json:"exact_coverage_before"`
	ExactCoverageAfter    bool                                  `json:"exact_coverage_after"`
	StableCallCounter     compactStorageMeasurementAvailability `json:"stable_call_counter"`
	WallTimeNanos         int64                                 `json:"wall_time_nanos"`
	BeforeVisibleFrontier uint64                                `json:"before_visible_frontier"`
	AfterVisibleFrontier  uint64                                `json:"after_visible_frontier"`
	BeforeDurableFrontier uint64                                `json:"before_durable_frontier"`
	AfterDurableFrontier  uint64                                `json:"after_durable_frontier"`
	StableCalls           uint64                                `json:"stable_calls"`
}

type compactStorageMeasurementLatency struct {
	Count int   `json:"count"`
	P50   int64 `json:"p50_nanos"`
	P95   int64 `json:"p95_nanos"`
	P99   int64 `json:"p99_nanos"`
	Max   int64 `json:"max_nanos"`
}

type compactStorageMeasurementAllocation struct {
	TotalAllocBytes   uint64 `json:"total_alloc_bytes"`
	AllocationObjects uint64 `json:"allocation_objects"`
	HeapAllocBefore   uint64 `json:"heap_alloc_before"`
	HeapAllocAfter    uint64 `json:"heap_alloc_after"`
	HeapInuseBefore   uint64 `json:"heap_inuse_before"`
	HeapInuseAfter    uint64 `json:"heap_inuse_after"`
	HeapSysBefore     uint64 `json:"heap_sys_before"`
	HeapSysAfter      uint64 `json:"heap_sys_after"`
}

type compactStorageMeasurement struct {
	SchemaVersion      int                                   `json:"schema_version"`
	ArtifactName       string                                `json:"artifact_name"`
	Fixture            compactStorageMeasurementFixture      `json:"fixture"`
	TotalWallTimeNanos int64                                 `json:"total_wall_time_nanos"`
	ApplyWallTimeNanos int64                                 `json:"apply_wall_time_nanos"`
	Phases             []compactStorageMeasurementPhase      `json:"phases"`
	Audit              CompactStorageAuditStats              `json:"audit"`
	LeafPack           compactStorageMeasurementLeafPack     `json:"leaf_pack"`
	ValueLog           compactStorageMeasurementValueLog     `json:"value_log"`
	StableCalls        []compactStorageMeasurementStableCall `json:"stable_calls"`
	Checkpoints        []compactStorageMeasurementCheckpoint `json:"checkpoints"`
	Vacuum             compactStorageMeasurementVacuum       `json:"vacuum"`
	Allocation         compactStorageMeasurementAllocation   `json:"allocation"`
	ForegroundWrites   compactStorageMeasurementLatency      `json:"foreground_writes"`
	IdleWrites         compactStorageMeasurementLatency      `json:"idle_writes"`
}

type compactStorageMeasurementRecorder interface {
	measurements() []compactStorageMeasurementStableCall
	checkpointMeasurements([]CompactStoragePhaseStats) []compactStorageMeasurementCheckpoint
	phaseCallCount(string) uint64
}

func newCompactStorageMeasurement(fixture compactStorageMeasurementFixture, artifactName string, totalWallTimeNanos int64, stats CompactStorageStats, recorder compactStorageMeasurementRecorder) compactStorageMeasurement {
	return newCompactStorageMeasurementWithPlan(
		fixture, artifactName, totalWallTimeNanos, stats.ValueLogRewritePlan, stats, recorder,
	)
}

func newCompactStorageMeasurementWithPlan(
	fixture compactStorageMeasurementFixture,
	artifactName string,
	totalWallTimeNanos int64,
	valueLogRewritePlan ValueLogRewritePlan,
	stats CompactStorageStats,
	recorder compactStorageMeasurementRecorder,
) compactStorageMeasurement {
	m := compactStorageMeasurement{
		SchemaVersion:      compactStorageMeasurementSchemaVersion,
		ArtifactName:       artifactName,
		Fixture:            fixture,
		TotalWallTimeNanos: totalWallTimeNanos,
		Audit:              stats.Audit,
		Vacuum: compactStorageMeasurementVacuum{
			Availability:      compactStorageMeasurementObserved,
			StableCallCounter: compactStorageMeasurementUnavailable,
			PlanReason:        "production-index-vacuum-invoked",
		},
	}
	for _, phase := range stats.Phases {
		m.Phases = append(m.Phases, compactStorageMeasurementPhase{
			Name: phase.Name, Status: phase.Status, Required: phase.Required, Reason: phase.Reason,
			Skipped: phase.Skipped, SkipReason: phase.SkipReason, WallTimeNanos: phase.WallTimeNanos,
		})
		m.ApplyWallTimeNanos += phase.WallTimeNanos
		if phase.Name == "index-vacuum" || phase.Name == "index-vacuum-settle" {
			m.Vacuum.Status = phase.Status
			m.Vacuum.Required = phase.Required
			m.Vacuum.Attempted = phase.Required && phase.Status != CompactStoragePhaseStatusPlanned
			m.Vacuum.Ran = phase.Status == CompactStoragePhaseStatusSucceeded
			m.Vacuum.PlanReason = phase.Reason
			m.Vacuum.SkipReason = phase.SkipReason
		}
	}
	m.Vacuum.ClonePages = stats.IndexVacuum.PrecloneTraversalPages +
		stats.IndexVacuum.RecloneTraversalPages + stats.IndexVacuum.CutoverCloneTraversalPages
	m.Vacuum.CloneBytes = int64(m.Vacuum.ClonePages) * int64(page.PageSize)
	beforeIndexBytes := compactStorageMeasurementUsageBytes(stats.Before, "index")
	afterIndexBytes := compactStorageMeasurementUsageBytes(stats.After, "index")
	if afterIndexBytes > 0 {
		m.Vacuum.RewriteBytes = afterIndexBytes
		m.Vacuum.RewritePages = uint64((afterIndexBytes + int64(page.PageSize) - 1) / int64(page.PageSize))
	}
	if beforeIndexBytes > afterIndexBytes {
		m.Vacuum.ReclaimedBytes = beforeIndexBytes - afterIndexBytes
		m.Vacuum.ReclaimedPages = int(m.Vacuum.ReclaimedBytes / int64(page.PageSize))
	}
	m.Vacuum.TotalWallTimeNanos = stats.IndexVacuum.TotalDuration.Nanoseconds()
	m.Vacuum.UserTreeTimeNanos = stats.IndexVacuum.UserTreeDuration.Nanoseconds()
	m.Vacuum.SystemReserveTimeNanos = stats.IndexVacuum.SystemReserveDuration.Nanoseconds()
	m.Vacuum.CollectionBasisTimeNanos = stats.IndexVacuum.CollectionBasisDuration.Nanoseconds()
	m.Vacuum.PreflushTimeNanos = stats.IndexVacuum.PreflushDuration.Nanoseconds()
	m.Vacuum.CutoverTimeNanos = stats.IndexVacuum.CutoverDuration.Nanoseconds()
	m.Vacuum.SystemTreeTimeNanos = stats.IndexVacuum.SystemTreeDuration.Nanoseconds()
	m.Vacuum.FinalPagerSyncTimeNanos = stats.IndexVacuum.FinalPagerSyncDuration.Nanoseconds()
	m.Vacuum.SwapPublishTimeNanos = stats.IndexVacuum.SwapPublishDuration.Nanoseconds()
	m.Vacuum.MaxWriterPauseNanos = stats.IndexVacuum.MaxWriterPause.Nanoseconds()
	for _, run := range stats.LeafGenerationPacks {
		m.LeafPack.Attempts++
		m.LeafPack.GenerationsPlanned += len(run.Selection.GenerationIDs)
		m.LeafPack.BytesPlanned += run.Selection.BytesToCopy
		if !run.Ran {
			continue
		}
		m.LeafPack.Runs++
		m.LeafPack.GenerationsCopied += run.Pack.GenerationsMatched
		m.LeafPack.GenerationsPublished += len(run.Pack.CreatedFileIDs)
		m.LeafPack.PagesCopied += run.Pack.LeafPagesCopied
		m.LeafPack.BytesCopied += run.Pack.BytesCopied
		m.LeafPack.RecordsCopied += run.Pack.LeafFramesWritten
		m.LeafPack.CopyTimeNanos += run.Pack.CopyTimeNanos
		m.LeafPack.PublishWaitNanos += run.Pack.PublishWaitNanos
		m.LeafPack.PublishHoldNanos += run.Pack.PublishHoldNanos
		m.LeafPack.TreeRewriteTimeNanos += run.Pack.ApplyStages.TreeRewriteTimeNanos
		m.LeafPack.RelocationPlanTimeNanos += run.Pack.ApplyStages.RelocationTimeNanos
		m.LeafPack.PageSyncTimeNanos += run.Pack.ApplyStages.PageSyncTimeNanos
		m.LeafPack.CopiedLeafSyncTimeNanos += run.Pack.ApplyStages.LeafSyncTimeNanos
		m.LeafPack.DirectorySyncTimeNanos += run.Pack.ApplyStages.DirectorySyncTimeNanos
		m.LeafPack.DirectorySyncWaitNanos += run.Pack.ApplyStages.DirectorySyncWaitTimeNanos
		m.LeafPack.FinalizeTimeNanos += run.Pack.ApplyStages.FinalizeTimeNanos
		m.LeafPack.PublicationPostWorkNanos += run.Pack.ApplyStages.PostWorkTimeNanos
	}
	m.LeafPack.ReclaimedGenerations = stats.LeafGenerationGC.GenerationsDeleted
	m.LeafPack.ReclaimedBytes = stats.LeafGenerationGC.BytesDeleted
	m.LeafPack.RemainingGenerationDebt = stats.RemainingDebt.LeafPackGenerations
	m.LeafPack.RemainingGenerationBytes = stats.RemainingDebt.LeafPackBytes
	m.ValueLog = compactStorageMeasurementValueLog{
		PlanSegments:          valueLogRewritePlan.SegmentsSelected,
		PlanBytesTotal:        valueLogRewritePlan.SelectedBytesTotal,
		PlanBytesLive:         valueLogRewritePlan.SelectedBytesLive,
		PlanBytesStale:        valueLogRewritePlan.SelectedBytesStale,
		SourceSegments:        stats.ValueLogRewrite.SourceSegmentsRequested,
		SourceBytes:           stats.ValueLogRewrite.SourceBytesRequested,
		RecordsCopied:         stats.ValueLogRewrite.RecordsCopied,
		ValueBytesCopied:      stats.ValueLogRewrite.ValueBytesCopied,
		GCSegmentsEligible:    stats.ValueLogGC.SegmentsEligible,
		GCSegmentsDeleted:     stats.ValueLogGC.SegmentsDeleted,
		GCBytesEligible:       stats.ValueLogGC.BytesEligible,
		GCBytesDeleted:        stats.ValueLogGC.BytesDeleted,
		RemainingRewriteBytes: stats.RemainingDebt.ValueLogRewriteBytes,
		RemainingGCBytes:      stats.RemainingDebt.ValueLogGCBytes,
	}
	if recorder != nil {
		m.StableCalls = recorder.measurements()
		m.Checkpoints = recorder.checkpointMeasurements(stats.Phases)
		m.Vacuum.StableCalls = recorder.phaseCallCount("index-vacuum") + recorder.phaseCallCount("index-vacuum-settle")
		m.Vacuum.StableCallCounter = compactStorageMeasurementObserved
	}
	return m
}

func compactStorageMeasurementUsageBytes(usages []CompactStorageUsage, name string) int64 {
	for _, usage := range usages {
		if usage.Name == name {
			return usage.Bytes
		}
	}
	return 0
}

func compactStorageMeasurementLatencyFor(values []time.Duration) compactStorageMeasurementLatency {
	if len(values) == 0 {
		return compactStorageMeasurementLatency{}
	}
	sorted := append([]time.Duration(nil), values...)
	sortDurations(sorted)
	return compactStorageMeasurementLatency{
		Count: len(sorted),
		P50:   int64(durationPercentile(sorted, 50)),
		P95:   int64(durationPercentile(sorted, 95)),
		P99:   int64(durationPercentile(sorted, 99)),
		Max:   int64(sorted[len(sorted)-1]),
	}
}

func sortDurations(values []time.Duration) {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
}

func durationPercentile(sorted []time.Duration, percentile int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := (len(sorted)*percentile + 99) / 100
	if index < 1 {
		index = 1
	}
	if index > len(sorted) {
		index = len(sorted)
	}
	return sorted[index-1]
}
