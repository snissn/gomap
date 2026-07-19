package db

import (
	"sort"
	"time"
)

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
	Name          string `json:"name"`
	Skipped       bool   `json:"skipped"`
	SkipReason    string `json:"skip_reason"`
	WallTimeNanos int64  `json:"wall_time_nanos"`
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
	Availability      compactStorageMeasurementAvailability `json:"availability"`
	Attempted         bool                                  `json:"attempted"`
	Ran               bool                                  `json:"ran"`
	PlanReason        string                                `json:"plan_reason"`
	SkipReason        string                                `json:"skip_reason"`
	ClonePages        uint64                                `json:"clone_pages"`
	RewritePages      uint64                                `json:"rewrite_pages"`
	CloneBytes        int64                                 `json:"clone_bytes"`
	RewriteBytes      int64                                 `json:"rewrite_bytes"`
	ReclaimedPages    int                                   `json:"reclaimed_pages"`
	ReclaimedBytes    int64                                 `json:"reclaimed_bytes"`
	StableCalls       uint64                                `json:"stable_calls"`
	StableCallCounter compactStorageMeasurementAvailability `json:"stable_call_counter"`
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
	m := compactStorageMeasurement{
		SchemaVersion:      1,
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
			Name: phase.Name, Skipped: phase.Skipped, SkipReason: phase.SkipReason, WallTimeNanos: phase.WallTimeNanos,
		})
		m.ApplyWallTimeNanos += phase.WallTimeNanos
		if phase.Name == "index-vacuum" {
			m.Vacuum.Attempted = true
			m.Vacuum.Ran = !phase.Skipped
			m.Vacuum.SkipReason = phase.SkipReason
			if phase.Skipped {
				m.Vacuum.PlanReason = "production-index-vacuum-unavailable"
			}
		}
	}
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
		m.LeafPack.DirectorySyncTimeNanos += run.Pack.ApplyStages.DirectorySyncWaitTimeNanos
		m.LeafPack.FinalizeTimeNanos += run.Pack.ApplyStages.FinalizeTimeNanos
		m.LeafPack.PublicationPostWorkNanos += run.Pack.ApplyStages.PostWorkTimeNanos
	}
	m.LeafPack.ReclaimedGenerations = stats.LeafGenerationGC.GenerationsDeleted
	m.LeafPack.ReclaimedBytes = stats.LeafGenerationGC.BytesDeleted
	m.LeafPack.RemainingGenerationDebt = stats.RemainingDebt.LeafPackGenerations
	m.LeafPack.RemainingGenerationBytes = stats.RemainingDebt.LeafPackBytes
	m.ValueLog = compactStorageMeasurementValueLog{
		PlanSegments:          stats.ValueLogRewritePlan.SegmentsSelected,
		PlanBytesTotal:        stats.ValueLogRewritePlan.SelectedBytesTotal,
		PlanBytesLive:         stats.ValueLogRewritePlan.SelectedBytesLive,
		PlanBytesStale:        stats.ValueLogRewritePlan.SelectedBytesStale,
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
		m.Vacuum.StableCalls = recorder.phaseCallCount("index-vacuum")
		m.Vacuum.StableCallCounter = compactStorageMeasurementObserved
	}
	return m
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
