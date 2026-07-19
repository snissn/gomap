package db

// CompactStorageMeasurementFixture is the stable, benchmark-facing description
// of a CompactStorage workload. It deliberately contains no paths or host
// state so a checked-in fixture can be compared across machines.
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

// CompactStorageM0Fixtures is the fixed fixture catalog for #3733. Individual
// benchmark implementations may be added without changing artifact names or
// the metadata required to interpret their measurements.
var compactStorageM0Fixtures = []compactStorageMeasurementFixture{
	{Name: "one-generation-per-pass", Seed: 3733001, KeyDistribution: "fixed-prefix-sequential", ValueDistribution: "fixed-256", ValueLogPointerThreshold: 1, WALMode: "off-relaxed", GenerationCount: 4, PageCount: 0, ExpectedMaintenanceWork: "forced leaf-pack one generation per pass", DatabaseBytesAvailability: "unavailable", PageCountAvailability: "unavailable", DebtAvailability: "unavailable"},
	{Name: "full-default", Seed: 3733002, KeyDistribution: "fixed-prefix-sequential", ValueDistribution: "fixed-256", ValueLogPointerThreshold: 1, WALMode: "off-relaxed", ExpectedMaintenanceWork: "full policy with 1GiB leaf-pack byte cap", DatabaseBytesAvailability: "unavailable", PageCountAvailability: "unavailable", DebtAvailability: "unavailable"},
	{Name: "full-low-debt", Seed: 3733003, KeyDistribution: "fixed-prefix-sequential", ValueDistribution: "fixed-256", ValueLogPointerThreshold: 1, WALMode: "off-relaxed", ExpectedMaintenanceWork: "bounded live/dead debt with low reclaim", DatabaseBytesAvailability: "unavailable", PageCountAvailability: "unavailable", DebtAvailability: "unavailable"},
	{Name: "full-high-debt", Seed: 3733004, KeyDistribution: "fixed-prefix-sequential", ValueDistribution: "fixed-256", ValueLogPointerThreshold: 1, WALMode: "off-relaxed", ExpectedMaintenanceWork: "deterministic reclaim opportunity", DatabaseBytesAvailability: "unavailable", PageCountAvailability: "unavailable", DebtAvailability: "unavailable"},
	{Name: "exhaustive-control", Seed: 3733005, KeyDistribution: "fixed-prefix-sequential", ValueDistribution: "fixed-256", ValueLogPointerThreshold: 1, WALMode: "off-relaxed", ExpectedMaintenanceWork: "all eligible reclaim work", DatabaseBytesAvailability: "unavailable", PageCountAvailability: "unavailable", DebtAvailability: "unavailable"},
	{Name: "foreground-writes", Seed: 3733006, KeyDistribution: "fixed-prefix-sequential", ValueDistribution: "fixed-256", ValueLogPointerThreshold: 1, WALMode: "off-relaxed", ExpectedMaintenanceWork: "maintenance with matched idle-write control", DatabaseBytesAvailability: "unavailable", PageCountAvailability: "unavailable", DebtAvailability: "unavailable"},
}

// CompactStorageMeasurementAvailability prevents benchmark artifacts from
// silently treating unavailable production counters as zeros.
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

type compactStorageMeasurementVacuum struct {
	Availability      compactStorageMeasurementAvailability `json:"availability"`
	Attempted         bool                                  `json:"attempted"`
	SkipReason        string                                `json:"skip_reason"`
	PlanBytes         int64                                 `json:"plan_bytes"`
	CloneBytes        int64                                 `json:"clone_bytes"`
	RewriteBytes      int64                                 `json:"rewrite_bytes"`
	EligiblePages     int                                   `json:"eligible_pages"`
	ReclaimedPages    int                                   `json:"reclaimed_pages"`
	EligibleBytes     int64                                 `json:"eligible_bytes"`
	ReclaimedBytes    int64                                 `json:"reclaimed_bytes"`
	StableCallCounter compactStorageMeasurementAvailability `json:"stable_call_counter"`
}

type compactStorageMeasurementCheckpoint struct {
	Availability          compactStorageMeasurementAvailability `json:"availability"`
	CoverageReason        string                                `json:"coverage_reason"`
	ExactCoverageObserved bool                                  `json:"exact_coverage_observed"`
	StableCallCounter     compactStorageMeasurementAvailability `json:"stable_call_counter"`
	WallTimeNanos         int64                                 `json:"wall_time_nanos"`
	BeforeVisibleFrontier uint64                                `json:"before_visible_frontier"`
	AfterVisibleFrontier  uint64                                `json:"after_visible_frontier"`
	BeforeDurableFrontier uint64                                `json:"before_durable_frontier"`
	AfterDurableFrontier  uint64                                `json:"after_durable_frontier"`
	StableCalls           uint64                                `json:"stable_calls"`
}

// CompactStorageMeasurement is the canonical artifact shape for M0. It is an
// adapter over CompactStorageStats; it adds no production-path instrumentation.
type compactStorageMeasurement struct {
	Fixture            compactStorageMeasurementFixture    `json:"fixture"`
	TotalWallTimeNanos int64                               `json:"total_wall_time_nanos"`
	ApplyWallTimeNanos int64                               `json:"apply_wall_time_nanos"`
	Phases             []compactStorageMeasurementPhase    `json:"phases"`
	Audit              CompactStorageAuditStats            `json:"audit"`
	Vacuum             compactStorageMeasurementVacuum     `json:"vacuum"`
	Checkpoint         compactStorageMeasurementCheckpoint `json:"checkpoint"`
}

// NewCompactStorageMeasurement converts existing CompactStorage telemetry into
// a deterministic artifact. totalWallTimeNanos is supplied by the benchmark so
// fixture construction and reporting remain outside the timed boundary.
func newCompactStorageMeasurement(fixture compactStorageMeasurementFixture, totalWallTimeNanos int64, stats CompactStorageStats) compactStorageMeasurement {
	m := compactStorageMeasurement{
		Fixture:            fixture,
		TotalWallTimeNanos: totalWallTimeNanos,
		Audit:              stats.Audit,
		Vacuum: compactStorageMeasurementVacuum{
			Availability:      compactStorageMeasurementUnavailable,
			StableCallCounter: compactStorageMeasurementUnavailable,
		},
		Checkpoint: compactStorageMeasurementCheckpoint{
			Availability:      compactStorageMeasurementUnavailable,
			CoverageReason:    "not-in-compact-storage-stats",
			StableCallCounter: compactStorageMeasurementUnavailable,
		},
	}
	for _, phase := range stats.Phases {
		m.Phases = append(m.Phases, compactStorageMeasurementPhase{Name: phase.Name, Skipped: phase.Skipped, SkipReason: phase.SkipReason, WallTimeNanos: phase.WallTimeNanos})
		m.ApplyWallTimeNanos += phase.WallTimeNanos
		if phase.Name == "index-vacuum" {
			m.Vacuum.Attempted = true
			m.Vacuum.SkipReason = phase.SkipReason
		}
	}
	m.Vacuum.PlanBytes = stats.LeafGenerationPlan.CandidateBytesTotal
	m.Vacuum.CloneBytes = stats.LeafGenerationPlan.CandidateBytesLive
	m.Vacuum.RewriteBytes = stats.LeafGenerationPlan.CandidateBytesToCopy
	m.Vacuum.EligiblePages = stats.LeafGenerationPlan.CandidateLivePages
	m.Vacuum.ReclaimedBytes = stats.LeafGenerationGC.BytesDeleted
	m.Vacuum.ReclaimedPages = stats.LeafGenerationGC.GenerationsDeleted
	if m.Vacuum.PlanBytes != 0 || m.Vacuum.ReclaimedBytes != 0 {
		m.Vacuum.Availability = compactStorageMeasurementObserved
	}
	return m
}
