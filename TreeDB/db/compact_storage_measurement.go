package db

// CompactStorageMeasurementFixture is the stable, benchmark-facing description
// of a CompactStorage workload. It deliberately contains no paths or host
// state so a checked-in fixture can be compared across machines.
type CompactStorageMeasurementFixture struct {
	Name                     string `json:"name"`
	Seed                     uint64 `json:"seed"`
	KeyDistribution          string `json:"key_distribution"`
	ValueDistribution        string `json:"value_distribution"`
	ValueLogPointerThreshold int    `json:"value_log_pointer_threshold"`
	WALMode                  string `json:"wal_mode"`
	DatabaseBytes            int64  `json:"database_bytes"`
	GenerationCount          int    `json:"generation_count"`
	PageCount                int    `json:"page_count"`
	LiveDebtBytes            int64  `json:"live_debt_bytes"`
	DeadDebtBytes            int64  `json:"dead_debt_bytes"`
	ExpectedMaintenanceWork  string `json:"expected_maintenance_work"`
}

// CompactStorageM0Fixtures is the fixed fixture catalog for #3733. Individual
// benchmark implementations may be added without changing artifact names or
// the metadata required to interpret their measurements.
var CompactStorageM0Fixtures = []CompactStorageMeasurementFixture{
	{Name: "one-generation-per-pass", Seed: 3733001, KeyDistribution: "fixed-prefix-sequential", ValueDistribution: "fixed-256", ValueLogPointerThreshold: 1, WALMode: "off-relaxed", GenerationCount: 4, PageCount: 0, ExpectedMaintenanceWork: "forced leaf-pack one generation per pass"},
	{Name: "full-default", Seed: 3733002, KeyDistribution: "fixed-prefix-sequential", ValueDistribution: "fixed-256", ValueLogPointerThreshold: 1, WALMode: "off-relaxed", ExpectedMaintenanceWork: "full policy with 1GiB leaf-pack byte cap"},
	{Name: "full-low-debt", Seed: 3733003, KeyDistribution: "fixed-prefix-sequential", ValueDistribution: "fixed-256", ValueLogPointerThreshold: 1, WALMode: "off-relaxed", ExpectedMaintenanceWork: "bounded live/dead debt with low reclaim"},
	{Name: "full-high-debt", Seed: 3733004, KeyDistribution: "fixed-prefix-sequential", ValueDistribution: "fixed-256", ValueLogPointerThreshold: 1, WALMode: "off-relaxed", ExpectedMaintenanceWork: "deterministic reclaim opportunity"},
	{Name: "exhaustive-control", Seed: 3733005, KeyDistribution: "fixed-prefix-sequential", ValueDistribution: "fixed-256", ValueLogPointerThreshold: 1, WALMode: "off-relaxed", ExpectedMaintenanceWork: "all eligible reclaim work"},
	{Name: "foreground-writes", Seed: 3733006, KeyDistribution: "fixed-prefix-sequential", ValueDistribution: "fixed-256", ValueLogPointerThreshold: 1, WALMode: "off-relaxed", ExpectedMaintenanceWork: "maintenance with matched idle-write control"},
}

// CompactStorageMeasurementAvailability prevents benchmark artifacts from
// silently treating unavailable production counters as zeros.
type CompactStorageMeasurementAvailability string

const (
	CompactStorageMeasurementObserved    CompactStorageMeasurementAvailability = "observed"
	CompactStorageMeasurementUnavailable CompactStorageMeasurementAvailability = "unavailable"
)

type CompactStorageMeasurementPhase struct {
	Name          string `json:"name"`
	Skipped       bool   `json:"skipped"`
	SkipReason    string `json:"skip_reason"`
	WallTimeNanos int64  `json:"wall_time_nanos"`
}

type CompactStorageMeasurementVacuum struct {
	Availability      CompactStorageMeasurementAvailability `json:"availability"`
	Attempted         bool                                  `json:"attempted"`
	SkipReason        string                                `json:"skip_reason"`
	EligiblePages     int                                   `json:"eligible_pages"`
	ReclaimedPages    int                                   `json:"reclaimed_pages"`
	EligibleBytes     int64                                 `json:"eligible_bytes"`
	ReclaimedBytes    int64                                 `json:"reclaimed_bytes"`
	StableCallCounter CompactStorageMeasurementAvailability `json:"stable_call_counter"`
}

type CompactStorageMeasurementCheckpoint struct {
	Availability          CompactStorageMeasurementAvailability `json:"availability"`
	CoverageReason        string                                `json:"coverage_reason"`
	ExactCoverageObserved bool                                  `json:"exact_coverage_observed"`
	StableCallCounter     CompactStorageMeasurementAvailability `json:"stable_call_counter"`
}

// CompactStorageMeasurement is the canonical artifact shape for M0. It is an
// adapter over CompactStorageStats; it adds no production-path instrumentation.
type CompactStorageMeasurement struct {
	Fixture            CompactStorageMeasurementFixture    `json:"fixture"`
	TotalWallTimeNanos int64                               `json:"total_wall_time_nanos"`
	ApplyWallTimeNanos int64                               `json:"apply_wall_time_nanos"`
	Phases             []CompactStorageMeasurementPhase    `json:"phases"`
	Audit              CompactStorageAuditStats            `json:"audit"`
	Vacuum             CompactStorageMeasurementVacuum     `json:"vacuum"`
	Checkpoint         CompactStorageMeasurementCheckpoint `json:"checkpoint"`
}

// NewCompactStorageMeasurement converts existing CompactStorage telemetry into
// a deterministic artifact. totalWallTimeNanos is supplied by the benchmark so
// fixture construction and reporting remain outside the timed boundary.
func NewCompactStorageMeasurement(fixture CompactStorageMeasurementFixture, totalWallTimeNanos int64, stats CompactStorageStats) CompactStorageMeasurement {
	m := CompactStorageMeasurement{
		Fixture:            fixture,
		TotalWallTimeNanos: totalWallTimeNanos,
		Audit:              stats.Audit,
		Vacuum: CompactStorageMeasurementVacuum{
			Availability:      CompactStorageMeasurementUnavailable,
			StableCallCounter: CompactStorageMeasurementUnavailable,
		},
		Checkpoint: CompactStorageMeasurementCheckpoint{
			Availability:      CompactStorageMeasurementUnavailable,
			CoverageReason:    "not-in-compact-storage-stats",
			StableCallCounter: CompactStorageMeasurementUnavailable,
		},
	}
	for _, phase := range stats.Phases {
		m.Phases = append(m.Phases, CompactStorageMeasurementPhase{Name: phase.Name, Skipped: phase.Skipped, SkipReason: phase.SkipReason, WallTimeNanos: phase.WallTimeNanos})
		if !phase.Skipped {
			m.ApplyWallTimeNanos += phase.WallTimeNanos
		}
		if phase.Name == "index-vacuum" {
			m.Vacuum.Attempted = true
			m.Vacuum.SkipReason = phase.SkipReason
		}
	}
	return m
}
