package treedb

// Profiles are intentionally defined in the public package so downstream users
// can pick a coherent option bundle without duplicating the mapping from
// “intent” (durable vs fast vs bench) to low-level knobs.

// Profile is a documented, high-level preset for TreeDB Options.
//
// Why profiles exist
// ------------------
// TreeDB exposes many low-level knobs because different workloads want different
// trade-offs (durability vs throughput, steady-state vs benchmark determinism,
// background maintenance vs predictable latency).
//
// In practice, most callers want one of a small number of "bundles":
//
//   - "Durable": the safest defaults; favors crash-recovery and integrity.
//   - "Fast":    higher throughput by relaxing durability/integrity knobs.
//   - "Bench":   a deterministic variant intended for benchmarking; disables
//     background workers that can otherwise inject "random" work
//     (e.g. index vacuum firing mid-run).
//
// Profiles are intentionally conservative:
//   - They set the *meaningful policy knobs* (durability/integrity/background),
//     while leaving the many throughput/capacity tuning knobs at their usual
//     defaults.
//   - They do not require TreeDB internals to infer "intent" from combinations
//     of flags. You pick the intent explicitly.
//
// How to use
// ----------
//
//  1. New DB (recommended):
//     opts := treedb.OptionsFor(treedb.ProfileDurable, "/path/to/db")
//     opts.FlushThreshold = 128 << 20 // optional tuning
//     db, err := treedb.Open(opts)
//
//  2. Existing Options (merge without clobbering explicit overrides):
//     opts := treedb.Options{Dir: "/path/to/db"}
//     treedb.ApplyProfile(&opts, treedb.ProfileBench)
//     opts.FlushThreshold = 64 << 20 // explicit overrides always win
//     db, err := treedb.Open(opts)
//
// Note: Profiles are a convenience API. TreeDB still honors the established
// "0 uses a default; <0 disables" convention for background knobs, and callers
// can always override any field directly after applying a profile.
type Profile string

const (
	// ProfileDurable is the recommended default for production use when you care
	// about durability and corruption detection.
	//
	// It keeps WAL and checksums enabled and leaves background maintenance at
	// their default settings.
	ProfileDurable Profile = "durable"

	// ProfileFast prioritizes throughput by relaxing durability/integrity knobs.
	//
	// This profile is appropriate when:
	//   - you are running on top of an external durability boundary (e.g. you
	//     snapshot at higher layers), or
	//   - you are exploring performance limits, and crashes/corruption detection
	//     are acceptable trade-offs.
	//
	// It also pins the current run_celestia-style value-log compression policy:
	// auto compression, balanced auto policy, snappy block fallback, and medium
	// autotune.
	//
	// Background maintenance is left enabled by default; it is generally helpful
	// for keeping the index compact and read-friendly.
	ProfileFast Profile = "fast"

	// ProfileWALOnFast is a "WAL on + relaxed durability" profile intended for
	// write-heavy benchmarks and ingest workloads.
	//
	// It keeps WAL enabled but disables fsync and value-log read checksums.
	// It also pins the same value-log compression policy as ProfileFast.
	ProfileWALOnFast Profile = "wal_on_fast"

	// ProfileBench is a "fast + deterministic" profile intended specifically for
	// benchmarking.
	//
	// It disables background workers that can inject heavy work mid-run (e.g.
	// background index vacuum). This makes comparisons more stable across runs.
	//
	// IMPORTANT: This is not a recommended production profile.
	ProfileBench Profile = "bench"
)

// OptionsFor returns a copy of Options pre-filled for the given Profile.
//
// The returned Options still follow TreeDB's normal defaulting rules for fields
// left as zero values (e.g. ChunkSize, KeepRecent, backpressure thresholds).
func OptionsFor(profile Profile, dir string) Options {
	opts := Options{Dir: dir}
	ApplyProfile(&opts, profile)
	return opts
}

// ApplyProfile applies a profile to opts without overwriting explicit caller
// overrides.
//
// For numeric/duration fields, "explicit override" means the field is already
// set to a non-zero value. For background intervals, note TreeDB conventions:
//   - `0` means "use default"
//   - `<0` means "disable"
//
// For booleans, Go does not provide a way to distinguish “unset” from “explicit
// false”, so profiles set boolean policy knobs to match the profile. If you
// want the opposite policy, apply the profile and then override the boolean
// explicitly.
func ApplyProfile(opts *Options, profile Profile) {
	if opts == nil {
		return
	}

	switch profile {
	case ProfileDurable:
		applyDurableProfile(opts)
	case ProfileFast:
		applyFastProfile(opts)
	case ProfileWALOnFast:
		applyWALOnFastProfile(opts)
	case ProfileBench:
		applyBenchProfile(opts)
	default:
		// Unknown profile: no-op (callers can still use Options directly).
	}
}

func applyDurableProfile(opts *Options) {
	opts.Durability = DurabilityDurable
	opts.ValueLog.ReadIntegrity = IntegrityVerify
	opts.IndexOuterLeavesInValueLog = true
}

func applyIndexOptimizationsProfile(opts *Options) {
	opts.LeafPrefixCompression = true
	opts.IndexColumnarLeaves = true
	opts.IndexPackedValuePtr = true
	opts.IndexInternalBaseDelta = true
	if opts.IndexOuterLeavesInValueLog {
		// Leaf refs encode value-log pointers in internal child IDs, which are
		// incompatible with internal base-delta child ID encodings.
		opts.IndexInternalBaseDelta = false
	}
}

func applyRunCelestiaVLogCompressionProfile(opts *Options) {
	if opts.ValueLog.Compression == 0 {
		opts.ValueLog.Compression = ValueLogCompressionAuto
	}
	if opts.ValueLog.BlockCodec == 0 {
		opts.ValueLog.BlockCodec = ValueLogBlockSnappy
	}
	if opts.ValueLog.AutoPolicy == 0 {
		opts.ValueLog.AutoPolicy = ValueLogAutoBalanced
	}
	if opts.ValueLog.CompressionAutotune.Mode == AutotuneUnset {
		opts.ValueLog.CompressionAutotune.Mode = AutotuneMedium
	}
}

func applyFastPagerSyncProfile(opts *Options) {
	if opts.PagerSyncConcurrency == 0 {
		opts.PagerSyncConcurrency = 4
	}
}

func applyFastProfile(opts *Options) {
	opts.Durability = DurabilityWALOffRelaxed
	opts.ValueLog.ReadIntegrity = IntegritySkipChecksums
	opts.IndexOuterLeavesInValueLog = true
	applyFastPagerSyncProfile(opts)
	applyRunCelestiaVLogCompressionProfile(opts)
	if opts.ValueLog.DictIncompressibleHoldBytes == 0 {
		opts.ValueLog.DictIncompressibleHoldBytes = 64 << 20
	}
	if opts.ValueLog.DictProbeIntervalBytes == 0 {
		opts.ValueLog.DictProbeIntervalBytes = 32 << 20
	}

	// Prefer appending new pages for throughput under churn unless caller opted
	// out. This can trade disk growth for write speed.
	if !opts.PreferAppendAlloc {
		opts.PreferAppendAlloc = true
	}
	applyIndexOptimizationsProfile(opts)
}

func applyWALOnFastProfile(opts *Options) {
	opts.Durability = DurabilityWALOnRelaxed
	opts.ValueLog.ReadIntegrity = IntegritySkipChecksums
	opts.IndexOuterLeavesInValueLog = true
	applyFastPagerSyncProfile(opts)
	applyRunCelestiaVLogCompressionProfile(opts)
	if opts.ValueLog.DictIncompressibleHoldBytes == 0 {
		opts.ValueLog.DictIncompressibleHoldBytes = 64 << 20
	}
	if opts.ValueLog.DictProbeIntervalBytes == 0 {
		opts.ValueLog.DictProbeIntervalBytes = 32 << 20
	}

	// Prefer appending new pages for throughput under churn.
	opts.PreferAppendAlloc = true
	applyIndexOptimizationsProfile(opts)
}

func applyBenchProfile(opts *Options) {
	applyFastProfile(opts)

	// Determinism: disable background workers that can inject large, unrelated
	// work mid-benchmark.
	//
	// Auto-checkpointing only matters when WAL is enabled, but for bench-mode we
	// also disable it explicitly to reduce background wakeups if the caller later
	// enables WAL.
	if opts.BackgroundCheckpointInterval == 0 {
		opts.BackgroundCheckpointInterval = -1
	}
	if opts.BackgroundCheckpointIdleDuration == 0 {
		opts.BackgroundCheckpointIdleDuration = -1
	}
	if opts.MaxWALBytes == 0 {
		opts.MaxWALBytes = -1
	}
	if opts.BackgroundIndexVacuumInterval == 0 {
		opts.BackgroundIndexVacuumInterval = -1
	}
	if opts.ValueLog.Generational.Policy == ValueLogGenerationDefault {
		opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	}

	// Background pruner: disable concurrent pruning to avoid allocator work in the
	// background. This may increase commit cost, but makes the workload more
	// deterministic.
	if !opts.DisableBackgroundPrune {
		opts.DisableBackgroundPrune = true
	}
}
