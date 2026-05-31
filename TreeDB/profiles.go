package treedb

import "strings"

// Profiles are intentionally defined in the public package so downstream users
// can pick a coherent option bundle without duplicating the mapping from
// “intent” (WAL path, ACK guarantee, and layout policy) to low-level knobs.

// Profile is a documented, high-level preset for TreeDB Options.
//
// Why profiles exist
// ------------------
// TreeDB exposes many low-level knobs because different workloads want different
// trade-offs (durability vs throughput, steady-state vs benchmark determinism,
// background maintenance vs predictable latency).
//
// In practice, most callers want one of a small number of explicit bundles:
//
//   - "Command WAL durable": command WAL plus durable sync/integrity.
//   - "Command WAL relaxed": command WAL plus relaxed sync/integrity.
//   - "Legacy WAL durable":  pre-command-WAL durable path.
//   - "No WAL fast":         higher throughput by relaxing durability/integrity.
//   - "Bench":               a deterministic variant intended for benchmarking.
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
//     opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, "/path/to/db")
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
	// ProfileCommandWALDurable is the explicit command-WAL production profile for
	// callers that want command-WAL recovery, durable fsync boundaries, corruption
	// detection, and the fast collection/index layout bundle.
	ProfileCommandWALDurable Profile = "command_wal_durable"

	// ProfileCommandWALRelaxed is the explicit command-WAL relaxed profile for
	// write-heavy benchmark and ingest workloads. It enables command WAL but uses
	// relaxed sync and read-integrity semantics.
	ProfileCommandWALRelaxed Profile = "command_wal_relaxed"

	// ProfileLegacyWALDurable is the explicit name for the pre-command-WAL durable
	// profile. It keeps WAL and checksums enabled, but does not enable CommandWAL.
	ProfileLegacyWALDurable Profile = "legacy_wal_durable"

	// ProfileLegacyWALRelaxedFast is the explicit name for the pre-command-WAL
	// relaxed fast profile. It is equivalent to ProfileWALOnFast.
	ProfileLegacyWALRelaxedFast Profile = "legacy_wal_relaxed_fast"

	// ProfileNoWALFast is the explicit name for the no-WAL fast profile. It is
	// equivalent to ProfileFast.
	ProfileNoWALFast Profile = "no_wal_fast"

	// ProfileDurable is the legacy-compatible short name for
	// ProfileLegacyWALDurable.
	//
	// Prefer ProfileCommandWALDurable for new command-WAL durability decisions, or
	// ProfileLegacyWALDurable when the old/raw WAL path is intentional.
	//
	// This profile keeps WAL and checksums enabled and leaves background
	// maintenance at its default settings, but it does not enable CommandWAL.
	//
	// Deprecated: use ProfileCommandWALDurable or ProfileLegacyWALDurable to make
	// the WAL path explicit.
	ProfileDurable Profile = "durable"

	// ProfileFast prioritizes throughput by relaxing durability/integrity knobs.
	//
	// This profile is appropriate when:
	//   - you are running on top of an external durability boundary (e.g. you
	//     snapshot at higher layers), or
	//   - you are exploring performance limits, and crashes/corruption detection
	//     are acceptable trade-offs.
	//
	// Collection writes under this profile use DurabilityWALOffRelaxed. A
	// successful collection write is not a durable-at-ack promise; use Flush,
	// FlushAll, Checkpoint, or Close for a persistence boundary.
	//
	// It also pins the current run_celestia-style value-log compression policy:
	// auto compression, balanced auto policy, snappy block fallback, and medium
	// autotune.
	//
	// Background maintenance is left enabled by default; it is generally helpful
	// for keeping the index compact and read-friendly.
	//
	// Deprecated: use ProfileNoWALFast when the no-WAL path is intentional.
	ProfileFast Profile = "fast"

	// ProfileWALOnFast is a legacy-compatible short name for
	// ProfileLegacyWALRelaxedFast. It is a "WAL on + relaxed durability" profile
	// intended for write-heavy benchmarks and ingest workloads.
	//
	// It keeps the legacy/raw WAL path enabled but disables fsync and value-log
	// read checksums.
	//
	// It also pins the same value-log compression policy as ProfileNoWALFast.
	//
	// Deprecated: use ProfileLegacyWALRelaxedFast when the legacy/raw WAL path is
	// intentional, or ProfileCommandWALRelaxed for the command-WAL relaxed path.
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

// ProfileFlagHelp is the recommended profile vocabulary for CLI flag help.
const ProfileFlagHelp = "command_wal_durable, command_wal_relaxed, legacy_wal_durable, legacy_wal_relaxed_fast, no_wal_fast, or bench (aliases: durable, wal_on_fast, fast)"

const fastProfileChunkSize = 4 << 20

// ParseProfile parses a profile name using TreeDB's standard profile vocabulary.
// Empty input returns fallback. The bool reports whether a known name or empty
// fallback was accepted.
func ParseProfile(raw string, fallback Profile) (Profile, bool) {
	if strings.TrimSpace(raw) == "" {
		return fallback, true
	}
	return NormalizeProfile(Profile(raw))
}

// NormalizeProfile normalizes supported profile names and aliases to their
// public Profile constants.
func NormalizeProfile(profile Profile) (Profile, bool) {
	switch normalizeProfileToken(string(profile)) {
	case string(ProfileCommandWALDurable):
		return ProfileCommandWALDurable, true
	case string(ProfileCommandWALRelaxed):
		return ProfileCommandWALRelaxed, true
	case string(ProfileLegacyWALDurable):
		return ProfileLegacyWALDurable, true
	case string(ProfileLegacyWALRelaxedFast):
		return ProfileLegacyWALRelaxedFast, true
	case string(ProfileNoWALFast):
		return ProfileNoWALFast, true
	case string(ProfileDurable):
		return ProfileDurable, true
	case string(ProfileFast):
		return ProfileFast, true
	case string(ProfileWALOnFast), "walonfast":
		return ProfileWALOnFast, true
	case string(ProfileBench):
		return ProfileBench, true
	default:
		return "", false
	}
}

func normalizeProfileToken(raw string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(raw)), "-", "_")
}

// OptionsFor returns a copy of Options pre-filled for the given Profile.
//
// The returned Options still follow TreeDB's normal defaulting rules for fields
// left as zero values unless the selected profile intentionally owns that knob
// (e.g. fast profiles set ChunkSize; KeepRecent, allocator policy, and
// backpressure thresholds still use normal defaults).
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

	normalized, ok := NormalizeProfile(profile)
	if !ok {
		// Unknown profile: no-op (callers can still use Options directly).
		return
	}

	switch normalized {
	case ProfileCommandWALDurable:
		applyCommandWALDurableProfile(opts)
	case ProfileCommandWALRelaxed:
		applyCommandWALRelaxedProfile(opts)
	case ProfileDurable, ProfileLegacyWALDurable:
		applyDurableProfile(opts)
	case ProfileFast, ProfileNoWALFast:
		applyFastProfile(opts)
	case ProfileWALOnFast, ProfileLegacyWALRelaxedFast:
		applyWALOnFastProfile(opts)
	case ProfileBench:
		applyBenchProfile(opts)
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
	if opts.ChunkSize == 0 {
		opts.ChunkSize = fastProfileChunkSize
	}
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
	opts.ValueLog.CurrentWritableMmap = true

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
	opts.ValueLog.CurrentWritableMmap = true

	applyIndexOptimizationsProfile(opts)
}

func applyCommandWALDurableProfile(opts *Options) {
	applyWALOnFastProfile(opts)
	opts.CommandWAL = true
	opts.Durability = DurabilityDurable
	opts.ValueLog.ReadIntegrity = IntegrityVerify
}

func applyCommandWALRelaxedProfile(opts *Options) {
	applyWALOnFastProfile(opts)
	opts.CommandWAL = true
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
