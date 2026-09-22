package treedb

import (
	"fmt"
	"strings"

	"github.com/snissn/gomap/TreeDB/db"
)

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
// In practice, most public callers should choose one of these explicit bundles:
//
//   - "Command WAL durable": command WAL plus durable ordinary ACKs.
//   - "Command WAL relaxed": command WAL plus relaxed ordinary ACKs.
//   - "No WAL fast":         no WAL plus relaxed ordinary ACKs.
//   - "Bench unsafe":        benchmark/test-only ceiling with no durability promise.
//
// Additional legacy/no-WAL constants remain temporarily available for
// compatibility and low-level tests while the public profile surface is being
// narrowed. New server, collection, and Mongo gateway entry points should not
// advertise those names as normal choices.
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
//  2. Explicit benchmark/test-only options:
//     opts := treedb.Options{Dir: "/path/to/db"}
//     treedb.ApplyBenchmarkProfile(&opts, treedb.ProfileBenchUnsafe)
//     opts.FlushThreshold = 64 << 20
//     db, err := treedb.Open(opts)
//
// The resolved profile is immutable at Open: profile-owned durability and
// integrity fields are reapplied so later low-level mutations cannot silently
// change the selected contract. Non-contract tuning fields remain caller-owned.
// A profile-less reopen adopts an existing DB's persisted production profile;
// only a new DB defaults to command_wal_durable. Persisted bench_unsafe DBs
// still require the explicit benchmark constructor boundary.
type Profile = db.DurabilityProfile

const commandWALProfileSegmentBytes int64 = 256 << 20

const (
	// The four canonical profile strings are the only accepted parser tokens.
	ProfileCommandWALDurable = db.ProfileCommandWALDurable
	ProfileCommandWALRelaxed = db.ProfileCommandWALRelaxed
	ProfileNoWALFast         = db.ProfileNoWALFast
	ProfileBenchUnsafe       = db.ProfileBenchUnsafe

	// Deprecated Go aliases. They retain distinct source tokens so resolution can
	// expose a deprecation signal, but each maps to exactly one canonical profile.
	ProfileDurable              Profile = "durable"
	ProfileWALOnFast            Profile = "wal_on_fast"
	ProfileFast                 Profile = "fast"
	ProfileBench                Profile = "bench"
	ProfileLegacyWALDurable     Profile = "legacy_wal_durable"
	ProfileLegacyWALRelaxedFast Profile = "legacy_wal_relaxed_fast"
)

// ProfileFlagHelp is the recommended public profile vocabulary for CLI flag
// help. Legacy profile constants are intentionally omitted here; they are
// transitional/internal compatibility surfaces, not normal public choices.
const ProfileFlagHelp = "command_wal_durable, command_wal_relaxed, or no_wal_fast"

// BenchmarkProfileFlagHelp is the explicit benchmark/test parser vocabulary.
const BenchmarkProfileFlagHelp = "command_wal_durable, command_wal_relaxed, no_wal_fast, or bench_unsafe"

const fastProfileChunkSize = 4 << 20

// ParseProfile parses a profile name using TreeDB's standard profile vocabulary.
// Empty input returns fallback. The bool reports whether a known name or empty
// fallback was accepted.
func ParseProfile(raw string, fallback Profile) (Profile, bool) {
	if strings.TrimSpace(raw) == "" {
		return parseCanonicalProfile(string(fallback), true)
	}
	return parseCanonicalProfile(raw, true)
}

// ParsePublicProfile parses the public server/profile vocabulary. Empty input
// returns fallback only when fallback is also a public profile. Deprecated
// compatibility names such as fast, wal_on_fast, durable, and legacy_wal_* are
// intentionally rejected here.
func ParsePublicProfile(raw string, fallback Profile) (Profile, bool) {
	if strings.TrimSpace(raw) == "" {
		return parseCanonicalProfile(string(fallback), false)
	}
	return parseCanonicalProfile(raw, false)
}

// ParseBenchmarkProfile parses the explicit benchmark/test vocabulary. Unlike
// ParsePublicProfile, it accepts bench_unsafe.
func ParseBenchmarkProfile(raw string, fallback Profile) (Profile, bool) {
	if strings.TrimSpace(raw) == "" {
		return parseCanonicalProfile(string(fallback), true)
	}
	return parseCanonicalProfile(raw, true)
}

func parseCanonicalProfile(raw string, allowBenchUnsafe bool) (Profile, bool) {
	switch strings.TrimSpace(raw) {
	case string(ProfileCommandWALDurable):
		return ProfileCommandWALDurable, true
	case string(ProfileCommandWALRelaxed):
		return ProfileCommandWALRelaxed, true
	case string(ProfileNoWALFast):
		return ProfileNoWALFast, true
	case string(ProfileBenchUnsafe):
		if allowBenchUnsafe {
			return ProfileBenchUnsafe, true
		}
	}
	return "", false
}

// NormalizePublicProfile accepts only canonical production profile values.
func NormalizePublicProfile(profile Profile) (Profile, bool) {
	return parseCanonicalProfile(string(profile), false)
}

// NormalizeProfile resolves canonical profiles and deprecated Go aliases to one
// of the four immutable canonical values. String parsers must use ParseProfile,
// ParsePublicProfile, or ParseBenchmarkProfile so ambiguous alias tokens never
// become configuration vocabulary.
func NormalizeProfile(profile Profile) (Profile, bool) {
	resolved, _, ok := resolveProfile(profile)
	return resolved, ok
}

func resolveProfile(profile Profile) (resolved Profile, deprecatedAlias Profile, ok bool) {
	switch profile {
	case ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast, ProfileBenchUnsafe:
		return profile, "", true
	case ProfileDurable, ProfileLegacyWALDurable:
		return ProfileCommandWALDurable, profile, true
	case ProfileWALOnFast, ProfileLegacyWALRelaxedFast:
		return ProfileCommandWALRelaxed, profile, true
	case ProfileFast:
		return ProfileNoWALFast, profile, true
	case ProfileBench:
		return ProfileBenchUnsafe, profile, true
	default:
		return "", "", false
	}
}

func resolveOpenProfileOptions(opts *Options) error {
	if opts == nil {
		return fmt.Errorf("treedb: nil options")
	}
	if err := validateProfileOwnedOptionEnums(*opts); err != nil {
		return err
	}
	source := Profile(opts.ResolvedProfile)
	applyPreset := false
	if source == "" {
		layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
		if err != nil {
			return err
		}
		persisted, ok, err := db.LoadPersistedDurabilityProfile(layout.mainDir)
		if err != nil {
			return err
		}
		if ok {
			source = Profile(persisted)
		} else {
			source = ProfileCommandWALDurable
		}
		applyPreset = true
	}
	resolved, sourceAlias, ok := resolveProfile(source)
	if !ok {
		return fmt.Errorf("treedb: unsupported resolved profile %q", source)
	}
	if resolved == ProfileBenchUnsafe && (applyPreset || !opts.UnsafeBenchmarkProfile) {
		return fmt.Errorf("treedb: profile %q requires OptionsForBenchmark or ApplyBenchmarkProfile", resolved)
	}

	alias := Profile(opts.DeprecatedProfileAlias)
	if alias != "" {
		aliasResolved, _, aliasOK := resolveProfile(alias)
		if !aliasOK || aliasResolved != resolved || alias == resolved {
			return fmt.Errorf("treedb: invalid deprecated profile alias %q for %q", alias, resolved)
		}
	} else {
		alias = sourceAlias
	}

	if applyPreset {
		applyResolvedProfile(opts, resolved, false)
	} else {
		applyResolvedDurabilityContract(opts, resolved)
	}
	opts.DeprecatedProfileAlias = alias
	return nil
}

// validateProfileOwnedOptionEnums rejects corrupt or misspelled low-level enum
// values before the selected profile reapplies its immutable contract. Valid
// legacy values may be superseded by the resolved profile, but invalid values
// must never be silently normalized into a different configuration.
func validateProfileOwnedOptionEnums(opts Options) error {
	switch opts.Durability {
	case DurabilityDurable, DurabilityWALOnRelaxed, DurabilityWALOffRelaxed:
	default:
		return fmt.Errorf("treedb: invalid durability mode %d", opts.Durability)
	}
	switch opts.ValueLog.ReadIntegrity {
	case IntegrityVerify, IntegritySkipChecksums:
	default:
		return fmt.Errorf("treedb: invalid value-log integrity mode %d", opts.ValueLog.ReadIntegrity)
	}
	return nil
}

// applyResolvedDurabilityContract makes the selected acknowledgement,
// durability, and integrity contract immutable at Open while preserving
// caller-owned layout and tuning overrides applied after OptionsFor.
func applyResolvedDurabilityContract(opts *Options, resolved Profile) {
	opts.ResolvedProfile = resolved
	opts.UnsafeBenchmarkProfile = resolved == ProfileBenchUnsafe
	switch resolved {
	case ProfileCommandWALDurable:
		opts.CommandWAL = true
		opts.Durability = DurabilityDurable
		opts.ValueLog.ReadIntegrity = IntegrityVerify
		opts.ValueLog.CurrentWritableMmap = false
	case ProfileCommandWALRelaxed:
		opts.CommandWAL = true
		opts.Durability = DurabilityWALOnRelaxed
		opts.ValueLog.ReadIntegrity = IntegrityVerify
		opts.ValueLog.CurrentWritableMmap = false
	case ProfileNoWALFast:
		opts.CommandWAL = false
		opts.Durability = DurabilityWALOffRelaxed
		opts.ValueLog.ReadIntegrity = IntegrityVerify
		opts.ValueLog.CurrentWritableMmap = false
	case ProfileBenchUnsafe:
		opts.CommandWAL = false
		opts.Durability = DurabilityWALOffRelaxed
	}
}

// OptionsFor returns a copy of Options pre-filled for the given Profile.
//
// The returned Options still follow TreeDB's normal defaulting rules for fields
// left as zero values unless the selected profile intentionally owns that knob
// (e.g. fast profiles set ChunkSize; KeepRecent, allocator policy, and
// backpressure thresholds still use normal defaults).
//
// OptionsFor panics for unknown profiles. Parse public CLI/env strings with
// ParsePublicProfile before calling OptionsFor.
func OptionsFor(profile Profile, dir string) Options {
	opts := Options{Dir: dir}
	ApplyProfile(&opts, profile)
	return opts
}

// OptionsForBenchmark is the explicit benchmark/test constructor boundary. It
// is the only options constructor that can select bench_unsafe.
func OptionsForBenchmark(profile Profile, dir string) Options {
	opts := Options{Dir: dir}
	ApplyBenchmarkProfile(&opts, profile)
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
//
// ApplyProfile panics for unknown profiles so misspelled programmatic profile
// tokens cannot silently select bare default options.
func ApplyProfile(opts *Options, profile Profile) {
	applyResolvedProfile(opts, profile, false)
}

// ApplyBenchmarkProfile is the explicit benchmark/test application boundary.
// Production code should use ApplyProfile.
func ApplyBenchmarkProfile(opts *Options, profile Profile) {
	applyResolvedProfile(opts, profile, true)
}

func applyResolvedProfile(opts *Options, profile Profile, allowBenchUnsafe bool) {
	resolved, deprecatedAlias, ok := resolveProfile(profile)
	if !ok || (resolved == ProfileBenchUnsafe && !allowBenchUnsafe) {
		panic(fmt.Sprintf("treedb: unsupported profile %q", profile))
	}
	if opts == nil {
		return
	}

	opts.ResolvedProfile = resolved
	opts.DeprecatedProfileAlias = deprecatedAlias
	opts.UnsafeBenchmarkProfile = resolved == ProfileBenchUnsafe
	switch resolved {
	case ProfileCommandWALDurable:
		applyCommandWALDurableProfile(opts)
	case ProfileCommandWALRelaxed:
		applyCommandWALRelaxedProfile(opts)
	case ProfileNoWALFast:
		applyNoWALFastProfile(opts)
	case ProfileBenchUnsafe:
		applyBenchUnsafeProfile(opts)
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

func applyUnsafeFastProfile(opts *Options) {
	opts.CommandWAL = false
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

func applyNoWALFastProfile(opts *Options) {
	applyUnsafeFastProfile(opts)
	opts.ValueLog.ReadIntegrity = IntegrityVerify
	opts.ValueLog.CurrentWritableMmap = false
}

func applyCommandWALDurableProfile(opts *Options) {
	applyNoWALFastProfile(opts)
	opts.CommandWAL = true
	opts.Durability = DurabilityDurable
	applyCommandWALProfileSegmentDefaults(opts)
}

func applyCommandWALRelaxedProfile(opts *Options) {
	applyNoWALFastProfile(opts)
	opts.CommandWAL = true
	opts.Durability = DurabilityWALOnRelaxed
	opts.ValueLog.CurrentWritableMmap = false
	applyCommandWALProfileSegmentDefaults(opts)
}

func applyCommandWALProfileSegmentDefaults(opts *Options) {
	if opts.WALMaxSegmentBytes == 0 {
		opts.WALMaxSegmentBytes = commandWALProfileSegmentBytes
	}
	if opts.CommandWALSegmentTargetBytes == 0 {
		opts.CommandWALSegmentTargetBytes = opts.WALMaxSegmentBytes
	}
}

func applyBenchUnsafeProfile(opts *Options) {
	applyUnsafeFastProfile(opts)

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
