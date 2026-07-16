package treedb

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestOptionsFor_ProfileSetsDir(t *testing.T) {
	opts := OptionsFor(ProfileDurable, "/tmp/treedb-profiles-test")
	if opts.Dir != "/tmp/treedb-profiles-test" {
		t.Fatalf("Dir mismatch: got %q", opts.Dir)
	}
	if opts.ResolvedProfile != ProfileCommandWALDurable || opts.DeprecatedProfileAlias != ProfileDurable {
		t.Fatalf("profile resolution=(%q,%q), want (%q,%q)", opts.ResolvedProfile, opts.DeprecatedProfileAlias, ProfileCommandWALDurable, ProfileDurable)
	}
}

func TestOptionsFor_UnknownProfilePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("OptionsFor accepted unknown profile, want panic")
		}
	}()
	_ = OptionsFor(Profile("not_a_profile"), t.TempDir())
}

func TestApplyProfile_UnknownProfilePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("ApplyProfile accepted unknown profile, want panic")
		}
	}()
	var opts Options
	ApplyProfile(&opts, Profile("not_a_profile"))
}

func TestApplyProfile_UnknownProfilePanicsWithNilOptions(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("ApplyProfile accepted unknown profile with nil options, want panic")
		}
	}()
	ApplyProfile(nil, Profile("not_a_profile"))
}

func TestProductionProfileAPIsRejectBenchUnsafe(t *testing.T) {
	for _, profile := range []Profile{ProfileBench, ProfileBenchUnsafe} {
		t.Run(string(profile), func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("ApplyProfile(%q) accepted benchmark-only profile", profile)
				}
			}()
			var opts Options
			ApplyProfile(&opts, profile)
		})
	}

	opts := OptionsForBenchmark(ProfileBenchUnsafe, t.TempDir())
	if opts.ResolvedProfile != ProfileBenchUnsafe || !opts.UnsafeBenchmarkProfile {
		t.Fatalf("OptionsForBenchmark did not mark explicit unsafe boundary: %+v", opts)
	}
}

func TestOpen_DefaultsToCommandWALDurableResolvedProfile(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open default profile: %v", err)
	}
	defer database.Close()

	if got := database.ResolvedProfile(); got != ProfileCommandWALDurable {
		t.Fatalf("ResolvedProfile=%q want %q", got, ProfileCommandWALDurable)
	}
	stats := database.Stats()
	for key, want := range map[string]string{
		"treedb.profile.resolved":           string(ProfileCommandWALDurable),
		"treedb.profile.ordinary_ack_class": "durable_wal_prefix",
		"treedb.profile.production":         "true",
		"treedb.profile.bench_unsafe":       "false",
	} {
		if got := stats[key]; got != want {
			t.Fatalf("%s=%q want %q", key, got, want)
		}
	}
}

func TestOpen_ReappliesImmutableResolvedProfileContract(t *testing.T) {
	opts := OptionsFor(ProfileCommandWALRelaxed, t.TempDir())
	opts.CommandWAL = false
	opts.Durability = DurabilityDurable
	opts.ValueLog.ReadIntegrity = IntegritySkipChecksums

	database, err := Open(opts)
	if err != nil {
		t.Fatalf("Open canonical profile after low-level mutation: %v", err)
	}
	defer database.Close()
	if got := database.ResolvedProfile(); got != ProfileCommandWALRelaxed {
		t.Fatalf("ResolvedProfile=%q want %q", got, ProfileCommandWALRelaxed)
	}
	stats := database.Stats()
	if got := stats["treedb.command_wal.enabled"]; got != "true" {
		t.Fatalf("command WAL=%q want true", got)
	}
	if got := stats["treedb.vlog.read_integrity"]; got != "verify" {
		t.Fatalf("read integrity=%q want verify", got)
	}
}

func TestOpen_RejectsBenchUnsafeWithoutExplicitBoundary(t *testing.T) {
	_, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileBenchUnsafe})
	if err == nil || !strings.Contains(err.Error(), "OptionsForBenchmark") {
		t.Fatalf("Open bench_unsafe error=%v, want explicit-boundary rejection", err)
	}
}

func TestOpen_PersistedProductionProfileImplicitlyReopensSameContract(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			database, err := Open(OptionsFor(profile, dir))
			if err != nil {
				t.Fatalf("Open %s: %v", profile, err)
			}
			if err := database.Close(); err != nil {
				t.Fatalf("Close %s: %v", profile, err)
			}

			reopen, err := Open(Options{Dir: dir})
			if err != nil {
				t.Fatalf("implicit reopen %s: %v", profile, err)
			}
			if got := reopen.ResolvedProfile(); got != profile {
				t.Fatalf("implicit reopen profile=%q want %q", got, profile)
			}
			if err := reopen.Close(); err != nil {
				t.Fatalf("close implicit reopen %s: %v", profile, err)
			}
		})
	}
}

func TestOpen_PersistedProfileCannotChangeContracts(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.ValueLog.PointerThreshold = 1
	database, err := Open(opts)
	if err != nil {
		t.Fatalf("Open no_wal_fast: %v", err)
	}
	if err := database.Set([]byte("profile/reopen"), []byte("persisted value-log record")); err != nil {
		_ = database.Close()
		t.Fatalf("Set no_wal_fast: %v", err)
	}
	if err := database.Checkpoint(); err != nil {
		_ = database.Close()
		t.Fatalf("Checkpoint no_wal_fast: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close no_wal_fast: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, IgnoreFormatConfig: true})
	if err != nil {
		t.Fatalf("IgnoreFormatConfig implicit reopen no_wal_fast: %v", err)
	}
	if got := reopen.ResolvedProfile(); got != ProfileNoWALFast {
		t.Fatalf("IgnoreFormatConfig implicit reopen profile=%q want %q", got, ProfileNoWALFast)
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("close IgnoreFormatConfig implicit reopen no_wal_fast: %v", err)
	}

	backend, cleanup, err := OpenBackend(Options{Dir: dir})
	if err != nil {
		t.Fatalf("OpenBackend implicit no_wal_fast: %v", err)
	}
	if got := backend.ResolvedProfile(); got != ProfileNoWALFast {
		t.Fatalf("OpenBackend implicit profile=%q want %q", got, ProfileNoWALFast)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("OpenBackend cleanup: %v", err)
	}
	if err := VacuumIndexOffline(Options{Dir: dir}); err != nil {
		t.Fatalf("implicit-profile offline vacuum: %v", err)
	}
	if _, err := ValueLogRewriteOffline(Options{Dir: dir}); err != nil {
		t.Fatalf("implicit-profile offline rewrite: %v", err)
	}

	if _, err := Open(OptionsFor(ProfileCommandWALDurable, dir)); !errors.Is(err, ErrLegacyFormatRebuildRequired) {
		t.Fatalf("explicit mismatched reopen error=%v, want ErrLegacyFormatRebuildRequired", err)
	}
}

func TestOpen_BenchUnsafeManifestCannotBecomeProductionDefault(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(OptionsForBenchmark(ProfileBenchUnsafe, dir))
	if err != nil {
		t.Fatalf("Open bench_unsafe: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close bench_unsafe: %v", err)
	}

	if _, err := Open(Options{Dir: dir}); err == nil || !strings.Contains(err.Error(), "OptionsForBenchmark") {
		t.Fatalf("production reopen error=%v, want explicit benchmark-boundary rejection", err)
	}
	if _, err := Open(Options{Dir: dir, UnsafeBenchmarkProfile: true}); err == nil || !strings.Contains(err.Error(), "OptionsForBenchmark") {
		t.Fatalf("flag-only benchmark reopen error=%v, want explicit benchmark-boundary rejection", err)
	}

	reopen, err := Open(OptionsForBenchmark(ProfileBenchUnsafe, dir))
	if err != nil {
		t.Fatalf("explicit benchmark reopen: %v", err)
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("close explicit benchmark reopen: %v", err)
	}
}

func TestApplyProfile_FastSetsPolicyBools(t *testing.T) {
	var opts Options
	ApplyProfile(&opts, ProfileFast)

	if opts.Durability != DurabilityWALOffRelaxed {
		t.Fatalf("expected DurabilityWALOffRelaxed for fast profile, got %v", opts.Durability)
	}
	if opts.ValueLog.ReadIntegrity != IntegrityVerify {
		t.Fatalf("expected IntegrityVerify for fast alias, got %v", opts.ValueLog.ReadIntegrity)
	}
	if !opts.IndexOuterLeavesInValueLog {
		t.Fatalf("expected IndexOuterLeavesInValueLog=true for fast profile")
	}
	if opts.PreferAppendAlloc {
		t.Fatalf("expected PreferAppendAlloc=false for fast profile")
	}
	if opts.ValueLog.Compression != ValueLogCompressionAuto {
		t.Fatalf("expected ValueLog.Compression=ValueLogCompressionAuto for fast profile, got %v", opts.ValueLog.Compression)
	}
	if opts.ValueLog.BlockCodec != ValueLogBlockSnappy {
		t.Fatalf("expected ValueLog.BlockCodec=ValueLogBlockSnappy for fast profile, got %v", opts.ValueLog.BlockCodec)
	}
	if opts.ValueLog.AutoPolicy != ValueLogAutoBalanced {
		t.Fatalf("expected ValueLog.AutoPolicy=ValueLogAutoBalanced for fast profile, got %v", opts.ValueLog.AutoPolicy)
	}
	if opts.ValueLog.CompressionAutotune.Mode != AutotuneMedium {
		t.Fatalf("expected ValueLog.CompressionAutotune.Mode=AutotuneMedium for fast profile, got %v", opts.ValueLog.CompressionAutotune.Mode)
	}
	if opts.ValueLog.DictIncompressibleHoldBytes != 64<<20 {
		t.Fatalf("expected DictIncompressibleHoldBytes=64MiB for fast profile, got %d", opts.ValueLog.DictIncompressibleHoldBytes)
	}
	if opts.ValueLog.DictProbeIntervalBytes != 32<<20 {
		t.Fatalf("expected DictProbeIntervalBytes=32MiB for fast profile, got %d", opts.ValueLog.DictProbeIntervalBytes)
	}
	if opts.ValueLog.ForcePointers {
		t.Fatalf("expected ValueLog.ForcePointers=false for fast profile")
	}
	if opts.ValueLog.CurrentWritableMmap {
		t.Fatalf("expected ValueLog.CurrentWritableMmap=false for production no_wal_fast profile")
	}
	if !opts.LeafPrefixCompression {
		t.Fatalf("expected LeafPrefixCompression=true for fast profile")
	}
	if !opts.IndexColumnarLeaves {
		t.Fatalf("expected IndexColumnarLeaves=true for fast profile")
	}
	if !opts.IndexPackedValuePtr {
		t.Fatalf("expected IndexPackedValuePtr=true for fast profile")
	}
	if opts.IndexInternalBaseDelta {
		t.Fatalf("expected IndexInternalBaseDelta=false for fast profile (incompatible with outer leaves in value log)")
	}
	if opts.PagerSyncConcurrency != 4 {
		t.Fatalf("expected PagerSyncConcurrency=4 for fast profile, got %d", opts.PagerSyncConcurrency)
	}
	if opts.ChunkSize != fastProfileChunkSize {
		t.Fatalf("expected ChunkSize=%d for fast profile, got %d", fastProfileChunkSize, opts.ChunkSize)
	}
	if opts.ResolvedProfile != ProfileNoWALFast || opts.DeprecatedProfileAlias != ProfileFast {
		t.Fatalf("profile resolution=(%q,%q), want (%q,%q)", opts.ResolvedProfile, opts.DeprecatedProfileAlias, ProfileNoWALFast, ProfileFast)
	}
}

func TestApplyProfile_WALOnFastKeepsWALOn(t *testing.T) {
	var opts Options
	ApplyProfile(&opts, ProfileWALOnFast)

	if opts.Durability != DurabilityWALOnRelaxed {
		t.Fatalf("expected DurabilityWALOnRelaxed for wal_on_fast profile, got %v", opts.Durability)
	}
	if opts.ValueLog.ReadIntegrity != IntegrityVerify {
		t.Fatalf("expected IntegrityVerify for wal_on_fast alias, got %v", opts.ValueLog.ReadIntegrity)
	}
	if !opts.IndexOuterLeavesInValueLog {
		t.Fatalf("expected IndexOuterLeavesInValueLog=true for wal_on_fast profile")
	}
	if opts.PreferAppendAlloc {
		t.Fatalf("expected PreferAppendAlloc=false for wal_on_fast profile")
	}
	if opts.ValueLog.Compression != ValueLogCompressionAuto {
		t.Fatalf("expected ValueLog.Compression=ValueLogCompressionAuto for wal_on_fast profile, got %v", opts.ValueLog.Compression)
	}
	if opts.ValueLog.BlockCodec != ValueLogBlockSnappy {
		t.Fatalf("expected ValueLog.BlockCodec=ValueLogBlockSnappy for wal_on_fast profile, got %v", opts.ValueLog.BlockCodec)
	}
	if opts.ValueLog.AutoPolicy != ValueLogAutoBalanced {
		t.Fatalf("expected ValueLog.AutoPolicy=ValueLogAutoBalanced for wal_on_fast profile, got %v", opts.ValueLog.AutoPolicy)
	}
	if opts.ValueLog.CompressionAutotune.Mode != AutotuneMedium {
		t.Fatalf("expected ValueLog.CompressionAutotune.Mode=AutotuneMedium for wal_on_fast profile, got %v", opts.ValueLog.CompressionAutotune.Mode)
	}
	if opts.ValueLog.DictIncompressibleHoldBytes != 64<<20 {
		t.Fatalf("expected DictIncompressibleHoldBytes=64MiB for wal_on_fast profile, got %d", opts.ValueLog.DictIncompressibleHoldBytes)
	}
	if opts.ValueLog.DictProbeIntervalBytes != 32<<20 {
		t.Fatalf("expected DictProbeIntervalBytes=32MiB for wal_on_fast profile, got %d", opts.ValueLog.DictProbeIntervalBytes)
	}
	if opts.ValueLog.ForcePointers {
		t.Fatalf("expected ValueLog.ForcePointers=false for wal_on_fast profile")
	}
	if opts.ValueLog.CurrentWritableMmap {
		t.Fatalf("expected ValueLog.CurrentWritableMmap=false for command_wal_relaxed")
	}
	if !opts.LeafPrefixCompression {
		t.Fatalf("expected LeafPrefixCompression=true for wal_on_fast profile")
	}
	if !opts.IndexColumnarLeaves {
		t.Fatalf("expected IndexColumnarLeaves=true for wal_on_fast profile")
	}
	if !opts.IndexPackedValuePtr {
		t.Fatalf("expected IndexPackedValuePtr=true for wal_on_fast profile")
	}
	if opts.IndexInternalBaseDelta {
		t.Fatalf("expected IndexInternalBaseDelta=false for wal_on_fast profile (incompatible with outer leaves in value log)")
	}
	if opts.PagerSyncConcurrency != 4 {
		t.Fatalf("expected PagerSyncConcurrency=4 for wal_on_fast profile, got %d", opts.PagerSyncConcurrency)
	}
	if opts.ChunkSize != fastProfileChunkSize {
		t.Fatalf("expected ChunkSize=%d for wal_on_fast profile, got %d", fastProfileChunkSize, opts.ChunkSize)
	}
	if !opts.CommandWAL {
		t.Fatal("wal_on_fast alias did not resolve to command WAL")
	}
	if opts.ResolvedProfile != ProfileCommandWALRelaxed || opts.DeprecatedProfileAlias != ProfileWALOnFast {
		t.Fatalf("profile resolution=(%q,%q), want (%q,%q)", opts.ResolvedProfile, opts.DeprecatedProfileAlias, ProfileCommandWALRelaxed, ProfileWALOnFast)
	}
}

func TestParseProfile_AcceptsOnlyCanonicalNames(t *testing.T) {
	tests := []struct {
		raw      string
		fallback Profile
		want     Profile
		wantOK   bool
	}{
		{raw: "", fallback: ProfileCommandWALDurable, want: ProfileCommandWALDurable, wantOK: true},
		{raw: " command_wal_durable ", want: ProfileCommandWALDurable, wantOK: true},
		{raw: "command_wal_relaxed", want: ProfileCommandWALRelaxed, wantOK: true},
		{raw: "no_wal_fast", want: ProfileNoWALFast, wantOK: true},
		{raw: "bench_unsafe", want: ProfileBenchUnsafe, wantOK: true},
		{raw: "COMMAND_WAL_DURABLE", wantOK: false},
		{raw: "command-wal-durable", wantOK: false},
		{raw: "durable", wantOK: false},
		{raw: "wal_on_fast", wantOK: false},
		{raw: "fast", wantOK: false},
		{raw: "bench", wantOK: false},
		{raw: "raw", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, ok := ParseProfile(tt.raw, tt.fallback)
			if ok != tt.wantOK {
				t.Fatalf("ok=%v want %v", ok, tt.wantOK)
			}
			if got != tt.want {
				t.Fatalf("profile=%q want %q", got, tt.want)
			}
		})
	}
}

func TestParsePublicProfile_AllowedNamesAndAliases(t *testing.T) {
	tests := []struct {
		raw      string
		fallback Profile
		want     Profile
	}{
		{raw: "", fallback: ProfileCommandWALDurable, want: ProfileCommandWALDurable},
		{raw: "", fallback: ProfileCommandWALRelaxed, want: ProfileCommandWALRelaxed},
		{raw: "", fallback: ProfileNoWALFast, want: ProfileNoWALFast},
		{raw: " command_wal_durable ", want: ProfileCommandWALDurable},
		{raw: "command_wal_relaxed", want: ProfileCommandWALRelaxed},
		{raw: "no_wal_fast", want: ProfileNoWALFast},
	}
	for _, tt := range tests {
		t.Run(tt.raw+"/"+string(tt.fallback), func(t *testing.T) {
			got, ok := ParsePublicProfile(tt.raw, tt.fallback)
			if !ok {
				t.Fatalf("ParsePublicProfile(%q, %q) rejected, want %q", tt.raw, tt.fallback, tt.want)
			}
			if got != tt.want {
				t.Fatalf("profile=%q want %q", got, tt.want)
			}
		})
	}
}

func TestParsePublicProfile_RejectsDeprecatedAndUnknownNames(t *testing.T) {
	rejected := []string{
		"fast",
		"wal_on_fast",
		"walonfast",
		"durable",
		"legacy_wal_durable",
		"legacy_wal_relaxed_fast",
		"bench",
		"bench_unsafe",
		"command-wal-durable",
		"COMMAND_WAL_DURABLE",
		"raw",
	}
	for _, raw := range rejected {
		t.Run(raw, func(t *testing.T) {
			if got, ok := ParsePublicProfile(raw, ProfileCommandWALRelaxed); ok {
				t.Fatalf("ParsePublicProfile(%q) = %q, want rejection", raw, got)
			}
		})
	}
	if got, ok := ParsePublicProfile("", ProfileFast); ok {
		t.Fatalf("ParsePublicProfile empty fallback ProfileFast = %q, want rejection", got)
	}
}

func TestParseBenchmarkProfile_RequiresCanonicalBenchUnsafe(t *testing.T) {
	for _, raw := range []string{"command_wal_durable", "command_wal_relaxed", "no_wal_fast", "bench_unsafe"} {
		if got, ok := ParseBenchmarkProfile(raw, ProfileCommandWALDurable); !ok || string(got) != raw {
			t.Fatalf("ParseBenchmarkProfile(%q)=(%q,%t)", raw, got, ok)
		}
	}
	for _, raw := range []string{"bench", "fast", "durable", "wal_on_fast"} {
		if got, ok := ParseBenchmarkProfile(raw, ProfileBenchUnsafe); ok {
			t.Fatalf("ParseBenchmarkProfile(%q)=%q, want rejection", raw, got)
		}
	}
}

func TestApplyProfile_CommandWALDurableSetsCommandWALAndFastDurablePolicy(t *testing.T) {
	var opts Options
	ApplyProfile(&opts, ProfileCommandWALDurable)

	if !opts.CommandWAL {
		t.Fatal("expected CommandWAL=true for command_wal_durable profile")
	}
	if opts.Durability != DurabilityDurable {
		t.Fatalf("expected DurabilityDurable for command_wal_durable profile, got %v", opts.Durability)
	}
	if opts.ValueLog.ReadIntegrity != IntegrityVerify {
		t.Fatalf("expected IntegrityVerify for command_wal_durable profile, got %v", opts.ValueLog.ReadIntegrity)
	}
	if !opts.IndexOuterLeavesInValueLog || !opts.LeafPrefixCompression || !opts.IndexColumnarLeaves || !opts.IndexPackedValuePtr {
		t.Fatalf("expected command_wal_durable to keep the fast collection/index layout bundle: %+v", opts)
	}
	if opts.ValueLog.CurrentWritableMmap {
		t.Fatalf("expected command_wal_durable to disable current writable mmap for production memory bounds")
	}
	if opts.IndexInternalBaseDelta {
		t.Fatalf("expected IndexInternalBaseDelta=false for command_wal_durable profile")
	}
	if opts.WALMaxSegmentBytes != commandWALProfileSegmentBytes {
		t.Fatalf("WALMaxSegmentBytes=%d want %d", opts.WALMaxSegmentBytes, commandWALProfileSegmentBytes)
	}
	if opts.CommandWALSegmentTargetBytes != commandWALProfileSegmentBytes {
		t.Fatalf("CommandWALSegmentTargetBytes=%d want %d", opts.CommandWALSegmentTargetBytes, commandWALProfileSegmentBytes)
	}
}

func TestApplyProfile_CommandWALRelaxedSetsCommandWALAndRelaxedPolicy(t *testing.T) {
	var opts Options
	ApplyProfile(&opts, ProfileCommandWALRelaxed)

	if !opts.CommandWAL {
		t.Fatal("expected CommandWAL=true for command_wal_relaxed profile")
	}
	if opts.Durability != DurabilityWALOnRelaxed {
		t.Fatalf("expected DurabilityWALOnRelaxed for command_wal_relaxed profile, got %v", opts.Durability)
	}
	if opts.ValueLog.ReadIntegrity != IntegrityVerify {
		t.Fatalf("expected IntegrityVerify for command_wal_relaxed profile, got %v", opts.ValueLog.ReadIntegrity)
	}
	if !opts.IndexOuterLeavesInValueLog || !opts.LeafPrefixCompression || !opts.IndexColumnarLeaves || !opts.IndexPackedValuePtr {
		t.Fatalf("expected command_wal_relaxed to keep the fast collection/index layout bundle: %+v", opts)
	}
	if opts.ValueLog.CurrentWritableMmap {
		t.Fatalf("expected command_wal_relaxed to disable current writable mmap for production memory bounds")
	}
	if opts.IndexInternalBaseDelta {
		t.Fatalf("expected IndexInternalBaseDelta=false for command_wal_relaxed profile")
	}
	if opts.WALMaxSegmentBytes != commandWALProfileSegmentBytes {
		t.Fatalf("WALMaxSegmentBytes=%d want %d", opts.WALMaxSegmentBytes, commandWALProfileSegmentBytes)
	}
	if opts.CommandWALSegmentTargetBytes != commandWALProfileSegmentBytes {
		t.Fatalf("CommandWALSegmentTargetBytes=%d want %d", opts.CommandWALSegmentTargetBytes, commandWALProfileSegmentBytes)
	}
}

func TestApplyProfile_CommandWALPreservesExplicitSegmentOverrides(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			opts := Options{
				WALMaxSegmentBytes:           1024,
				CommandWALSegmentTargetBytes: 2048,
			}
			ApplyProfile(&opts, profile)

			if opts.WALMaxSegmentBytes != 1024 {
				t.Fatalf("WALMaxSegmentBytes=%d want explicit 1024", opts.WALMaxSegmentBytes)
			}
			if opts.CommandWALSegmentTargetBytes != 2048 {
				t.Fatalf("CommandWALSegmentTargetBytes=%d want explicit 2048", opts.CommandWALSegmentTargetBytes)
			}
		})
	}
}

func TestApplyProfile_DeprecatedAliasesResolveToFrozenMappings(t *testing.T) {
	tests := []struct {
		profile        Profile
		wantResolved   Profile
		wantDurability DurabilityMode
		wantCommandWAL bool
	}{
		{profile: ProfileDurable, wantResolved: ProfileCommandWALDurable, wantDurability: DurabilityDurable, wantCommandWAL: true},
		{profile: ProfileLegacyWALDurable, wantResolved: ProfileCommandWALDurable, wantDurability: DurabilityDurable, wantCommandWAL: true},
		{profile: ProfileWALOnFast, wantResolved: ProfileCommandWALRelaxed, wantDurability: DurabilityWALOnRelaxed, wantCommandWAL: true},
		{profile: ProfileLegacyWALRelaxedFast, wantResolved: ProfileCommandWALRelaxed, wantDurability: DurabilityWALOnRelaxed, wantCommandWAL: true},
		{profile: ProfileFast, wantResolved: ProfileNoWALFast, wantDurability: DurabilityWALOffRelaxed},
	}
	for _, tt := range tests {
		t.Run(string(tt.profile), func(t *testing.T) {
			var opts Options
			ApplyProfile(&opts, tt.profile)
			if opts.CommandWAL != tt.wantCommandWAL {
				t.Fatalf("CommandWAL=%t want %t for alias %q", opts.CommandWAL, tt.wantCommandWAL, tt.profile)
			}
			if opts.Durability != tt.wantDurability {
				t.Fatalf("Durability=%v want %v", opts.Durability, tt.wantDurability)
			}
			if opts.ResolvedProfile != tt.wantResolved || opts.DeprecatedProfileAlias != tt.profile {
				t.Fatalf("profile resolution=(%q,%q), want (%q,%q)", opts.ResolvedProfile, opts.DeprecatedProfileAlias, tt.wantResolved, tt.profile)
			}
		})
	}
}

func TestApplyProfile_BenchDisablesBackgroundDefaults(t *testing.T) {
	var opts Options
	ApplyBenchmarkProfile(&opts, ProfileBench)

	if opts.BackgroundCheckpointInterval >= 0 {
		t.Fatalf("expected BackgroundCheckpointInterval < 0 for bench profile, got %v", opts.BackgroundCheckpointInterval)
	}
	if opts.BackgroundCheckpointIdleDuration >= 0 {
		t.Fatalf("expected BackgroundCheckpointIdleDuration < 0 for bench profile, got %v", opts.BackgroundCheckpointIdleDuration)
	}
	if opts.MaxWALBytes >= 0 {
		t.Fatalf("expected MaxWALBytes < 0 for bench profile, got %d", opts.MaxWALBytes)
	}
	if !opts.DisableBackgroundPrune {
		t.Fatalf("expected DisableBackgroundPrune=true for bench profile")
	}
	if opts.ValueLog.ForcePointers {
		t.Fatalf("expected ValueLog.ForcePointers=false for bench profile")
	}
	if !opts.ValueLog.CurrentWritableMmap {
		t.Fatalf("expected ValueLog.CurrentWritableMmap=true for bench profile")
	}
	if !opts.LeafPrefixCompression {
		t.Fatalf("expected LeafPrefixCompression=true for bench profile")
	}
	if !opts.IndexColumnarLeaves {
		t.Fatalf("expected IndexColumnarLeaves=true for bench profile")
	}
	if !opts.IndexPackedValuePtr {
		t.Fatalf("expected IndexPackedValuePtr=true for bench profile")
	}
	if opts.IndexInternalBaseDelta {
		t.Fatalf("expected IndexInternalBaseDelta=false for bench profile (incompatible with outer leaves in value log)")
	}
	if opts.ChunkSize != fastProfileChunkSize {
		t.Fatalf("expected ChunkSize=%d for bench profile, got %d", fastProfileChunkSize, opts.ChunkSize)
	}
	if opts.ResolvedProfile != ProfileBenchUnsafe || opts.DeprecatedProfileAlias != ProfileBench || !opts.UnsafeBenchmarkProfile {
		t.Fatalf("benchmark profile resolution=(%q,%q,%t)", opts.ResolvedProfile, opts.DeprecatedProfileAlias, opts.UnsafeBenchmarkProfile)
	}
}

func TestApplyProfile_DurableAliasUsesCanonicalCommandWALBundle(t *testing.T) {
	var opts Options
	ApplyProfile(&opts, ProfileDurable)
	if !opts.IndexOuterLeavesInValueLog {
		t.Fatalf("expected IndexOuterLeavesInValueLog=true for durable profile")
	}
	if opts.ValueLog.ForcePointers {
		t.Fatalf("expected ValueLog.ForcePointers=false for durable profile")
	}
	if !opts.LeafPrefixCompression {
		t.Fatalf("expected LeafPrefixCompression=true for durable alias")
	}
	if !opts.IndexColumnarLeaves {
		t.Fatalf("expected IndexColumnarLeaves=true for durable alias")
	}
	if !opts.IndexPackedValuePtr {
		t.Fatalf("expected IndexPackedValuePtr=true for durable alias")
	}
	if opts.IndexInternalBaseDelta {
		t.Fatalf("expected IndexInternalBaseDelta=false for durable profile")
	}
	if opts.ValueLog.CurrentWritableMmap {
		t.Fatalf("expected ValueLog.CurrentWritableMmap=false for durable profile")
	}
	if !opts.CommandWAL || opts.ResolvedProfile != ProfileCommandWALDurable {
		t.Fatalf("durable alias did not resolve to canonical command WAL: %+v", opts)
	}
}

func TestApplyProfile_DoesNotOverrideNonZeroNumericFields(t *testing.T) {
	opts := Options{
		BackgroundCheckpointInterval:     7 * time.Second,
		BackgroundCheckpointIdleDuration: 3 * time.Second,
		MaxWALBytes:                      123,
		ChunkSize:                        2 << 20,
		PagerSyncConcurrency:             2,
	}
	ApplyBenchmarkProfile(&opts, ProfileBench)

	if opts.BackgroundCheckpointInterval != 7*time.Second {
		t.Fatalf("BackgroundCheckpointInterval overridden: got %v", opts.BackgroundCheckpointInterval)
	}
	if opts.BackgroundCheckpointIdleDuration != 3*time.Second {
		t.Fatalf("BackgroundCheckpointIdleDuration overridden: got %v", opts.BackgroundCheckpointIdleDuration)
	}
	if opts.MaxWALBytes != 123 {
		t.Fatalf("MaxWALBytes overridden: got %d", opts.MaxWALBytes)
	}
	if opts.ChunkSize != 2<<20 {
		t.Fatalf("ChunkSize overridden: got %d", opts.ChunkSize)
	}
	if opts.PagerSyncConcurrency != 2 {
		t.Fatalf("PagerSyncConcurrency overridden: got %d", opts.PagerSyncConcurrency)
	}
}

func TestApplyProfile_PreservesNegativeDictHoldProbeValues(t *testing.T) {
	for _, profile := range []Profile{ProfileFast, ProfileNoWALFast, ProfileWALOnFast, ProfileLegacyWALRelaxedFast, ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			opts := Options{
				ValueLog: ValueLogOptions{
					DictIncompressibleHoldBytes: -1,
					DictProbeIntervalBytes:      -1,
				},
			}
			ApplyProfile(&opts, profile)

			if opts.ValueLog.DictIncompressibleHoldBytes != -1 {
				t.Fatalf("DictIncompressibleHoldBytes overridden: got %d", opts.ValueLog.DictIncompressibleHoldBytes)
			}
			if opts.ValueLog.DictProbeIntervalBytes != -1 {
				t.Fatalf("DictProbeIntervalBytes overridden: got %d", opts.ValueLog.DictProbeIntervalBytes)
			}
		})
	}
}

func TestApplyProfile_PreservesExplicitVLogCompressionOverrides(t *testing.T) {
	for _, profile := range []Profile{ProfileFast, ProfileNoWALFast, ProfileWALOnFast, ProfileLegacyWALRelaxedFast, ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			opts := Options{
				ValueLog: ValueLogOptions{
					Compression: ValueLogCompressionBlock,
					BlockCodec:  ValueLogBlockLZ4,
					AutoPolicy:  ValueLogAutoSize,
					CompressionAutotune: AutotuneOptions{
						Mode: AutotuneAggressive,
					},
				},
			}
			ApplyProfile(&opts, profile)

			if opts.ValueLog.Compression != ValueLogCompressionBlock {
				t.Fatalf("ValueLog.Compression overridden: got %v", opts.ValueLog.Compression)
			}
			if opts.ValueLog.BlockCodec != ValueLogBlockLZ4 {
				t.Fatalf("ValueLog.BlockCodec overridden: got %v", opts.ValueLog.BlockCodec)
			}
			if opts.ValueLog.AutoPolicy != ValueLogAutoSize {
				t.Fatalf("ValueLog.AutoPolicy overridden: got %v", opts.ValueLog.AutoPolicy)
			}
			if opts.ValueLog.CompressionAutotune.Mode != AutotuneAggressive {
				t.Fatalf("ValueLog.CompressionAutotune overridden: got %v", opts.ValueLog.CompressionAutotune.Mode)
			}
		})
	}
}
