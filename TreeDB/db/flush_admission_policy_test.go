package db

import (
	"runtime"
	"strings"
	"testing"
)

func TestNormalizeFlushAdmission_DefaultAutoHardwareAware(t *testing.T) {
	cases := []struct {
		name     string
		gomax    int
		physical int
		want     int
		wantCap  string
	}{
		{name: "two_physical_two_gomax_admits_c2", gomax: 2, physical: 2, want: 2, wantCap: FlushAdmissionConcurrencyCapDefaultPhysicalCores},
		{name: "four_physical_four_gomax_admits_c4", gomax: 4, physical: 4, want: 4, wantCap: FlushAdmissionConcurrencyCapDefaultPhysicalCores},
		{name: "eight_physical_eight_gomax_admits_c8", gomax: 8, physical: 8, want: 8, wantCap: FlushAdmissionConcurrencyCapDefaultAutoCap},
		{name: "six_physical_twelve_gomax", gomax: 12, physical: 6, want: 6, wantCap: FlushAdmissionConcurrencyCapDefaultPhysicalCores},
		{name: "six_physical_sixteen_gomax", gomax: 16, physical: 6, want: 6, wantCap: FlushAdmissionConcurrencyCapDefaultPhysicalCores},
		{name: "twelve_physical_caps_at_eight", gomax: 16, physical: 12, want: 8, wantCap: FlushAdmissionConcurrencyCapDefaultAutoCap},
		{name: "unknown_physical_falls_back_to_capped_gomax", gomax: 16, physical: 0, want: 8, wantCap: FlushAdmissionConcurrencyCapDefaultAutoCap},
		{name: "gomax_below_physical_caps", gomax: 4, physical: 12, want: 4, wantCap: FlushAdmissionConcurrencyCapDefaultGOMAXPROCS},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{}
			decision := computeFlushAdmissionDecisionForHardware(&opts, tc.gomax, tc.physical)

			if decision.Policy != FlushAdmissionPolicyAuto {
				t.Fatalf("policy=%s want auto", decision.Policy)
			}
			if !decision.Admitted {
				t.Fatalf("admitted=false want true; reason=%q", decision.Reason)
			}
			if decision.Reason != FlushAdmissionReasonAutoAdmittedHardwareAware {
				t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonAutoAdmittedHardwareAware)
			}
			if opts.FlushApplyConcurrency != tc.want || decision.FlushApplyConcurrency != tc.want {
				t.Fatalf("auto concurrency opts/decision=%d/%d want %d", opts.FlushApplyConcurrency, decision.FlushApplyConcurrency, tc.want)
			}
			if decision.FlushApplyConcurrencyConfigured != 0 {
				t.Fatalf("configured concurrency=%d want 0", decision.FlushApplyConcurrencyConfigured)
			}
			if decision.FlushApplyConcurrencyCapReason != tc.wantCap {
				t.Fatalf("cap reason=%q want %q", decision.FlushApplyConcurrencyCapReason, tc.wantCap)
			}
			if !decision.FlushApplyConcurrencyDefaulted {
				t.Fatalf("defaulted=false want true")
			}
			if decision.RuntimeGOMAXPROCS != tc.gomax || decision.PhysicalCores != tc.physical {
				t.Fatalf("hardware fields gomax/physical=%d/%d want %d/%d", decision.RuntimeGOMAXPROCS, decision.PhysicalCores, tc.gomax, tc.physical)
			}
			if !opts.FlushApplySpanNative || !opts.FlushBacklogCoalescing {
				t.Fatalf("default auto did not enable span/backlog: span=%t backlog=%t", opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
			}
			if opts.FlushApplyMinEntries != 1 || opts.FlushApplyMinSpans != 1 || opts.FlushApplyMinBytes != 1 {
				t.Fatalf("default auto did not select measured min gates: entries=%d spans=%d bytes=%d", opts.FlushApplyMinEntries, opts.FlushApplyMinSpans, opts.FlushApplyMinBytes)
			}
			if opts.LeafPageReadCacheWriteAdmission != LeafPageReadCacheWriteAdmissionAdaptive {
				t.Fatalf("default auto cache admission=%s want adaptive", opts.LeafPageReadCacheWriteAdmission)
			}
		})
	}
}

func TestNormalizeFlushAdmission_DefaultAutoDeclinesLowConcurrency(t *testing.T) {
	opts := Options{}
	decision := computeFlushAdmissionDecisionForHardware(&opts, 1, 6)

	if decision.Policy != FlushAdmissionPolicyAuto {
		t.Fatalf("policy=%s want auto", decision.Policy)
	}
	if decision.Admitted {
		t.Fatalf("admitted=true want false")
	}
	if decision.Reason != FlushAdmissionReasonLowConcurrency {
		t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonLowConcurrency)
	}
	if opts.FlushApplyConcurrency != 0 || opts.FlushApplySpanNative || opts.FlushBacklogCoalescing {
		t.Fatalf("auto low-concurrency decline did not force off: concurrency=%d span=%t backlog=%t", opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
	if decision.FlushApplyConcurrencyConfigured != 0 || decision.FlushApplyConcurrency != 0 || !decision.FlushApplyConcurrencyDefaulted || decision.FlushApplyConcurrencyCapReason != FlushAdmissionConcurrencyCapDisabled {
		t.Fatalf("concurrency configured/selected/defaulted/cap=%d/%d/%t/%q want 0/0/true/%q",
			decision.FlushApplyConcurrencyConfigured, decision.FlushApplyConcurrency, decision.FlushApplyConcurrencyDefaulted, decision.FlushApplyConcurrencyCapReason, FlushAdmissionConcurrencyCapDisabled)
	}
	if opts.LeafPageReadCacheWriteAdmission != LeafPageReadCacheWriteAdmissionImmediate {
		t.Fatalf("auto low-concurrency decline changed cache admission to %s", opts.LeafPageReadCacheWriteAdmission)
	}
}

func TestNormalizeFlushAdmission_OffForcesCandidateOff(t *testing.T) {
	opts := Options{
		FlushAdmissionPolicy:            FlushAdmissionPolicyOff,
		FlushApplyConcurrency:           4,
		FlushApplySpanNative:            true,
		FlushBacklogCoalescing:          true,
		FlushApplyMinEntries:            1,
		FlushApplyMinSpans:              1,
		FlushApplyMinBytes:              1,
		LeafPageReadCacheWriteAdmission: LeafPageReadCacheWriteAdmissionAdaptive,
	}
	decision := computeFlushAdmissionDecisionForHardware(&opts, 16, 6)

	if decision.Admitted {
		t.Fatalf("admitted=true want false")
	}
	if decision.Reason != FlushAdmissionReasonPolicyOff {
		t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonPolicyOff)
	}
	if opts.FlushApplyConcurrency != 0 || opts.FlushApplySpanNative || opts.FlushBacklogCoalescing {
		t.Fatalf("off policy did not force off: concurrency=%d span=%t backlog=%t", opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
	if decision.FlushApplyConcurrencyConfigured != 4 || decision.FlushApplyConcurrency != 0 || decision.FlushApplyConcurrencyDefaulted || decision.FlushApplyConcurrencyCapReason != FlushAdmissionConcurrencyCapDisabled {
		t.Fatalf("off configured/selected/defaulted/cap=%d/%d/%t/%q want 4/0/false/%q",
			decision.FlushApplyConcurrencyConfigured, decision.FlushApplyConcurrency, decision.FlushApplyConcurrencyDefaulted, decision.FlushApplyConcurrencyCapReason, FlushAdmissionConcurrencyCapDisabled)
	}
	if opts.LeafPageReadCacheWriteAdmission != LeafPageReadCacheWriteAdmissionAdaptive {
		t.Fatalf("off policy should not rewrite explicit cache admission: %s", opts.LeafPageReadCacheWriteAdmission)
	}
}

func TestNormalizeFlushAdmission_ExplicitOptInKeepsC1Compatibility(t *testing.T) {
	opts := Options{
		FlushAdmissionPolicy:   FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency:  1,
		FlushApplySpanNative:   true,
		FlushBacklogCoalescing: true,
	}
	decision := computeFlushAdmissionDecisionForHardware(&opts, 4, 4)

	if !decision.Admitted {
		t.Fatalf("admitted=false want true")
	}
	if decision.Reason != FlushAdmissionReasonExplicitOptIn {
		t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonExplicitOptIn)
	}
	if opts.FlushApplyConcurrency != 1 || !opts.FlushApplySpanNative || !opts.FlushBacklogCoalescing {
		t.Fatalf("explicit policy mutated opt-in opts: concurrency=%d span=%t backlog=%t", opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
	if decision.FlushApplyConcurrency != 0 {
		t.Fatalf("effective concurrency=%d want 0 for c1", decision.FlushApplyConcurrency)
	}
	if decision.FlushApplyConcurrencyConfigured != 1 || decision.FlushApplyConcurrencyCapReason != FlushAdmissionConcurrencyCapDisabled {
		t.Fatalf("configured/cap=%d/%q want 1/%q", decision.FlushApplyConcurrencyConfigured, decision.FlushApplyConcurrencyCapReason, FlushAdmissionConcurrencyCapDisabled)
	}
}

func TestNormalizeFlushAdmission_ExplicitOptInKeepsC16Compatibility(t *testing.T) {
	opts := Options{
		FlushAdmissionPolicy:   FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency:  16,
		FlushApplySpanNative:   true,
		FlushBacklogCoalescing: true,
	}
	decision := computeFlushAdmissionDecisionForHardware(&opts, 16, 6)

	if !decision.Admitted {
		t.Fatalf("admitted=false want true")
	}
	if decision.Reason != FlushAdmissionReasonExplicitOptIn {
		t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonExplicitOptIn)
	}
	if opts.FlushApplyConcurrency != 16 || !opts.FlushApplySpanNative || !opts.FlushBacklogCoalescing {
		t.Fatalf("explicit c16 mutated opts: concurrency=%d span=%t backlog=%t", opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
	if decision.FlushApplyConcurrency != 16 {
		t.Fatalf("effective concurrency=%d want 16", decision.FlushApplyConcurrency)
	}
	if decision.FlushApplyConcurrencyConfigured != 16 || decision.FlushApplyConcurrencyCapReason != FlushAdmissionConcurrencyCapConfigured {
		t.Fatalf("configured/cap=%d/%q want 16/%q", decision.FlushApplyConcurrencyConfigured, decision.FlushApplyConcurrencyCapReason, FlushAdmissionConcurrencyCapConfigured)
	}
}

func TestNormalizeFlushAdmission_ExplicitOptInReportsC16CappedByGOMAXPROCS(t *testing.T) {
	opts := Options{
		FlushAdmissionPolicy:   FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency:  16,
		FlushApplySpanNative:   true,
		FlushBacklogCoalescing: true,
	}
	decision := computeFlushAdmissionDecisionForHardware(&opts, 8, 32)

	if !decision.Admitted {
		t.Fatalf("admitted=false want true")
	}
	if decision.Reason != FlushAdmissionReasonExplicitOptIn {
		t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonExplicitOptIn)
	}
	if opts.FlushApplyConcurrency != 16 || !opts.FlushApplySpanNative || !opts.FlushBacklogCoalescing {
		t.Fatalf("explicit c16 mutated opts: concurrency=%d span=%t backlog=%t", opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
	if decision.FlushApplyConcurrency != 8 {
		t.Fatalf("effective concurrency=%d want 8", decision.FlushApplyConcurrency)
	}
	if decision.FlushApplyConcurrencyConfigured != 16 || decision.FlushApplyConcurrencyCapReason != FlushAdmissionConcurrencyCapConfiguredGOMAXPROCS {
		t.Fatalf("configured/cap=%d/%q want 16/%q", decision.FlushApplyConcurrencyConfigured, decision.FlushApplyConcurrencyCapReason, FlushAdmissionConcurrencyCapConfiguredGOMAXPROCS)
	}
}

func TestNormalizeFlushAdmission_AutoDeclinesConfiguredC1RegressionShape(t *testing.T) {
	opts := Options{
		FlushAdmissionPolicy:   FlushAdmissionPolicyAuto,
		FlushApplyConcurrency:  1,
		FlushApplySpanNative:   true,
		FlushBacklogCoalescing: true,
	}
	decision := computeFlushAdmissionDecisionForHardware(&opts, 4, 4)

	if decision.Admitted {
		t.Fatalf("admitted=true want false")
	}
	if decision.Reason != FlushAdmissionReasonLowConcurrency {
		t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonLowConcurrency)
	}
	if opts.FlushApplyConcurrency != 0 || opts.FlushApplySpanNative || opts.FlushBacklogCoalescing {
		t.Fatalf("auto c1 decline did not force off: concurrency=%d span=%t backlog=%t", opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
	if decision.FlushApplyConcurrencyConfigured != 1 || decision.FlushApplyConcurrency != 0 || decision.FlushApplyConcurrencyDefaulted || decision.FlushApplyConcurrencyCapReason != FlushAdmissionConcurrencyCapDisabled {
		t.Fatalf("auto c1 configured/selected/defaulted/cap=%d/%d/%t/%q want 1/0/false/%q",
			decision.FlushApplyConcurrencyConfigured, decision.FlushApplyConcurrency, decision.FlushApplyConcurrencyDefaulted, decision.FlushApplyConcurrencyCapReason, FlushAdmissionConcurrencyCapDisabled)
	}
}

func TestNormalizeFlushAdmission_AutoConfiguredConcurrencyIsAuthoritative(t *testing.T) {
	opts := Options{
		FlushAdmissionPolicy:  FlushAdmissionPolicyAuto,
		FlushApplyConcurrency: 16,
	}
	decision := computeFlushAdmissionDecisionForHardware(&opts, 16, 6)

	if !decision.Admitted {
		t.Fatalf("admitted=false want true; reason=%q", decision.Reason)
	}
	if opts.FlushApplyConcurrency != 16 || decision.FlushApplyConcurrency != 16 {
		t.Fatalf("auto configured concurrency opts/decision=%d/%d want 16", opts.FlushApplyConcurrency, decision.FlushApplyConcurrency)
	}
	if decision.FlushApplyConcurrencyDefaulted {
		t.Fatalf("defaulted=true want false for configured override")
	}
	if decision.FlushApplyConcurrencyConfigured != 16 || decision.FlushApplyConcurrencyCapReason != FlushAdmissionConcurrencyCapConfigured {
		t.Fatalf("configured/cap=%d/%q want 16/%q", decision.FlushApplyConcurrencyConfigured, decision.FlushApplyConcurrencyCapReason, FlushAdmissionConcurrencyCapConfigured)
	}
	if decision.Reason != FlushAdmissionReasonAutoAdmittedHardwareAware {
		t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonAutoAdmittedHardwareAware)
	}
}

func TestNormalizeFlushAdmission_AutoConfiguredC16CapsByGOMAXPROCS(t *testing.T) {
	opts := Options{
		FlushAdmissionPolicy:  FlushAdmissionPolicyAuto,
		FlushApplyConcurrency: 16,
	}
	decision := computeFlushAdmissionDecisionForHardware(&opts, 8, 32)

	if !decision.Admitted {
		t.Fatalf("admitted=false want true; reason=%q", decision.Reason)
	}
	if opts.FlushApplyConcurrency != 8 || decision.FlushApplyConcurrency != 8 {
		t.Fatalf("auto configured c16 opts/decision=%d/%d want 8", opts.FlushApplyConcurrency, decision.FlushApplyConcurrency)
	}
	if decision.FlushApplyConcurrencyConfigured != 16 {
		t.Fatalf("configured concurrency=%d want 16", decision.FlushApplyConcurrencyConfigured)
	}
	if decision.FlushApplyConcurrencyCapReason != FlushAdmissionConcurrencyCapConfiguredGOMAXPROCS {
		t.Fatalf("cap reason=%q want %q", decision.FlushApplyConcurrencyCapReason, FlushAdmissionConcurrencyCapConfiguredGOMAXPROCS)
	}
	if decision.FlushApplyConcurrencyDefaulted {
		t.Fatalf("defaulted=true want false for configured c16")
	}
	if decision.RuntimeGOMAXPROCS != 8 || decision.PhysicalCores != 32 {
		t.Fatalf("hardware fields gomax/physical=%d/%d want 8/32", decision.RuntimeGOMAXPROCS, decision.PhysicalCores)
	}
	if !opts.FlushApplySpanNative || !opts.FlushBacklogCoalescing {
		t.Fatalf("auto configured c16 did not enable span/backlog: span=%t backlog=%t", opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
}

func TestNormalizeFlushAdmission_AutoDeclinesWALOffUnsafeDurability(t *testing.T) {
	opts := Options{Durability: DurabilityWALOffRelaxed}
	decision := computeFlushAdmissionDecisionForHardware(&opts, 4, 4)

	if decision.Admitted {
		t.Fatalf("admitted=true want false")
	}
	if decision.Reason != FlushAdmissionReasonUnsafeDurability {
		t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonUnsafeDurability)
	}
	if opts.FlushApplyConcurrency != 0 || opts.FlushApplySpanNative || opts.FlushBacklogCoalescing {
		t.Fatalf("auto unsafe-durability decline did not force off: concurrency=%d span=%t backlog=%t", opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
	if decision.FlushApplyConcurrencyConfigured != 0 || decision.FlushApplyConcurrency != 0 || !decision.FlushApplyConcurrencyDefaulted || decision.FlushApplyConcurrencyCapReason != FlushAdmissionConcurrencyCapDisabled {
		t.Fatalf("unsafe configured/selected/defaulted/cap=%d/%d/%t/%q want 0/0/true/%q",
			decision.FlushApplyConcurrencyConfigured, decision.FlushApplyConcurrency, decision.FlushApplyConcurrencyDefaulted, decision.FlushApplyConcurrencyCapReason, FlushAdmissionConcurrencyCapDisabled)
	}
}

func TestNormalizeFlushAdmission_AutoAdmitsResolvedNoWALFastProfile(t *testing.T) {
	opts := Options{
		Durability:      DurabilityWALOffRelaxed,
		ResolvedProfile: ProfileNoWALFast,
	}
	decision := computeFlushAdmissionDecisionForHardware(&opts, 4, 4)

	if !decision.Admitted || decision.Reason != FlushAdmissionReasonAutoAdmittedHardwareAware {
		t.Fatalf("admitted/reason=%t/%q want true/%q", decision.Admitted, decision.Reason, FlushAdmissionReasonAutoAdmittedHardwareAware)
	}
	if !opts.FlushApplySpanNative || !opts.FlushBacklogCoalescing {
		t.Fatalf("resolved no_wal_fast did not enable span/backlog: span=%t backlog=%t", opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
}

func TestFlushAdmissionStatsExposePolicyReasonHardwareAndCandidate(t *testing.T) {
	opts := Options{}
	decision := computeFlushAdmissionDecisionForHardware(&opts, 16, 6)
	db := &DB{flushAdmission: decision}
	stats := map[string]string{}
	db.appendFlushApplyStats(stats)

	if got := stats["treedb.flush_admission.policy"]; got != "auto" {
		t.Fatalf("stats policy=%q want auto", got)
	}
	if got := stats["treedb.flush_admission.admitted"]; got != "true" {
		t.Fatalf("stats admitted=%q want true", got)
	}
	if got := stats["treedb.flush_admission.reason"]; got != FlushAdmissionReasonAutoAdmittedHardwareAware {
		t.Fatalf("stats reason=%q want %q", got, FlushAdmissionReasonAutoAdmittedHardwareAware)
	}
	if got := stats["treedb.flush_admission.flush_apply_concurrency_configured"]; got != "0" {
		t.Fatalf("stats configured concurrency=%q want 0", got)
	}
	if got := stats["treedb.flush_admission.flush_apply_concurrency"]; got != "6" {
		t.Fatalf("stats concurrency=%q want 6", got)
	}
	if got := stats["treedb.flush_admission.flush_apply_concurrency_cap_reason"]; got != FlushAdmissionConcurrencyCapDefaultPhysicalCores {
		t.Fatalf("stats cap reason=%q want %q", got, FlushAdmissionConcurrencyCapDefaultPhysicalCores)
	}
	if got := stats["treedb.flush_admission.flush_apply_concurrency_defaulted"]; got != "true" {
		t.Fatalf("stats concurrency defaulted=%q want true", got)
	}
	if got := stats["treedb.flush_admission.gomaxprocs"]; got != "16" {
		t.Fatalf("stats gomaxprocs=%q want 16", got)
	}
	if got := stats["treedb.flush_admission.physical_cores"]; got != "6" {
		t.Fatalf("stats physical_cores=%q want 6", got)
	}
	if got := stats["treedb.flush_admission.flush_apply_span_native"]; got != "true" {
		t.Fatalf("stats span native=%q want true", got)
	}
	if got := stats["treedb.flush_admission.flush_backlog_coalescing"]; got != "true" {
		t.Fatalf("stats backlog=%q want true", got)
	}
	if got := stats["treedb.flush_admission.leaf_page_read_cache_write_admission"]; got != "adaptive" {
		t.Fatalf("stats cache admission=%q want adaptive", got)
	}
}

func TestFlushAdmissionStatsPreserveConfiguredConcurrencyThroughDirectOpen(t *testing.T) {
	prev := runtime.GOMAXPROCS(8)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	db, err := Open(Options{
		Dir:                   t.TempDir(),
		FlushAdmissionPolicy:  FlushAdmissionPolicyAuto,
		FlushApplyConcurrency: 16,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	stats := db.Stats()
	if got := stats["treedb.flush_admission.policy"]; got != "auto" {
		t.Fatalf("stats policy=%q want auto", got)
	}
	if got := stats["treedb.flush_admission.admitted"]; got != "true" {
		t.Fatalf("stats admitted=%q want true", got)
	}
	if got := stats["treedb.flush_admission.flush_apply_concurrency_configured"]; got != "16" {
		t.Fatalf("stats configured concurrency=%q want 16", got)
	}
	if got := stats["treedb.flush_admission.flush_apply_concurrency"]; got != "8" {
		t.Fatalf("stats selected concurrency=%q want 8", got)
	}
	if got := stats["treedb.flush_admission.flush_apply_concurrency_cap_reason"]; got != FlushAdmissionConcurrencyCapConfiguredGOMAXPROCS {
		t.Fatalf("stats cap reason=%q want %q", got, FlushAdmissionConcurrencyCapConfiguredGOMAXPROCS)
	}
	if got := stats["treedb.flush_admission.flush_apply_concurrency_defaulted"]; got != "false" {
		t.Fatalf("stats defaulted=%q want false", got)
	}
	if got := stats["treedb.flush_admission.gomaxprocs"]; got != "8" {
		t.Fatalf("stats gomaxprocs=%q want 8", got)
	}
	if got := stats["treedb.flush_admission.physical_cores"]; got == "" {
		t.Fatal("missing physical core admission stat")
	}
}

func TestParseFlushAdmissionPolicy(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want FlushAdmissionPolicy
	}{
		{raw: "", want: FlushAdmissionPolicyAuto},
		{raw: "default", want: FlushAdmissionPolicyAuto},
		{raw: "auto", want: FlushAdmissionPolicyAuto},
		{raw: "explicit", want: FlushAdmissionPolicyExplicit},
		{raw: "off", want: FlushAdmissionPolicyOff},
	} {
		got, err := ParseFlushAdmissionPolicy(tc.raw)
		if err != nil {
			t.Fatalf("ParseFlushAdmissionPolicy(%q): %v", tc.raw, err)
		}
		if got != tc.want {
			t.Fatalf("ParseFlushAdmissionPolicy(%q)=%s want %s", tc.raw, got, tc.want)
		}
	}
	if _, err := ParseFlushAdmissionPolicy("bad"); err == nil {
		t.Fatal("ParseFlushAdmissionPolicy accepted bad policy")
	}
}

func TestNormalizeFlushAdmission_StatsDefaultsKeepExplicitNoOptIn(t *testing.T) {
	d := FlushAdmissionDecision{Policy: FlushAdmissionPolicyExplicit}.withStatsDefaults()
	if d.Admitted {
		t.Fatalf("admitted=true want false")
	}
	if !strings.Contains(d.Reason, FlushAdmissionReasonNoExplicitOptIn) {
		t.Fatalf("reason=%q missing %q", d.Reason, FlushAdmissionReasonNoExplicitOptIn)
	}
}
