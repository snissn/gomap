package db

import (
	"runtime"
	"strings"
	"testing"
)

func TestNormalizeFlushAdmission_DefaultAutoAdmitsC8CappedAdaptive(t *testing.T) {
	oldGOMAXPROCS := runtime.GOMAXPROCS(12)
	defer runtime.GOMAXPROCS(oldGOMAXPROCS)

	opts := Options{}
	decision := NormalizeFlushAdmissionOptions(&opts)

	if decision.Policy != FlushAdmissionPolicyAuto {
		t.Fatalf("policy=%s want auto", decision.Policy)
	}
	if !decision.Admitted {
		t.Fatalf("admitted=false want true; reason=%q", decision.Reason)
	}
	if decision.Reason != FlushAdmissionReasonAutoAdmittedCappedAdapt {
		t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonAutoAdmittedCappedAdapt)
	}
	if opts.FlushApplyConcurrency != 8 || !opts.FlushApplySpanNative || !opts.FlushBacklogCoalescing {
		t.Fatalf("default auto did not select c8-capped span/backlog: concurrency=%d span=%t backlog=%t", opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
	if opts.FlushApplyMinEntries != 1 || opts.FlushApplyMinSpans != 1 || opts.FlushApplyMinBytes != 1 {
		t.Fatalf("default auto did not select measured min gates: entries=%d spans=%d bytes=%d", opts.FlushApplyMinEntries, opts.FlushApplyMinSpans, opts.FlushApplyMinBytes)
	}
	if opts.LeafPageReadCacheWriteAdmission != LeafPageReadCacheWriteAdmissionAdaptive {
		t.Fatalf("default auto cache admission=%s want adaptive", opts.LeafPageReadCacheWriteAdmission)
	}
	if decision.FlushApplyConcurrency != 8 || !decision.FlushApplySpanNative || !decision.FlushBacklogCoalescing || decision.LeafPageReadCacheWriteAdmission != LeafPageReadCacheWriteAdmissionAdaptive {
		t.Fatalf("decision did not report c8-capped adaptive candidate: %+v", decision)
	}
}

func TestNormalizeFlushAdmission_DefaultAutoCapsToGOMAXPROCS(t *testing.T) {
	oldGOMAXPROCS := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(oldGOMAXPROCS)

	opts := Options{}
	decision := NormalizeFlushAdmissionOptions(&opts)

	if !decision.Admitted {
		t.Fatalf("admitted=false want true; reason=%q", decision.Reason)
	}
	if opts.FlushApplyConcurrency != 4 || decision.FlushApplyConcurrency != 4 {
		t.Fatalf("default auto should cap to GOMAXPROCS: opts=%d decision=%d", opts.FlushApplyConcurrency, decision.FlushApplyConcurrency)
	}
	if decision.Reason != FlushAdmissionReasonAutoAdmittedCappedAdapt {
		t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonAutoAdmittedCappedAdapt)
	}
}

func TestNormalizeFlushAdmission_DefaultAutoDeclinesLowConcurrency(t *testing.T) {
	oldGOMAXPROCS := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldGOMAXPROCS)

	opts := Options{}
	decision := NormalizeFlushAdmissionOptions(&opts)

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
	decision := NormalizeFlushAdmissionOptions(&opts)

	if decision.Admitted {
		t.Fatalf("admitted=true want false")
	}
	if decision.Reason != FlushAdmissionReasonPolicyOff {
		t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonPolicyOff)
	}
	if opts.FlushApplyConcurrency != 0 || opts.FlushApplySpanNative || opts.FlushBacklogCoalescing {
		t.Fatalf("off policy did not force off: concurrency=%d span=%t backlog=%t", opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
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
	decision := NormalizeFlushAdmissionOptions(&opts)

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
}

func TestNormalizeFlushAdmission_ExplicitOptInKeepsC4Compatibility(t *testing.T) {
	oldGOMAXPROCS := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(oldGOMAXPROCS)

	opts := Options{
		FlushAdmissionPolicy:   FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency:  4,
		FlushApplySpanNative:   true,
		FlushBacklogCoalescing: true,
	}
	decision := NormalizeFlushAdmissionOptions(&opts)

	if !decision.Admitted {
		t.Fatalf("admitted=false want true")
	}
	if decision.Reason != FlushAdmissionReasonExplicitOptIn {
		t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonExplicitOptIn)
	}
	if opts.FlushApplyConcurrency != 4 || !opts.FlushApplySpanNative || !opts.FlushBacklogCoalescing {
		t.Fatalf("explicit c4 mutated opts: concurrency=%d span=%t backlog=%t", opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
	if decision.FlushApplyConcurrency <= 1 {
		t.Fatalf("effective concurrency=%d want >1 for c4", decision.FlushApplyConcurrency)
	}
}

func TestNormalizeFlushAdmission_AutoDeclinesConfiguredC1RegressionShape(t *testing.T) {
	oldGOMAXPROCS := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(oldGOMAXPROCS)

	opts := Options{
		FlushAdmissionPolicy:   FlushAdmissionPolicyAuto,
		FlushApplyConcurrency:  1,
		FlushApplySpanNative:   true,
		FlushBacklogCoalescing: true,
	}
	decision := NormalizeFlushAdmissionOptions(&opts)

	if decision.Admitted {
		t.Fatalf("admitted=true want false")
	}
	if decision.Reason != FlushAdmissionReasonLowConcurrency {
		t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonLowConcurrency)
	}
	if opts.FlushApplyConcurrency != 0 || opts.FlushApplySpanNative || opts.FlushBacklogCoalescing {
		t.Fatalf("auto c1 decline did not force off: concurrency=%d span=%t backlog=%t", opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
}

func TestNormalizeFlushAdmission_AutoCapsConfiguredConcurrencyToC8Candidate(t *testing.T) {
	oldGOMAXPROCS := runtime.GOMAXPROCS(16)
	defer runtime.GOMAXPROCS(oldGOMAXPROCS)

	opts := Options{
		FlushAdmissionPolicy:  FlushAdmissionPolicyAuto,
		FlushApplyConcurrency: 16,
	}
	decision := NormalizeFlushAdmissionOptions(&opts)

	if !decision.Admitted {
		t.Fatalf("admitted=false want true; reason=%q", decision.Reason)
	}
	if opts.FlushApplyConcurrency != 8 || decision.FlushApplyConcurrency != 8 {
		t.Fatalf("auto should cap to c8 candidate: opts=%d decision=%d", opts.FlushApplyConcurrency, decision.FlushApplyConcurrency)
	}
	if decision.Reason != FlushAdmissionReasonAutoAdmittedCappedAdapt {
		t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonAutoAdmittedCappedAdapt)
	}
}

func TestNormalizeFlushAdmission_AutoDeclinesWALOffUnsafeDurability(t *testing.T) {
	oldGOMAXPROCS := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(oldGOMAXPROCS)

	opts := Options{Durability: DurabilityWALOffRelaxed}
	decision := NormalizeFlushAdmissionOptions(&opts)

	if decision.Admitted {
		t.Fatalf("admitted=true want false")
	}
	if decision.Reason != FlushAdmissionReasonUnsafeDurability {
		t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonUnsafeDurability)
	}
	if opts.FlushApplyConcurrency != 0 || opts.FlushApplySpanNative || opts.FlushBacklogCoalescing {
		t.Fatalf("auto unsafe-durability decline did not force off: concurrency=%d span=%t backlog=%t", opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
}

func TestFlushAdmissionStatsExposePolicyReasonAndCandidate(t *testing.T) {
	oldGOMAXPROCS := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(oldGOMAXPROCS)

	opts := Options{}
	decision := NormalizeFlushAdmissionOptions(&opts)
	db := &DB{flushAdmission: decision}
	stats := map[string]string{}
	db.appendFlushApplyStats(stats)

	if got := stats["treedb.flush_admission.policy"]; got != "auto" {
		t.Fatalf("stats policy=%q want auto", got)
	}
	if got := stats["treedb.flush_admission.admitted"]; got != "true" {
		t.Fatalf("stats admitted=%q want true", got)
	}
	if got := stats["treedb.flush_admission.reason"]; got != FlushAdmissionReasonAutoAdmittedCappedAdapt {
		t.Fatalf("stats reason=%q want %q", got, FlushAdmissionReasonAutoAdmittedCappedAdapt)
	}
	if got := stats["treedb.flush_admission.flush_apply_concurrency"]; got != "4" {
		t.Fatalf("stats concurrency=%q want 4", got)
	}
	if got := stats["treedb.flush_admission.flush_apply_span_native"]; got != "true" {
		t.Fatalf("stats span native=%q want true", got)
	}
	if got := stats["treedb.flush_admission.leaf_page_read_cache_write_admission"]; got != "adaptive" {
		t.Fatalf("stats cache admission=%q want adaptive", got)
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
