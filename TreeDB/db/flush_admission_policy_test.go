package db

import (
	"runtime"
	"strings"
	"testing"
)

func TestNormalizeFlushAdmission_DefaultExplicitUnconfiguredStaysOff(t *testing.T) {
	opts := Options{}
	decision := NormalizeFlushAdmissionOptions(&opts)

	if decision.Policy != FlushAdmissionPolicyExplicit {
		t.Fatalf("policy=%s want explicit", decision.Policy)
	}
	if decision.Admitted {
		t.Fatalf("admitted=true want false")
	}
	if decision.Reason != FlushAdmissionReasonNoExplicitOptIn {
		t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonNoExplicitOptIn)
	}
	if opts.FlushApplySpanNative || opts.FlushBacklogCoalescing || opts.FlushApplyConcurrency != 0 {
		t.Fatalf("default explicit mutated opts: span=%t backlog=%t concurrency=%d", opts.FlushApplySpanNative, opts.FlushBacklogCoalescing, opts.FlushApplyConcurrency)
	}
}

func TestNormalizeFlushAdmission_OffForcesCandidateOff(t *testing.T) {
	opts := Options{
		FlushAdmissionPolicy:   FlushAdmissionPolicyOff,
		FlushApplyConcurrency:  4,
		FlushApplySpanNative:   true,
		FlushBacklogCoalescing: true,
		FlushApplyMinEntries:   1,
		FlushApplyMinSpans:     1,
		FlushApplyMinBytes:     1,
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

func TestNormalizeFlushAdmission_AutoDeclinesLowConcurrencyAndCheckpointDebt(t *testing.T) {
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
	for _, want := range []string{FlushAdmissionReasonLowConcurrency, FlushAdmissionReasonCheckpointDebt} {
		if !strings.Contains(decision.Reason, want) {
			t.Fatalf("reason=%q missing %q", decision.Reason, want)
		}
	}
	if opts.FlushApplyConcurrency != 0 || opts.FlushApplySpanNative || opts.FlushBacklogCoalescing {
		t.Fatalf("auto decline did not force off: concurrency=%d span=%t backlog=%t", opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
}

func TestNormalizeFlushAdmission_AutoDeclinesUnresolvedCheckpointDebtAtC4(t *testing.T) {
	oldGOMAXPROCS := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(oldGOMAXPROCS)

	opts := Options{
		FlushAdmissionPolicy:  FlushAdmissionPolicyAuto,
		FlushApplyConcurrency: 4,
	}
	decision := NormalizeFlushAdmissionOptions(&opts)

	if decision.Admitted {
		t.Fatalf("admitted=true want false")
	}
	if decision.Reason != FlushAdmissionReasonCheckpointDebt {
		t.Fatalf("reason=%q want %q", decision.Reason, FlushAdmissionReasonCheckpointDebt)
	}
	if opts.FlushApplyConcurrency != 0 || opts.FlushApplySpanNative || opts.FlushBacklogCoalescing {
		t.Fatalf("auto checkpoint decline did not force off: concurrency=%d span=%t backlog=%t", opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
}

func TestFlushAdmissionStatsExposePolicyAndReason(t *testing.T) {
	opts := Options{FlushAdmissionPolicy: FlushAdmissionPolicyAuto, FlushApplyConcurrency: 1, FlushApplySpanNative: true}
	decision := NormalizeFlushAdmissionOptions(&opts)
	db := &DB{flushAdmission: decision}
	stats := map[string]string{}
	db.appendFlushApplyStats(stats)

	if got := stats["treedb.flush_admission.policy"]; got != "auto" {
		t.Fatalf("stats policy=%q want auto", got)
	}
	if got := stats["treedb.flush_admission.admitted"]; got != "false" {
		t.Fatalf("stats admitted=%q want false", got)
	}
	if got := stats["treedb.flush_admission.reason"]; !strings.Contains(got, FlushAdmissionReasonLowConcurrency) || !strings.Contains(got, FlushAdmissionReasonCheckpointDebt) {
		t.Fatalf("stats reason=%q missing low concurrency/checkpoint debt", got)
	}
	if got := stats["treedb.flush_admission.flush_apply_span_native"]; got != "false" {
		t.Fatalf("stats span native=%q want false", got)
	}
}

func TestParseFlushAdmissionPolicy(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want FlushAdmissionPolicy
	}{
		{raw: "", want: FlushAdmissionPolicyExplicit},
		{raw: "default", want: FlushAdmissionPolicyExplicit},
		{raw: "explicit", want: FlushAdmissionPolicyExplicit},
		{raw: "off", want: FlushAdmissionPolicyOff},
		{raw: "auto", want: FlushAdmissionPolicyAuto},
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
