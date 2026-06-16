package db

import (
	"fmt"
	"strings"
)

// FlushAdmissionPolicy controls how TreeDB admits the span-native/backlog
// flush/apply candidate path. The zero value preserves the pre-existing
// explicit-opt-in behavior: defaults remain off, and callers that set the
// existing FlushApply*/FlushBacklog* knobs keep their requested behavior.
type FlushAdmissionPolicy uint8

const (
	// FlushAdmissionPolicyExplicit preserves existing explicit knobs. This is the
	// default policy and does not infer or enable span-native/backlog behavior.
	FlushAdmissionPolicyExplicit FlushAdmissionPolicy = iota
	// FlushAdmissionPolicyOff force-disables span-native, backlog coalescing, and
	// flush-apply worker-pool concurrency as a rollback/fail-closed policy.
	FlushAdmissionPolicyOff
	// FlushAdmissionPolicyAuto is the future-default selector path. It currently
	// fails closed while checkpoint debt remains unresolved and when configured
	// concurrency is too low to avoid the known c1 regression shape.
	FlushAdmissionPolicyAuto
)

const (
	FlushAdmissionReasonNoExplicitOptIn = "no_explicit_opt_in"
	FlushAdmissionReasonExplicitOptIn   = "explicit_opt_in"
	FlushAdmissionReasonPolicyOff       = "policy_off"
	FlushAdmissionReasonAutoAdmitted    = "auto_admitted"
	FlushAdmissionReasonLowConcurrency  = "low_concurrency"
	FlushAdmissionReasonCheckpointDebt  = "checkpoint_debt_unresolved"
	FlushAdmissionReasonInvalidPolicy   = "invalid_policy"
)

// Keep the unresolved #2794/B1 checkpoint debt represented in code, not only in
// benchmark notes. A future checkpoint-debt mitigation can flip this seam (or
// replace it with a measured runtime signal) and the auto path will still keep
// the low-concurrency guardrail below.
const flushAdmissionCheckpointDebtResolved = false

// FlushAdmissionDecision is the normalized admission result reported through
// DB.Stats and benchmark option reports.
type FlushAdmissionDecision struct {
	Policy                 FlushAdmissionPolicy
	Admitted               bool
	Reason                 string
	FlushApplyConcurrency  int
	FlushApplySpanNative   bool
	FlushBacklogCoalescing bool
}

func (p FlushAdmissionPolicy) String() string {
	switch p {
	case FlushAdmissionPolicyExplicit:
		return "explicit"
	case FlushAdmissionPolicyOff:
		return "off"
	case FlushAdmissionPolicyAuto:
		return "auto"
	default:
		return fmt.Sprintf("unknown(%d)", p)
	}
}

func (p FlushAdmissionPolicy) Valid() bool {
	switch p {
	case FlushAdmissionPolicyExplicit, FlushAdmissionPolicyOff, FlushAdmissionPolicyAuto:
		return true
	default:
		return false
	}
}

// ParseFlushAdmissionPolicy parses off|explicit|auto policy values. Empty and
// "default" preserve the current explicit-opt-in default.
func ParseFlushAdmissionPolicy(raw string) (FlushAdmissionPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "default", "unset", "explicit":
		return FlushAdmissionPolicyExplicit, nil
	case "off", "false", "0", "disabled", "disable":
		return FlushAdmissionPolicyOff, nil
	case "auto", "adaptive":
		return FlushAdmissionPolicyAuto, nil
	default:
		return FlushAdmissionPolicyExplicit, fmt.Errorf("unsupported flush admission policy %q (expected off|explicit|auto)", raw)
	}
}

// NormalizeFlushAdmissionOptions applies the fail-closed admission seam to opts
// and stores the decision in the options for Open/Stats. It is idempotent so
// public wrappers can normalize before passing options to the backend without
// losing the original decline reason.
func NormalizeFlushAdmissionOptions(opts *Options) FlushAdmissionDecision {
	if opts == nil {
		return FlushAdmissionDecision{Policy: FlushAdmissionPolicyExplicit, Reason: FlushAdmissionReasonNoExplicitOptIn}
	}
	if opts.flushAdmissionNormalized {
		return opts.flushAdmissionDecision.withStatsDefaults()
	}

	decision := computeFlushAdmissionDecision(opts)
	opts.flushAdmissionDecision = decision
	opts.flushAdmissionNormalized = true
	return decision.withStatsDefaults()
}

// FlushAdmissionDecisionForOptions returns the decision already stored in opts,
// or computes the decision for a copy without mutating the caller's options.
func FlushAdmissionDecisionForOptions(opts Options) FlushAdmissionDecision {
	if opts.flushAdmissionNormalized {
		return opts.flushAdmissionDecision.withStatsDefaults()
	}
	return NormalizeFlushAdmissionOptions(&opts)
}

func computeFlushAdmissionDecision(opts *Options) FlushAdmissionDecision {
	policy := opts.FlushAdmissionPolicy
	effectiveConcurrency := normalizeFlushApplyConcurrency(opts.FlushApplyConcurrency)
	decision := FlushAdmissionDecision{
		Policy:                 policy,
		FlushApplyConcurrency:  effectiveConcurrency,
		FlushApplySpanNative:   opts.FlushApplySpanNative,
		FlushBacklogCoalescing: opts.FlushBacklogCoalescing,
	}

	if !policy.Valid() {
		opts.FlushApplyConcurrency = 0
		opts.FlushApplySpanNative = false
		opts.FlushBacklogCoalescing = false
		decision.Admitted = false
		decision.Reason = FlushAdmissionReasonInvalidPolicy
		decision.FlushApplyConcurrency = 0
		decision.FlushApplySpanNative = false
		decision.FlushBacklogCoalescing = false
		return decision
	}

	switch policy {
	case FlushAdmissionPolicyOff:
		opts.FlushApplyConcurrency = 0
		opts.FlushApplySpanNative = false
		opts.FlushBacklogCoalescing = false
		decision.Admitted = false
		decision.Reason = FlushAdmissionReasonPolicyOff
		decision.FlushApplyConcurrency = 0
		decision.FlushApplySpanNative = false
		decision.FlushBacklogCoalescing = false
		return decision
	case FlushAdmissionPolicyExplicit:
		if opts.FlushApplySpanNative || opts.FlushBacklogCoalescing || effectiveConcurrency > 1 {
			decision.Admitted = true
			decision.Reason = FlushAdmissionReasonExplicitOptIn
		} else {
			decision.Admitted = false
			decision.Reason = FlushAdmissionReasonNoExplicitOptIn
		}
		return decision
	case FlushAdmissionPolicyAuto:
		reasons := make([]string, 0, 2)
		if effectiveConcurrency <= 1 {
			reasons = append(reasons, FlushAdmissionReasonLowConcurrency)
		}
		if !flushAdmissionCheckpointDebtResolved {
			reasons = append(reasons, FlushAdmissionReasonCheckpointDebt)
		}
		if len(reasons) > 0 {
			opts.FlushApplyConcurrency = 0
			opts.FlushApplySpanNative = false
			opts.FlushBacklogCoalescing = false
			decision.Admitted = false
			decision.Reason = strings.Join(reasons, ",")
			decision.FlushApplyConcurrency = 0
			decision.FlushApplySpanNative = false
			decision.FlushBacklogCoalescing = false
			return decision
		}

		// Unreachable while checkpoint debt remains unresolved. Keep the admission
		// side explicit for #2794/#2788: if the debt gate is resolved later, auto
		// chooses the measured span-native/backlog candidate only with safe
		// configured concurrency.
		opts.FlushApplyConcurrency = effectiveConcurrency
		opts.FlushApplySpanNative = true
		opts.FlushBacklogCoalescing = true
		decision.Admitted = true
		decision.Reason = FlushAdmissionReasonAutoAdmitted
		decision.FlushApplyConcurrency = effectiveConcurrency
		decision.FlushApplySpanNative = true
		decision.FlushBacklogCoalescing = true
		return decision
	default:
		// Defensive fallback; policy.Valid handled this above.
		opts.FlushApplyConcurrency = 0
		opts.FlushApplySpanNative = false
		opts.FlushBacklogCoalescing = false
		decision.Admitted = false
		decision.Reason = FlushAdmissionReasonInvalidPolicy
		decision.FlushApplyConcurrency = 0
		decision.FlushApplySpanNative = false
		decision.FlushBacklogCoalescing = false
		return decision
	}
}

func (d FlushAdmissionDecision) withStatsDefaults() FlushAdmissionDecision {
	if !d.Policy.Valid() {
		if d.Reason == "" {
			d.Reason = FlushAdmissionReasonInvalidPolicy
		}
		return d
	}
	if d.Reason == "" {
		if d.Admitted {
			d.Reason = FlushAdmissionReasonExplicitOptIn
		} else {
			d.Reason = FlushAdmissionReasonNoExplicitOptIn
		}
	}
	return d
}
