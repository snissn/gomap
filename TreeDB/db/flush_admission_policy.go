package db

import (
	"fmt"
	"strings"
)

const (
	defaultFlushAdmissionAutoConcurrencyCap = 8
	defaultFlushAdmissionAutoMinConcurrency = 2
	defaultFlushAdmissionAutoMinEntries     = 1
	defaultFlushAdmissionAutoMinSpans       = 1
	defaultFlushAdmissionAutoMinBytes       = 1
)

// FlushAdmissionPolicy controls how TreeDB admits the span-native/backlog
// flush/apply candidate path. The zero value is the default auto selector: it
// admits the measured hardware-aware span-native/backlog/adaptive candidate on
// sufficiently parallel hosts, declines low-concurrency shapes, and leaves
// rollback and explicit opt-in policies available.
type FlushAdmissionPolicy uint8

const (
	// FlushAdmissionPolicyAuto is the default selector. It admits the measured
	// span-native/backlog/adaptive-cache candidate at the detected physical core
	// count, capped by GOMAXPROCS and a conservative upper bound, only when the
	// low-concurrency guardrail passes; otherwise it fails closed to the serial
	// path. If physical-core detection is unavailable, auto falls back to the
	// existing GOMAXPROCS-capped bound.
	FlushAdmissionPolicyAuto FlushAdmissionPolicy = iota
	// FlushAdmissionPolicyExplicit preserves existing explicit knobs. Use this to
	// opt in to a non-default span-native/backlog/concurrency/cache shape.
	FlushAdmissionPolicyExplicit
	// FlushAdmissionPolicyOff force-disables span-native, backlog coalescing, and
	// flush-apply worker-pool concurrency as a rollback/fail-closed policy.
	FlushAdmissionPolicyOff
)

const (
	FlushAdmissionReasonNoExplicitOptIn           = "no_explicit_opt_in"
	FlushAdmissionReasonExplicitOptIn             = "explicit_opt_in"
	FlushAdmissionReasonPolicyOff                 = "policy_off"
	FlushAdmissionReasonAutoAdmitted              = "auto_admitted"
	FlushAdmissionReasonAutoAdmittedCappedAdapt   = "auto_admitted_capped_adaptive"
	FlushAdmissionReasonAutoAdmittedHardwareAware = "auto_admitted_hardware_aware"
	FlushAdmissionReasonLowConcurrency            = "low_concurrency"
	FlushAdmissionReasonUnsafeDurability          = "unsafe_durability"
	FlushAdmissionReasonCheckpointDebt            = "checkpoint_debt_unresolved"
	FlushAdmissionReasonInvalidPolicy             = "invalid_policy"
)

const (
	FlushAdmissionConcurrencyCapDisabled             = "disabled"
	FlushAdmissionConcurrencyCapConfigured           = "configured"
	FlushAdmissionConcurrencyCapConfiguredGOMAXPROCS = "configured_gomaxprocs_cap"
	FlushAdmissionConcurrencyCapDefaultPhysicalCores = "default_physical_cores"
	FlushAdmissionConcurrencyCapDefaultAutoCap       = "default_auto_cap"
	FlushAdmissionConcurrencyCapDefaultGOMAXPROCS    = "default_gomaxprocs_cap"
)

// #2794/B1 checkpoint debt is accepted for this cycle as a bounded analytics
// model tradeoff, not as a mandatory runtime-mitigation blocker. Keep this seam
// explicit so a future gate can fail closed again without changing callers.
const flushAdmissionCheckpointDebtAccepted = true

// FlushAdmissionDecision is the normalized admission result reported through
// DB.Stats and benchmark option reports.
type FlushAdmissionDecision struct {
	Policy                          FlushAdmissionPolicy
	Admitted                        bool
	Reason                          string
	FlushApplyConcurrencyConfigured int
	FlushApplyConcurrency           int
	FlushApplyConcurrencyCapReason  string
	FlushApplyConcurrencyDefaulted  bool
	RuntimeGOMAXPROCS               int
	PhysicalCores                   int
	FlushApplySpanNative            bool
	FlushBacklogCoalescing          bool
	LeafPageReadCacheWriteAdmission LeafPageReadCacheWriteAdmissionPolicy
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
// "default" use the current default auto selector.
func ParseFlushAdmissionPolicy(raw string) (FlushAdmissionPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "default", "unset", "auto", "adaptive":
		return FlushAdmissionPolicyAuto, nil
	case "explicit", "manual":
		return FlushAdmissionPolicyExplicit, nil
	case "off", "false", "0", "disabled", "disable":
		return FlushAdmissionPolicyOff, nil
	default:
		return FlushAdmissionPolicyAuto, fmt.Errorf("unsupported flush admission policy %q (expected off|explicit|auto)", raw)
	}
}

// NormalizeFlushAdmissionOptions applies the fail-closed admission seam to opts
// and stores the decision in the options for Open/Stats. It is idempotent so
// public wrappers can normalize before passing options to the backend without
// losing the original decline reason.
func NormalizeFlushAdmissionOptions(opts *Options) FlushAdmissionDecision {
	if opts == nil {
		return FlushAdmissionDecision{Policy: FlushAdmissionPolicyAuto, Reason: FlushAdmissionReasonLowConcurrency, FlushApplyConcurrencyDefaulted: true, FlushApplyConcurrencyCapReason: FlushAdmissionConcurrencyCapDisabled}
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
	return computeFlushAdmissionDecisionForHardware(opts, runtimeGOMAXPROCS(), DetectPhysicalCores())
}

func computeFlushAdmissionDecisionForHardware(opts *Options, gomax, physicalCores int) FlushAdmissionDecision {
	policy := opts.FlushAdmissionPolicy
	configuredConcurrency := opts.FlushApplyConcurrency
	effectiveConcurrency := normalizeFlushApplyConcurrencyForGOMAXPROCS(configuredConcurrency, gomax)
	decision := FlushAdmissionDecision{
		Policy:                          policy,
		FlushApplyConcurrencyConfigured: configuredConcurrency,
		FlushApplyConcurrency:           effectiveConcurrency,
		FlushApplyConcurrencyCapReason:  flushAdmissionConcurrencyCapReason(configuredConcurrency, gomax, physicalCores, effectiveConcurrency),
		FlushApplySpanNative:            opts.FlushApplySpanNative,
		FlushBacklogCoalescing:          opts.FlushBacklogCoalescing,
		FlushApplyConcurrencyDefaulted:  configuredConcurrency <= 0,
		RuntimeGOMAXPROCS:               gomax,
		PhysicalCores:                   physicalCores,
		LeafPageReadCacheWriteAdmission: opts.LeafPageReadCacheWriteAdmission,
	}

	if !policy.Valid() {
		decision.disableAll(opts, FlushAdmissionReasonInvalidPolicy)
		return decision
	}

	switch policy {
	case FlushAdmissionPolicyOff:
		decision.disableAll(opts, FlushAdmissionReasonPolicyOff)
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
		candidateConcurrency := autoFlushApplyConcurrencyForHardware(opts.FlushApplyConcurrency, gomax, physicalCores)
		if candidateConcurrency < defaultFlushAdmissionAutoMinConcurrency {
			reasons = append(reasons, FlushAdmissionReasonLowConcurrency)
		}
		if opts.Durability == DurabilityWALOffRelaxed && opts.ResolvedProfile != ProfileNoWALFast {
			reasons = append(reasons, FlushAdmissionReasonUnsafeDurability)
		}
		if !flushAdmissionCheckpointDebtAccepted {
			reasons = append(reasons, FlushAdmissionReasonCheckpointDebt)
		}
		if len(reasons) > 0 {
			decision.disableAll(opts, strings.Join(reasons, ","))
			return decision
		}

		opts.FlushApplyConcurrency = candidateConcurrency
		opts.FlushApplyMinEntries = defaultFlushAdmissionAutoMinEntries
		opts.FlushApplyMinSpans = defaultFlushAdmissionAutoMinSpans
		opts.FlushApplyMinBytes = defaultFlushAdmissionAutoMinBytes
		opts.FlushApplySpanNative = true
		opts.FlushBacklogCoalescing = true
		if opts.LeafPageReadCacheWriteAdmission == LeafPageReadCacheWriteAdmissionImmediate || opts.LeafPageReadCacheWriteAdmission == LeafPageReadCacheWriteAdmissionAdaptive {
			opts.LeafPageReadCacheWriteAdmission = LeafPageReadCacheWriteAdmissionAdaptive
		}

		decision.Admitted = true
		decision.Reason = FlushAdmissionReasonAutoAdmittedHardwareAware
		decision.FlushApplyConcurrency = candidateConcurrency
		decision.FlushApplyConcurrencyCapReason = flushAdmissionConcurrencyCapReason(configuredConcurrency, gomax, physicalCores, candidateConcurrency)
		decision.FlushApplySpanNative = true
		decision.FlushBacklogCoalescing = true
		decision.LeafPageReadCacheWriteAdmission = opts.LeafPageReadCacheWriteAdmission
		return decision
	default:
		// Defensive fallback; policy.Valid handled this above.
		decision.disableAll(opts, FlushAdmissionReasonInvalidPolicy)
		return decision
	}
}

func autoFlushApplyConcurrency(configured int) int {
	return autoFlushApplyConcurrencyForHardware(configured, runtimeGOMAXPROCS(), DetectPhysicalCores())
}

func flushAdmissionConcurrencyCapReason(configured, gomax, physicalCores, selected int) string {
	if selected <= 0 {
		return FlushAdmissionConcurrencyCapDisabled
	}
	if configured > 0 {
		if selected < configured {
			return FlushAdmissionConcurrencyCapConfiguredGOMAXPROCS
		}
		return FlushAdmissionConcurrencyCapConfigured
	}

	defaultCap := defaultFlushAdmissionAutoConcurrencyCap
	if physicalCores > 0 && physicalCores < defaultCap {
		defaultCap = physicalCores
	}
	if gomax < 1 {
		gomax = 1
	}
	if selected < defaultCap && selected == gomax {
		return FlushAdmissionConcurrencyCapDefaultGOMAXPROCS
	}
	if physicalCores > 0 && selected == physicalCores && physicalCores < defaultFlushAdmissionAutoConcurrencyCap {
		return FlushAdmissionConcurrencyCapDefaultPhysicalCores
	}
	return FlushAdmissionConcurrencyCapDefaultAutoCap
}

func (d *FlushAdmissionDecision) disableAll(opts *Options, reason string) {
	if opts != nil {
		opts.FlushApplyConcurrency = 0
		opts.FlushApplySpanNative = false
		opts.FlushBacklogCoalescing = false
	}
	d.Admitted = false
	d.Reason = reason
	d.FlushApplyConcurrency = 0
	d.FlushApplyConcurrencyCapReason = FlushAdmissionConcurrencyCapDisabled
	d.FlushApplySpanNative = false
	d.FlushBacklogCoalescing = false
	if opts != nil {
		d.LeafPageReadCacheWriteAdmission = opts.LeafPageReadCacheWriteAdmission
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
			switch d.Policy {
			case FlushAdmissionPolicyAuto:
				d.Reason = FlushAdmissionReasonAutoAdmitted
			default:
				d.Reason = FlushAdmissionReasonExplicitOptIn
			}
		} else if d.Policy == FlushAdmissionPolicyExplicit {
			d.Reason = FlushAdmissionReasonNoExplicitOptIn
		} else {
			d.Reason = FlushAdmissionReasonLowConcurrency
		}
	}
	if d.FlushApplyConcurrencyCapReason == "" {
		d.FlushApplyConcurrencyCapReason = flushAdmissionConcurrencyCapReason(d.FlushApplyConcurrencyConfigured, d.RuntimeGOMAXPROCS, d.PhysicalCores, d.FlushApplyConcurrency)
	}
	return d
}
