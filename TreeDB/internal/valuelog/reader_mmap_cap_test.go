package valuelog

import "testing"

func withDeadMappingCapConfig(t *testing.T, maxMappings int, explicit, adaptive bool) {
	t.Helper()
	prevMax := MaxDeadMappings
	prevExplicit := maxDeadMappingsExplicit
	prevAdaptive := adaptiveDeadMappings
	MaxDeadMappings = maxMappings
	maxDeadMappingsExplicit = explicit
	adaptiveDeadMappings = adaptive
	t.Cleanup(func() {
		MaxDeadMappings = prevMax
		maxDeadMappingsExplicit = prevExplicit
		adaptiveDeadMappings = prevAdaptive
	})
}

func TestEffectiveMaxDeadMappings_AdaptiveScalingAndClamp(t *testing.T) {
	withDeadMappingCapConfig(t, defaultMaxDeadMappings, false, true)

	if got := effectiveMaxDeadMappings(0); got != defaultMaxDeadMappings {
		t.Fatalf("effective cap at zero mapped len: got=%d want=%d", got, defaultMaxDeadMappings)
	}
	if got := effectiveMaxDeadMappings(deadMappingBytesPerStep - 1); got != defaultMaxDeadMappings {
		t.Fatalf("effective cap below first step: got=%d want=%d", got, defaultMaxDeadMappings)
	}
	if got := effectiveMaxDeadMappings(deadMappingBytesPerStep); got != defaultMaxDeadMappings+1 {
		t.Fatalf("effective cap at first step: got=%d want=%d", got, defaultMaxDeadMappings+1)
	}
	mappedLen := (maxAdaptiveDeadMappings - defaultMaxDeadMappings + 128) * deadMappingBytesPerStep
	if got := effectiveMaxDeadMappings(mappedLen); got != maxAdaptiveDeadMappings {
		t.Fatalf("effective cap clamp: got=%d want=%d", got, maxAdaptiveDeadMappings)
	}
}

func TestEffectiveMaxDeadMappings_DoesNotClampBelowConfiguredBase(t *testing.T) {
	base := maxAdaptiveDeadMappings + 128
	withDeadMappingCapConfig(t, base, false, true)

	if got := effectiveMaxDeadMappings(deadMappingBytesPerStep * 8); got != base {
		t.Fatalf("effective cap should preserve configured base: got=%d want=%d", got, base)
	}
}

func TestEffectiveMaxDeadMappings_ExplicitAndAdaptiveOff(t *testing.T) {
	const base = 123
	mappedLen := deadMappingBytesPerStep * 512

	withDeadMappingCapConfig(t, base, true, true)
	if got := effectiveMaxDeadMappings(mappedLen); got != base {
		t.Fatalf("effective cap with explicit base: got=%d want=%d", got, base)
	}

	withDeadMappingCapConfig(t, base, false, false)
	if got := effectiveMaxDeadMappings(mappedLen); got != base {
		t.Fatalf("effective cap with adaptive disabled: got=%d want=%d", got, base)
	}
}

func TestDeadMappingsCapExhausted_Boundaries(t *testing.T) {
	withDeadMappingCapConfig(t, 0, false, true)
	if deadMappingsCapExhausted(1, deadMappingBytesPerStep) {
		t.Fatalf("expected non-positive cap to disable exhaustion checks")
	}

	withDeadMappingCapConfig(t, -1, false, true)
	if deadMappingsCapExhausted(1, deadMappingBytesPerStep) {
		t.Fatalf("expected negative cap to disable exhaustion checks")
	}

	withDeadMappingCapConfig(t, 2, false, true)
	mappedLen := deadMappingBytesPerStep * 4 // effective cap = 2 + 4 = 6
	if deadMappingsCapExhausted(5, mappedLen) {
		t.Fatalf("expected cap to be unexhausted below effective threshold")
	}
	if !deadMappingsCapExhausted(6, mappedLen) {
		t.Fatalf("expected cap to be exhausted at effective threshold")
	}
}
