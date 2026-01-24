package valuelog

import "testing"

func TestNormalizeAutotuneOptions_ExplicitOffStaysOff(t *testing.T) {
	in := AutotuneOptions{Mode: AutotuneOff}
	out := NormalizeAutotuneOptions(in, true /* splitValueLog */)
	if out.Mode != AutotuneOff {
		t.Fatalf("expected explicit AutotuneOff to remain off, got %v", out.Mode)
	}
}

func TestNormalizeAutotuneOptions_UnsetDefaultsToMediumWhenSplitValueLog(t *testing.T) {
	out := NormalizeAutotuneOptions(AutotuneOptions{}, true /* splitValueLog */)
	if out.Mode != AutotuneMedium {
		t.Fatalf("expected unset to default to AutotuneMedium when splitValueLog=true, got %v", out.Mode)
	}
}

func TestNormalizeAutotuneOptions_UnsetDefaultsToOffWhenNotSplitValueLog(t *testing.T) {
	out := NormalizeAutotuneOptions(AutotuneOptions{}, false /* splitValueLog */)
	if out.Mode != AutotuneOff {
		t.Fatalf("expected unset to default to AutotuneOff when splitValueLog=false, got %v", out.Mode)
	}
}
