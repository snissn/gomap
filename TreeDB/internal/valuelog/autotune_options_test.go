package valuelog

import "testing"

func TestNormalizeAutotuneOptions_ExplicitOffStaysOff(t *testing.T) {
	in := AutotuneOptions{Mode: AutotuneOff}
	out := NormalizeAutotuneOptions(in, true /* valueLogEnabled */)
	if out.Mode != AutotuneOff {
		t.Fatalf("expected explicit AutotuneOff to remain off, got %v", out.Mode)
	}
}

func TestNormalizeAutotuneOptions_UnsetDefaultsToMediumWhenValueLogEnabled(t *testing.T) {
	out := NormalizeAutotuneOptions(AutotuneOptions{}, true /* valueLogEnabled */)
	if out.Mode != AutotuneMedium {
		t.Fatalf("expected unset to default to AutotuneMedium when valueLogEnabled=true, got %v", out.Mode)
	}
}

func TestNormalizeAutotuneOptions_UnsetDefaultsToOffWhenValueLogDisabled(t *testing.T) {
	out := NormalizeAutotuneOptions(AutotuneOptions{}, false /* valueLogEnabled */)
	if out.Mode != AutotuneOff {
		t.Fatalf("expected unset to default to AutotuneOff when valueLogEnabled=false, got %v", out.Mode)
	}
}
