package valuelog

import "testing"

func TestChooseBlockGroupK_TargetAndClamp(t *testing.T) {
	k := ChooseBlockGroupK(128, 128*1024, 4096, 0.5)
	if k < 1 || k > MaxFrameK {
		t.Fatalf("k out of range: %d", k)
	}
	if k <= 1 {
		t.Fatalf("expected grouped k>1 for compressible stream, got %d", k)
	}
}

func TestChooseBlockGroupK_ExpansionRiskForcesOne(t *testing.T) {
	k := ChooseBlockGroupK(64, 64*1024, 4096, 1.01)
	if k != 1 {
		t.Fatalf("expected k=1 for expansion risk, got %d", k)
	}
}

func TestNormalizeBlockTargetCompressedBytes(t *testing.T) {
	if got := NormalizeBlockTargetCompressedBytes(0); got != defaultBlockTargetCompressedBytes {
		t.Fatalf("default target mismatch: got=%d want=%d", got, defaultBlockTargetCompressedBytes)
	}
	if got := NormalizeBlockTargetCompressedBytes(1); got != minBlockTargetCompressedBytes {
		t.Fatalf("min clamp mismatch: got=%d want=%d", got, minBlockTargetCompressedBytes)
	}
	if got := NormalizeBlockTargetCompressedBytes(1 << 30); got != maxBlockTargetCompressedBytes {
		t.Fatalf("max clamp mismatch: got=%d want=%d", got, maxBlockTargetCompressedBytes)
	}
}
