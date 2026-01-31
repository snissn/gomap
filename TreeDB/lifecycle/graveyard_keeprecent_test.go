package lifecycle

import "testing"

func TestGraveyard_KeepRecentDefersReclaim(t *testing.T) {
	gy := NewGraveyard()

	// Retire at seq=10.
	gy.Add(10, []uint64{1, 2, 3})

	// With keepRecent=1 and currentSeq=10, safeHistory=current-keepRecent=9,
	// so retired seq=10 is not eligible.
	freed := gy.Extract(^uint64(0), 10, 1)
	if len(freed) != 0 {
		t.Fatalf("expected no freed pages (keepRecent), got %v", freed)
	}
}

func TestGraveyard_AdvancingSeqAllowsReclaim(t *testing.T) {
	gy := NewGraveyard()

	gy.Add(10, []uint64{1, 2, 3})

	// Advance currentSeq far enough beyond keepRecent so retired seq becomes eligible.
	// With keepRecent=1 and currentSeq=11, safeHistory=10, so seq=10 is eligible.
	freed := gy.Extract(^uint64(0), 11, 1)
	if len(freed) != 3 {
		t.Fatalf("expected 3 freed pages, got %d (%v)", len(freed), freed)
	}
}
