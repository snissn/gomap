package lifecycle

import "testing"

func TestGraveyard_KeepRecentDefersReclaim(t *testing.T) {
	gy := NewGraveyard()

	// Retire at seq=10.
	gy.Add(10, []uint64{1, 2, 3})

	// With keepRecent=1 and currentSeq=11, safeHistory=current-keepRecent=10,
	// so limit is at most 10. Retired seq=10 is NOT strictly less than 10.
	freed := gy.Extract(^uint64(0), 11, 1)
	if len(freed) != 0 {
		t.Fatalf("expected no freed pages (keepRecent), got %v", freed)
	}
}

func TestGraveyard_AdvancingSeqAllowsReclaim(t *testing.T) {
	gy := NewGraveyard()

	gy.Add(10, []uint64{1, 2, 3})

	// Advance currentSeq far enough beyond keepRecent so retired seq becomes eligible.
	freed := gy.Extract(^uint64(0), 12, 1) // safeHistory=11, so 10 < 11 => eligible.
	if len(freed) != 3 {
		t.Fatalf("expected 3 freed pages, got %d (%v)", len(freed), freed)
	}
}
