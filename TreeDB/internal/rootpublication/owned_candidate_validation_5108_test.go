package rootpublication

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/pager"
)

// The public queued publisher passes owned COW backing through CandidateSpec.
// Validating its scalar authority must neither export nor retain that backing.
func TestDurableRootCandidateValidationPreservesOwnedGeneration5108(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(4); err != nil {
		t.Fatal(err)
	}
	allocator := freelist.New(p, 0)
	defer allocator.CloseCOWOwnersAfterShutdownV1()
	if err := allocator.EnableNewCOWGenerationV1(1, 4, nil); err != nil {
		t.Fatal(err)
	}
	capability, err := freelist.NewReuseCapability(1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	id := freelist.CandidateIDV1{1}
	prepared, err := allocator.PrepareOwnedCOWCandidateRetiringWithLimitsV1(
		2, 2, id, capability, nil, 0, freelist.NewCandidatePageSinkV1(), nil)
	if err != nil {
		t.Fatal(err)
	}
	info, err := prepared.InfoV1()
	if err != nil {
		t.Fatal(err)
	}
	tx, err := NewDurableRootTransaction(DurableRootTransactionSpec{
		Lineage: DurableRootLineageID{1}, Sequence: 2, PreparedCOW: prepared,
		Activate: func(input DurableRootCallbackInput) error { return allocator.ActivateCOWCandidateV1(input.PreparedCOW) },
		Consume: func(input DurableRootCallbackInput) error {
			return allocator.PublishCOWCandidateV1(input.PreparedCOW, capability)
		},
		Abort: func(input DurableRootCallbackInput) error { return allocator.AbortCOWCandidateV1(input.PreparedCOW) },
		Fail: func(input DurableRootCallbackInput, cause error) error {
			return allocator.FailCOWCandidateV1(input.PreparedCOW, cause)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	assertOwned := func() {
		t.Helper()
		profile := allocator.ResidentGenerationProfileV1()
		if profile.RawGenerationEscaped || profile.RawWriterEscaped || profile.RawLedgerEscaped {
			t.Fatalf("scalar candidate validation exported owned backing: %+v", profile)
		}
	}
	assertOwned()
	valid := CandidateSpec{Frontier: NewFrontier(2, 2, 3, 0, 0), TotalPages: info.HighWater(), DurableRoot: tx}
	for _, name := range []string{"commit", "total-pages", "legacy-head"} {
		t.Run(name, func(t *testing.T) {
			spec := valid
			switch name {
			case "commit":
				spec.Frontier = NewFrontier(3, 2, 3, 0, 0)
			case "total-pages":
				spec.TotalPages++
			case "legacy-head":
				spec.FreelistHeadID = 1
			}
			if _, err := NewPreparedRootCandidate(spec); !errors.Is(err, ErrInvalidCandidate) {
				t.Fatalf("mismatched authority accepted: %v", err)
			}
			if tx.Owner() != ResourceOwnerBuilder {
				t.Fatalf("refusal changed owner: %v", tx.Owner())
			}
			if got, err := prepared.InfoV1(); err != nil || got != info {
				t.Fatalf("refusal changed candidate: %+v %v", got, err)
			}
			assertOwned()
		})
	}
	candidate, err := NewPreparedRootCandidate(valid)
	if err != nil {
		t.Fatal(err)
	}
	if tx.Owner() != ResourceOwnerCandidate {
		t.Fatalf("owner=%v", tx.Owner())
	}
	assertOwned()
	if _, err := NewPreparedRootCandidate(valid); !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("moved candidate accepted: %v", err)
	}
	assertOwned()
	if err := candidate.Abandon(); err != nil {
		t.Fatal(err)
	}
	if err := prepared.ClearTerminalBackingV1(); err != nil {
		t.Fatal(err)
	}
	assertOwned()
	if _, err := prepared.InfoV1(); err == nil {
		t.Fatal("terminal backing still readable")
	}
	dead, err := NewDurableRootTransaction(DurableRootTransactionSpec{
		Lineage: DurableRootLineageID{2}, Sequence: 2, PreparedCOW: prepared,
		Activate: func(DurableRootCallbackInput) error { return nil },
		Consume:  func(DurableRootCallbackInput) error { return nil },
		Abort:    func(DurableRootCallbackInput) error { return nil },
		Fail:     func(DurableRootCallbackInput, error) error { return nil },
	})
	if dead != nil || !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("terminal candidate accepted: %v", err)
	}
}
