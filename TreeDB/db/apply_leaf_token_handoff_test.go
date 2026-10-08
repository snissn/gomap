package db

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func directLeafTestToken(t *testing.T, generation uint64, frontier uint64, released *int) *rootpublication.StableResourceToken {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "raw-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := file.Truncate(4096); err != nil {
		t.Fatal(err)
	}
	token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
		Kind: rootpublication.ResourceOuterLeafLog, LogicalLane: "test-leaf", ResourceID: fmt.Sprint(generation), Generation: generation,
		DiagnosticPath: "leaf_vlog/test.vlog", File: file, Frontier: rootpublication.DurableFrontier{Bytes: frontier},
		Reachability: rootpublication.ReachabilityOuterLeafRawPointer, OnRelease: func() { *released++ },
	})
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestApplyLeafTokenHandoffRejectsIncompleteAuthority(t *testing.T) {
	id := page.ValueLogFileID(11)
	for _, name := range []string{"nil", "partial", "extra", "short", "overflow", "zero-length", "terminal"} {
		t.Run(name, func(t *testing.T) {
			released := 0
			capture := &applyLeafResourceCapture{builder: rootpublication.NewStableResourceSetBuilder()}
			defer func() {
				if capture.builder != nil {
					capture.builder.Abandon()
				}
			}()
			ptrs := []page.ValuePtr{{FileID: id, Offset: 32, Length: 16}}
			tokens := []*rootpublication.StableResourceToken{directLeafTestToken(t, uint64(id), 4096, &released)}
			switch name {
			case "nil":
				tokens[0].Release()
				tokens = nil
			case "partial":
				ptrs = append(ptrs, page.ValuePtr{FileID: page.ValueLogFileID(12), Offset: 32, Length: 16})
			case "extra":
				tokens = append(tokens, directLeafTestToken(t, uint64(page.ValueLogFileID(12)), 4096, &released))
			case "short":
				tokens[0].Release()
				tokens[0] = directLeafTestToken(t, uint64(id), 32, &released)
			case "overflow":
				ptrs[0].Offset = ^uint64(0) - 8
			case "zero-length":
				ptrs[0].Length = 0
			case "terminal":
				capture.builder.Abandon()
				capture.builder = nil
			}
			before := released
			if err := capture.acceptRaw(ptrs, tokens); err == nil {
				t.Fatal("accepted incomplete raw authority")
			}
			if released != before+len(tokens) {
				t.Fatalf("release count=%d want %d", released, before+len(tokens))
			}
			if capture.builder != nil {
				set, err := capture.builder.Freeze()
				if err != nil {
					t.Fatal(err)
				}
				defer set.Release()
				if set.Len() != 0 {
					t.Fatal("validation failure mutated Apply inventory")
				}
			}
		})
	}
}

func TestApplyLeafTokenHandoffPartialAddAbortAndRetry(t *testing.T) {
	id := page.ValueLogFileID(11)
	released := 0
	capture := &applyLeafResourceCapture{builder: rootpublication.NewStableResourceSetBuilder()}
	// Same logical generation, different exact handles: the second Add conflicts.
	tokens := []*rootpublication.StableResourceToken{directLeafTestToken(t, uint64(id), 4096, &released), directLeafTestToken(t, uint64(id), 4096, &released)}
	ptrs := []page.ValuePtr{{FileID: id, Offset: 32, Length: page.ValuePtrMarkGrouped(16, 127)}}
	err := capture.acceptRaw(ptrs, tokens)
	if !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("Add failure=%v", err)
	}
	(&applyLeafResourceLog{capture: capture}).abandon()
	if released != 2 {
		t.Fatalf("abort releases=%d want 2", released)
	}
	// A new attempt cannot inherit the abandoned producer inventory.
	retry := &applyLeafResourceLog{capture: &applyLeafResourceCapture{builder: rootpublication.NewStableResourceSetBuilder()}}
	if err := retry.capture.acceptRaw(ptrs, []*rootpublication.StableResourceToken{directLeafTestToken(t, uint64(id), 48, &released)}); err != nil {
		t.Fatal(err)
	}
	set, err := retry.freeze()
	if err != nil {
		t.Fatal(err)
	}
	if released != 2 {
		t.Fatal("freeze released live output")
	}
	if _, err := retry.freeze(); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("second freeze=%v", err)
	}
	set.Release()
	if released != 3 {
		t.Fatalf("final releases=%d want 3", released)
	}
	child := stableContractResourceSet(t, stableContractDescriptor{generation: uint64(id), kind: rootpublication.ResourceOuterLeafLog, reachability: rootpublication.ReachabilityOuterLeafRawPointer, frontier: 4096})
	if err := retry.capture.acceptChild(child); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("terminal child=%v", err)
	}
	if child.Owner() != rootpublication.ResourceOwnerBuilder {
		t.Fatal("failed child merge consumed source")
	}
	child.Release()
}

type unsupportedApplyLeafProvider struct{ *stableContractTestLeafLog }

type applyProducerOwnerWitness struct {
	*stableContractTestLeafLog
	releases int
}

func (w *applyProducerOwnerWitness) LeafPageLogForApply(func([]page.ValuePtr, []*rootpublication.StableResourceToken) error, func(*rootpublication.StableResourceSet) error) (LeafPageLog, bool, error) {
	return w, true, nil
}

func (w *applyProducerOwnerWitness) ReleaseLeafPageLogApplyResources() { w.releases++ }

func TestApplyLeafTokenHandoffProducerOwnerThroughHintFreezeAndAbort(t *testing.T) {
	for _, abort := range []bool{false, true} {
		t.Run(fmt.Sprint("abort=", abort), func(t *testing.T) {
			owner := &applyProducerOwnerWitness{stableContractTestLeafLog: &stableContractTestLeafLog{}}
			log, err := newApplyLeafResourceLog(&leafPageLogWithRecordLengthHints{inner: owner})
			if err != nil {
				t.Fatal(err)
			}
			released := 0
			id := page.ValueLogFileID(11)
			if err := log.capture.acceptRaw([]page.ValuePtr{{FileID: id, Offset: 32, Length: 16}}, []*rootpublication.StableResourceToken{directLeafTestToken(t, uint64(id), 48, &released)}); err != nil {
				t.Fatal(err)
			}
			if owner.releases != 0 {
				t.Fatal("owner ended before joined freeze/abort")
			}
			if abort {
				log.abandon()
			} else {
				set, err := log.freeze()
				if err != nil {
					t.Fatal(err)
				}
				if released != 0 {
					t.Fatal("owner completion released independent output")
				}
				set.Release()
			}
			log.abandon()
			if owner.releases != 1 || released != 1 {
				t.Fatalf("owner/output releases=%d/%d", owner.releases, released)
			}
		})
	}
}

func (*unsupportedApplyLeafProvider) LeafPageLogForApply(func([]page.ValuePtr, []*rootpublication.StableResourceToken) error, func(*rootpublication.StableResourceSet) error) (LeafPageLog, bool, error) {
	return nil, false, nil
}

func TestApplyLeafTokenHandoffUnknownHintProviderUsesValidatedPublicPath(t *testing.T) {
	ptr := page.LeafLogPtr{FileID: 11, Offset: 32, RecordLengthHint: 16}
	for _, unsupportedFactory := range []bool{false, true} {
		t.Run(fmt.Sprint("unsupportedFactory=", unsupportedFactory), func(t *testing.T) {
			inner := &stableContractTestLeafLog{ptrs: []page.LeafLogPtr{ptr}}
			var provider LeafPageLog = inner
			if unsupportedFactory {
				provider = &unsupportedApplyLeafProvider{inner}
			}
			hints := &leafPageLogWithRecordLengthHints{inner: provider}
			log, err := newApplyLeafResourceLog(hints)
			if err != nil {
				t.Fatal(err)
			}
			defer log.abandon()
			if log.direct != nil {
				t.Fatal("hint factory presence inferred producer authority")
			}
			if _, err := log.AppendLeafPage([]byte{1}); !errors.Is(err, rootpublication.ErrUnresolvedResource) {
				t.Fatalf("public nil authority=%v", err)
			}
		})
	}
}

func TestApplyLeafTokenHandoffCoalescesImmutableRotationFrontiers(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "rotation-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := file.Truncate(4096); err != nil {
		t.Fatal(err)
	}
	id := page.ValueLogFileID(11)
	released := 0
	var tokens []*rootpublication.StableResourceToken
	for _, frontier := range []uint64{32, 48} {
		token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
			Kind: rootpublication.ResourceOuterLeafLog, LogicalLane: "outer-leaf-0", ResourceID: "11", Generation: uint64(id),
			DiagnosticPath: "leaf_vlog/test.vlog", File: file, Frontier: rootpublication.DurableFrontier{Bytes: frontier},
			Reachability: rootpublication.ReachabilityOuterLeafRawPointer, OnRelease: func() { released++ },
		})
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, token)
	}
	capture := &applyLeafResourceCapture{builder: rootpublication.NewStableResourceSetBuilder()}
	if err := capture.acceptRaw([]page.ValuePtr{{FileID: id, Offset: 32, Length: 16}}, tokens); err != nil {
		t.Fatal(err)
	}
	set, err := (&applyLeafResourceLog{capture: capture}).freeze()
	if err != nil {
		t.Fatal(err)
	}
	if set.Len() != 1 || set.PhysicalDescriptors()[0].Frontier().Bytes != 48 {
		t.Fatal("lost rotation maximum certificate")
	}
	if tokens[0].Frontier().Bytes != 32 {
		t.Fatal("mutated immutable token frontier")
	}
	set.Release()
	if released != 2 {
		t.Fatalf("release count=%d want 2", released)
	}
}
