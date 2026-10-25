package collections

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
)

// A real captured catalog/reducer produces independent rows, then semantic
// publication consumes them after the same original lease loses ended credit.
// Rebuilding from the unchanged capture is the existing reprepare contract.
func TestNativeTextPhaseSuccess(t *testing.T) {
	for _, version := range []TextIndexVersion{TextIndexVersionV1, TextIndexVersionV2} {
		for _, format := range []DocumentFormat{DocumentFormatJSON, DocumentFormatTemplateV1} {
			t.Run(string(version)+"/"+string(format), func(t *testing.T) {
				var budget nativeHNSWAdmissionBudget
				t.Cleanup(func() {
					if budget != nil {
						if s := budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
							t.Errorf("full Close: %+v", s)
						}
					}
				})
				_, _, c := openNativeTextPublicFormat(t, t.TempDir(), treedb.ProfileNoWALFast, true, format, version)
				document := func(raw string) []byte {
					data := []byte(raw)
					if format == DocumentFormatTemplateV1 {
						encoded, err := EncodeTemplateV1DocumentJSON(data)
						if err != nil {
							t.Fatal(err)
						}
						return encoded
					}
					return data
				}
				oldDocument := document(`{"tenant":"a","content":"refund refund policy","embedding":[1,0]}`)
				newDocument := document(`{"tenant":"a","content":"shipping shipping policy","embedding":[0,1],"shipping_extension":{"new_field":"staged"}}`)
				updatedDocument := document(`{"tenant":"a","content":"returns returns policy","embedding":[1,0],"returns_extension":{"new_field":"staged"}}`)
				if _, err := c.Insert([]byte("old"), oldDocument); err != nil {
					t.Fatal(err)
				}
				held, err := c.nativeJSON.Capture(c.name)
				if err != nil {
					t.Fatal(err)
				}
				defer held.Close()
				budget = held.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
				txn, err := c.nativeJSON.Begin(c.name)
				if err != nil {
					t.Fatal(err)
				}
				defer txn.Close()
				if err = txn.InsertBatch([][]byte{[]byte("new")}, [][]byte{newDocument}); err != nil {
					t.Fatal(err)
				}
				g := &txn.bindings[0]
				// Template staging has its resolver already bound to the original
				// stage. Its direct admissions must remain, not be swept as credit.
				if _, err = txn.UpdateBatch([]UpdateBatchItem{{DocumentID: []byte("old"), Update: func([]byte) ([]byte, bool, error) {
					return updatedDocument, true, nil
				}}}); err != nil {
					t.Fatal(err)
				}
				before := g.stageCharge
				phase, err := txn.buildNativeTextPhase(g)
				if err != nil {
					t.Fatal(err)
				}
				if phase.before != before || phase.admitted <= phase.retained || len(phase.domains) == 0 {
					t.Fatalf("no ended phase: %+v", phase)
				}
				all := g.stageCharge
				if err = txn.completeNativeTextPhase(g, phase); err != nil {
					t.Fatal(err)
				}
				want := all - (phase.admitted - phase.retained)
				if g.stageCharge != want || want < before+phase.retained {
					t.Fatalf("ledger before=%d all=%d admitted=%d retained=%d got=%d want=%d", before, all, phase.admitted, phase.retained, g.stageCharge, want)
				}
				// Subsequent growth uses the corrected original ledger. A stale
				// charge would silently restore ended credit on this Resize.
				if err = txn.admitStage(g, memtable.COWAllocationCharge(17)); err != nil {
					t.Fatal(err)
				}
				if g.stageCharge != want+memtable.COWAllocationCharge(17) {
					t.Fatal("later stage growth restored ended credit")
				}
				t.Logf("B=%d A=%d R=%d credit=%d remaining=%d", before, all-before, want-before, all-want, want)
				// A second completed build must equal the retained first image;
				// reset tables and later scratch cannot overwrite cloned output.
				again, err := txn.buildNativeTextPhase(g)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(phase.domains, again.domains) {
					t.Fatal("output changed after reset/reprepare")
				}
				if err = txn.completeNativeTextPhase(g, again); err != nil {
					t.Fatal(err)
				}
				// Drop test-only output aliases before Commit can close its stage.
				phase, again = nativeTextPhase{}, nativeTextPhase{}
				if err = txn.Commit(); err != nil {
					t.Fatal(err)
				}
				if _, err := held.Get([]byte("new")); !errors.Is(err, tree.ErrKeyNotFound) {
					t.Fatalf("old capture changed: %v", err)
				}
				requireNativeTextIDs(t, c, "shipping", "new")
				requireNativeTextIDs(t, c, "returns", "old")
				requireNativeTextIDs(t, c, "refund")
			})
		}
	}
}

func TestNativeTextPhaseFailureKeepsOriginalCharge(t *testing.T) {
	var finalBudget nativeHNSWAdmissionBudget
	t.Cleanup(func() {
		if finalBudget != nil {
			if s := finalBudget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
				t.Errorf("failure full Close: %+v", s)
			}
		}
	})
	_, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	txn, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Close()
	if err = txn.InsertBatch([][]byte{[]byte("new")}, [][]byte{[]byte(`{"tenant":"a","content":"refund policy","embedding":[1,0]}`)}); err != nil {
		t.Fatal(err)
	}
	g := &txn.bindings[0]
	before := g.stageCharge
	budget := txn.core.NativeAllocator().(nativeHNSWAdmissionBudget)
	finalBudget = budget
	sequence, lsn := txn.core.LogicalSequence(), txn.db.CommandWALNextLSN()
	blocker, err := txn.core.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
	if err = blocker.Resize(available); err != nil {
		t.Fatal(err)
	}
	if _, err = txn.buildNativeTextPhase(g); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("expected real phase capacity refusal: %v", err)
	}
	if g.stageCharge != before {
		t.Fatal("failed first admission changed stage ledger")
	}
	blocker.Close()
	cut, err := txn.db.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	if cut.LogicalSequence() != sequence || txn.db.CommandWALNextLSN() != lsn {
		t.Fatal("refused phase published logical/WAL effects")
	}
	if err = cut.Close(); err != nil {
		t.Fatal(err)
	}
	// Invalid source after an admitted catalog exercises the later error exit,
	// without forging an allocator or allowing partial outputs to publish.
	original := g.rows.rows[0].current
	g.rows.rows[0].current = []byte(`{"content":`)
	if _, err = txn.buildNativeTextPhase(g); err == nil {
		t.Fatal("malformed source accepted")
	}
	if g.stageCharge <= before {
		t.Fatal("error refunded successful pre-error admissions")
	}
	g.rows.rows[0].current = original
	if err = txn.Commit(); err != nil {
		t.Fatal(err)
	}
	requireNativeTextIDs(t, c, "refund", "new")
}

func TestNativeTextPhaseRetainedCapacityAndOverflow(t *testing.T) {
	// Non-length capacities and a TrimPrefix-style name alias must all remain
	// charged. The original long qualified allocation cannot be inferred from
	// the short relative name alone without retaining its prefix obligation.
	key, value := make([]byte, 1, 65), make([]byte, 2, 129)
	copy(key, "k")
	copy(value, "vv")
	entries := make([]memtable.COWMutation, 1, 3)
	entries[0] = memtable.COWMutation{Key: key, Value: value}
	domains := make([]nativeJSONDomainChange, 1, 4)
	domains[0] = nativeJSONDomainChange{name: "text/content", entries: entries}
	got, err := nativeTextPhaseRetained(domains, 80)
	want := memtable.COWAllocationCharge(4*uint64(unsafe.Sizeof(nativeJSONDomainChange{}))) + memtable.COWAllocationCharge(81+uint64(len(domains[0].name))) + memtable.COWAllocationCharge(3*uint64(unsafe.Sizeof(memtable.COWMutation{}))) + memtable.COWAllocationCharge(65) + memtable.COWAllocationCharge(129)
	if err != nil || got != want || !bytes.Equal(entries[0].Value, []byte("vv")) {
		t.Fatalf("actual backing got=%d want=%d err=%v", got, want, err)
	}
	if _, err = nativeTextPhaseRetained(domains, ^uint64(0)); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("name overflow: %v", err)
	}
	// Invalid floors/overflow must fail before touching any receipt or ledger.
	g := nativeJSONTxnBinding{stageCharge: 17}
	txn := NativeJSONTxn{}
	for _, phase := range []nativeTextPhase{{before: ^uint64(0), admitted: 1}, {before: 16, admitted: 2}, {before: 1, admitted: 1, retained: 2}} {
		if err = txn.completeNativeTextPhase(&g, phase); !errors.Is(err, memtable.ErrCOWCapacity) || g.stageCharge != 17 {
			t.Fatalf("invalid floor changed ledger: %v %+v", err, g)
		}
	}
}
