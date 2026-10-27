package collections

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Candidate-only: the original-source causal test remains independently frozen.
// These are actual finite-budget refusals, never an admission callback returning
// an injected error. The public callback positions the same blocker after all
// preceding update controls, without rerunning that callback or transaction.
func TestNativeJSONScalarStateCopyPartialAdmission(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, refused := range []int{2, 3} {
		t.Run(fmt.Sprintf("backing_%d", refused), func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval, opts.BackgroundCheckpointIdleDuration = -1, -1
			owner, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			var manager *CollectionManager
			var txn *NativeJSONTxn
			var blocker *memtable.COWExternalLease
			var budget interface {
				Stats() memtable.COWStats
				Limits() memtable.COWLimits
			}
			t.Cleanup(func() {
				if blocker != nil {
					blocker.Close()
				}
				if txn != nil {
					if e := txn.Close(); e != nil {
						t.Error(e)
					}
				}
				if manager != nil {
					if e := manager.Close(); e != nil {
						t.Error(e)
					}
				}
				if e := owner.Close(); e != nil {
					t.Error(e)
				}
				if budget != nil {
					if s := budget.Stats(); s.TotalBytes != 0 || s.ReservedBytes != 0 || s.ExternalLeases != 0 {
						t.Errorf("full Close retained scalar staging: %+v", s)
					}
				}
			})
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager = NewCollectionManager(backend)
			if _, err = manager.CreateCollection(&CollectionMeta{Name: "partial_scalar", Indexes: []IndexDefinition{
				{Name: "city", Field: "city", ValueType: IndexValueString},
				{Name: "rank", Field: "rank", ValueType: IndexValueInt64},
				{Name: "optional", Field: "optional", ValueType: IndexValueString},
			}}); err != nil {
				t.Fatal(err)
			}
			c, err := manager.OpenCollection("partial_scalar")
			if err != nil {
				t.Fatal(err)
			}
			txn, err = c.nativeJSON.Begin(c.name)
			if err != nil {
				t.Fatal(err)
			}
			budget = txn.core.NativeAllocator().(interface {
				Stats() memtable.COWStats
				Limits() memtable.COWLimits
			})
			id, staged := []byte("row"), []byte(`{"city":"east","rank":1}`)
			if err = txn.InsertBatch([][]byte{id}, [][]byte{staged}); err != nil {
				t.Fatal(err)
			}
			g := &txn.bindings[0]
			row := g.rows.find(id)
			if row == nil || len(row.currentState) != 3 {
				t.Fatal("missing actual staged scalar state")
			}
			state := row.currentState
			var encodedBytes, values int
			for _, entry := range state {
				if len(entry) > 1 {
					t.Fatal("fixture is not scalar")
				}
				if len(entry) == 1 {
					encodedBytes += len(entry[0])
					values++
				}
			}
			if values != 2 || encodedBytes == 0 {
				t.Fatal("fixture must include present and missing fields")
			}
			charges := [3]uint64{
				memtable.COWAllocationCharge(uint64(encodedBytes)),
				memtable.COWAllocationCharge(uint64(len(state)) * uint64(unsafe.Sizeof([][]byte{}))),
				memtable.COWAllocationCharge(uint64(values) * uint64(unsafe.Sizeof([]byte{}))),
			}
			var prefix uint64
			for _, charge := range charges[:refused-1] {
				prefix += charge
			}
			blocker, err = txn.core.NativeAllocator().AdmitNativeBytes(0)
			if err != nil {
				t.Fatal(err)
			}
			leave := func(headroom uint64) error {
				available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
				if available <= headroom {
					return fmt.Errorf("fixture headroom=%d need more than %d", available, headroom)
				}
				return blocker.Resize(available - headroom)
			}
			if err = leave(prefix); err != nil {
				t.Fatal(err)
			}
			beforeStage, beforeBudget := g.stageCharge, budget.Stats()
			calls := 0
			owned, refusal := retainNativeJSONScalarState(state, func(n uint64) error {
				calls++
				if calls > len(charges) || n != charges[calls-1] {
					t.Fatalf("copy admission %d bytes=%d charges=%v", calls, n, charges)
				}
				return txn.admitStage(g, n)
			})
			if owned != nil || !errors.Is(refusal, memtable.ErrCOWCapacity) || calls != refused {
				t.Fatalf("copy refusal=%d calls=%d owned=%v err=%v", refused, calls, owned, refusal)
			}
			owned = nil
			afterBudget := budget.Stats()
			if g.stageCharge != beforeStage+prefix || afterBudget.ReservedBytes != beforeBudget.ReservedBytes+prefix || afterBudget.ExternalLeases != beforeBudget.ExternalLeases {
				t.Fatalf("successful prefix lost: stage=%d want=%d before=%+v after=%+v", g.stageCharge, beforeStage+prefix, beforeBudget, afterBudget)
			}
			if err = blocker.Resize(0); err != nil {
				t.Fatal(err)
			}
			if g.stageCharge != beforeStage+prefix {
				t.Fatal("releasing blocker credited the transaction prefix")
			}

			// Equal-length scalar replacements reuse the already admitted validation
			// arena. Only admitRowDocument and the scalar-copy prefix follow this
			// callback before the chosen second/third backing refuses.
			replacement := []byte(`{"city":"west","rank":2}`)
			rowCharge := 2*memtable.COWAllocationCharge(uint64(len(id))) + memtable.COWAllocationCharge(uint64(len(replacement)))
			validationCharge := g.validationCharge
			sequence, lsn := txn.core.LogicalSequence(), backend.CommandWALNextLSN()
			var callbackStage uint64
			callbackCalls := 0
			result, refusal := txn.UpdateBatch([]UpdateBatchItem{{DocumentID: id, Update: func([]byte) ([]byte, bool, error) {
				callbackCalls++
				callbackStage = g.stageCharge
				if e := leave(rowCharge + prefix); e != nil {
					return nil, false, e
				}
				return replacement, true, nil
			}}})
			if !errors.Is(refusal, memtable.ErrCOWCapacity) || result != nil || callbackCalls != 1 || backenddb.CommitPublicationAccepted(refusal) {
				t.Fatalf("public refusal result=%+v calls=%d err=%v", result, callbackCalls, refusal)
			}
			if g.validationCharge != validationCharge || g.stageCharge != callbackStage+rowCharge+prefix {
				t.Fatalf("wrong public boundary: validation=%d/%d stage=%d want=%d", g.validationCharge, validationCharge, g.stageCharge, callbackStage+rowCharge+prefix)
			}
			if !bytes.Equal(row.current, staged) || !errors.Is(txn.failure, memtable.ErrCOWCapacity) || txn.core.LogicalSequence() != sequence || backend.CommandWALNextLSN() != lsn {
				t.Fatal("refusal changed staged document, poison or publication")
			}
			// Drop test aliases before the original Commit failure path closes the
			// complete binding and its stage receipt. Explicit Close is idempotent.
			state, row, g = nil, nil, nil
			blocker.Close()
			if err = txn.Commit(); !errors.Is(err, memtable.ErrCOWCapacity) {
				t.Fatalf("poisoned Commit: %v", err)
			}
			if err = txn.Close(); err != nil {
				t.Fatal(err)
			}
			if backend.CommandWALNextLSN() != lsn {
				t.Fatal("poisoned Commit advanced WAL")
			}
			if doc, found, e := c.GetInto(id, nil); e != nil || found || len(doc) != 0 {
				t.Fatalf("earlier staged row published: %q %v %v", doc, found, e)
			}
			for _, city := range []string{"east", "west"} {
				if ids, e := c.FindByIndexValue("city", city); e != nil || len(ids) != 0 {
					t.Fatalf("refused index %s=%q: %v", city, ids, e)
				}
			}
		})
	}
}
