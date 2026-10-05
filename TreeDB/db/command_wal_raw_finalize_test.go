package db

import (
	"bytes"
	"errors"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestRawKVPreparedFinalizerHasCanonicalRIDBeforeAppend(t *testing.T) {
	for _, materialized := range []bool{false, true} {
		name, mode, wantOp := "external", RawKVCommandWALAppendRelaxed, commitlog.RawKVOpSetRID
		if materialized {
			name, mode, wantOp = "materialized", RawKVCommandWALAppendDurable, commitlog.RawKVOpSetMaterializedRID
		}
		t.Run(name, func(t *testing.T) {
			d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, Durability: DurabilityWALOnRelaxed, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			entry := appendCommandWALPointerEntry(t, d, []byte("key"), bytes.Repeat([]byte("v"), 256))
			if !materialized {
				entry.Value = nil
			}
			baselinePins := d.valueLogIdentityPins.ActivePins()
			before := d.CommandWALNextLSN()
			finalized, appendReached := false, false
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Point == durabilitycut.BeforeDependencyAppend && event.Resource == durabilitycut.ResourceCommandWAL {
					appendReached = true
					if !finalized {
						t.Fatal("append reached before canonical finalizer")
					}
				}
				return nil
			})
			defer restore()
			var canonical []byte
			lsn, err := d.AppendRawKVCommandWALOrderedEntryScanWithHintPreparedFinalizedAndMode(
				func() error { entry.Revision = 48; return nil },
				func(payload []byte, lookup func(page.ValuePtr) (uint64, bool)) error {
					if appendReached || d.CommandWALNextLSN() != before {
						t.Fatal("finalizer observed accepted command")
					}
					ops, err := commitlog.DecodeRawKVBatchPayload(payload)
					if err != nil {
						return err
					}
					rid, found := lookup(entry.ValuePtr)
					if len(ops) != 1 || ops[0].Op != wantOp || ops[0].Revision != 48 || string(ops[0].Key) != "key" || !found || rid == 0 || ops[0].RID != rid {
						t.Fatalf("final metadata lost: ops=%+v rid=%d found=%v", ops, rid, found)
					}
					if !materialized && d.valueLogIdentityPins.ActivePins() <= baselinePins {
						t.Fatal("finalizer ran before external dependency closure was acquired")
					}
					canonical = append([]byte(nil), payload...)
					finalized = true
					return nil
				},
				func(emit func(batchpkg.Entry) error) error { return emit(entry) }, 1, mode)
			if err != nil || lsn != before || !finalized || !appendReached {
				t.Fatalf("append lsn=%d want=%d finalized=%v reached=%v err=%v", lsn, before, finalized, appendReached, err)
			}
			env := readFirstRawKVCommandWALEnvelope(t, d)
			if !bytes.Equal(env.Payload, canonical) {
				t.Fatal("accepted frame differs from canonical finalizer payload")
			}
		})
	}
}

func TestRawKVPreparedFinalizerRefusalReleasesDependencyWithoutAppend(t *testing.T) {
	d, entry := openCommandWALPointerDependencyTestDB(t)
	defer d.Close()
	baselinePins, before := d.valueLogIdentityPins.ActivePins(), d.CommandWALNextLSN()
	want := errors.New("candidate live-lease refusal")
	called := false
	lsn, err := d.AppendRawKVCommandWALOrderedEntryScanWithHintPreparedFinalizedAndMode(
		func() error { entry.Revision = 49; return nil },
		func(_ []byte, lookup func(page.ValuePtr) (uint64, bool)) error {
			called = true
			if rid, ok := lookup(entry.ValuePtr); !ok || rid == 0 {
				t.Fatal("RID authority cleared before refusal callback")
			}
			return want
		},
		func(emit func(batchpkg.Entry) error) error { return emit(entry) }, 1, RawKVCommandWALAppendRelaxed)
	if !called || !errors.Is(err, want) || lsn != 0 || d.CommandWALNextLSN() != before {
		t.Fatalf("refusal called=%v lsn=%d next=%d want=%d err=%v", called, lsn, d.CommandWALNextLSN(), before, err)
	}
	if got := d.valueLogIdentityPins.ActivePins(); got != baselinePins {
		t.Fatalf("dependency leaked: pins=%d baseline=%d", got, baselinePins)
	}
}
