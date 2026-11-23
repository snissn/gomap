package db

import (
	"bytes"
	"errors"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestPrivateCOWInlinePointFinalizerRequiresOriginalOwner(t *testing.T) {
	d, err := OpenForNativeSemanticOwner(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	owner := transactionowner.New()
	if err := d.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	entry := batchpkg.Entry{Type: batchpkg.OpPut, Key: []byte("key"), Value: []byte("value"), Revision: 42}
	rejectPreparation := func() error {
		t.Fatal("ineligible point reached preparation")
		return nil
	}
	finalize := func(commitlog.RawKVOperation) error {
		t.Fatal("ineligible point reached finalization")
		return nil
	}
	if err := d.FinalizePrivateCOWInlinePoint(transactionowner.New(), entry, rejectPreparation, finalize); !errors.Is(err, ErrNativeSemanticUnsupported) {
		t.Fatalf("foreign owner error = %v", err)
	}
	for _, invalid := range []batchpkg.Entry{
		{Type: batchpkg.OpPut, IsPtr: true, Revision: 42},
		{Type: batchpkg.OpPut, Revision: page.LegacyEntryRevision},
		{Type: batchpkg.OpDelete, Value: []byte("value"), Revision: 42},
		{Type: batchpkg.OpDeleteRange, Revision: 42},
	} {
		if err := d.FinalizePrivateCOWInlinePoint(owner, invalid, rejectPreparation, finalize); !errors.Is(err, ErrNativeSemanticUnsupported) {
			t.Fatalf("ineligible entry %+v error = %v", invalid, err)
		}
	}
}

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

func TestRawKVPreparedFinalizerWithoutWALUsesSameProducerIdentity(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	appender, err := newReplayInlineAppender(d, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer appender.close()
	d.SetValueLogAppender(appender)
	defer d.SetValueLogAppender(nil)
	entry := appendCommandWALPointerEntry(t, d, []byte("no-wal"), bytes.Repeat([]byte("v"), 256))
	entry.Value = nil
	baselinePins := d.valueLogIdentityPins.ActivePins()
	called := false
	lsn, err := d.AppendRawKVCommandWALOrderedEntryScanWithHintPreparedFinalizedAndMode(
		func() error { entry.Revision = 50; return nil },
		func(payload []byte, lookup func(page.ValuePtr) (uint64, bool)) error {
			called = true
			if got := d.valueLogIdentityPins.ActivePins(); got != baselinePins {
				t.Fatalf("WAL-off finalizer acquired journal-custody pins: %d want %d", got, baselinePins)
			}
			ops, err := commitlog.DecodeRawKVBatchPayload(payload)
			if err != nil {
				return err
			}
			rid, found := lookup(entry.ValuePtr)
			if len(ops) != 1 || ops[0].Op != commitlog.RawKVOpSetRID || ops[0].Revision != 50 || !found || rid == 0 || ops[0].RID != rid {
				t.Fatalf("no-WAL identity lost: ops=%+v rid=%d found=%v", ops, rid, found)
			}
			return nil
		},
		func(emit func(batchpkg.Entry) error) error { return emit(entry) }, 1, RawKVCommandWALAppendRelaxed)
	if err != nil || !called || lsn != 0 || d.commandJournal != nil {
		t.Fatalf("no-WAL finalization: called=%v lsn=%d journal=%v err=%v", called, lsn, d.commandJournal != nil, err)
	}
	if got := d.valueLogIdentityPins.ActivePins(); got != baselinePins {
		t.Fatalf("temporary no-WAL dependency leaked: pins=%d baseline=%d", got, baselinePins)
	}
}

func TestPrivateCOWPublicationDirectPointerInlineDelete(t *testing.T) {
	dir := t.TempDir()
	d, err := OpenForNativeSemanticOwner(Options{Dir: dir, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := d.BindPrivateCOWOwner(owner); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	appender, err := newReplayInlineAppender(d, nil, nil)
	if err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	defer appender.close()
	d.SetValueLogAppender(appender)
	defer d.SetValueLogAppender(nil)

	ptrEntry := appendCommandWALPointerEntry(t, d, []byte("ptr-key"), bytes.Repeat([]byte("p"), 256))
	ptrEntry.Value = nil
	entries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: []byte("inline-key"), Value: []byte("inline-val"), Revision: 101},
		ptrEntry,
		{Type: batchpkg.OpDelete, Key: []byte("del-key"), Revision: 103},
	}
	baselinePins := d.valueLogIdentityPins.ActivePins()
	called := false
	err = d.FinalizePrivateCOWPublicationDirect(
		owner,
		func() error {
			entries[1].Revision = 102
			return nil
		},
		func(scan func(func(commitlog.RawKVOperation) error) error, lookup func(page.ValuePtr) (uint64, bool)) error {
			called = true
			if got := d.valueLogIdentityPins.ActivePins(); got != baselinePins {
				t.Fatalf("WAL-off direct finalizer acquired journal pins: %d want %d", got, baselinePins)
			}
			var ops []commitlog.RawKVOperation
			if err := scan(func(op commitlog.RawKVOperation) error {
				ops = append(ops, op)
				return nil
			}); err != nil {
				return err
			}
			if len(ops) != 3 {
				t.Fatalf("ops=%d want 3", len(ops))
			}
			if ops[0].Op != commitlog.RawKVOpSet || string(ops[0].Key) != "inline-key" || string(ops[0].Value) != "inline-val" || ops[0].Revision != 101 {
				t.Fatalf("op[0]=%+v", ops[0])
			}
			rid, ok := lookup(entries[1].ValuePtr)
			if ops[1].Op != commitlog.RawKVOpSetRID || string(ops[1].Key) != "ptr-key" || len(ops[1].Value) != 0 || ops[1].Revision != 102 || !ok || rid == 0 || ops[1].RID != rid {
				t.Fatalf("op[1]=%+v rid=%d ok=%v", ops[1], rid, ok)
			}
			if ops[2].Op != commitlog.RawKVOpDelete || string(ops[2].Key) != "del-key" || len(ops[2].Value) != 0 || ops[2].Revision != 103 {
				t.Fatalf("op[2]=%+v", ops[2])
			}
			return nil
		},
		func(emit func(batchpkg.Entry) error) error {
			for i := range entries {
				if err := emit(entries[i]); err != nil {
					return err
				}
			}
			return nil
		},
		len(entries),
	)
	if err != nil || !called {
		_ = d.Close()
		t.Fatalf("direct finalization err=%v called=%v", err, called)
	}
	if got := d.valueLogIdentityPins.ActivePins(); got != baselinePins {
		_ = d.Close()
		t.Fatalf("direct finalization leaked pins: %d want %d", got, baselinePins)
	}

	// Empty scan returns nil without calling finalize.
	if err := d.FinalizePrivateCOWPublicationDirect(owner, func() error { return nil }, func(func(func(commitlog.RawKVOperation) error) error, func(page.ValuePtr) (uint64, bool)) error {
		t.Fatal("empty scan invoked finalize")
		return nil
	}, func(func(batchpkg.Entry) error) error { return nil }, 0); err != nil {
		_ = d.Close()
		t.Fatalf("empty scan err=%v", err)
	}

	// Wrong owner rejected before prepare.
	if err := d.FinalizePrivateCOWPublicationDirect(transactionowner.New(), func() error {
		t.Fatal("foreign owner reached prepare")
		return nil
	}, func(func(func(commitlog.RawKVOperation) error) error, func(page.ValuePtr) (uint64, bool)) error {
		return nil
	}, func(func(batchpkg.Entry) error) error { return nil }, 1); !errors.Is(err, ErrNativeSemanticUnsupported) {
		_ = d.Close()
		t.Fatalf("foreign owner err=%v", err)
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.FinalizePrivateCOWPublicationDirect(owner, func() error {
		t.Fatal("closed db reached prepare")
		return nil
	}, func(func(func(commitlog.RawKVOperation) error) error, func(page.ValuePtr) (uint64, bool)) error {
		return nil
	}, func(func(batchpkg.Entry) error) error { return nil }, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed db err=%v", err)
	}

	// Read-only DB returns ErrReadOnly before prepare().
	ro, err := Open(Options{Dir: dir, ReadOnly: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	roOwner := transactionowner.New()
	ro.privateCOWOwner = roOwner
	if err := ro.FinalizePrivateCOWPublicationDirect(roOwner, func() error {
		t.Fatal("read-only db reached prepare")
		return nil
	}, func(func(func(commitlog.RawKVOperation) error) error, func(page.ValuePtr) (uint64, bool)) error {
		return nil
	}, func(func(batchpkg.Entry) error) error { return nil }, 1); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only db err=%v, want ErrReadOnly", err)
	}
}
