package caching

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type cowPrivateDirectFinalizer interface {
	FinalizePrivateCOWPublicationDirect(
		owner *transactionowner.Owner,
		prepare func() error,
		finalize func(scan func(func(commitlog.RawKVOperation) error) error, lookup func(page.ValuePtr) (uint64, bool)) error,
		scanEntries func(func(batch.Entry) error) error,
		opHint int,
	) error
}

type cowDirectCountingBackend struct {
	*backenddb.DB
	directCalls  atomic.Int64
	encodedCalls atomic.Int64
}

func (w *cowDirectCountingBackend) FinalizePrivateCOWPublicationDirect(
	owner *transactionowner.Owner,
	prepare func() error,
	finalize func(scan func(func(commitlog.RawKVOperation) error) error, lookup func(page.ValuePtr) (uint64, bool)) error,
	scanEntries func(func(batch.Entry) error) error,
	opHint int,
) error {
	w.directCalls.Add(1)
	direct, ok := any(w.DB).(cowPrivateDirectFinalizer)
	if !ok {
		return backenddb.ErrNativeSemanticUnsupported
	}
	return direct.FinalizePrivateCOWPublicationDirect(owner, prepare, finalize, scanEntries, opHint)
}

func (w *cowDirectCountingBackend) FinalizePrivateCOWPublication(
	owner *transactionowner.Owner,
	prepare func() error,
	finalize backenddb.RawKVCommandWALFinalize,
	scanEntries func(func(batch.Entry) error) error,
	opHint int,
) error {
	w.encodedCalls.Add(1)
	return w.DB.FinalizePrivateCOWPublication(owner, prepare, finalize, scanEntries, opHint)
}

func openPrivateCOWWALOffFixture(tb testing.TB, dir string, durability backenddb.DurabilityMode, ptrThreshold int) (*DB, *backenddb.DB, *transactionowner.Owner) {
	tb.Helper()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		Durability:             durability,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		tb.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		_ = backend.Close()
		tb.Fatal(err)
	}
	db, err := Open(dir, backend, Options{
		TransactionOwner:         owner,
		DisableWAL:               true,
		AllowUnsafe:              true,
		MemtableMode:             "cow_btree",
		MemtableShards:           4,
		FlushThreshold:           1 << 30,
		ValueLogPointerThreshold: ptrThreshold,
		ValueLogCompression:      uint8(vlogCompressionOff),
	})
	if err != nil {
		_ = backend.Close()
		tb.Fatal(err)
	}
	return db, backend, owner
}

func TestCOWBatchWALOffInlineDeletePointerReopen(t *testing.T) {
	dir := t.TempDir()
	db, backend, owner := openPrivateCOWWALOffFixture(t, dir, backenddb.DurabilityWALOffRelaxed, 64)
	if _, ok := any(backend).(cowPrivateDirectFinalizer); !ok {
		_ = db.Close()
		t.Fatalf("backend *db.DB does not implement FinalizePrivateCOWPublicationDirect")
	}
	wrapper := &cowDirectCountingBackend{DB: backend}
	db.backend = wrapper
	_ = owner

	valInlineA := []byte("inline-a-v1")
	valInlineB := []byte("inline-b-v1")
	valPtrA := bytes.Repeat([]byte("ptr-a-v1-"), 32)
	valPtrB := bytes.Repeat([]byte("ptr-b-v1-"), 32)

	// Round 1: mixed inline puts + pointer puts.
	b := db.NewBatchWithSize(8)
	for _, kv := range []struct {
		k string
		v []byte
	}{
		{"k-inline-keep", valInlineA},
		{"k-inline-del", valInlineB},
		{"k-inline-to-ptr", []byte("small-before")},
		{"k-ptr-keep", valPtrA},
		{"k-ptr-del", valPtrB},
		{"k-ptr-to-inline", bytes.Repeat([]byte("big-before-"), 32)},
	} {
		if err := b.Set([]byte(kv.k), kv.v); err != nil {
			_ = b.Close()
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("round 1 write: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if wrapper.directCalls.Load() != 1 || wrapper.encodedCalls.Load() != 0 {
		_ = db.Close()
		t.Fatalf("round 1 finalizer calls direct=%d encoded=%d, want 1/0", wrapper.directCalls.Load(), wrapper.encodedCalls.Load())
	}

	snapBeforeRound2 := db.AcquireSnapshot()

	// Round 2: inline puts + deletes (inline, pointer, missing) + pointer puts + updates.
	valInlineNew := []byte("inline-new-v2")
	valPtrNew := bytes.Repeat([]byte("ptr-new-v2-"), 40)
	valPromotedPtr := bytes.Repeat([]byte("promoted-to-ptr-"), 32)
	valDemotedInline := []byte("demoted-inline")

	b2 := db.NewBatchWithSize(8)
	if err := b2.Delete([]byte("k-inline-del")); err != nil {
		t.Fatal(err)
	}
	if err := b2.Delete([]byte("k-ptr-del")); err != nil {
		t.Fatal(err)
	}
	if err := b2.Delete([]byte("k-missing-del")); err != nil {
		t.Fatal(err)
	}
	if err := b2.Set([]byte("k-inline-to-ptr"), valPromotedPtr); err != nil {
		t.Fatal(err)
	}
	if err := b2.Set([]byte("k-ptr-to-inline"), valDemotedInline); err != nil {
		t.Fatal(err)
	}
	if err := b2.Set([]byte("k-inline-new"), valInlineNew); err != nil {
		t.Fatal(err)
	}
	if err := b2.Set([]byte("k-ptr-new"), valPtrNew); err != nil {
		t.Fatal(err)
	}
	if err := b2.Write(); err != nil {
		_ = b2.Close()
		_ = snapBeforeRound2.Close()
		_ = db.Close()
		t.Fatalf("round 2 write: %v", err)
	}
	if err := b2.Close(); err != nil {
		_ = snapBeforeRound2.Close()
		_ = db.Close()
		t.Fatal(err)
	}
	if wrapper.directCalls.Load() != 2 || wrapper.encodedCalls.Load() != 0 {
		_ = snapBeforeRound2.Close()
		_ = db.Close()
		t.Fatalf("round 2 finalizer calls direct=%d encoded=%d, want 2/0", wrapper.directCalls.Load(), wrapper.encodedCalls.Load())
	}

	// Verify old snapshot still sees pre-round-2 state.
	if got, err := snapBeforeRound2.Get([]byte("k-inline-del")); err != nil || !bytes.Equal(got, valInlineB) {
		_ = snapBeforeRound2.Close()
		_ = db.Close()
		t.Fatalf("old snapshot k-inline-del = %q err=%v, want %q", got, err, valInlineB)
	}
	if got, err := snapBeforeRound2.Get([]byte("k-ptr-del")); err != nil || !bytes.Equal(got, valPtrB) {
		_ = snapBeforeRound2.Close()
		_ = db.Close()
		t.Fatalf("old snapshot k-ptr-del mismatch err=%v", err)
	}
	if got, err := snapBeforeRound2.Get([]byte("k-inline-new")); (err != nil && !errors.Is(err, tree.ErrKeyNotFound)) || got != nil {
		_ = snapBeforeRound2.Close()
		_ = db.Close()
		t.Fatalf("old snapshot saw new key: %q err=%v", got, err)
	}
	if err := snapBeforeRound2.Close(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}

	// Verify syncWrite stays on encoded path (does NOT call direct finalizer).
	bSync := db.NewBatchWithSize(2)
	if err := bSync.Set([]byte("k-sync-inline"), []byte("sync-val")); err != nil {
		t.Fatal(err)
	}
	if err := bSync.WriteSync(); err != nil {
		_ = bSync.Close()
		_ = db.Close()
		t.Fatalf("sync write: %v", err)
	}
	if err := bSync.Close(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if wrapper.directCalls.Load() != 2 || wrapper.encodedCalls.Load() != 1 {
		_ = db.Close()
		t.Fatalf("after WriteSync calls direct=%d encoded=%d, want 2/1", wrapper.directCalls.Load(), wrapper.encodedCalls.Load())
	}

	wantLive := map[string][]byte{
		"k-inline-keep":   valInlineA,
		"k-ptr-keep":      valPtrA,
		"k-inline-to-ptr": valPromotedPtr,
		"k-ptr-to-inline": valDemotedInline,
		"k-inline-new":    valInlineNew,
		"k-ptr-new":       valPtrNew,
		"k-sync-inline":   []byte("sync-val"),
	}
	wantAbsent := []string{"k-inline-del", "k-ptr-del", "k-missing-del"}

	verifyState := func(label string, target *DB) {
		t.Helper()
		for k, want := range wantLive {
			got, err := target.Get([]byte(k))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("%s Get(%q) len=%d err=%v, want len=%d", label, k, len(got), err, len(want))
			}
		}
		for _, k := range wantAbsent {
			got, err := target.Get([]byte(k))
			if (err != nil && !errors.Is(err, tree.ErrKeyNotFound)) || got != nil {
				t.Fatalf("%s deleted key %q still visible: %q err=%v", label, k, got, err)
			}
		}
	}

	verifyState("live-pre-checkpoint", db)
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint: %v", err)
	}
	verifyState("live-post-checkpoint", db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Fresh reopen and read-back verification.
	reopened, _, _ := openPrivateCOWWALOffFixture(t, dir, backenddb.DurabilityWALOffRelaxed, 64)
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("reopened close: %v", err)
		}
	}()
	verifyState("reopened", reopened)
}

func TestCOWBatchWALOffDirectFinalizerErrorsAndGuards(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	direct, ok := any(backend).(cowPrivateDirectFinalizer)
	if !ok {
		_ = backend.Close()
		t.Fatalf("backend *db.DB does not implement FinalizePrivateCOWPublicationDirect")
	}

	rejectPrepare := func() error {
		t.Fatal("ineligible direct call reached prepare()")
		return nil
	}
	rejectFinalize := func(func(func(commitlog.RawKVOperation) error) error, func(page.ValuePtr) (uint64, bool)) error {
		t.Fatal("ineligible direct call reached finalize()")
		return nil
	}
	emptyScan := func(func(batch.Entry) error) error { return nil }
	singleScan := func(emit func(batch.Entry) error) error {
		return emit(batch.Entry{Type: batch.OpPut, Key: []byte("k"), Value: []byte("v"), Revision: 7})
	}

	// Foreign owner / nil owner / nil callbacks rejected BEFORE prepare().
	if err := direct.FinalizePrivateCOWPublicationDirect(transactionowner.New(), rejectPrepare, rejectFinalize, singleScan, 1); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("foreign owner err=%v, want ErrNativeSemanticUnsupported", err)
	}
	if err := direct.FinalizePrivateCOWPublicationDirect(nil, rejectPrepare, rejectFinalize, singleScan, 1); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("nil owner err=%v, want ErrNativeSemanticUnsupported", err)
	}
	if err := direct.FinalizePrivateCOWPublicationDirect(owner, nil, rejectFinalize, singleScan, 1); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("nil prepare err=%v, want ErrNativeSemanticUnsupported", err)
	}
	if err := direct.FinalizePrivateCOWPublicationDirect(owner, rejectPrepare, nil, singleScan, 1); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("nil finalize err=%v, want ErrNativeSemanticUnsupported", err)
	}
	if err := direct.FinalizePrivateCOWPublicationDirect(owner, rejectPrepare, rejectFinalize, nil, 1); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("nil scanEntries err=%v, want ErrNativeSemanticUnsupported", err)
	}

	// Empty scan (plan.Count == 0) returns nil without calling finalize.
	preparedEmpty := false
	if err := direct.FinalizePrivateCOWPublicationDirect(owner, func() error {
		preparedEmpty = true
		return nil
	}, rejectFinalize, emptyScan, 0); err != nil || !preparedEmpty {
		t.Fatalf("empty scan err=%v prepared=%v, want nil/true", err, preparedEmpty)
	}

	// Closed DB returns ErrClosed before prepare().
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if err := direct.FinalizePrivateCOWPublicationDirect(owner, rejectPrepare, rejectFinalize, singleScan, 1); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("closed DB err=%v, want ErrClosed", err)
	}

	// Read-only DB cannot bind a private COW owner and rejects direct calls before prepare().
	roBackend, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		ReadOnly:               true,
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer roBackend.Close()
	roDirect := any(roBackend).(cowPrivateDirectFinalizer)
	if err := roDirect.FinalizePrivateCOWPublicationDirect(owner, rejectPrepare, rejectFinalize, singleScan, 1); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("read-only unbound DB direct err=%v, want ErrNativeSemanticUnsupported", err)
	}
	if err := roBackend.FinalizeRawKVEntryScanForCachedPublication(rejectPrepare, func([]byte, func(page.ValuePtr) (uint64, bool)) error {
		t.Fatal("read-only DB reached finalize()")
		return nil
	}, singleScan, 1); !errors.Is(err, backenddb.ErrReadOnly) {
		t.Fatalf("read-only DB cached finalizer err=%v, want ErrReadOnly", err)
	}
}

func TestCOWBatchWALOffNonWALOffDurabilityFallback(t *testing.T) {
	dir := t.TempDir()
	// Backend is NOT DurabilityWALOffRelaxed, even though caching DB sets DisableWAL: true.
	db, backend, owner := openPrivateCOWWALOffFixture(t, dir, backenddb.DurabilityWALOnRelaxed, 64)
	direct, ok := any(backend).(cowPrivateDirectFinalizer)
	if !ok {
		_ = db.Close()
		t.Fatalf("backend *db.DB does not implement FinalizePrivateCOWPublicationDirect")
	}

	// Direct call itself must refuse with ErrNativeSemanticUnsupported BEFORE prepare().
	if err := direct.FinalizePrivateCOWPublicationDirect(owner, func() error {
		t.Fatal("non-WAL-off durability reached prepare() in direct finalizer")
		return nil
	}, func(func(func(commitlog.RawKVOperation) error) error, func(page.ValuePtr) (uint64, bool)) error {
		t.Fatal("non-WAL-off durability reached finalize() in direct finalizer")
		return nil
	}, func(emit func(batch.Entry) error) error {
		return emit(batch.Entry{Type: batch.OpPut, Key: []byte("k"), Value: []byte("v"), Revision: 1})
	}, 1); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		_ = db.Close()
		t.Fatalf("non-WAL-off direct call err=%v, want ErrNativeSemanticUnsupported", err)
	}

	wrapper := &cowDirectCountingBackend{DB: backend}
	db.backend = wrapper

	ptrVal := bytes.Repeat([]byte("fallback-ptr-"), 32)
	b := db.NewBatchWithSize(4)
	if err := b.Set([]byte("fb-inline"), []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("fb-ptr"), ptrVal); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete([]byte("fb-del")); err != nil {
		t.Fatal(err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("fallback batch write failed: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if wrapper.directCalls.Load() != 1 || wrapper.encodedCalls.Load() != 1 {
		_ = db.Close()
		t.Fatalf("fallback calls direct=%d encoded=%d, want 1/1", wrapper.directCalls.Load(), wrapper.encodedCalls.Load())
	}
	if got, err := db.Get([]byte("fb-inline")); err != nil || string(got) != "ok" {
		_ = db.Close()
		t.Fatalf("fb-inline=%q err=%v", got, err)
	}
	if got, err := db.Get([]byte("fb-ptr")); err != nil || !bytes.Equal(got, ptrVal) {
		_ = db.Close()
		t.Fatalf("fb-ptr mismatch err=%v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCOWBatchWALOffCanonicalValidationMismatches(t *testing.T) {
	ptr := page.ValuePtr{FileID: 11, Offset: 256, Length: 128}
	baseEntries := []batch.Entry{
		{Type: batch.OpPut, Key: []byte("inline"), Value: []byte("val"), Revision: 10},
		{Type: batch.OpPut, Key: []byte("ext-ptr"), IsPtr: true, ValuePtr: ptr, Revision: 11},
		{Type: batch.OpPut, Key: []byte("mat-ptr"), Value: []byte("owned-val"), IsPtr: true, ValuePtr: ptr, Revision: 12},
		{Type: batch.OpDelete, Key: []byte("del"), Revision: 13},
	}
	baseOps := []commitlog.RawKVOperation{
		{Op: commitlog.RawKVOpSet, Key: []byte("inline"), Value: []byte("val"), Revision: 10},
		{Op: commitlog.RawKVOpSetRID, Key: []byte("ext-ptr"), RID: 99, Revision: 11},
		{Op: commitlog.RawKVOpSetMaterializedRID, Key: []byte("mat-ptr"), Value: []byte("owned-val"), RID: 99, Revision: 12},
		{Op: commitlog.RawKVOpDelete, Key: []byte("del"), Revision: 13},
	}
	lookup := func(p page.ValuePtr) (uint64, bool) { return 99, p == ptr }
	scanSlice := func(ops []commitlog.RawKVOperation) func(func(commitlog.RawKVOperation) error) error {
		return func(emit func(commitlog.RawKVOperation) error) error {
			for i := range ops {
				if err := emit(ops[i]); err != nil {
					return err
				}
			}
			return nil
		}
	}

	// Happy path must pass.
	if err := validateCOWCanonicalOperationStream(scanSlice(baseOps), baseEntries, lookup); err != nil {
		t.Fatalf("valid stream rejected: %v", err)
	}

	// Negative mismatch cases.
	for _, tc := range []struct {
		name  string
		alter func(entries []batch.Entry, ops []commitlog.RawKVOperation) ([]batch.Entry, []commitlog.RawKVOperation, func(page.ValuePtr) (uint64, bool))
	}{
		{
			name: "wrong-key",
			alter: func(e []batch.Entry, o []commitlog.RawKVOperation) ([]batch.Entry, []commitlog.RawKVOperation, func(page.ValuePtr) (uint64, bool)) {
				o[0].Key = []byte("other")
				return e, o, lookup
			},
		},
		{
			name: "wrong-inline-value",
			alter: func(e []batch.Entry, o []commitlog.RawKVOperation) ([]batch.Entry, []commitlog.RawKVOperation, func(page.ValuePtr) (uint64, bool)) {
				o[0].Value = []byte("tampered")
				return e, o, lookup
			},
		},
		{
			name: "inline-nonzero-rid",
			alter: func(e []batch.Entry, o []commitlog.RawKVOperation) ([]batch.Entry, []commitlog.RawKVOperation, func(page.ValuePtr) (uint64, bool)) {
				o[0].RID = 1
				return e, o, lookup
			},
		},
		{
			name: "wrong-revision",
			alter: func(e []batch.Entry, o []commitlog.RawKVOperation) ([]batch.Entry, []commitlog.RawKVOperation, func(page.ValuePtr) (uint64, bool)) {
				o[0].Revision++
				return e, o, lookup
			},
		},
		{
			name: "wrong-rid",
			alter: func(e []batch.Entry, o []commitlog.RawKVOperation) ([]batch.Entry, []commitlog.RawKVOperation, func(page.ValuePtr) (uint64, bool)) {
				o[1].RID = 100
				return e, o, lookup
			},
		},
		{
			name: "zero-rid",
			alter: func(e []batch.Entry, o []commitlog.RawKVOperation) ([]batch.Entry, []commitlog.RawKVOperation, func(page.ValuePtr) (uint64, bool)) {
				o[1].RID = 0
				return e, o, lookup
			},
		},
		{
			name: "setrid-nonempty-value",
			alter: func(e []batch.Entry, o []commitlog.RawKVOperation) ([]batch.Entry, []commitlog.RawKVOperation, func(page.ValuePtr) (uint64, bool)) {
				o[1].Value = []byte("unexpected")
				return e, o, lookup
			},
		},
		{
			name: "pointer-lookup-miss",
			alter: func(e []batch.Entry, o []commitlog.RawKVOperation) ([]batch.Entry, []commitlog.RawKVOperation, func(page.ValuePtr) (uint64, bool)) {
				e[1].ValuePtr.Offset++
				return e, o, lookup
			},
		},
		{
			name: "pointer-nil-lookup",
			alter: func(e []batch.Entry, o []commitlog.RawKVOperation) ([]batch.Entry, []commitlog.RawKVOperation, func(page.ValuePtr) (uint64, bool)) {
				return e, o, nil
			},
		},
		{
			name: "materialized-wrong-value",
			alter: func(e []batch.Entry, o []commitlog.RawKVOperation) ([]batch.Entry, []commitlog.RawKVOperation, func(page.ValuePtr) (uint64, bool)) {
				o[2].Value = []byte("wrong")
				return e, o, lookup
			},
		},
		{
			name: "delete-wrong-op",
			alter: func(e []batch.Entry, o []commitlog.RawKVOperation) ([]batch.Entry, []commitlog.RawKVOperation, func(page.ValuePtr) (uint64, bool)) {
				o[3].Op = commitlog.RawKVOpSet
				return e, o, lookup
			},
		},
		{
			name: "delete-nonempty-value",
			alter: func(e []batch.Entry, o []commitlog.RawKVOperation) ([]batch.Entry, []commitlog.RawKVOperation, func(page.ValuePtr) (uint64, bool)) {
				o[3].Value = []byte("v")
				return e, o, lookup
			},
		},
		{
			name: "delete-nonzero-rid",
			alter: func(e []batch.Entry, o []commitlog.RawKVOperation) ([]batch.Entry, []commitlog.RawKVOperation, func(page.ValuePtr) (uint64, bool)) {
				o[3].RID = 5
				return e, o, lookup
			},
		},
		{
			name: "unsupported-entry-type",
			alter: func(e []batch.Entry, o []commitlog.RawKVOperation) ([]batch.Entry, []commitlog.RawKVOperation, func(page.ValuePtr) (uint64, bool)) {
				e[0].Type = batch.OpDeleteRange
				return e, o, lookup
			},
		},
		{
			name: "op-count-underflow",
			alter: func(e []batch.Entry, o []commitlog.RawKVOperation) ([]batch.Entry, []commitlog.RawKVOperation, func(page.ValuePtr) (uint64, bool)) {
				return e, o[:3], lookup
			},
		},
		{
			name: "op-count-overflow",
			alter: func(e []batch.Entry, o []commitlog.RawKVOperation) ([]batch.Entry, []commitlog.RawKVOperation, func(page.ValuePtr) (uint64, bool)) {
				return e[:3], o, lookup
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entries := append([]batch.Entry(nil), baseEntries...)
			ops := append([]commitlog.RawKVOperation(nil), baseOps...)
			entries, ops, fn := tc.alter(entries, ops)
			if err := validateCOWCanonicalOperationStream(scanSlice(ops), entries, fn); err == nil {
				t.Fatalf("case %s accepted tampered stream", tc.name)
			}
		})
	}

	if err := validateCOWCanonicalOperationStream(nil, baseEntries, lookup); err == nil {
		t.Fatal("nil scanner accepted")
	}

	// Unprepared batch must reject finalizeCOWPublicationDirect.
	unprepared := &Batch{entries: baseEntries, cowState: &cowBatchState{}}
	if err := unprepared.finalizeCOWPublicationDirect(scanSlice(baseOps), lookup); err == nil {
		t.Fatal("unprepared direct finalizer accepted")
	}

	// Refusal during FinalizePrivateCOWPublicationDirect cancels private roots and refunds leases.
	dir := t.TempDir()
	db, backend, owner := openPrivateCOWWALOffFixture(t, dir, backenddb.DurabilityWALOffRelaxed, 64)
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	c := db.cow
	b := &Batch{
		db:       db,
		cowState: &cowBatchState{inPublication: true},
		entries:  []batch.Entry{{Type: batch.OpPut, Key: []byte("k-cancel"), Value: []byte("v-cancel"), Revision: 29}},
	}
	c.writerMu.Lock()
	if err := c.ensureTxnJournal(); err != nil {
		c.writerMu.Unlock()
		t.Fatal(err)
	}
	beforeLeases := c.budget.Stats().ExternalLeases
	err := backend.FinalizePrivateCOWPublicationDirect(owner, b.PrepareExternalCommandWALPublication, func(scan func(func(commitlog.RawKVOperation) error) error, lk func(page.ValuePtr) (uint64, bool)) error {
		tamperedScan := func(emit func(commitlog.RawKVOperation) error) error {
			return scan(func(op commitlog.RawKVOperation) error {
				op.Revision++
				return emit(op)
			})
		}
		return b.finalizeCOWPublicationDirect(tamperedScan, lk)
	}, b.Replay, 1)
	p, cancelErr := b.cancelCOWPublication()
	c.writerMu.Unlock()
	if cancelErr != nil {
		t.Errorf("cancel err: %v", cancelErr)
	}
	if p != nil {
		if drainErr := p.drainCancelled(); drainErr != nil {
			t.Errorf("drain err: %v", drainErr)
		}
	}
	if err == nil {
		t.Fatal("mismatched revision accepted via direct finalizer")
	}
	if got := c.budget.Stats().ExternalLeases; got != beforeLeases {
		t.Fatalf("external leases=%d want=%d", got, beforeLeases)
	}
	if got, err := db.Get([]byte("k-cancel")); (err != nil && !errors.Is(err, tree.ErrKeyNotFound)) || got != nil {
		t.Fatalf("cancelled key visible: %q err=%v", got, err)
	}
}

func BenchmarkCOWBatchWALOffFinalize(b *testing.B) {
	const batchSize = 64
	cases := []struct {
		name         string
		ptrThreshold int
		valueLen     int
		withDeletes  bool
	}{
		{name: "Inline64", ptrThreshold: 1 << 20, valueLen: 128, withDeletes: false},
		{name: "MixedPointerDelete64", ptrThreshold: 64, valueLen: 256, withDeletes: true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			db, _, _ := openPrivateCOWWALOffFixture(b, dir, backenddb.DurabilityWALOffRelaxed, tc.ptrThreshold)
			defer func() {
				if err := db.Close(); err != nil {
					b.Errorf("close: %v", err)
				}
			}()

			keys := make([][]byte, batchSize)
			vals := make([][]byte, batchSize)
			for i := 0; i < batchSize; i++ {
				keys[i] = []byte(fmt.Sprintf("bench-key-%06d", i))
				v := bytes.Repeat([]byte{byte('a' + (i % 26))}, tc.valueLen)
				if tc.withDeletes && i%4 == 0 {
					v = v[:32] // inline
				}
				vals[i] = v
			}

			batchObj := db.NewBatchWithSize(batchSize)
			defer func() {
				if err := batchObj.Close(); err != nil {
					b.Errorf("batch close: %v", err)
				}
			}()

			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				for i := 0; i < batchSize; i++ {
					if tc.withDeletes && i%8 == 7 {
						if err := batchObj.Delete(keys[i]); err != nil {
							b.Fatal(err)
						}
						continue
					}
					if err := batchObj.Set(keys[i], vals[i]); err != nil {
						b.Fatal(err)
					}
				}
				if err := batchObj.Write(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
