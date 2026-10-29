package collections

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

type nativeTextStateEffectTable struct {
	memtable.Table
	effects *int
}

func (t *nativeTextStateEffectTable) SetSteal(k, v []byte) { (*t.effects)++; t.Table.SetSteal(k, v) }
func (t *nativeTextStateEffectTable) DeleteSteal(k []byte) { (*t.effects)++; t.Table.DeleteSteal(k) }
func (t *nativeTextStateEffectTable) Release()             { resetCollectionRunTable(t.Table) }

// A tiny real primary row and a larger parseable persisted state deliberately
// violate publisher coherence. The original finite allocator must refuse the
// restored general promise before any reducer key reaches a mutation table.
func TestNativeTextMismatchedStateOriginalParentBeforeEffects(t *testing.T) {
	var budget nativeHNSWAdmissionBudget
	t.Cleanup(func() {
		if budget != nil {
			if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Errorf("full Close leaked: %+v", stats)
			}
		}
	})
	_, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true, TextIndexVersionV1)
	id := []byte("tiny")
	document := []byte(`{"content":"a"}`)
	if _, err := c.Insert(id, document); err != nil {
		t.Fatal(err)
	}
	forged := textDocumentStateValue{Fields: []textDocumentFieldState{{Field: "content"}}}
	for i := 0; i < 96; i++ {
		forged.Fields[0].Terms = append(forged.Fields[0].Terms, textDocumentTermState{Term: fmt.Sprintf("term%03d%s", i, strings.Repeat("x", 80))})
	}
	for i := 0; i < 32; i++ {
		forged.Fields = append(forged.Fields, textDocumentFieldState{Field: fmt.Sprintf("extra%03d%s", i, strings.Repeat("f", 80))})
	}
	raw := encodeTextDocumentStateValue(forged)
	// Compare with the maintained decoder: zero-length decoded term lanes
	// own empty slices, while the fault builder leaves those lanes nil.
	wantDecoded, err := decodeTextDocumentStateValue(raw)
	if err != nil {
		t.Fatal(err)
	}
	stateRoot := collectionTextStateRootName(c.name, "content")
	key := encodeTextStateKey(id)
	// Install only the test fault through the real conditional native owner.
	tx, err := c.nativeJSON.db.BeginNativeConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer tx.Close()
		base := tx.State().(*nativeEngineVersion)
		current, ok := base.names.Get(c.name)
		if !ok {
			t.Fatal("missing collection")
		}
		name := strings.TrimPrefix(stateRoot, c.name+"/")
		domain, ok := current.domains.Get(name)
		if !ok {
			t.Fatal("missing state domain")
		}
		next, err := prepareNativeNamedDomains(base, []nativeNamedCollectionChange{{collection: current, sourceGeneration: current.documentGeneration, domains: []nativeNamedDomainChange{{name: name, expectedInterpretation: domain.interpretation, entries: []memtable.COWMutation{{Key: key, Value: raw, Flags: node.FlagInline}}}}}})
		if err != nil {
			if next != nil {
				err = errors.Join(err, finishPrivateNative(next.Release()))
			}
			t.Fatal(err)
		}
		pub := &backenddb.NativePreparedPublication{Basis: base, State: next, StartSequence: tx.LogicalSequence()}
		result, err := tx.CommitNative(pub, nil, false)
		if !result.Consumed {
			err = errors.Join(err, cancelPrivateNativePublication(pub))
		}
		if err != nil || !result.LogicalInstalled {
			t.Fatalf("fault install=%+v err=%v", result, err)
		}
	}()
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	budget = read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	// Bind the existing read scratch before selecting the precise codec boundary;
	// that independently admitted read resource remains held through the attempt.
	observed, err := read.namedDomainGet(strings.TrimPrefix(stateRoot, c.name+"/"), key)
	if err != nil || !bytes.Equal(observed, raw) {
		t.Fatalf("fault read: %v", err)
	}
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	pressure, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer pressure.Close()
	var charged uint64
	var refused error
	var refusedBytes uint64
	var refusedAt memtable.COWStats
	admit := func(n uint64) error {
		if n > ^uint64(0)-charged {
			return memtable.ErrCOWCapacity
		}
		if err := lease.Resize(charged + n); err != nil {
			refused = err
			refusedBytes = n
			refusedAt = budget.Stats()
			return err
		}
		charged += n
		return nil
	}
	catalog, err := newNativeTextCatalog(read, admit)
	if err != nil {
		t.Fatal(err)
	}
	catalog.nativeText.mutationCodecs = true
	opts, err := collectionPlannerOptions(read.meta)
	if err != nil {
		t.Fatal(err)
	}
	opts.textMaterializationAdmit = admit
	effects := 0
	opts.textTableFactory = func(n int) memtable.Table {
		return &nativeTextStateEffectTable{Table: nativeTextTable(n), effects: &effects}
	}
	if err := admit(2*memtable.COWAllocationCharge(uint64(unsafe.Sizeof(textDocumentStateValue{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(textDocumentMutation{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeTextPreparation{})))); err != nil {
		t.Fatal(err)
	}
	mutations := []textDocumentMutation{{documentID: id, oldDocument: document, deleteOld: true}}
	preparation := nativeTextPreparation{states: make([]textDocumentStateValue, 1), openingStates: make([]textDocumentStateValue, 1), mutations: mutations}
	rows := []nativeJSONRow{{id: id, opening: document}}
	remaining, err := nativeTextSourceChargePrepared(read.meta, rows, opts, admit, &preparation)
	if err != nil {
		t.Fatal(err)
	}
	if err := admit(remaining); err != nil {
		t.Fatal(err)
	}
	if mutations[0].preparedOld != &preparation.openingStates[0] || textDocumentStatesEqual(forged, *mutations[0].preparedOld) {
		t.Fatal("mismatch control not bound to prepared opening")
	}
	var decodeCharge uint64
	if _, err := decodeTextDocumentStateValueAdmitted(raw, func(n uint64) error { decodeCharge += n; return nil }); err != nil {
		t.Fatal(err)
	}
	prefix := memtable.COWAllocationCharge(uint64(len(raw))) + decodeCharge
	var generalCharge uint64
	original := nativeTextCatalog{admit: func(n uint64) error { generalCharge = n; return nil }}
	if err := original.admitRecord(memtable.COWAllocationCharge(uint64(len(raw)))); err != nil {
		t.Fatal(err)
	}
	// Acquire/resize the unrelated pressure before the target reducer starts.
	available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
	if prefix > available {
		t.Fatal("fixture decoder cannot fit finite original parent")
	}
	if err := pressure.Resize(available - prefix); err != nil {
		t.Fatal(err)
	}
	before := charged
	var names []string
	var policies []backenddb.OrderedRootStoragePolicy
	var tables []memtable.Table
	err = appendSingleTextIndexMutationDeltas(read.capture.Snapshot(), catalog, opts, read.meta.TextIndexes[0], mutations, &names, nil, &policies, &tables)
	resetCollectionTables(tables)
	if !errors.Is(err, memtable.ErrCOWCapacity) || err != refused || errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("original refusal lost: err=%v refused=%v", err, refused)
	}
	if refusedBytes != generalCharge {
		t.Fatalf("refused different boundary: bytes=%d want original general=%d", refusedBytes, generalCharge)
	}
	// Independent owner leases may end during reducer cleanup. The actual
	// refused callback must lack room for its full original general promise.
	if effects != 0 || len(names) != 0 || len(tables) != 0 || charged != before+prefix || refusedAt.ReservedBytes > budget.Limits().MaxInFlightBytes || refusedBytes <= budget.Limits().MaxInFlightBytes-refusedAt.ReservedBytes {
		t.Fatalf("refusal effects=%d prefix=%d want=%d at=%+v after=%+v", effects, charged-before, prefix, refusedAt, budget.Stats())
	}
	pressure.Close()
	// Removing real pressure lets the SAME actual codec load retain the complete
	// mismatched state. Admission must not turn parseable input into corruption.
	retryBefore := charged
	actual, err := loadTextDocumentStateForMutation(read.capture.Snapshot(), catalog, read.meta.TextIndexes[0], id, document, opts, mutations[0].preparedOld)
	if err != nil || !reflect.DeepEqual(actual, wantDecoded) {
		t.Fatalf("retry state=%+v err=%v", actual, err)
	}
	if charged != retryBefore+prefix+generalCharge {
		t.Fatalf("retry promise=%d want=%d", charged-retryBefore, prefix+generalCharge)
	}
	current, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	if current.collection != read.collection {
		t.Fatal("refused reducer partially published")
	}
	got, err := current.Get(id)
	if err != nil || !bytes.Equal(got, document) {
		t.Fatalf("primary changed: %q err=%v", got, err)
	}
	stored, err := current.namedDomainGet(strings.TrimPrefix(stateRoot, c.name+"/"), key)
	if err != nil || !bytes.Equal(stored, raw) {
		t.Fatalf("persisted state changed: err=%v", err)
	}
	clear(stored)
	if !reflect.DeepEqual(actual, wantDecoded) {
		t.Fatal("decoded retry aliases source")
	}
}
