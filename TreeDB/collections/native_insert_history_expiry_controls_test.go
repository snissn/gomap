package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/tree"
)

func historyExpiryFixture(t *testing.T, meta *CollectionMeta) (*treedb.DB, *backenddb.DB, *CollectionManager, *Collection) {
	t.Helper()
	owner, err := treedb.Open(nativeTypedGroupOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err = manager.CreateCollection(meta); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	return owner, backend, manager, col
}

func TestNativeInsertHistoryExpiryRoutes(t *testing.T) {
	for _, route := range []string{"manager", "indexed", "template", "ordinary-graph"} {
		t.Run(route, func(t *testing.T) {
			meta := &CollectionMeta{Name: "target"}
			doc := []byte(`{"city":"east","embedding":[1,0]}`)
			switch route {
			case "indexed":
				meta.Indexes = []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString, Unique: true}}
			case "template":
				meta.Options.DocumentFormat = DocumentFormatTemplateV1
			case "ordinary-graph":
				meta.Options.ColumnStore = &ColumnStoreConfig{Enabled: true, RetainedPayload: ColumnRetainedPayloadFull, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON, Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 2, Owner: TypedStorageOwnerColumnPart}}}
				meta.VectorIndexes = []VectorIndexDefinition{{Name: "vec", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, Strategy: VectorIndexStrategyColumnGraph}}
			}
			expected := bytes.Clone(doc)
			if route == "template" {
				var err error
				doc, err = EncodeTemplateV1DocumentJSON(doc)
				if err != nil {
					t.Fatal(err)
				}
			}
			owner, backend, _, col := historyExpiryFixture(t, meta)
			before := backend.CommandWALNextLSN()
			var admissions atomic.Int32
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Point == durabilitycut.BeforeCOWAdmission && admissions.Add(1) == 1 {
					return backenddb.ErrTxnTooOld
				}
				return nil
			})
			var err error
			var stats CollectionInsertStats
			if route == "manager" {
				err = col.nativeJSON.InsertBatch("target", [][]byte{[]byte("id")}, [][]byte{doc})
			} else {
				_, stats, err = col.InsertBatchWithStats([][]byte{[]byte("id")}, [][]byte{doc})
			}
			restore()
			if err != nil || admissions.Load() != 2 || backend.CommandWALNextLSN() != before+1 {
				t.Fatalf("route=%s attempts=%d LSN=%d before=%d err=%v", route, admissions.Load(), backend.CommandWALNextLSN(), before, err)
			}
			if route != "manager" && (stats.HistoryExpiryRetries != 1 || stats.Documents != 1 || !reflect.DeepEqual(stats, col.LastInsertStats())) {
				t.Fatalf("stats=%+v", stats)
			}
			if route == "indexed" && (stats.SecondaryEntries != 1 || len(stats.SecondaryRuns) != 1 || stats.SecondaryRuns[0].Entries != 1) {
				t.Fatalf("refused plan counted twice: %+v", stats)
			}
			verify := func() {
				t.Helper()
				got, e := col.Get([]byte("id"))
				if route == "template" && e == nil {
					var materializer *StoredDocumentJSONMaterializer
					materializer, e = col.NewStoredDocumentJSONMaterializer()
					if e == nil {
						got, e = materializer.StoredDocumentJSON(got)
						e = errors.Join(e, materializer.Close())
					}
					var actualValue, expectedValue any
					if e != nil || json.Unmarshal(got, &actualValue) != nil || json.Unmarshal(expected, &expectedValue) != nil || !reflect.DeepEqual(actualValue, expectedValue) {
						t.Fatalf("template contents: %s err=%v", got, e)
					}
					return
				}
				if e != nil || !bytes.Equal(got, expected) {
					t.Fatalf("contents: %s err=%v", got, e)
				}
			}
			verify()
			if err = owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			verify()
		})
	}
}

// Exhaustion and fresh duplicate checks use the actual preassignment publication
// seam. Only the primary regression generates real history eviction.
func TestNativeInsertHistoryExpiryExhaustion(t *testing.T) {
	_, backend, _, col := historyExpiryFixture(t, &CollectionMeta{Name: "target"})
	before := backend.CommandWALNextLSN()
	var admissions atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeCOWAdmission {
			admissions.Add(1)
			return backenddb.ErrTxnTooOld
		}
		return nil
	})
	ids, stats, err := col.InsertBatchWithStats([][]byte{[]byte("id")}, [][]byte{[]byte(`{"value":1}`)})
	restore()
	if !errors.Is(err, backenddb.ErrTxnTooOld) || admissions.Load() != 2 || len(ids) != 0 || backend.CommandWALNextLSN() != before || backenddb.CommitPublicationAccepted(err) || !reflect.DeepEqual(stats, CollectionInsertStats{}) || !reflect.DeepEqual(col.LastInsertStats(), CollectionInsertStats{}) {
		t.Fatalf("attempts=%d ids=%q stats=%+v LSN=%d err=%v", admissions.Load(), ids, stats, backend.CommandWALNextLSN(), err)
	}
	if got, err := col.Get([]byte("id")); err != nil || got != nil {
		t.Fatalf("refused row=%s err=%v", got, err)
	}
}

func TestNativeInsertHistoryExpiryFreshDuplicate(t *testing.T) {
	_, backend, _, col := historyExpiryFixture(t, &CollectionMeta{Name: "target"})
	before := backend.CommandWALNextLSN()
	original := []byte(`{"winner":"accepted writer"}`)
	var admissions atomic.Int32
	var winnerNext atomic.Uint64
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeCOWAdmission && admissions.Add(1) == 1 {
			// Shared scalar admission allows a real independent winning insertion.
			if _, err := col.InsertBatch([][]byte{[]byte("id")}, [][]byte{original}); err != nil {
				return err
			}
			// The overlapping durable intents may add a prefix barrier. Bind
			// the actual winner frontier, not an assumed one-LSN request.
			winnerNext.Store(backend.CommandWALNextLSN())
			return backenddb.ErrTxnTooOld
		}
		return nil
	})
	ids, stats, err := col.InsertBatchWithStats([][]byte{[]byte("id")}, [][]byte{[]byte(`{"winner":"must not replace"}`)})
	restore()
	if !errors.Is(err, ErrDocumentExists) || len(ids) != 0 || winnerNext.Load() <= before || backend.CommandWALNextLSN() != winnerNext.Load() || !reflect.DeepEqual(stats, CollectionInsertStats{}) {
		t.Fatalf("ids=%q before=%d winner_next=%d next=%d stats=%+v err=%v", ids, before, winnerNext.Load(), backend.CommandWALNextLSN(), stats, err)
	}
	if got, e := col.Get([]byte("id")); e != nil || !bytes.Equal(got, original) {
		t.Fatalf("winner=%s err=%v", got, e)
	}
}

func TestNativeInsertHistoryExpiryExplicitTransaction(t *testing.T) {
	owner, backend, manager, col := historyExpiryFixture(t, &CollectionMeta{Name: "target"})
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "advance"}); err != nil {
		t.Fatal(err)
	}
	advance, err := manager.OpenCollection("advance")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := col.nativeJSON.Begin("target")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Close(); err != nil {
			t.Error(err)
		}
	}()
	opening := tx.core.LogicalSequence()
	if err := tx.InsertBatch([][]byte{[]byte("id")}, [][]byte{[]byte(`{"value":1}`)}); err != nil {
		t.Fatal(err)
	}
	for start := 0; start < 4097; start += 1024 {
		n := min(1024, 4097-start)
		ids, docs := make([][]byte, n), make([][]byte, n)
		for i := range n {
			ids[i] = []byte(fmt.Sprintf("advance-%05d", start+i))
			docs[i] = []byte(`{"value":true}`)
		}
		if got, err := advance.InsertBatch(ids, docs); err != nil || len(got) != n {
			t.Fatalf("advance ACKs=%d err=%v", len(got), err)
		}
	}
	floor, err := strconv.ParseUint(owner.Stats()["treedb.cache.cow.txn.history_floor"], 10, 64)
	if err != nil || floor <= opening {
		t.Fatalf("floor=%d opening=%d err=%v", floor, opening, err)
	}
	before := backend.CommandWALNextLSN()
	if err := tx.Commit(); !errors.Is(err, backenddb.ErrTxnTooOld) || backenddb.CommitPublicationAccepted(err) {
		t.Fatalf("explicit commit: %v", err)
	}
	if backend.CommandWALNextLSN() != before {
		t.Fatal("expired explicit txn assigned WAL")
	}
	if got, err := col.Get([]byte("id")); err != nil || got != nil {
		t.Fatalf("explicit refused row=%s err=%v", got, err)
	}
}

type historyExpiryResultTxn struct {
	backenddb.NativeConditionalTxn
	result                backenddb.NativePublicationResult
	failure, closeFailure error
}

func (t *historyExpiryResultTxn) CommitNative(*backenddb.NativePreparedPublication, *backenddb.CommandWALIntent, bool) (backenddb.NativePublicationResult, error) {
	return t.result, t.failure
}
func (t *historyExpiryResultTxn) Close() error {
	return errors.Join(t.NativeConditionalTxn.Close(), t.closeFailure)
}

func TestNativeInsertHistoryExpiryReceiptAndCleanup(t *testing.T) {
	cases := []struct {
		name                  string
		result                backenddb.NativePublicationResult
		failure, closeFailure error
		wantRetry             bool
	}{
		{name: "clean-refusal", failure: backenddb.ErrTxnTooOld, wantRetry: true},
		{name: "joined-lower-cleanup", failure: errors.Join(backenddb.ErrTxnTooOld, errors.New("lower close failed"))},
		{name: "sentinel-wrapped-cleanup", failure: backenddb.ErrTxnTooOld, closeFailure: fmt.Errorf("close failure: %w", backenddb.ErrTxnTooOld)},
		{name: "assigned", result: backenddb.NativePublicationResult{Assigned: true, AssignedLSN: 7}, failure: backenddb.ErrTxnTooOld},
		{name: "lsn-only", result: backenddb.NativePublicationResult{AssignedLSN: 7}, failure: backenddb.ErrTxnTooOld},
		{name: "uncertain", result: backenddb.NativePublicationResult{Uncertain: true}, failure: backenddb.ErrTxnTooOld},
		{name: "installed", result: backenddb.NativePublicationResult{LogicalInstalled: true}, failure: backenddb.ErrTxnTooOld},
		{name: "other-error", failure: errors.New("unrelated failure")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, col := historyExpiryFixture(t, &CollectionMeta{Name: "target"})
			tx, err := col.nativeJSON.Begin("target")
			if err != nil {
				t.Fatal(err)
			}
			tx.core = &historyExpiryResultTxn{NativeConditionalTxn: tx.core, result: tc.result, failure: tc.failure, closeFailure: tc.closeFailure}
			_, retry, err := finishNativeJSONInsert(tx, nil, [][]byte{[]byte("id")}, [][]byte{[]byte(`{"value":1}`)}, nil, false, nil)
			if err == nil || retry != tc.wantRetry || !errors.Is(err, tc.failure) {
				t.Fatalf("retry=%v want=%v err=%v", retry, tc.wantRetry, err)
			}
			if tc.closeFailure != nil && !errors.Is(err, tc.closeFailure) {
				t.Fatalf("lost cleanup error: %v", err)
			}
			if got, e := col.nativeJSON.Get("target", []byte("id")); !errors.Is(e, tree.ErrKeyNotFound) || got != nil {
				t.Fatalf("private plan published: %s %v", got, e)
			}
		})
	}
}

func TestNativeInsertHistoryExpiryPlainTypedExclusion(t *testing.T) {
	owner, backend, manager, col := openNativeTypedGroup(t, t.TempDir(), true)
	defer func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	}()
	defer func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	}()
	before := backend.CommandWALNextLSN()
	var admissions atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeCOWAdmission {
			admissions.Add(1)
			return backenddb.ErrTxnTooOld
		}
		return nil
	})
	ids, err := col.InsertBatch([][]byte{[]byte("id")}, [][]byte{[]byte(`{"id":"id","payload":"original","label":"label-id","embedding":[1,2,3]}`)})
	restore()
	if !errors.Is(err, backenddb.ErrTxnTooOld) || admissions.Load() != 1 || len(ids) != 0 || backend.CommandWALNextLSN() != before {
		t.Fatalf("typed exclusion attempts=%d ids=%q LSN=%d err=%v", admissions.Load(), ids, backend.CommandWALNextLSN(), err)
	}
}

func TestNativeInsertHistoryExpiryAfterActualAppend(t *testing.T) {
	owner, err := treedb.Open(nativeTypedGroupOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	// The injected postappend refusal can poison completion; retain and check
	// that exact cleanup outcome rather than treating it as an acknowledged write.
	defer func() {
		if e := owner.Close(); e != nil && !errors.Is(e, backenddb.ErrTxnTooOld) {
			t.Error(e)
		}
	}()
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	defer func() {
		if e := manager.Close(); e != nil && !errors.Is(e, backenddb.ErrTxnTooOld) {
			t.Error(e)
		}
	}()
	if _, err = manager.CreateCollection(&CollectionMeta{Name: "target"}); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("target")
	if err != nil {
		t.Fatal(err)
	}
	before := backend.CommandWALNextLSN()
	var admissions, appends atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeCOWAdmission {
			admissions.Add(1)
		}
		if e.Point == durabilitycut.AfterDependencyAppend && e.Resource == durabilitycut.ResourceCommandWAL {
			appends.Add(1)
			return backenddb.ErrTxnTooOld
		}
		return nil
	})
	ids, err := col.InsertBatch([][]byte{[]byte("uncertain")}, [][]byte{[]byte(`{"value":"not an ACK"}`)})
	restore()
	if len(ids) != 0 || !errors.Is(err, backenddb.ErrTxnTooOld) || !errors.Is(err, ErrCommitAmbiguous) || admissions.Load() != 1 || appends.Load() != 1 || backend.CommandWALNextLSN() != before+1 {
		t.Fatalf("after append ids=%q admissions=%d appends=%d LSN=%d before=%d err=%v", ids, admissions.Load(), appends.Load(), backend.CommandWALNextLSN(), before, err)
	}
}
