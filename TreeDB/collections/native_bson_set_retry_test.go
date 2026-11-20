package collections

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const bsonSetRetryCollection = "docs"

var bsonSetRetryID = []byte("id")

type bsonSetRetryFixture struct {
	owner   *treedb.DB
	backend *backenddb.DB
	manager *CollectionManager
	col     *Collection
	close   func() error
}

func bsonSetRetryOptions(t *testing.T, profile treedb.Profile) treedb.Options {
	t.Helper()
	opts := treedb.OptionsFor(profile, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval, opts.BackgroundCheckpointIdleDuration = -1, -1
	opts.MaxWALBytes = -1
	return opts
}

func openBSONSetRetryFixture(t *testing.T, opts treedb.Options, create bool, indexes ...IndexDefinition) bsonSetRetryFixture {
	t.Helper()
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var manager *CollectionManager
	var closeErr error
	closeAll := sync.OnceValue(func() error {
		if manager != nil {
			closeErr = manager.Close()
		}
		return errors.Join(closeErr, owner.Close())
	})
	// Fault cases explicitly check the close error before reopening.
	closed := false
	t.Cleanup(func() {
		if !closed {
			if err := closeAll(); err != nil {
				t.Errorf("close BSON owner: %v", err)
			}
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = NewCollectionManager(backend)
	if !manager.UsesNativeCommandWALApply() {
		t.Fatal("native collection owner did not activate")
	}
	if create {
		meta := CollectionMeta{Name: bsonSetRetryCollection, Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}, Indexes: indexes}
		if _, err := manager.CreateCollection(&meta); err != nil {
			t.Fatal(err)
		}
	}
	col, err := manager.OpenCollection(bsonSetRetryCollection)
	if err != nil {
		t.Fatal(err)
	}
	if create {
		if _, err := col.InsertBatchValidatedBSON([][]byte{bsonSetRetryID}, [][]byte{bsonSetRetryDoc(t, "id", 0, 0)}); err != nil {
			t.Fatal(err)
		}
	}
	return bsonSetRetryFixture{owner: owner, backend: backend, manager: manager, col: col, close: func() error {
		closed = true
		return closeAll()
	}}
}

func bsonSetRetryDoc(t *testing.T, id string, a, b int64) []byte {
	t.Helper()
	doc, err := bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "a", Value: a}, {Key: "b", Value: b}})
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func requireBSONSetRetryDoc(t *testing.T, col *Collection, a, b int64) {
	t.Helper()
	got, err := col.Get(bsonSetRetryID)
	if err != nil || !bytes.Equal(got, bsonSetRetryDoc(t, "id", a, b)) {
		t.Fatalf("document=%v err=%v, want a=%d b=%d", bson.Raw(got), err, a, b)
	}
}

func requireBSONSetRetryFrames(t *testing.T, backend *backenddb.DB, start uint64, docs ...[]byte) {
	t.Helper()
	var updates []commitlog.CommandEnvelope
	for _, frame := range collectionCommandWALFrames(t, backend.Dir()) {
		if frame.LSN >= start {
			updates = append(updates, frame)
		}
	}
	if len(updates) != len(docs) {
		t.Fatalf("saw %d command WAL frames from LSN %d, want %d", len(updates), start, len(docs))
	}
	for i, frame := range updates {
		if frame.Kind != commitlog.CommandKindCollectionUpdateBatchByID || frame.LSN != start+uint64(i) {
			t.Fatalf("update %d frame LSN=%d kind=%v", i, frame.LSN, frame.Kind)
		}
		body, err := commitlog.DecodeCollectionUpdateBatchByIDPayload(frame.Payload)
		if err != nil || body.Collection != bsonSetRetryCollection || len(body.Documents) != 1 ||
			!bytes.Equal(body.Documents[0].ID, bsonSetRetryID) || !bytes.Equal(body.Documents[0].Document, docs[i]) {
			t.Fatalf("update %d decoded body=%+v err=%v, want replacement=%v", i, body, err, bson.Raw(docs[i]))
		}
	}
}

func bsonSetRetryEventRoot(root string, event durabilitycut.Event) bool {
	rel, err := filepath.Rel(root, event.Root)
	return err == nil && !filepath.IsAbs(rel) && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

type bsonSetRetryOutcome struct {
	matched, modified bool
	err               error
}

func runBSONSetRetryUpdate(col *Collection, id []byte, fields []BSONSetField) bsonSetRetryOutcome {
	m, n, err := col.UpdateBSONSet(id, fields)
	return bsonSetRetryOutcome{matched: m, modified: n, err: err}
}

func (r bsonSetRetryOutcome) requireModified() error {
	if r.err != nil {
		return r.err
	}
	if !r.matched || !r.modified {
		return fmt.Errorf("BSON update matched=%t modified=%t", r.matched, r.modified)
	}
	return nil
}

// Pause the original prepared singleton outside all publication gates. The
// second operation must complete before the first attempts canonical admission.
func withPausedBSONSetRetry(t *testing.T, root string, first func() bsonSetRetryOutcome, advance func() error) (bsonSetRetryOutcome, int32, int32) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var admissions, appends atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if !bsonSetRetryEventRoot(root, e) {
			return nil
		}
		if e.Point == durabilitycut.BeforeCOWAdmission && admissions.Add(1) == 1 {
			close(entered)
			<-release
		}
		if e.Point == durabilitycut.AfterDependencyAppend && e.Resource == durabilitycut.ResourceCommandWAL && e.LSN != 0 {
			appends.Add(1)
		}
		return nil
	})
	done, advanced := make(chan struct{}), make(chan struct{})
	var result bsonSetRetryOutcome
	var advanceErr error
	startedAdvance := false
	wait := func(ch <-chan struct{}) bool {
		select {
		case <-ch:
			return true
		case <-time.After(collectionTestTimeout(t, 5*time.Second)):
			return false
		}
	}
	defer func() {
		unblock()
		if !wait(done) || startedAdvance && !wait(advanced) {
			t.Error("timed out joining BSON mutation after releasing barrier")
		}
		restore()
	}()
	go func() { result = first(); close(done) }()
	if !wait(entered) {
		t.Fatal("original BSON update never reached preassignment barrier")
	}
	startedAdvance = true
	go func() { advanceErr = advance(); close(advanced) }()
	if !wait(advanced) {
		t.Fatal("competing mutation could not complete while original BSON update was paused")
	}
	if advanceErr != nil {
		t.Fatal(advanceErr)
	}
	unblock()
	if !wait(done) {
		t.Fatal("original BSON update did not complete after release")
	}
	return result, admissions.Load(), appends.Load()
}

func TestNativeBSONSetRetrySameRow(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile treedb.Profile
		large   bool
		cold    bool
	}{
		{name: "compact", profile: treedb.ProfileCommandWALRelaxed},
		{name: "standalone_cold", profile: treedb.ProfileCommandWALRelaxed, large: true, cold: true},
		{name: "no_wal", profile: treedb.ProfileNoWALFast},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := bsonSetRetryOptions(t, tc.profile)
			if tc.large {
				opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()
			}
			f := openBSONSetRetryFixture(t, opts, true)
			if err := f.owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if tc.cold {
				if err := f.close(); err != nil {
					t.Fatal(err)
				}
				f = openBSONSetRetryFixture(t, opts, false)
			}
			withWAL := tc.profile != treedb.ProfileNoWALFast
			if f.backend.CommandWALEnabled() != withWAL {
				t.Fatal("unexpected command WAL mode")
			}
			held, err := f.col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := held.Close(); err != nil {
					t.Error(err)
				}
			}()
			opening, err := f.backend.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			budget := opening.NativeAllocator().(interface{ Stats() memtable.COWStats })
			seq := opening.LogicalSequence()
			cold := opening.State() == nil
			if err := opening.Close(); err != nil {
				t.Fatal(err)
			}
			if tc.cold && !cold {
				t.Fatal("expected a cold reopened native opening")
			}
			before := f.backend.CommandWALNextLSN()
			a := []BSONSetField{{Key: "a", Value: mustBSONRawValue(t, int64(1))}}
			b := []BSONSetField{{Key: "b", Value: mustBSONRawValue(t, int64(1))}}
			first, admits, appends := withPausedBSONSetRetry(t, opts.Dir, func() bsonSetRetryOutcome {
				return runBSONSetRetryUpdate(f.col, bsonSetRetryID, a)
			}, func() error { return runBSONSetRetryUpdate(f.col, bsonSetRetryID, b).requireModified() })
			if err := first.requireModified(); err != nil || admits != 3 {
				t.Fatalf("original update err=%v admissions=%d, want 3", err, admits)
			}
			accepted, err := f.backend.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			gotSeq := accepted.LogicalSequence()
			if err := accepted.Close(); err != nil {
				t.Fatal(err)
			}
			if gotSeq != seq+2 {
				t.Fatalf("logical sequence=%d, want %d", gotSeq, seq+2)
			}
			if withWAL {
				if appends != 2 || f.backend.CommandWALNextLSN() != before+2 {
					t.Fatalf("appends=%d nextLSN=%d, want 2/%d", appends, f.backend.CommandWALNextLSN(), before+2)
				}
				requireBSONSetRetryFrames(t, f.backend, before, bsonSetRetryDoc(t, "id", 0, 1), bsonSetRetryDoc(t, "id", 1, 1))
			} else if appends != 0 || f.backend.CommandWALNextLSN() != before {
				t.Fatalf("WAL-off update wrote command WAL: appends=%d nextLSN=%d before=%d", appends, f.backend.CommandWALNextLSN(), before)
			}
			requireBSONSetRetryDoc(t, f.col, 1, 1)
			if err := f.owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if withWAL && f.backend.State().AppliedCommandLSN != before+1 {
				t.Fatalf("checkpoint applied LSN=%d, want %d", f.backend.State().AppliedCommandLSN, before+1)
			}
			read, err := held.FetchDocumentsByID([][]byte{bsonSetRetryID}, DocumentFetchOptions{})
			if err != nil || len(read.Results) != 1 || !read.Results[0].Found || !bytes.Equal(read.Results[0].Document, bsonSetRetryDoc(t, "id", 0, 0)) {
				t.Fatalf("held read crossed update/checkpoint: %+v err=%v", read.Results, err)
			}
			if err := errors.Join(held.Close(), f.close()); err != nil {
				t.Fatal(err)
			}
			if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Fatalf("close did not drain original budget: %+v", stats)
			}
			f = openBSONSetRetryFixture(t, opts, false)
			requireBSONSetRetryDoc(t, f.col, 1, 1)
			if err := f.close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNativeBSONSetRetryIndexedAndNoop(t *testing.T) {
	opts := bsonSetRetryOptions(t, treedb.ProfileCommandWALRelaxed)
	index := IndexDefinition{Name: "a", Field: "a", ValueType: IndexValueInt64, Unique: true}
	f := openBSONSetRetryFixture(t, opts, true, index)
	a := []BSONSetField{{Key: "a", Value: mustBSONRawValue(t, int64(1))}}
	b := []BSONSetField{{Key: "b", Value: mustBSONRawValue(t, int64(1))}}
	first, admits, appends := withPausedBSONSetRetry(t, opts.Dir, func() bsonSetRetryOutcome {
		return runBSONSetRetryUpdate(f.col, bsonSetRetryID, a)
	}, func() error { return runBSONSetRetryUpdate(f.col, bsonSetRetryID, b).requireModified() })
	if err := first.requireModified(); err != nil || admits != 3 || appends != 2 {
		t.Fatalf("indexed BSON retry err=%v admissions=%d appends=%d", err, admits, appends)
	}
	checkIndex := func(col *Collection) {
		t.Helper()
		old, err := col.FindByIndexValue("a", int64(0))
		if err != nil || len(old) != 0 {
			t.Fatalf("old index keys=%q err=%v", old, err)
		}
		got, err := col.FindByIndexValue("a", int64(1))
		if err != nil || !slices.EqualFunc(got, [][]byte{bsonSetRetryID}, bytes.Equal) {
			t.Fatalf("new index keys=%q err=%v", got, err)
		}
	}
	checkIndex(f.col)
	before := f.backend.CommandWALNextLSN()
	if got := runBSONSetRetryUpdate(f.col, bsonSetRetryID, a); !got.matched || got.modified || got.err != nil {
		t.Fatalf("same-value result: %+v", got)
	}
	if got := runBSONSetRetryUpdate(f.col, []byte("missing"), a); got.matched || got.modified || got.err != nil {
		t.Fatalf("missing-row result: %+v", got)
	}
	if got := f.backend.CommandWALNextLSN(); got != before {
		t.Fatalf("no-op or missing row assigned WAL: got %d want %d", got, before)
	}
	// Both requests start as modifications, but the second installs the exact
	// requested value. On reconstruction the original call matches a no-op.
	b[0].Value = mustBSONRawValue(t, int64(2))
	first, admits, appends = withPausedBSONSetRetry(t, opts.Dir, func() bsonSetRetryOutcome {
		return runBSONSetRetryUpdate(f.col, bsonSetRetryID, b)
	}, func() error { return runBSONSetRetryUpdate(f.col, bsonSetRetryID, b).requireModified() })
	if !first.matched || first.modified || first.err != nil || admits != 2 || appends != 1 || f.backend.CommandWALNextLSN() != before+1 {
		t.Fatalf("reconstructed no-op=%+v admissions=%d appends=%d", first, admits, appends)
	}
	if err := errors.Join(f.owner.Checkpoint(), f.close()); err != nil {
		t.Fatal(err)
	}
	f = openBSONSetRetryFixture(t, opts, false)
	requireBSONSetRetryDoc(t, f.col, 1, 2)
	checkIndex(f.col)
}

func TestNativeBSONSetRetryRejectsCatalogChange(t *testing.T) {
	for _, gap := range []bool{false, true} {
		t.Run(fmt.Sprintf("between_attempts=%t", gap), func(t *testing.T) {
			opts := bsonSetRetryOptions(t, treedb.ProfileCommandWALRelaxed)
			f := openBSONSetRetryFixture(t, opts, true)
			a := []BSONSetField{{Key: "a", Value: mustBSONRawValue(t, int64(1))}}
			b := []BSONSetField{{Key: "b", Value: mustBSONRawValue(t, int64(1))}}
			index := IndexDefinition{Name: "a", Field: "a", ValueType: IndexValueInt64}
			before := f.backend.CommandWALNextLSN()
			if !gap {
				first, admits, appends := withPausedBSONSetRetry(t, opts.Dir, func() bsonSetRetryOutcome {
					return runBSONSetRetryUpdate(f.col, bsonSetRetryID, a)
				}, func() error {
					if err := runBSONSetRetryUpdate(f.col, bsonSetRetryID, b).requireModified(); err != nil {
						return err
					}
					// The facade's schema gate normally serializes these calls.
					// Use the real DDL publisher to force the stale opening while
					// retaining its catalog WAL effect and normal index build.
					_, err := f.manager.nativeJSON.mutateIndexFamilies(f.col, f.col.name, &index, nil, false, nil, "", nil)
					return err
				})
				if first.matched || first.modified || !errors.Is(first.err, backenddb.ErrConcurrentModification) || admits != 3 || appends != 2 || f.backend.CommandWALNextLSN() != before+2 {
					t.Fatalf("catalog and row change result=%+v admits=%d appends=%d LSN=%d", first, admits, appends, f.backend.CommandWALNextLSN())
				}
			} else {
				items := []BSONSetUpdateBatchItem{{DocumentID: bsonSetRetryID, Fields: a}}
				retry := nativeBSONSetRetry{id: bsonSetRetryID}
				first, admits, appends := withPausedBSONSetRetry(t, opts.Dir, func() bsonSetRetryOutcome {
					_, _, err := f.col.nativeUpdateBatchModeObserved(nil, updateBatchModeAny, items, &retry)
					return bsonSetRetryOutcome{err: err}
				}, func() error { return runBSONSetRetryUpdate(f.col, bsonSetRetryID, b).requireModified() })
				if !retry.rowConflict || !errors.Is(first.err, backenddb.ErrConcurrentModification) || admits != 2 || appends != 1 {
					t.Fatalf("no initial row-only refusal: result=%+v retry=%+v admissions=%d appends=%d", first, retry, admits, appends)
				}
				// Restore the catalog's visible shape under the same incarnation.
				// Its two accepted catalog effects must still invalidate replay.
				if _, err := f.col.CreateIndex(index); err != nil {
					t.Fatal(err)
				}
				meta, err := f.col.DropIndex(index.Name)
				if err != nil || len(meta.Indexes) != 0 {
					t.Fatalf("catalog ABA meta=%+v err=%v", meta, err)
				}
				var admissions atomic.Int32
				restore := durabilitycut.Install(func(e durabilitycut.Event) error {
					if bsonSetRetryEventRoot(opts.Dir, e) && e.Point == durabilitycut.BeforeCOWAdmission {
						admissions.Add(1)
					}
					return nil
				})
				_, _, err = f.col.nativeUpdateBatchModeObserved(nil, updateBatchModeAny, items, &retry)
				restore()
				if retry.rowConflict || !errors.Is(err, backenddb.ErrConcurrentModification) || admissions.Load() != 0 || f.backend.CommandWALNextLSN() != before+3 {
					t.Fatalf("retry entered publication across catalog ABA: retry=%+v err=%v admissions=%d LSN=%d", retry, err, admissions.Load(), f.backend.CommandWALNextLSN())
				}
			}
			requireBSONSetRetryDoc(t, f.col, 0, 1)
			if err := errors.Join(f.owner.Checkpoint(), f.close()); err != nil {
				t.Fatal(err)
			}
			f = openBSONSetRetryFixture(t, opts, false)
			requireBSONSetRetryDoc(t, f.col, 0, 1)
		})
	}
}

func TestNativeBSONSetRetryKeepsExplicitAndBatchConflict(t *testing.T) {
	opts := bsonSetRetryOptions(t, treedb.ProfileCommandWALRelaxed)
	f := openBSONSetRetryFixture(t, opts, true)
	a := []BSONSetField{{Key: "a", Value: mustBSONRawValue(t, int64(1))}}
	b := []BSONSetField{{Key: "b", Value: mustBSONRawValue(t, int64(1))}}
	first, admits, appends := withPausedBSONSetRetry(t, opts.Dir, func() bsonSetRetryOutcome {
		results, _, err := f.col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]BSONSetUpdateBatchItem{{DocumentID: bsonSetRetryID, Fields: a}})
		if len(results) != 0 && err == nil {
			return bsonSetRetryOutcome{matched: results[0].Matched, modified: results[0].Modified}
		}
		return bsonSetRetryOutcome{err: err}
	}, func() error { return runBSONSetRetryUpdate(f.col, bsonSetRetryID, b).requireModified() })
	if first.matched || first.modified || !errors.Is(first.err, backenddb.ErrConcurrentModification) || admits != 2 || appends != 1 {
		t.Fatalf("singleton-shaped batch=%+v admissions=%d appends=%d", first, admits, appends)
	}
	tx, err := f.manager.nativeJSON.Begin(bsonSetRetryCollection)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Close(); err != nil {
			t.Error(err)
		}
	}()
	calls := 0
	replacement := bsonSetRetryDoc(t, "id", 1, 1)
	if _, err := tx.UpdateBatch([]UpdateBatchItem{{DocumentID: bsonSetRetryID, Update: func([]byte) ([]byte, bool, error) {
		calls++
		return replacement, true, nil
	}}}); err != nil {
		t.Fatal(err)
	}
	b[0].Value = mustBSONRawValue(t, int64(2))
	if err := runBSONSetRetryUpdate(f.col, bsonSetRetryID, b).requireModified(); err != nil {
		t.Fatal(err)
	}
	before := f.backend.CommandWALNextLSN()
	if err := tx.Commit(); !errors.Is(err, backenddb.ErrConcurrentModification) || calls != 1 || f.backend.CommandWALNextLSN() != before {
		t.Fatalf("explicit transaction err=%v callbacks=%d nextLSN=%d", err, calls, f.backend.CommandWALNextLSN())
	}
	requireBSONSetRetryDoc(t, f.col, 0, 2)
}

func TestNativeBSONSetRetryRequiresCleanPreassignmentRefusal(t *testing.T) {
	opts := bsonSetRetryOptions(t, treedb.ProfileCommandWALRelaxed)
	f := openBSONSetRetryFixture(t, opts, true)
	a := []BSONSetField{{Key: "a", Value: mustBSONRawValue(t, int64(1))}}
	items := []BSONSetUpdateBatchItem{{DocumentID: bsonSetRetryID, Fields: a}}
	conflict := backenddb.ErrConcurrentModification
	cleanupFailure := errors.New("BSON update cleanup failed")
	closeFailure := errors.New("BSON update close failed")
	for _, tc := range []struct {
		name                string
		alter               func(*backenddb.NativePublicationResult)
		failure             error
		cancelErr, closeErr error
		wrongID, wantRetry  bool
	}{
		{name: "clean", wantRetry: true},
		{name: "unknown conflict", alter: func(r *backenddb.NativePublicationResult) { r.OnlyPointConflict = false }},
		{name: "wrong row dependency", wrongID: true},
		{name: "assigned", alter: func(r *backenddb.NativePublicationResult) { r.Assigned = true }},
		{name: "lsn only", alter: func(r *backenddb.NativePublicationResult) { r.AssignedLSN = 7 }},
		{name: "uncertain", alter: func(r *backenddb.NativePublicationResult) { r.Uncertain = true }},
		{name: "installed", alter: func(r *backenddb.NativePublicationResult) { r.LogicalInstalled = true }},
		{name: "completed", alter: func(r *backenddb.NativePublicationResult) { r.WALCompleted = true }},
		{name: "stable", alter: func(r *backenddb.NativePublicationResult) { r.WALStable = true }},
		{name: "capacity", failure: memtable.ErrCOWCapacity},
		{name: "history expiry", failure: backenddb.ErrTxnTooOld},
		{name: "already ambiguous", failure: commitAmbiguousError("injection", conflict)},
		{name: "lower cleanup join", failure: errors.Join(conflict, cleanupFailure)},
		{name: "private cancellation", cancelErr: cleanupFailure},
		{name: "close failure", closeErr: closeFailure},
		{name: "same sentinel from cleanup", cancelErr: conflict, closeErr: conflict},
		{name: "cancellation and close", cancelErr: cleanupFailure, closeErr: closeFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := f.manager.nativeJSON.Begin(bsonSetRetryCollection)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := tx.Close(); err != nil {
					t.Error(err)
				}
			}()
			if _, _, err := tx.stageNativeCommandWALBSONSet(bsonSetRetryCollection, items); err != nil {
				t.Fatal(err)
			}
			original := tc.failure
			if original == nil {
				original = conflict
			}
			receipt := backenddb.NativePublicationResult{OnlyPointConflict: true}
			if tc.alter != nil {
				tc.alter(&receipt)
			}
			attempts := 0
			state := &historyExpiryPrivateCleanupState{failure: tc.cancelErr}
			state.cleanup.Work = state
			tx.core = &historyExpiryResultTxn{
				NativeConditionalTxn: tx.core, result: receipt, failure: original, closeFailure: tc.closeErr,
				beforeReturn: func(native *backenddb.NativePreparedPublication) {
					attempts++
					if got := native.ObservePointConflict != 0; got == tc.wrongID {
						t.Errorf("unexpected point observation for wrongID=%t: %d", tc.wrongID, native.ObservePointConflict)
					}
					native.State = state
				},
			}
			retry := nativeBSONSetRetry{id: bsonSetRetryID}
			if tc.wrongID {
				retry.id = []byte("different")
			}
			tx.structuredBSONUpdate = true
			before := f.backend.CommandWALNextLSN()
			err = tx.commitNativePlanObserved(false, nil, false, nil, nil, &retry, nil)
			if retry.rowConflict != tc.wantRetry || !errors.Is(err, original) || attempts != 1 || state.releases != 1 || state.finishes != 1 || f.backend.CommandWALNextLSN() != before {
				t.Fatalf("retry=%t err=%v attempts=%d releases=%d finishes=%d", retry.rowConflict, err, attempts, state.releases, state.finishes)
			}
			for _, cause := range []error{tc.cancelErr, tc.closeErr} {
				if cause != nil && !errors.Is(err, cause) {
					t.Fatalf("lost cleanup error %v in %v", cause, err)
				}
			}
			requireBSONSetRetryDoc(t, f.col, 0, 0)
		})
	}
}

func TestNativeBSONSetRetryAfterActualAppend(t *testing.T) {
	opts := bsonSetRetryOptions(t, treedb.ProfileCommandWALRelaxed)
	f := openBSONSetRetryFixture(t, opts, true)
	if err := f.owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	before := f.backend.CommandWALNextLSN()
	a := []BSONSetField{{Key: "a", Value: mustBSONRawValue(t, int64(1))}}
	var admissions, appends, faults atomic.Int32
	var assigned atomic.Uint64
	fault := backenddb.ErrConcurrentModification
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if !bsonSetRetryEventRoot(opts.Dir, e) {
			return nil
		}
		if e.Point == durabilitycut.BeforeCOWAdmission {
			admissions.Add(1)
		}
		if e.Resource == durabilitycut.ResourceCommandWAL {
			if e.Point == durabilitycut.AfterDependencyAppend && e.LSN != 0 {
				appends.Add(1)
				assigned.Store(e.LSN)
			}
			// Fail after the real append reached the OS and before the command
			// can be acknowledged. The frame must still recover on open.
			if e.Point == durabilitycut.AfterUserspaceFlush && assigned.Load() != 0 && faults.Add(1) == 1 {
				return fault
			}
		}
		return nil
	})
	got := runBSONSetRetryUpdate(f.col, bsonSetRetryID, a)
	restore()
	if got.matched || got.modified || !errors.Is(got.err, fault) || !errors.Is(got.err, ErrCommitAmbiguous) ||
		admissions.Load() != 1 || appends.Load() != 1 || faults.Load() != 1 || assigned.Load() != before || f.backend.CommandWALNextLSN() != before+1 {
		t.Fatalf("postappend result=%+v admissions=%d appends=%d faults=%d assigned=%d next=%d", got, admissions.Load(), appends.Load(), faults.Load(), assigned.Load(), f.backend.CommandWALNextLSN())
	}
	requireBSONSetRetryFrames(t, f.backend, before, bsonSetRetryDoc(t, "id", 1, 0))
	if err := f.close(); err != nil && !errors.Is(err, fault) && !errors.Is(err, backenddb.ErrRecoveryRequired) {
		t.Fatalf("checked poisoned close: %v", err)
	}
	f = openBSONSetRetryFixture(t, opts, false)
	requireBSONSetRetryDoc(t, f.col, 1, 0)
	if got := f.backend.CommandWALNextLSN(); got != before+1 {
		t.Fatalf("reopen next command LSN=%d, want %d", got, before+1)
	}
}

func TestNativeBSONSetRetryRespectsBudget(t *testing.T) {
	opts := bsonSetRetryOptions(t, treedb.ProfileCommandWALRelaxed)
	f := openBSONSetRetryFixture(t, opts, true)
	if err := f.owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	before := f.backend.CommandWALNextLSN()
	a := []BSONSetField{{Key: "a", Value: mustBSONRawValue(t, int64(1))}}
	var competingFields [maxCollectionMutationRetries][]BSONSetField
	for i := range competingFields {
		competingFields[i] = []BSONSetField{{Key: "b", Value: mustBSONRawValue(t, int64(i+1))}}
	}
	attempts := 0
	competing := false
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point != durabilitycut.BeforeCOWAdmission || !bsonSetRetryEventRoot(opts.Dir, e) || competing {
			return nil
		}
		attempts++
		if attempts > len(competingFields) {
			return errors.New("exceeded expected attempt limit")
		}
		competing = true
		defer func() { competing = false }()
		return runBSONSetRetryUpdate(f.col, bsonSetRetryID, competingFields[attempts-1]).requireModified()
	})
	got := runBSONSetRetryUpdate(f.col, bsonSetRetryID, a)
	restore()
	if got.matched || got.modified || !errors.Is(got.err, backenddb.ErrConcurrentModification) || attempts != maxCollectionMutationRetries || f.backend.CommandWALNextLSN()-before != uint64(attempts) {
		t.Fatalf("retry exhausted result=%+v attempts=%d assigned=%d", got, attempts, f.backend.CommandWALNextLSN()-before)
	}
	requireBSONSetRetryDoc(t, f.col, 0, int64(attempts))
}
