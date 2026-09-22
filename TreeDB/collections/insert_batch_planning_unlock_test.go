package collections

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type indexedInsertPlanningFixture struct {
	db         *backenddb.DB
	manager    *CollectionManager
	collection *Collection
}

func newIndexedInsertPlanningFixture(t *testing.T, format DocumentFormat) indexedInsertPlanningFixture {
	t.Helper()

	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	var mgr *CollectionManager
	t.Cleanup(func() {
		if mgr != nil {
			if err := mgr.FlushAll(); err != nil && !errors.Is(err, backenddb.ErrClosed) {
				t.Errorf("flush insert planning fixture: %v", err)
			}
		}
		if err := d.Close(); err != nil && !errors.Is(err, backenddb.ErrClosed) {
			t.Errorf("close insert planning fixture DB: %v", err)
		}
	})

	opts := bufferedIndexedUpdateNoAsyncHighThresholdOptionsForTests()
	opts.DocumentFormat = format
	mgr = NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Options: opts,
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	return indexedInsertPlanningFixture{
		db:         d,
		manager:    mgr,
		collection: col,
	}
}

func installTestBeforeInsertBatchPlanningHookForTests(t *testing.T, fn func()) {
	t.Helper()

	testBeforeInsertBatchPlanningHook.installMu.Lock()
	testBeforeInsertBatchPlanningHook.ptr.Store(&testInsertBatchPlanningHook{fn: fn})
	t.Cleanup(func() {
		testBeforeInsertBatchPlanningHook.ptr.Store(nil)
		testBeforeInsertBatchPlanningHook.installMu.Unlock()
	})
}

func indexedPlanningTestDocumentBatch(t *testing.T, format DocumentFormat, id, email, city string) ([][]byte, [][]byte, bool) {
	t.Helper()

	switch normalizedDocumentFormat(format) {
	case DocumentFormatBSON:
		return [][]byte{[]byte(id)},
			[][]byte{mustBSONCollectionDocument(t, bson.D{
				{Key: "_id", Value: id},
				{Key: "email", Value: email},
				{Key: "city", Value: city},
			})},
			true
	case DocumentFormatTemplateV1:
		return [][]byte{[]byte(id)},
			[][]byte{mustTemplateV1Document(t, []string{"email", "city"}, []any{email, city})},
			false
	default:
		return [][]byte{[]byte(id)},
			[][]byte{[]byte(fmt.Sprintf(`{"email":%q,"city":%q}`, email, city))},
			false
	}
}

func insertIndexedPlanningTestDocument(t *testing.T, col *Collection, format DocumentFormat, id, email, city string) ([][]byte, error) {
	t.Helper()

	ids, docs, trustedValidBSON := indexedPlanningTestDocumentBatch(t, format, id, email, city)
	if trustedValidBSON {
		return col.InsertBatchValidatedBSON(ids, docs)
	}
	return col.InsertBatch(ids, docs)
}

func insertIndexedPlanningTestDocumentWithMutationWait(t *testing.T, col *Collection, format DocumentFormat, id, email, city string, wait time.Duration) ([][]byte, error) {
	t.Helper()

	ids, docs, trustedValidBSON := indexedPlanningTestDocumentBatch(t, format, id, email, city)
	unlockMutation := col.lockMutation()
	unlockMutation.wait = wait
	mutationLocked := true
	return col.insertBatchOnceWithLockState(ids, docs, trustedValidBSON, nil, nil, insertBatchExecutionOptions{returnResultIDs: true}, &unlockMutation, &mutationLocked)
}

func TestCollectionIndexedInsertBatchSingleDocumentPlanningUnlockByFormat(t *testing.T) {
	tests := []struct {
		name          string
		format        DocumentFormat
		wantLockCalls uint64
	}{
		{name: "default", format: DocumentFormatDefault, wantLockCalls: 2},
		{name: "json", format: DocumentFormatJSON, wantLockCalls: 2},
		{name: "bson", format: DocumentFormatBSON, wantLockCalls: 2},
		{name: "template-v1", format: DocumentFormatTemplateV1, wantLockCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newIndexedInsertPlanningFixture(t, tt.format)

			before := fixture.manager.StatsSnapshot()
			if _, err := insertIndexedPlanningTestDocumentWithMutationWait(t, fixture.collection, tt.format, "u1", "a@example.com", "hnl", indexedInsertPlanningUnlockMinWait); err != nil {
				t.Fatalf("insert batch: %v", err)
			}
			after := fixture.manager.StatsSnapshot()
			if got := after.MutationLockCalls - before.MutationLockCalls; got != tt.wantLockCalls {
				t.Fatalf("mutation lock calls delta=%d want %d", got, tt.wantLockCalls)
			}
			if got := after.IndexedStageDocs - before.IndexedStageDocs; got != 1 {
				t.Fatalf("indexed staged docs delta=%d want 1", got)
			}
			if _, err := fixture.collection.Get([]byte("u1")); err != nil {
				t.Fatalf("get inserted doc: %v", err)
			}
		})
	}
}

func TestCollectionIndexedInsertBatchContendedPublicInsertPlansBeforeMutationLock(t *testing.T) {
	fixture := newIndexedInsertPlanningFixture(t, DocumentFormatJSON)

	held := fixture.collection.lockMutation()
	planningStarted := make(chan struct{})
	var once sync.Once
	installTestBeforeInsertBatchPlanningHookForTests(t, func() {
		once.Do(func() { close(planningStarted) })
	})

	ids, docs, _ := indexedPlanningTestDocumentBatch(t, DocumentFormatJSON, "u1", "contended@example.com", "hnl")
	done := make(chan error, 1)
	go func() {
		_, err := fixture.collection.InsertBatch(ids, docs)
		done <- err
	}()

	select {
	case <-planningStarted:
	case err := <-done:
		held.Unlock()
		t.Fatalf("insert finished before optimistic planning hook: %v", err)
	case <-time.After(2 * time.Second):
		held.Unlock()
		t.Fatal("timed out waiting for optimistic planning while mutation lock was held")
	}
	held.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("contended insert: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for contended insert")
	}
	if _, err := fixture.collection.Get([]byte("u1")); err != nil {
		t.Fatalf("get inserted doc: %v", err)
	}
}

func TestCollectionIndexedInsertBatchUnlockedPlanningRejectsPendingConflicts(t *testing.T) {
	tests := []struct {
		name        string
		hookID      string
		hookEmail   string
		hookCity    string
		outerID     string
		outerEmail  string
		outerCity   string
		wantErr     error
		wantVisible string
	}{
		{
			name:        "primary",
			hookID:      "u1",
			hookEmail:   "hook-primary@example.com",
			hookCity:    "hnl",
			outerID:     "u1",
			outerEmail:  "outer-primary@example.com",
			outerCity:   "sea",
			wantErr:     ErrDocumentExists,
			wantVisible: "u1",
		},
		{
			name:        "unique",
			hookID:      "u1",
			hookEmail:   "dup@example.com",
			hookCity:    "hnl",
			outerID:     "u2",
			outerEmail:  "dup@example.com",
			outerCity:   "sea",
			wantErr:     ErrUniqueIndexConflict,
			wantVisible: "u1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newIndexedInsertPlanningFixture(t, DocumentFormatJSON)
			var injected bool
			var hookErr error
			installTestBeforeInsertBatchPlanningHookForTests(t, func() {
				if injected {
					return
				}
				injected = true
				_, hookErr = insertIndexedPlanningTestDocument(
					t,
					fixture.collection,
					DocumentFormatJSON,
					tt.hookID,
					tt.hookEmail,
					tt.hookCity,
				)
			})

			_, err := insertIndexedPlanningTestDocumentWithMutationWait(
				t,
				fixture.collection,
				DocumentFormatJSON,
				tt.outerID,
				tt.outerEmail,
				tt.outerCity,
				indexedInsertPlanningUnlockMinWait,
			)
			if hookErr != nil {
				t.Fatalf("hook insert: %v", hookErr)
			}
			if !injected {
				t.Fatal("planning hook did not run")
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("insert err=%v want %v", err, tt.wantErr)
			}
			if _, err := fixture.collection.Get([]byte(tt.wantVisible)); err != nil {
				t.Fatalf("get hook-inserted doc %q: %v", tt.wantVisible, err)
			}
		})
	}
}

func TestCollectionIndexedInsertBatchUnlockedPlanningRevalidatesDisjointPersistedRootDrift(t *testing.T) {
	fixture := newIndexedInsertPlanningFixture(t, DocumentFormatJSON)
	otherMgr := NewCollectionManager(fixture.db)
	otherCol, err := otherMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open users from second manager: %v", err)
	}

	var injected bool
	var hookErr error
	installTestBeforeInsertBatchPlanningHookForTests(t, func() {
		if injected {
			return
		}
		injected = true
		if _, hookErr = insertIndexedPlanningTestDocument(t, otherCol, DocumentFormatJSON, "u0", "other@example.com", "oak"); hookErr != nil {
			return
		}
		hookErr = otherCol.Flush()
	})

	if _, err := insertIndexedPlanningTestDocumentWithMutationWait(t, fixture.collection, DocumentFormatJSON, "u1", "outer@example.com", "hnl", indexedInsertPlanningUnlockMinWait); err != nil {
		t.Fatalf("insert after persisted root drift: %v", err)
	}
	if hookErr != nil {
		t.Fatalf("hook insert/flush: %v", hookErr)
	}
	if !injected {
		t.Fatal("planning hook did not run")
	}
	for _, id := range []string{"u0", "u1"} {
		if _, err := fixture.collection.Get([]byte(id)); err != nil {
			t.Fatalf("get %s after root drift revalidation: %v", id, err)
		}
	}
}
