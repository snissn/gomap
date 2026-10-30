package treedb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestNativeCollectionVacuumShutdownLifecycle(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	for _, route := range []string{"public", "background"} {
		t.Run(route, func(t *testing.T) {
			opts := OptionsFor(ProfileCommandWALDurable, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.BackgroundIndexVacuumInterval = -1
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.ValueLog.Generational.Policy = ValueLogGenerationOff
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					if err := database.Close(); err != nil {
						t.Errorf("cleanup DB Close: %v", err)
					}
				}
			})
			backend, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(backend)
			t.Cleanup(func() {
				if err := manager.Close(); err != nil {
					t.Errorf("cleanup manager Close: %v", err)
				}
			})
			if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "rows"}); err != nil {
				t.Fatal(err)
			}
			collection, err := manager.OpenCollection("rows")
			if err != nil {
				t.Fatal(err)
			}
			ids, documents := make([][]byte, 16), make([][]byte, 16)
			for i := range ids {
				ids[i] = []byte(fmt.Sprintf("row-%02d", i))
				documents[i] = []byte(fmt.Sprintf(`{"n":%d}`, i))
			}
			if _, err := collection.InsertBatch(ids, documents); err != nil {
				t.Fatal(err)
			}
			held, err := collection.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if held != nil {
					if err := held.Close(); err != nil {
						t.Errorf("cleanup held Close: %v", err)
					}
				}
			})
			updated := []byte(`{"n":100}`)
			if matched, modified, err := collection.Update(ids[0], func(current []byte) ([]byte, bool, error) {
				if !bytes.Equal(current, documents[0]) {
					return nil, false, fmt.Errorf("update read %q want %q", current, documents[0])
				}
				return bytes.Clone(updated), true, nil
			}); err != nil || !matched || !modified {
				t.Fatalf("update matched=%t modified=%t: %v", matched, modified, err)
			}
			if err := collection.Delete(ids[1]); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			// Native activation must continue rejecting the original unowned
			// staging entrypoint; maintenance needs the actual cache owner.
			guard, err := backend.LockCommandWALStagingGuardV1()
			if guard != nil {
				guard.Release()
			}
			if !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("unowned native staging guard: %v", err)
			}
			beforeLSN := backend.CommandWALNextLSN()
			if route == "public" {
				if err := database.VacuumIndexOnline(context.Background()); err != nil {
					t.Fatalf("native public vacuum: %v", err)
				}
			} else {
				// Force only debt eligibility. The worker, public maintenance
				// fence, backend vacuum and COW basis refresh remain real.
				restore := setBackgroundIndexVacuumTriggerReportHookForTest(func(*DB, context.Context) (backenddb.IndexVacuumTriggerReport, error) {
					return backenddb.IndexVacuumTriggerReport{UserPages: 1, UserSpanRatioPPM: 2_000_000}, nil
				})
				defer restore()
				database.bgVac.runOnce(database)
				stats := database.bgVac.Stats()
				if stats.VacuumAttempts != 1 || stats.VacuumWorkCompleted != 1 || stats.PermanentFailuresTotal != 0 {
					t.Fatalf("native background vacuum: %+v error=%v", stats, database.backgroundError())
				}
			}
			if stats := backend.VacuumOnlineStats(); !stats.WorkCompleted {
				t.Fatalf("backend vacuum did not complete: %+v", stats)
			}
			if err := database.backgroundError(); err != nil {
				t.Fatalf("retained background error: %v", err)
			}
			if got := backend.CommandWALNextLSN(); got != beforeLSN {
				t.Fatalf("physical vacuum assigned command LSN: %d want %d", got, beforeLSN)
			}
			checkCurrent := func(c *collections.Collection) {
				t.Helper()
				if got, err := c.Get(ids[0]); err != nil || !bytes.Equal(got, updated) {
					t.Fatalf("current update=%q: %v", got, err)
				}
				if _, found, err := c.GetInto(ids[1], nil); err != nil || found {
					t.Fatalf("current deleted row found=%t: %v", found, err)
				}
				for i := 2; i < len(ids); i++ {
					if got, err := c.Get(ids[i]); err != nil || !bytes.Equal(got, documents[i]) {
						t.Fatalf("current row %q=%q: %v", ids[i], got, err)
					}
				}
			}
			checkCurrent(collection)
			old, err := held.FetchDocumentsByID(ids[:2], collections.DocumentFetchOptions{})
			if err != nil || len(old.Results) != 2 {
				t.Fatalf("held fetch: %+v %v", old, err)
			}
			for i, result := range old.Results {
				if !result.Found || !bytes.Equal(result.Document, documents[i]) {
					t.Fatalf("held row %d after vacuum: %+v", i, result)
				}
			}
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			held = nil
			if _, err := collection.InsertBatch([][]byte{[]byte("after")}, [][]byte{[]byte(`{"after":true}`)}); err != nil {
				t.Fatalf("write after vacuum: %v", err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			cache := database.cached
			closed = true
			if err := database.Close(); err != nil {
				t.Fatalf("native public Close: %v", err)
			}
			if stats := cache.COWMemoryStats(); stats.TotalBytes != 0 || stats.Sources != 0 || stats.Views != 0 || stats.ExternalLeases != 0 || stats.Generations != 0 {
				t.Fatalf("final accounting: %+v", stats)
			}
			database, err = Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			closed = false
			backend, err = database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager = collections.NewCollectionManager(backend)
			collection, err = manager.OpenCollection("rows")
			if err != nil {
				t.Fatal(err)
			}
			checkCurrent(collection)
			if got, err := collection.Get([]byte("after")); err != nil || string(got) != `{"after":true}` {
				t.Fatalf("reopened suffix=%q: %v", got, err)
			}
		})
	}
}
