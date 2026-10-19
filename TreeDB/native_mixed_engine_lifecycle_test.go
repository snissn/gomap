package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Reuse a finite hot set through real accepted successors and disk handoffs.
// This is a default-budget engine progress guard, not a timing or GC benchmark.
func TestNativeMixedEngineLifecycle(t *testing.T) {
	opts := indexedSliceOptions(ProfileCommandWALDurable, t.TempDir())
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(handle)
	t.Cleanup(func() {
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	fixtures := nativeTypedMultiCases()
	ids := [][]byte{[]byte("hot0"), []byte("hot1")}
	// Each generation is constructed from original scalar values, independently
	// of pending reconstruction, physical storage and caller-returned documents.
	document := func(collection, key, generation int) []byte {
		id := string(ids[key])
		switch fixtures[collection].family {
		case "string":
			return []byte(fmt.Sprintf(`{"id":%q,"label":%q,"payload":%q}`, id, fmt.Sprintf("round %d \"quoted\" \\ label", generation), fmt.Sprintf("row%d/gen%d", key, generation)))
		case "float32_vector":
			return []byte(fmt.Sprintf(`{"id":%q,"embedding":[%d,%d,0.5],"payload":%q}`, id, generation+1, -generation-2, fmt.Sprintf("row%d/gen%d", key, generation)))
		default:
			return []byte(fmt.Sprintf(`{"id":%q,"n":%d,"payload":%q}`, id, generation, fmt.Sprintf("row%d/gen%d", key, generation)))
		}
	}
	var original, expected [3][2][]byte
	cols := make([]*collections.Collection, len(fixtures))
	for i, fixture := range fixtures {
		meta := &collections.CollectionMeta{Name: fixture.name}
		if fixture.family != "" {
			config := nativeColumnPendingCaseFor(t, fixture.family)
			config.typedSeed = &collections.TypedColumnBatch{} // Maintained JSON residual layout.
			if fixture.family == "string" {
				config.column.Owner = collections.TypedStorageOwnerRowAsset
			}
			meta = nativeColumnPendingMeta(config)
			meta.Name = fixture.name
		}
		if _, err := manager.CreateCollection(meta); err != nil {
			t.Fatalf("Create %s: %v", fixture.name, err)
		}
		cols[i], err = manager.OpenCollection(fixture.name)
		if err != nil {
			t.Fatal(err)
		}
		for k := range ids {
			original[i][k] = document(i, k, 0)
			expected[i][k] = original[i][k]
		}
		if _, err := cols[i].InsertBatch(ids, original[i][:]); err != nil {
			t.Fatalf("seed %s: %v", fixture.name, err)
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatalf("seed checkpoint %s: %v", fixture.name, err)
		}
	}
	rawKey := []byte("mixed-lifecycle/raw")
	if err := database.Set(rawKey, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	startLSN := handle.CommandWALNextLSN()
	if startLSN == 0 || indexedCRUDAppliedLSN(t, handle) != startLSN-1 {
		t.Fatal("setup did not establish the actual complete physical basis")
	}
	held := make([]*collections.CollectionReadView, len(cols))
	t.Cleanup(func() {
		for _, view := range held {
			if view != nil {
				if err := view.Close(); err != nil {
					t.Error(err)
				}
			}
		}
	})
	for i, col := range cols {
		held[i], err = col.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
	}
	checkHeld := func() {
		t.Helper()
		for i, view := range held {
			if view == nil {
				continue
			}
			got, err := view.FetchDocumentsByID(ids, collections.DocumentFetchOptions{})
			if err != nil || len(got.Results) != len(ids) {
				t.Fatalf("held %s: %v", fixtures[i].name, err)
			}
			for k, result := range got.Results {
				if !result.Found {
					t.Fatalf("held %s/%s disappeared", fixtures[i].name, ids[k])
				}
				requireNativeColumnJSON(t, result.Document, original[i][k])
				result.Document[0] = '!'
			}
		}
	}
	checkCurrent := func(raw []byte) {
		t.Helper()
		for i, fixture := range fixtures {
			for k, id := range ids {
				got, err := cols[i].Get(id)
				if err != nil {
					t.Fatalf("current %s/%s: %v", fixture.name, id, err)
				}
				requireNativeColumnJSON(t, got, expected[i][k])
				got[0] = '!'
			}
		}
		if got, err := database.Get(rawKey); err != nil || !bytes.Equal(got, raw) {
			t.Fatalf("current raw=%q want=%q: %v", got, raw, err)
		}
	}
	checkHeld()
	checkCurrent([]byte("seed"))
	native, err := collections.NewNativeJSONManager(handle)
	if err != nil {
		t.Fatal(err)
	}
	const commits, cohort, releaseAfter = 40, 4, 24
	accepted, checkpoints := 0, 0
	var lastRaw []byte
	for generation := 1; generation <= commits; generation++ {
		key := (generation - 1) % len(ids)
		next, sequence := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
		physical := indexedCRUDAppliedLSN(t, handle)
		tx, err := native.Begin("labels", "vectors", "docs")
		if err != nil {
			t.Fatalf("Begin generation%d accepted%d: %v", generation, accepted, err)
		}
		// Keep one ordinary public transaction, rather than single-shot fixtures.
		err = func() error {
			defer tx.Close()
			for i, fixture := range fixtures {
				count, err := tx.DeleteBatchFrom(fixture.name, [][]byte{ids[key]})
				if err != nil || count != 1 {
					return fmt.Errorf("delete %s count%d: %w", fixture.name, count, err)
				}
				if got, err := tx.GetFrom(fixture.name, ids[key]); !errors.Is(err, tree.ErrKeyNotFound) || got != nil {
					return fmt.Errorf("deleted %s visible=%t: %w", fixture.name, got != nil, err)
				}
				want := document(i, key, generation)
				id, input := bytes.Clone(ids[key]), bytes.Clone(want)
				if err := tx.InsertBatchInto(fixture.name, [][]byte{id}, [][]byte{input}); err != nil {
					return fmt.Errorf("reinsert %s: %w", fixture.name, err)
				}
				id[0], input[0] = '!', '!'
				for read := 0; read < 2; read++ {
					got, err := tx.GetFrom(fixture.name, ids[key])
					if err != nil {
						return err
					}
					requireNativeColumnJSON(t, got, want)
					got[0] = '!'
				}
			}
			lastRaw = []byte(fmt.Sprintf("generation%d", generation))
			if err := tx.SetRaw(rawKey, lastRaw); err != nil {
				return err
			}
			return tx.Commit()
		}()
		if err != nil {
			t.Fatalf("mixed generation%d accepted%d checkpoints%d: %v", generation, accepted, checkpoints, err)
		}
		accepted++
		requireNativeTypedOpsAdvance(t, database, handle, next, sequence)
		if indexedCRUDAppliedLSN(t, handle) != physical {
			t.Fatal("logical commit unexpectedly installed physical effects")
		}
		for i := range fixtures {
			expected[i][key] = document(i, key, generation)
		}
		checkCurrent(lastRaw)
		checkHeld()
		if generation%cohort == 0 {
			logMemory := func(phase string) {
				s := database.Stats()
				t.Logf("checkpoint generation%d %s maintenance=%s/%s cow=%+v", generation, phase,
					s["treedb.cache.cow.maintenance_used_bytes"], s["treedb.cache.cow.maintenance_capacity_bytes"], database.cached.COWMemoryStats())
			}
			logMemory("before")
			if err := database.Checkpoint(); err != nil {
				logMemory("failed")
				t.Fatalf("checkpoint generation%d accepted%d: %v", generation, accepted, err)
			}
			logMemory("after")
			checkpoints++
			if indexedCRUDAppliedLSN(t, handle) != next || handle.CommandWALNextLSN() != next+1 {
				t.Fatal("checkpoint omitted an accepted command or consumed another identity")
			}
			checkCurrent(lastRaw)
			checkHeld()
		}
		if generation == releaseAfter {
			for i, view := range held {
				if err := view.Close(); err != nil {
					t.Fatal(err)
				}
				held[i] = nil
			}
		}
	}
	if accepted != commits || checkpoints != commits/cohort || handle.CommandWALNextLSN() != startLSN+commits {
		t.Fatalf("bounded engine progress accepted%d checkpoints%d next%d", accepted, checkpoints, handle.CommandWALNextLSN())
	}
	t.Logf("accepted=%d checkpoint_generations=%d held_through=%d continued_after_release=%d", accepted, checkpoints, releaseAfter, accepted-releaseAfter)
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	requirePublicCOWFinalZero(t, cache)
	if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 || stats.ActiveStableNamespaceLinks != 0 {
		t.Fatalf("final engine resources: %+v", stats)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedHandle, err := reopened.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	reopenedManager := collections.NewCollectionManager(reopenedHandle)
	defer reopenedManager.Close()
	for i, fixture := range fixtures {
		col, err := reopenedManager.OpenCollection(fixture.name)
		if err != nil {
			t.Fatal(err)
		}
		for k, id := range ids {
			got, err := col.Get(id)
			if err != nil {
				t.Fatal(err)
			}
			requireNativeColumnJSON(t, got, expected[i][k])
		}
	}
	if got, err := reopened.Get(rawKey); err != nil || !bytes.Equal(got, lastRaw) {
		t.Fatalf("reopened raw=%q: %v", got, err)
	}
	if indexedCRUDAppliedLSN(t, reopenedHandle) != startLSN+commits-1 || reopenedHandle.CommandWALNextLSN() != startLSN+commits {
		t.Fatal("reopen changed the completed canonical frontier")
	}
	if err := reopenedManager.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedCache, reopenedRegistry := reopened.cached, reopenedHandle.ValueLogIdentityPinRegistry()
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	requirePublicCOWFinalZero(t, reopenedCache)
	if stats := reopenedRegistry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 || stats.ActiveStableNamespaceLinks != 0 {
		t.Fatalf("reopened final engine resources: %+v", stats)
	}
}
