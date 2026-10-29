package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Reuse a finite hot set through real accepted successors and disk handoffs.
// The same accepted group spans raw, JSON, typed string/vector, BSON multikey
// and template-v1 state. This is a default-budget engine progress guard, not a
// timing, process-kill, maintenance-GC or complete-Q qualification.
func TestNativeMixedEngineLifecycle(t *testing.T) {
	// Isolate the strict logical-Commit physical-prefix oracle from the
	// independent periodic maintenance publisher; explicit checkpoints remain.
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
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
	fixtures := append(nativeTypedMultiCases(),
		nativeTypedMultiCase{name: "bson_arrays", family: "bson_multikey"},
		nativeTypedMultiCase{name: "templates", family: "template_v1"},
	)
	ids := [][]byte{[]byte("hot0"), []byte("hot1")}
	// Each generation is constructed from original scalar values, independently
	// of pending reconstruction, physical storage and caller-returned documents.
	source := func(collection, key, generation int) []byte {
		id := string(ids[key])
		switch fixtures[collection].family {
		case "string":
			return []byte(fmt.Sprintf(`{"id":%q,"label":%q,"payload":%q}`, id, fmt.Sprintf("round %d \"quoted\" \\ label", generation), fmt.Sprintf("row%d/gen%d", key, generation)))
		case "bson_multikey":
			return []byte(fmt.Sprintf(`{"id":%q,"tags":[%q,%q],"aliases":[%q,%q],"payload":%q}`, id,
				fmt.Sprintf("tag%d", generation), fmt.Sprintf("tag%d", generation),
				fmt.Sprintf("owner%d/gen%d", key, generation), fmt.Sprintf("owner%d/gen%d", key, generation), fmt.Sprintf("row%d/gen%d", key, generation)))
		case "template_v1":
			return []byte(fmt.Sprintf(`{"id":%q,"owner":%q,"n":%d,"payload":%q}`, id, fmt.Sprintf("owner%d/gen%d", key, generation), generation, fmt.Sprintf("row%d/gen%d", key, generation)))
		case "float32_vector":
			return []byte(fmt.Sprintf(`{"id":%q,"embedding":[%d,%d,0.5],"payload":%q}`, id, generation+1, -generation-2, fmt.Sprintf("row%d/gen%d", key, generation)))
		default:
			return []byte(fmt.Sprintf(`{"id":%q,"n":%d,"payload":%q}`, id, generation, fmt.Sprintf("row%d/gen%d", key, generation)))
		}
	}
	document := func(collection, key, generation int) []byte {
		value := source(collection, key, generation)
		switch fixtures[collection].family {
		case "bson_multikey":
			return nativeIndexDDLDocument(t, collections.DocumentFormatBSON, string(value))
		case "template_v1":
			return nativeTemplateDoc(t, string(value))
		default:
			return value
		}
	}
	original := make([][2][]byte, len(fixtures))
	expected := make([][2][]byte, len(fixtures))
	var expectedGeneration [2]int
	cols := make([]*collections.Collection, len(fixtures))
	for i, fixture := range fixtures {
		meta := &collections.CollectionMeta{Name: fixture.name}
		switch fixture.family {
		case "bson_multikey":
			meta.Options.DocumentFormat = collections.DocumentFormatBSON
			meta.Indexes = []collections.IndexDefinition{
				{Name: "tags", Field: "tags", ValueType: collections.IndexValueString, MultiKey: true},
				{Name: "aliases", Field: "aliases", ValueType: collections.IndexValueString, MultiKey: true, Unique: true},
			}
		case "template_v1":
			meta.Options.DocumentFormat = collections.DocumentFormatTemplateV1
			meta.Indexes = []collections.IndexDefinition{
				{Name: "owner", Field: "owner", ValueType: collections.IndexValueString, Unique: true},
			}
		case "string", "float32_vector":
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
			original[i][k] = source(i, k, 0)
			expected[i][k] = original[i][k]
		}
		if _, err := cols[i].InsertBatch(ids, [][]byte{document(i, 0, 0), document(i, 1, 0)}); err != nil {
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
	var oldTemplate *collections.StoredDocumentJSONMaterializer
	t.Cleanup(func() {
		if oldTemplate != nil {
			if err := oldTemplate.Close(); err != nil {
				t.Error(err)
			}
		}
	})
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
	oldTemplate, err = cols[len(cols)-1].NewStoredDocumentJSONMaterializer()
	if err != nil {
		t.Fatal(err)
	}
	// Keep the original template resolver alongside the old document view.
	// Current and staged documents use the ordinary current materializer; all
	// generations deliberately preserve the seeded shape, without guessing IDs.
	checkDocument := func(i int, col *collections.Collection, stored, want []byte, old bool) {
		t.Helper()
		var materialized []byte
		var err error
		switch fixtures[i].family {
		case "template_v1":
			if !bytes.HasPrefix(stored, []byte("TD1D")) {
				t.Fatalf("template stored document prefix=%q", stored[:min(4, len(stored))])
			}
			if old {
				materialized, err = oldTemplate.StoredDocumentJSON(stored)
			} else {
				materialized, err = col.StoredDocumentJSON(stored)
			}
		case "bson_multikey":
			materialized, err = col.StoredDocumentJSON(stored)
		default:
			materialized = stored
		}
		if err != nil {
			t.Fatalf("materialize %s: %v", fixtures[i].name, err)
		}
		requireNativeColumnJSON(t, materialized, want)
	}
	checkIndexOwners := func(i int, query func(string, any) ([][]byte, error), generations [2]int) {
		t.Helper()
		index := "owner"
		if fixtures[i].family == "bson_multikey" {
			index = "aliases"
		}
		for k, generation := range generations {
			if got, err := query(index, fmt.Sprintf("owner%d/gen%d", k, generation)); err != nil || !reflect.DeepEqual(got, [][]byte{ids[k]}) {
				t.Fatalf("%s %s owner%d/gen%d=%q: %v", fixtures[i].name, index, k, generation, got, err)
			}
			if generation != 0 {
				if got, err := query(index, fmt.Sprintf("owner%d/gen0", k)); err != nil || len(got) != 0 {
					t.Fatalf("%s retained obsolete current owner%d/gen0=%q: %v", fixtures[i].name, k, got, err)
				}
			}
		}
		if fixtures[i].family == "bson_multikey" {
			for _, generation := range generations {
				var want [][]byte
				for k, current := range generations {
					if current == generation {
						want = append(want, ids[k])
					}
				}
				if got, err := query("tags", fmt.Sprintf("tag%d", generation)); err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("BSON effective array tag%d=%q want%q: %v", generation, got, want, err)
				}
			}
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
				checkDocument(i, cols[i], result.Document, original[i][k], true)
				result.Document[0] = '!'
			}
			if fixtures[i].family == "bson_multikey" || fixtures[i].family == "template_v1" {
				checkIndexOwners(i, func(index string, value any) ([][]byte, error) {
					var found [][]byte
					err := view.VisitIndexValueIDs(index, value, func(id []byte) error {
						found = append(found, bytes.Clone(id))
						return nil
					})
					return found, err
				}, [2]int{})
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
				checkDocument(i, cols[i], got, expected[i][k], false)
				got[0] = '!'
			}
			if fixture.family == "bson_multikey" || fixture.family == "template_v1" {
				checkIndexOwners(i, cols[i].FindByIndexValue, expectedGeneration)
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
		before := database.Stats()
		next, sequence := handle.CommandWALNextLSN(), before["treedb.cache.cow.txn.logical_seq"]
		tx, err := native.Begin("labels", "vectors", "docs", "bson_arrays", "templates")
		if err != nil {
			t.Fatalf("Begin generation%d accepted%d: %v", generation, accepted, err)
		}
		// Begin may drain prior accepted work before opening this transaction.
		// Keep the physical-prefix oracle around this transaction's mutations
		// and Commit; preparation must not assign a new logical command.
		before = database.Stats()
		if handle.CommandWALNextLSN() != next || before["treedb.cache.cow.txn.logical_seq"] != sequence {
			t.Fatal("Begin assigned a logical command during producer preparation")
		}
		physical := indexedCRUDAppliedLSN(t, handle)
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
					checkDocument(i, cols[i], got, source(i, key, generation), false)
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
		if applied := indexedCRUDAppliedLSN(t, handle); applied != physical {
			after := database.Stats()
			t.Fatalf("logical commit unexpectedly installed physical effects: generation%d accepted%d checkpoints%d physical=%d->%d assigned=%d next=%d checkpoint_runs=%s->%s rollovers=%s->%s handoffs=%s->%s logical_seq=%s->%s",
				generation, accepted, checkpoints, physical, applied, next, handle.CommandWALNextLSN(),
				before["treedb.cache.checkpoint.runs"], after["treedb.cache.checkpoint.runs"],
				before["treedb.cache.cow.rollovers_total"], after["treedb.cache.cow.rollovers_total"],
				before["treedb.cache.cow.handoffs_total"], after["treedb.cache.cow.handoffs_total"],
				sequence, after["treedb.cache.cow.txn.logical_seq"])
		}
		for i := range fixtures {
			expected[i][key] = source(i, key, generation)
		}
		expectedGeneration[key] = generation
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
			if err := oldTemplate.Close(); err != nil {
				t.Fatal(err)
			}
			oldTemplate = nil
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
	t.Logf("accepted_groups=%d collections_per_group=%d raw_writes_per_group=1 checkpoint_generations=%d held_through=%d continued_after_release=%d", accepted, len(fixtures), checkpoints, releaseAfter, accepted-releaseAfter)
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
			checkDocument(i, col, got, expected[i][k], false)
		}
		if fixture.family == "bson_multikey" || fixture.family == "template_v1" {
			checkIndexOwners(i, col.FindByIndexValue, expectedGeneration)
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
