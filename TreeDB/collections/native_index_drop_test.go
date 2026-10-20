package collections

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// Dropping the earlier declaration changes the surviving index's history-domain
// number, but not its name-keyed physical entries or captured/pending sources.
// Keeping those exact original descriptors proves the operation does not rebuild
// them and exercises their ownership after the opening capture is released.
func TestNativeIndexDropPreservesPendingDomains(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(handle)
	defer manager.Close()
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "users", Indexes: []IndexDefinition{
		{Name: "discard", Field: "city", ValueType: IndexValueString},
		{Name: "keep", Field: "email", ValueType: IndexValueString, Unique: true},
	}}); err != nil {
		t.Fatal(err)
	}
	users, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	seed := []byte(`{"city":"hnl","email":"seed@example.com"}`)
	pending := []byte(`{"city":"sea","email":"pending@example.com"}`)
	if _, err := users.Insert([]byte("seed"), seed); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, err := users.Insert([]byte("pending"), pending); err != nil {
		t.Fatal(err)
	}
	old, err := manager.nativeJSON.Capture("users")
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	keep, ok := old.collection.domains.Get("index/keep")
	if !ok || keep.baseRoot == 0 || (keep.pending == nil && keep.captured == nil) {
		t.Fatal("fixture must retain both physical and pending surviving index sources")
	}
	state, ok := old.collection.domains.Get("index-state")
	if !ok || (state.pending == nil && state.captured == nil) {
		t.Fatal("fixture must retain pending name-keyed index state")
	}
	// Diagnose the ORIGINAL descriptor invariant without retaining a new cut
	// across preparation. A physical rollover/handoff may replace descriptors
	// before canonical reprepare; distinguish that from changing its survivors.
	describe := func(capture backenddb.NativeSemanticCapture) string {
		token, valid := capture.Snapshot().StateToken()
		out := fmt.Sprintf("sequence=%d state=%p token_valid=%t token=%+v", capture.LogicalSequence(), capture.State(), valid, token)
		engine, ok := capture.State().(*nativeEngineVersion)
		if !ok || engine == nil {
			return out + " engine_unavailable"
		}
		collection, ok := engine.names.Get("users")
		if !ok {
			return out + " collection_missing"
		}
		for _, name := range []string{"index/keep", "index-state"} {
			d, found := collection.domains.Get(name)
			pending, pendingFound := collection.pending.Get(name)
			out += fmt.Sprintf(" %s={found:%t descriptor:%p pending_found:%t pending_descriptor:%p", name, found, d, pendingFound, pending)
			if found && d != nil {
				out += fmt.Sprintf(" root:%d interpretation:%d source:%p captured:%p typed:%p", d.baseRoot, d.interpretation, d.pending, d.captured, d.typed)
			}
			out += "}"
		}
		return out
	}
	counters := func() string {
		stats := database.Stats()
		return fmt.Sprintf("rollovers=%s handoffs=%s checkpoints=%s logical=%s", stats["treedb.cache.cow.rollovers_total"], stats["treedb.cache.cow.handoffs_total"], stats["treedb.cache.checkpoint.runs"], stats["treedb.cache.cow.txn.logical_seq"])
	}
	opening := describe(old.capture)
	before := counters()
	var preparationMu sync.Mutex
	var observations []string
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point != durabilitycut.BeforeCOWPreparation {
			return nil
		}
		preparationMu.Lock()
		defer preparationMu.Unlock()
		capture, err := handle.CaptureNativeSemanticState()
		if err != nil {
			observations = append(observations, fmt.Sprintf("root=%q capture_error=%v", event.Root, err))
			return nil // Observation never changes the original write outcome.
		}
		entry := fmt.Sprintf("root=%q %s", event.Root, describe(capture))
		if err := capture.Close(); err != nil {
			entry += fmt.Sprintf(" close_error=%v", err)
		}
		observations = append(observations, entry) // Strings only; no cut/source aliases escape.
		return nil
	})
	dropErr := func() error {
		defer restore()
		_, err := users.DropIndex("discard")
		return err
	}()
	preparationMu.Lock()
	preparation := append([]string(nil), observations...)
	preparationMu.Unlock()
	if dropErr != nil {
		t.Fatalf("drop error=%v opening=%s before=%s preparation=%q after=%s", dropErr, opening, before, preparation, counters())
	}
	current, err := manager.nativeJSON.Capture("users")
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	t.Logf("DROP_DOMAIN_DIAGNOSTIC opening=%s before=%s preparation=%q current=%s after=%s", opening, before, preparation, describe(current.capture), counters())
	for name, want := range map[string]*nativeDomainState{"index/keep": keep, "index-state": state} {
		got, ok := current.collection.domains.Get(name)
		if !ok || got != want {
			t.Fatalf("drop rebuilt original surviving domain %s found=%t got=%p want=%p; opening=%s preparation=%q current=%s before=%s after=%s", name, ok, got, want, opening, preparation, describe(current.capture), before, counters())
		}
		got, ok = current.collection.pending.Get(name)
		if !ok || got != want {
			t.Fatalf("drop lost original pending domain %s found=%t got=%p want=%p; opening=%s preparation=%q current=%s before=%s after=%s", name, ok, got, want, opening, preparation, describe(current.capture), before, counters())
		}
	}
	if _, ok := current.collection.domains.Get("index/discard"); ok {
		t.Fatal("removed domain still belongs to current cut")
	}
	if _, ok := old.collection.domains.Get("index/discard"); !ok {
		t.Fatal("drop changed held old schema")
	}
	// End the opening capture before using the surviving domains again: the new
	// immutable collection, rather than that old read, must own their edges.
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	check := func(read *NativeJSONRead, id string, want []byte) {
		t.Helper()
		got, err := read.Get([]byte(id))
		if err != nil {
			t.Fatal(err)
		}
		var a, b any
		if json.Unmarshal(got, &a) != nil || json.Unmarshal(want, &b) != nil {
			t.Fatalf("invalid document %s", id)
		}
		left, _ := json.Marshal(a)
		right, _ := json.Marshal(b)
		if !bytes.Equal(left, right) {
			t.Fatalf("document %s=%s want=%s", id, left, right)
		}
	}
	for _, item := range []struct{ id, email string }{{"seed", "seed@example.com"}, {"pending", "pending@example.com"}} {
		ids, err := users.FindByIndexValue("keep", item.email)
		if err != nil || len(ids) != 1 || string(ids[0]) != item.id {
			t.Fatalf("surviving index %s=%q err=%v", item.email, ids, err)
		}
	}
	if _, err := users.DropAllIndexes(); err != nil {
		t.Fatal(err)
	}
	last, err := manager.nativeJSON.Capture("users")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"index/keep", "index-state"} {
		if _, ok := last.collection.domains.Get(name); ok {
			t.Fatalf("last removal retained current domain %s", name)
		}
		if _, ok := last.collection.pending.Get(name); ok {
			t.Fatalf("last removal retained pending domain %s", name)
		}
	}
	check(last, "seed", seed)
	check(last, "pending", pending)
	if err := last.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// A held intermediate cut still owns both rows after final index removal
	// and the following physical handoff; it keeps its surviving old schema.
	check(current, "seed", seed)
	check(current, "pending", pending)
	if len(current.meta.Indexes) != 1 || current.meta.Indexes[0].Name != "keep" {
		t.Fatal("held partial-drop schema changed")
	}
	keep, state = nil, nil // End test-only borrowed descriptor aliases before final refund.
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if s := handle.ValueLogIdentityPinRegistry().Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 || s.ActiveStableNamespaceLinks != 0 {
		t.Fatalf("final original registry=%+v", s)
	}
}

// A public checkpoint seals the held pending sources. A subsequent read may
// privately import that physical cut, so DROP must preserve physical identity
// across independent imports and descriptor identity only for canonical state.
func TestNativeIndexDropAfterCheckpointPreservesLatestDomains(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(handle)
	defer manager.Close()
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "users", Indexes: []IndexDefinition{
		{Name: "discard", Field: "city", ValueType: IndexValueString},
		{Name: "keep", Field: "email", ValueType: IndexValueString, Unique: true},
	}}); err != nil {
		t.Fatal(err)
	}
	users, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	seed := []byte(`{"city":"hnl","email":"seed@example.com"}`)
	pending := []byte(`{"city":"sea","email":"pending@example.com"}`)
	if _, err := users.Insert([]byte("seed"), seed); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, err := users.Insert([]byte("pending"), pending); err != nil {
		t.Fatal(err)
	}
	old, err := manager.nativeJSON.Capture("users")
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	oldKeep, ok := old.collection.domains.Get("index/keep")
	if !ok || oldKeep.baseRoot == 0 || (oldKeep.pending == nil && oldKeep.captured == nil) {
		t.Fatal("fixture needs a physical root plus an unsealed pending survivor")
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	latest, err := manager.nativeJSON.Capture("users")
	if err != nil {
		t.Fatal(err)
	}
	defer latest.Close()
	latestKeep, ok := latest.collection.domains.Get("index/keep")
	if !ok || latestKeep.baseRoot == 0 || latestKeep.pending != nil || latestKeep.captured != nil {
		t.Fatal("public checkpoint did not replace the pending descriptor with its physical successor")
	}
	if latest.capture.LogicalSequence() != old.capture.LogicalSequence() {
		t.Fatal("physical checkpoint changed logical sequence")
	}
	before := database.Stats()
	beforeToken, beforeTokenValid := latest.capture.Snapshot().StateToken()
	if _, err := users.DropIndex("discard"); err != nil {
		t.Fatal(err)
	}
	current, err := manager.nativeJSON.Capture("users")
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	after := database.Stats()
	afterToken, afterTokenValid := current.capture.Snapshot().StateToken()
	if !beforeTokenValid || !afterTokenValid || beforeToken != afterToken {
		t.Fatalf("DROP changed the checkpointed physical basis: before=%+v valid=%t after=%+v valid=%t", beforeToken, beforeTokenValid, afterToken, afterTokenValid)
	}
	for _, key := range []string{"treedb.cache.cow.rollovers_total", "treedb.cache.cow.handoffs_total", "treedb.cache.checkpoint.runs"} {
		if before[key] == "" || before[key] != after[key] {
			t.Fatalf("DROP changed physical publication counter %s: before=%q after=%q", key, before[key], after[key])
		}
	}
	canonical := latest.capture.State() != nil && current.capture.State() != nil
	t.Logf("DROP_CHECKPOINT_BASIS latest_state=%p latest_private=%t current_state=%p current_private=%t physical=%+v canonical=%t", latest.capture.State(), latest.privateEngine != nil, current.capture.State(), current.privateEngine != nil, beforeToken, canonical)
	for _, name := range []string{"index/keep", "index-state"} {
		want, wanted := latest.collection.domains.Get(name)
		got, found := current.collection.domains.Get(name)
		if !wanted || !found || want == nil || got == nil {
			t.Fatalf("drop lost latest survivor %s wanted=%t found=%t got=%p want=%p", name, wanted, found, got, want)
		}
		if got.name != want.name || got.baseRoot != want.baseRoot || got.interpretation != want.interpretation || got.storagePolicy != want.storagePolicy || got.pending != nil || got.captured != nil || got.typed != nil {
			t.Fatalf("drop changed checkpointed survivor %s: got root=%d interpretation=%d policy=%v pending=%p captured=%p typed=%p; want root=%d interpretation=%d policy=%v", name, got.baseRoot, got.interpretation, got.storagePolicy, got.pending, got.captured, got.typed, want.baseRoot, want.interpretation, want.storagePolicy)
		}
		if canonical && got != want {
			t.Fatalf("drop replaced canonical surviving descriptor %s got=%p want=%p", name, got, want)
		}
		if _, found := current.collection.pending.Get(name); found {
			t.Fatalf("drop invented pending output for physically sealed survivor %s", name)
		}
	}
	if _, found := current.collection.domains.Get("index/discard"); found {
		t.Fatal("drop retained removed domain")
	}
	if _, found := old.collection.domains.Get("index/discard"); !found {
		t.Fatal("drop changed held historical schema")
	}
	for _, item := range []struct {
		id, email string
		document  []byte
	}{
		{"seed", "seed@example.com", seed}, {"pending", "pending@example.com", pending},
	} {
		for _, read := range []*NativeJSONRead{old, latest, current} {
			got, err := read.Get([]byte(item.id))
			if err != nil || !bytes.Equal(got, item.document) {
				t.Fatalf("held/current %s=%s error=%v", item.id, got, err)
			}
		}
		ids, err := users.FindByIndexValue("keep", item.email)
		if err != nil || len(ids) != 1 || string(ids[0]) != item.id {
			t.Fatalf("latest index %s=%q error=%v", item.email, ids, err)
		}
	}
	oldKeep, latestKeep = nil, nil
	for _, read := range []*NativeJSONRead{old, latest, current} {
		if err := read.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if s := handle.ValueLogIdentityPinRegistry().Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 || s.ActiveStableNamespaceLinks != 0 {
		t.Fatalf("final original registry=%+v", s)
	}
}

func TestNativeIndexDropRejectsDefinitionReplacement(t *testing.T) {
	before := []IndexDefinition{{Name: "discard", Field: "city", ValueType: IndexValueString}, {Name: "keep", Field: "email", ValueType: IndexValueString}}
	changed := before[1]
	changed.Unique = true
	if nativeIndexSubsetRemoval(before, []IndexDefinition{changed}) {
		t.Fatal("definition replacement was classified as removal")
	}
	if nativeIndexSubsetRemoval(before, []IndexDefinition{{Name: "new", Field: "email", ValueType: IndexValueString}}) {
		t.Fatal("new declaration was classified as removal")
	}
}
