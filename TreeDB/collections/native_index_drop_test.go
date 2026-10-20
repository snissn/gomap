package collections

import (
	"bytes"
	"encoding/json"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
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
	if _, err := users.DropIndex("discard"); err != nil {
		t.Fatal(err)
	}
	current, err := manager.nativeJSON.Capture("users")
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	for name, want := range map[string]*nativeDomainState{"index/keep": keep, "index-state": state} {
		got, ok := current.collection.domains.Get(name)
		if !ok || got != want {
			t.Fatalf("drop rebuilt original surviving domain %s", name)
		}
		got, ok = current.collection.pending.Get(name)
		if !ok || got != want {
			t.Fatalf("drop lost original pending domain %s", name)
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
