package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestPrepareDurableRootManifestResourcesCompatibility(t *testing.T) {
	newSet := func(t *testing.T, token *rootpublication.StableResourceToken) *rootpublication.StableResourceSet {
		t.Helper()
		builder := rootpublication.NewStableResourceSetBuilder()
		if token != nil {
			if err := builder.Add(token); err != nil {
				token.Release()
				t.Fatal(err)
			}
		}
		resources, err := builder.Freeze()
		if err != nil {
			builder.Abandon()
			t.Fatal(err)
		}
		return resources
	}
	newFixture := func(t *testing.T) (*DB, *leafGenerationManifest, *leafGenerationManifestStore) {
		t.Helper()
		store := newLeafGenerationManifestStore(t.TempDir(), nil, leafGenerationManifestCompatibility, nil)
		t.Cleanup(store.Close)
		initial := newLeafGenerationManifest(1)
		if token, err := store.Replace(initial); err != nil {
			t.Fatal(err)
		} else if token != nil {
			t.Fatal("compatibility store returned exact token")
		}
		candidate := initial.clone()
		if changed, err := candidate.registerCurrentGenerationFileID(7, 2); err != nil || !changed {
			t.Fatalf("register candidate file changed=%t err=%v", changed, err)
		}
		return &DB{leafGenerationManifestStore: store}, candidate, store
	}

	t.Run("no exact authority", func(t *testing.T) {
		database, candidate, store := newFixture(t)
		current := newSet(t, nil)
		older := newSet(t, nil)
		defer current.Release()
		defer older.Release()
		persisted, gotCurrent, gotOlder, err := database.prepareDurableRootManifestResourcesV1(candidate, current, older)
		if err != nil {
			t.Fatal(err)
		}
		if gotCurrent != current || gotOlder != older {
			t.Fatal("compatibility preparation replaced input resource sets")
		}
		if persisted.ManifestRevision <= candidate.ManifestRevision {
			t.Fatal("manifest revision did not advance")
		}
		store.Close()
		reopened := newLeafGenerationManifestStore(store.leafDir, nil, leafGenerationManifestCompatibility, nil)
		defer reopened.Close()
		loaded, ok, err := reopened.Load()
		if err != nil || !ok {
			t.Fatalf("load persisted manifest ok=%t err=%v", ok, err)
		}
		if loaded.ManifestRevision != persisted.ManifestRevision || !loaded.hasNonDeletedFileID(7) {
			t.Fatalf("persisted manifest revision=%d file7=%t, returned revision=%d", loaded.ManifestRevision, loaded.hasNonDeletedFileID(7), persisted.ManifestRevision)
		}
	})

	if !rootpublication.StableRelativeNamespaceSupported() {
		return
	}
	for _, exactAt := range []string{"current", "older"} {
		t.Run("reject exact "+exactAt, func(t *testing.T) {
			database, candidate, compatibilityStore := newFixture(t)
			exactStore := newLeafGenerationManifestStore(t.TempDir(), rootpublication.NewIdentityPinRegistry(), leafGenerationManifestStable, nil)
			defer exactStore.Close()
			token, err := exactStore.Replace(newLeafGenerationManifest(1))
			if err != nil {
				t.Fatal(err)
			}
			exact := newSet(t, token)
			empty := newSet(t, nil)
			current, older := exact, empty
			if exactAt == "older" {
				current, older = empty, exact
			}
			defer current.Release()
			defer older.Release()
			before, ok, err := compatibilityStore.Load()
			if err != nil || !ok {
				t.Fatalf("load before rejection ok=%t err=%v", ok, err)
			}
			if _, _, _, err := database.prepareDurableRootManifestResourcesV1(candidate, current, older); !errors.Is(err, rootpublication.ErrResourceConflict) {
				t.Fatalf("prepare error=%v want resource conflict", err)
			}
			after, ok, err := compatibilityStore.Load()
			if err != nil || !ok {
				t.Fatalf("load after rejection ok=%t err=%v", ok, err)
			}
			if after.ManifestRevision != before.ManifestRevision || after.hasNonDeletedFileID(7) {
				t.Fatalf("rejection persisted candidate: before=%d after=%d file7=%t", before.ManifestRevision, after.ManifestRevision, after.hasNonDeletedFileID(7))
			}
			if current.Owner() != rootpublication.ResourceOwnerBuilder || older.Owner() != rootpublication.ResourceOwnerBuilder {
				t.Fatalf("rejection consumed inputs: current=%v older=%v", current.Owner(), older.Owner())
			}
		})
	}
}
