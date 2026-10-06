package treedb

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/storagemaintenance"
)

func TestRecoveredDictionaryIndexGenerationPublicReopenWithoutFreshAppend(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("requires relative namespace")
	}
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := dictdb.New(database.dictdb)
	id, err := store.PutDictBytes(t.Context(), []byte("existing canonical dictionary generation"))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.backend.SetSync([]byte("plain"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	resources, err := store.CaptureDictionaryResources(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	requirements := rootpublication.StableLogicalObligationRequirements{ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityDictionaryGeneration}}
	if err := resources.WalkLogicalObligations(func(_ rootpublication.StableResourcePhysicalDescriptor, obligation rootpublication.StableLogicalObligation) error {
		requirements.Obligations = append(requirements.Obligations, obligation)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	table := func(key, value string) iterator.UnsafeIterator {
		mt := memtable.New()
		mt.Set([]byte(key), []byte(value))
		mt.Freeze()
		return mt.NewIterator(nil, nil)
	}
	_, roots, err := database.backend.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		storagemaintenance.ColumnAssetRewritePlan(), []db.StorageMaintenanceRootDeltaPublishInput{{Iter: table("side-root", "dictionary"), DurableResources: resources, DurableResourceRequirements: requirements}}, nil,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if len(rootIDs) != 1 || rootIDs[0] == 0 {
				return nil, fmt.Errorf("invalid root IDs: %v", rootIDs)
			}
			return table("dictionary-descriptor", fmt.Sprint(rootIDs[0])), nil
		},
	)
	if err != nil || len(roots) != 1 {
		t.Fatalf("publish: roots=%v err=%v", roots, err)
	}
	if err := database.backend.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	commit := database.backend.State().CommitSeq
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	// Each reopen starts from the stored closure and performs no new dictionary
	// append/capture. Read-only and direct backend constructors must install the
	// owning generation hook before selecting either durable slot.
	for _, mode := range []string{"public", "public-readonly", "backend", "backend-readonly"} {
		t.Run(mode, func(t *testing.T) {
			opts := Options{Dir: dir, DisableBackgroundPrune: true, ReadOnly: mode == "public-readonly" || mode == "backend-readonly"}
			var main, side *db.DB
			var closeMain func() error
			if mode == "public" || mode == "public-readonly" {
				reopened, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				main, side, closeMain = reopened.backend, reopened.dictdb, reopened.Close
			} else {
				opened, cleanup, err := OpenBackend(opts)
				if err != nil {
					t.Fatal(err)
				}
				main, closeMain = opened, cleanup
			}
			defer closeMain()
			// Public Close checkpoints cached command-WAL state after the explicit
			// backend checkpoint. Recovery must retain this publication or a newer
			// checkpoint of the same closure, rather than fall back before it.
			if main.State().CommitSeq < commit {
				t.Fatalf("recovery selected commit%d before dictionary publication%d", main.State().CommitSeq, commit)
			}
			if value, err := main.Get([]byte("plain")); err != nil || string(value) != "value" {
				t.Fatalf("recovered value=%q err=%v", value, err)
			}
			if mode == "public" {
				if err := side.VacuumIndexOnline(context.Background()); !errors.Is(err, rootpublication.ErrResourcePinned) {
					t.Fatalf("reopened closure did not fence dictdb namespace: %v", err)
				}
			}
			if err := closeMain(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
