package treedb_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type exactHandleProducer func(rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error)

// TestStableProducerConstructorPolicyAcceptsCanonicalSpecs is a lower-level
// constructor policy check. It is intentionally not the #3771 production
// witness registry: the latter is independently maintained in
// stable_resource_production_witness_registry_test.go and executes real
// producer-owned capture paths.
func TestStableProducerConstructorPolicyAcceptsCanonicalSpecs(t *testing.T) {
	producers := map[rootpublication.ReachabilityField]exactHandleProducer{
		rootpublication.ReachabilityIndexFile:                  db.NewStableDBResourceToken,
		rootpublication.ReachabilityValueLogPointer:            valuelog.NewStableValueLogResourceToken,
		rootpublication.ReachabilityOuterLeafRawPointer:        valuelog.NewStableOuterLeafRawResourceToken,
		rootpublication.ReachabilityOuterLeafPackedPointer:     db.NewStableOuterLeafResourceToken,
		rootpublication.ReachabilityOuterLeafGeneration:        db.NewStableOuterLeafResourceToken,
		rootpublication.ReachabilityDictionaryGeneration:       dictdb.NewStableDictionaryResourceToken,
		rootpublication.ReachabilityTemplateGeneration:         templatedb.NewStableTemplateResourceToken,
		rootpublication.ReachabilityColumnManifest:             collections.NewStableColumnAssetResourceToken,
		rootpublication.ReachabilityTypedColumnMultipart:       collections.NewStableColumnAssetResourceToken,
		rootpublication.ReachabilityTypedColumnValue:           collections.NewStableColumnAssetResourceToken,
		rootpublication.ReachabilityTypedColumnCode:            collections.NewStableColumnAssetResourceToken,
		rootpublication.ReachabilityHNSWSearchPack:             collections.NewStableColumnAssetResourceToken,
		rootpublication.ReachabilityVectorGraphPack:            collections.NewStableColumnAssetResourceToken,
		rootpublication.ReachabilityCommandWALActive:           commitlog.NewStableCommandWALResourceToken,
		rootpublication.ReachabilityCommandWALRotated:          commitlog.NewStableCommandWALResourceToken,
		rootpublication.ReachabilityCommandWALExternalRIDFence: valuelog.NewStableValueLogResourceToken,
	}

	registerable := 0
	for i, field := range rootpublication.RequiredReachabilityFields() {
		policy, ok := rootpublication.StableResourcePolicyFor(field)
		if !ok {
			t.Fatalf("required field %q has no policy", field)
		}
		producer, hasProducer := producers[field]
		if !policy.Registerable {
			if hasProducer {
				t.Errorf("excluded field %q has concrete candidate producer", field)
			}
			continue
		}
		registerable++
		if !hasProducer {
			t.Errorf("registerable field %q has no concrete producer entry point", field)
			continue
		}
		path := filepath.Join(t.TempDir(), fmt.Sprintf("resource-%d", i))
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte("exact-handle-resource")); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		spec := rootpublication.StableResourceSpec{
			Kind: policy.Kind, LogicalLane: "inventory", ResourceID: fmt.Sprint(i + 1), Generation: uint64(i + 1),
			DiagnosticPath: filepath.Base(path), File: file, Frontier: rootpublication.DurableFrontier{Bytes: 1},
			Digest: sha256.Sum256([]byte(field)), Reachability: field,
		}
		resource, err := producer(spec)
		_ = file.Close()
		if err != nil {
			t.Errorf("concrete producer for %q: %v", field, err)
			continue
		}
		if resource.Identity() == (rootpublication.StableIdentity{}) {
			t.Errorf("concrete producer for %q did not pin exact handle identity", field)
		}
		resource.Release()
	}
	if len(producers) != registerable {
		t.Errorf("concrete producer fields=%d registerable canonical fields=%d", len(producers), registerable)
	}
}
