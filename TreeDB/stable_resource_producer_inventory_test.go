package treedb_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/authorityinventory"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type exactHandleConstructor func(rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error)

func TestEveryRegisterableInventoryFieldHasConcreteExactHandleConstructor(t *testing.T) {
	constructors := map[rootpublication.ReachabilityField]exactHandleConstructor{
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
		constructor, hasConstructor := constructors[field]
		if !policy.Registerable {
			if hasConstructor {
				t.Errorf("excluded field %q has concrete token constructor", field)
			}
			continue
		}
		registerable++
		if !hasConstructor {
			t.Errorf("registerable field %q has no concrete token constructor", field)
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
		resource, err := constructor(spec)
		_ = file.Close()
		if err != nil {
			t.Errorf("concrete token constructor for %q: %v", field, err)
			continue
		}
		if resource.Identity() == (rootpublication.StableIdentity{}) {
			t.Errorf("concrete token constructor for %q did not pin exact handle identity", field)
		}
		resource.Release()
	}
	if len(constructors) != registerable {
		t.Errorf("concrete constructor fields=%d registerable canonical fields=%d", len(constructors), registerable)
	}
}

func TestLegacyVectorAuthorityInventoriesAgreeOnQuarantine(t *testing.T) {
	policy, ok := rootpublication.StableResourcePolicyFor(rootpublication.ReachabilityLegacyVectorSnapshot)
	if !ok || policy.Registerable || policy.Classification != "explicit-legacy-exclusion" {
		t.Fatalf("root-publication legacy vector policy=%+v want explicit exclusion", policy)
	}
	for _, row := range authorityinventory.Rows {
		if row.Field != "collections.LegacyVectorSidecar" {
			continue
		}
		if row.ActivationState != authorityinventory.ActivationQuarantined {
			t.Fatalf("authority inventory legacy vector state=%s want quarantined", row.ActivationState)
		}
		return
	}
	t.Fatal("authority inventory missing collections.LegacyVectorSidecar")
}
