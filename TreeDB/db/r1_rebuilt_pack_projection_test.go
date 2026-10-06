package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Exercise the ordinary current-root vacuum path, including its healthy
// exact-source projection gate, rather than calling the fallback scanner.
func TestR1RebuiltCurrentRootDropsUnreachablePackedDependency(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	dir := t.TempDir()
	opts := leafGenerationPackPublicationTestOptions(dir)
	opts.CommandWAL = true
	opts.Durability = DurabilityDurable
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			closeNoErr(t, database)
		}
	}()
	writeLeafGenerationKeys(t, database, "pack-concurrent", 512, 'a')
	writeLeafGenerationKeys(t, database, "pack-tail", 32, 'b')
	if _, err := database.CompactStorage(context.Background(), CompactStorageOptions{Mode: CompactStorageExhaustive}); err != nil {
		t.Fatal(err)
	}
	selected := database.durableRoot.slotResources[database.durableRoot.slot]
	var pack rootpublication.StableResourcePhysicalDescriptor
	for _, entry := range selected.PhysicalDescriptors() {
		if entry.Kind == rootpublication.ResourceOuterLeafPack {
			pack = entry
			break
		}
	}
	if pack.Kind == "" {
		t.Fatal("fixture did not publish an immutable packed dependency")
	}
	packPath := filepath.Join(dir, pack.DiagnosticPath())
	if _, err := os.Stat(packPath); err != nil {
		t.Fatal(err)
	}
	// This independently retained closure models an already prepared publication
	// member; the view separately retains the old readable root.
	prepared, err := rootpublication.CloneStableResourceSetExcludingKinds(selected)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Release()
	held := database.AcquireSnapshot()
	defer held.Close()
	old, err := held.Get(leafGenerationKey("pack-concurrent", 400))
	if err != nil {
		t.Fatal(err)
	}
	writeLeafGenerationKeys(t, database, "pack-concurrent", 512, 'c')
	writeLeafGenerationKeys(t, database, "pack-tail", 32, 'd')
	stats, err := database.VacuumIndexOnlineWithStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ordinary current vacuum: %+v", stats)
	if !stats.ExactCandidateScan {
		t.Fatal("packed current-root publication bypassed exact candidate scan")
	}
	contains := func(set *rootpublication.StableResourceSet) bool {
		for _, entry := range set.PhysicalDescriptors() {
			if entry.Kind == pack.Kind && entry.Identity() == pack.Identity() {
				return true
			}
		}
		return false
	}
	if contains(database.durableRoot.slotResources[database.durableRoot.slot]) {
		t.Fatal("rebuilt current closure retained exact-scan-unreachable packed file")
	}
	if !contains(database.durableRoot.slotResources[1-database.durableRoot.slot]) {
		t.Fatal("older recovery closure prematurely dropped inherited packed file")
	}
	before := database.State()
	nextLSN := database.CommandWALNextLSN()
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	if database.State().AppliedCommandLSN != before.AppliedCommandLSN || database.CommandWALNextLSN() != nextLSN {
		t.Fatal("fallback convergence changed command coverage")
	}
	for _, resources := range database.durableRoot.slotResources {
		if contains(resources) {
			t.Fatal("fallback did not converge to projected current closure")
		}
	}
	if _, err := database.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(packPath); err != nil {
		t.Fatalf("held/prepared pack lost: %v", err)
	}
	got, err := held.Get(leafGenerationKey("pack-concurrent", 400))
	if err != nil || !bytes.Equal(got, old) {
		t.Fatalf("held old row=(%q,%v), want %q", got, err, old)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := database.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(packPath); err != nil {
		t.Fatalf("prepared pack lost: %v", err)
	}
	prepared.Release()
	gc, err := database.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("after retained member release: %+v", gc)
	for _, path := range []string{packPath, packPath + ".lenidx"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("unreachable file still linked %s: %v", path, err)
		}
	}
	for i := 0; i < 512; i++ {
		expectLeafGenerationValue(t, database, leafGenerationKey("pack-concurrent", i), 'c')
	}
	newestSlot := database.metaPageID
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	// Force selection of the converged older durable slot after actual unlink.
	corruptIndexPageByte(t, dir, newestSlot)
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoErr(t, reopened)
	for i := 0; i < 512; i++ {
		expectLeafGenerationValue(t, reopened, leafGenerationKey("pack-concurrent", i), 'c')
	}
	for i := 0; i < 32; i++ {
		expectLeafGenerationValue(t, reopened, leafGenerationKey("pack-tail", i), 'd')
	}
}

// A scanner file ID cannot authorize an unrelated producer file. Reject both
// absent manager authority and a real, canonical ID bound to the wrong inode.
func TestR1RebuiltPackedCaptureRejectsWrongPhysicalAuthority(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	dir := t.TempDir()
	opts := leafGenerationPackPublicationTestOptions(dir)
	opts.CommandWAL, opts.Durability = true, DurabilityDurable
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoErr(t, database)
	writeLeafGenerationKeys(t, database, "authority", 512, 'a')
	if _, err := database.CompactStorage(context.Background(), CompactStorageOptions{Mode: CompactStorageExhaustive}); err != nil {
		t.Fatal(err)
	}
	var generation uint64
	for _, entry := range database.durableRoot.slotResources[database.durableRoot.slot].PhysicalDescriptors() {
		if entry.Kind == rootpublication.ResourceOuterLeafPack {
			generation = entry.Generation
			break
		}
	}
	if generation == 0 {
		t.Fatal("missing real registered pack")
	}
	t.Run("physical-alias", func(t *testing.T) {
		var packPath string
		for _, entry := range database.durableRoot.slotResources[database.durableRoot.slot].PhysicalDescriptors() {
			if entry.Kind == rootpublication.ResourceOuterLeafPack && entry.Generation == generation {
				packPath = filepath.Join(dir, entry.DiagnosticPath())
			}
		}
		file, err := os.Open(packPath)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		builder := rootpublication.NewStableResourceSetBuilder()
		defer builder.Abandon()
		for _, gen := range []uint64{generation, generation + 10000} {
			token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
				Kind: rootpublication.ResourceOuterLeafPack, LogicalLane: "leaf-generation-pack",
				ResourceID: strconv.FormatUint(gen, 10), Generation: gen, DiagnosticPath: "same-physical-pack", File: file,
				Frontier: rootpublication.DurableFrontier{Bytes: 1}, Digest: sha256.Sum256([]byte("same")),
				Reachability: rootpublication.ReachabilityOuterLeafPackedPointer,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := builder.Add(token); err != nil {
				token.Release()
				t.Fatal(err)
			}
		}
		source, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		defer source.Release()
		if source.Len() != 2 {
			t.Fatalf("alias fixture coalesced: %d", source.Len())
		}
		result, _, err := database.captureRebuiltIndexDurableResourcesWithWorkV1(database.idx.Load().pager, database.meta, source)
		if result != nil {
			result.Release()
			t.Fatal("returned candidate with ambiguous physical alias")
		}
		if !errors.Is(err, rootpublication.ErrResourceConflict) {
			t.Fatalf("alias error=%v", err)
		}
	})
	for _, name := range []string{"wrong-physical", "missing-manager", "malformed-id"} {
		t.Run(name, func(t *testing.T) {
			gen := generation
			if name == "missing-manager" {
				gen += 10000
			}
			resourceID := strconv.FormatUint(gen, 10)
			if name == "malformed-id" {
				resourceID = "0" + resourceID
			}
			file, err := os.CreateTemp(t.TempDir(), "unrelated-pack")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if _, err := file.Write([]byte("wrong producer bytes")); err != nil {
				t.Fatal(err)
			}
			token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
				Kind: rootpublication.ResourceOuterLeafPack, LogicalLane: "leaf-generation-pack",
				ResourceID: resourceID, Generation: gen, DiagnosticPath: "unrelated-pack", File: file,
				Frontier: rootpublication.DurableFrontier{Bytes: 1}, Digest: sha256.Sum256([]byte("wrong")),
				Reachability: rootpublication.ReachabilityOuterLeafPackedPointer,
			})
			if err != nil {
				t.Fatal(err)
			}
			builder := rootpublication.NewStableResourceSetBuilder()
			defer builder.Abandon()
			if err := builder.Add(token); err != nil {
				token.Release()
				t.Fatal(err)
			}
			source, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer source.Release()
			before := database.State()
			currentResources := database.durableRoot.slotResources[database.durableRoot.slot]
			result, _, err := database.captureRebuiltIndexDurableResourcesWithWorkV1(database.idx.Load().pager, database.meta, source)
			if result != nil {
				result.Release()
				t.Fatal("returned candidate with unproved physical authority")
			}
			if !errors.Is(err, rootpublication.ErrUnresolvedResource) {
				t.Fatalf("error=%v", err)
			}
			after := database.State()
			if after.CommitSeq != before.CommitSeq || after.RootPageID != before.RootPageID || after.SystemRootPageID != before.SystemRootPageID ||
				after.AppliedCommandLSN != before.AppliedCommandLSN || database.durableRoot.slotResources[database.durableRoot.slot] != currentResources {
				t.Fatal("failed capture mutated published authority")
			}
			expectLeafGenerationValue(t, database, leafGenerationKey("authority", 0), 'a')
		})
	}
}
