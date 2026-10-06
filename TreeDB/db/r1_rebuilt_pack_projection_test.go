package db

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
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
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	database.SetLeafPageLog(leafLog)
	defer closeNoErr(t, leafLog)
	candidate := prepareLeafGenerationPackTestCandidate(t, database, leafLog, 512)
	if _, err := database.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID}, Force: true, Sync: true,
	}); err != nil {
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
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
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
