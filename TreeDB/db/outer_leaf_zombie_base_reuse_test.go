package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestOuterLeafBaseDependencyReuseDeclinesRetiredZombieSegment(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	opts := Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
		DisableBackgroundPrune:     true,
		KeepRecent:                 1,
		Durability:                 DurabilityWALOffRelaxed,
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 0, d.valueLogIdentityPins)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	d.SetLeafPageLog(leafLog)
	defer func() {
		if closer, ok := d.leafPageLog.(io.Closer); ok {
			_ = closer.Close()
		}
		d.SetLeafPageLog(nil)
		_ = leafLog.Close()
	}()

	// Write generation 1 keys, then rotate to generation 2 and overwrite all keys
	// so generation 1 has 0 live pages while still present in the durable root's
	// predecessor base closure.
	val1 := []byte(strings.Repeat("gen1-payload-", 4))
	val2 := []byte(strings.Repeat("gen2-payload-", 4))
	for i := 0; i < 64; i++ {
		if err := d.Set([]byte(fmt.Sprintf("k/%04d", i)), val1); err != nil {
			t.Fatalf("Set gen1 %d: %v", i, err)
		}
	}
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf 1: %v", err)
	}
	for i := 0; i < 64; i++ {
		if err := d.Set([]byte(fmt.Sprintf("k/%04d", i)), val2); err != nil {
			t.Fatalf("Set gen2 %d: %v", i, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// Find generation 1's raw/encoded file ID and simulate the exact state where
	// LeafGenerationGC marks a 0-live-page segment zombie while the durable root
	// base closure still retains its predecessor ResourceOuterLeafLog descriptor.
	plan, err := d.LeafGenerationPlan(ctx, LeafGenerationPlanOptions{Force: true})
	if err != nil {
		t.Fatalf("LeafGenerationPlan: %v", err)
	}
	var deadFileID uint32
	for _, g := range plan.Generations {
		if g.LivePages == 0 && len(g.FileIDs) > 0 && g.State != leafGenerationStateWritable {
			deadFileID = page.ValueLogFileID(g.FileIDs[0])
			break
		}
	}
	if deadFileID == 0 {
		t.Fatalf("expected a 0-live-page sealed leaf generation, plan=%+v", plan.Generations)
	}

	// Hold a read snapshot so MarkZombie keeps the file registered as a zombie
	// (matching the live daemon state where value-l255-000184.log remained pinned
	// with IsZombie=true during subsequent writes).
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	if err := d.valueLogManager.MarkZombie(deadFileID); err != nil {
		_ = snap.Close()
		t.Fatalf("MarkZombie(%d): %v", deadFileID, err)
	}

	// Verify planOuterLeafBaseDependencyReuseV1 declines reuse when base contains
	// the zombied deadFileID (the multi-root / collection path where base can
	// retain a 0-live-page raw outer-leaf segment across rotations), and that
	// ordinary writes succeed via exact scan rather than permanently failing
	// with ErrUnresolvedResource.
	d.durablePublishMu.Lock()
	base := d.durableRoot.slotResources[d.durableRoot.slot]
	d.durablePublishMu.Unlock()
	if base == nil {
		_ = snap.Close()
		t.Fatalf("expected non-nil durable root slot resources")
	}
	baseWithDead := base
	diag, err := base.AcquirePhysicalDiagnostics()
	if err != nil {
		_ = snap.Close()
		t.Fatalf("AcquirePhysicalDiagnostics: %v", err)
	}
	hasDeadInBase := false
	for _, desc := range diag.Physical() {
		if desc.Kind == rootpublication.ResourceOuterLeafLog && desc.Generation == uint64(deadFileID) {
			hasDeadInBase = true
			break
		}
	}
	if err := diag.CloseWithError(); err != nil {
		_ = snap.Close()
		t.Fatalf("CloseWithError: %v", err)
	}
	if !hasDeadInBase {
		// Build a synthetic predecessor closure matching the ordered multi-root
		// case where base still carries the retired deadFileID alongside current
		// segments.
		baseWithDead = stableContractResourceSet(t, stableContractDescriptor{
			generation:   uint64(deadFileID),
			kind:         rootpublication.ResourceOuterLeafLog,
			reachability: rootpublication.ReachabilityOuterLeafRawPointer,
			frontier:     4096,
		})
		defer baseWithDead.Release()
	}
	refs, reused, err := d.planOuterLeafBaseDependencyReuseV1(baseWithDead, nil, d.meta, &valueLogRefDelta{outerLeafDependencyReuse: true})
	if err != nil {
		_ = snap.Close()
		t.Fatalf("planOuterLeafBaseDependencyReuseV1: %v", err)
	}
	if reused {
		_ = snap.Close()
		t.Fatalf("expected planOuterLeafBaseDependencyReuseV1 to decline reuse when base contains zombied file %d, got reused=true refs=%v", deadFileID, refs)
	}

	if err := d.Set([]byte("k/after-zombie"), val2); err != nil {
		_ = snap.Close()
		t.Fatalf("Set after MarkZombie(deadFileID=%d) failed: %v", deadFileID, err)
	}
	if err := d.SetSync([]byte("k/after-zombie-sync"), val2); err != nil {
		_ = snap.Close()
		t.Fatalf("SetSync after MarkZombie(deadFileID=%d) failed: %v", deadFileID, err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("snap.Close: %v", err)
	}

	// Conversely, if a genuinely live leaf generation is marked zombie, writes
	// must still fail closed with ErrUnresolvedResource via the exact scan.
	planAfter, err := d.LeafGenerationPlan(ctx, LeafGenerationPlanOptions{Force: true})
	if err != nil {
		t.Fatalf("LeafGenerationPlan after: %v", err)
	}
	var liveFileID uint32
	for _, g := range planAfter.Generations {
		if g.LivePages > 0 && len(g.FileIDs) > 0 {
			liveFileID = page.ValueLogFileID(g.FileIDs[0])
			break
		}
	}
	if liveFileID == 0 {
		t.Fatalf("expected a live leaf generation, plan=%+v", planAfter.Generations)
	}
	liveSnap := d.AcquireSnapshot()
	defer func() { _ = liveSnap.Close() }()
	if err := d.valueLogManager.MarkZombie(liveFileID); err != nil {
		t.Fatalf("MarkZombie(live=%d): %v", liveFileID, err)
	}
	err = d.SetSync([]byte("k/after-live-zombie"), val2)
	if !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("expected ErrUnresolvedResource when live leaf file %d is zombie, got: %v", liveFileID, err)
	}
}
