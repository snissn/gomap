package db

import (
	"context"
	"errors"
	"fmt"
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
		d.SetLeafPageLog(nil)
		_ = leafLog.Close()
	}()

	// Write generation 1 keys, then rotate to generation 2 and overwrite all keys
	// so generation 1 has 0 live pages while generation 2 holds all live pages.
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

	// Find generation 1's encoded file ID (0 live pages, sealed) and the current
	// writable leaf segment(s).
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
	currentSegments, err := leafPageLogCurrentSegments(d.leafPageLog)
	if err != nil {
		t.Fatalf("leafPageLogCurrentSegments: %v", err)
	}
	if len(currentSegments) == 0 {
		t.Fatalf("expected at least one current leaf segment")
	}

	// Build a predecessor closure matching the ordered multi-root case where base
	// carries BOTH the current writable leaf segment(s) AND the 0-live-page
	// deadFileID.
	descs := make([]stableContractDescriptor, 0, len(currentSegments)+1)
	seenDescs := make(map[uint32]struct{}, len(currentSegments)+1)
	for _, seg := range currentSegments {
		if seg.FileID == 0 {
			continue
		}
		if _, ok := seenDescs[seg.FileID]; ok {
			continue
		}
		seenDescs[seg.FileID] = struct{}{}
		descs = append(descs, stableContractDescriptor{
			generation:   uint64(seg.FileID),
			kind:         rootpublication.ResourceOuterLeafLog,
			reachability: rootpublication.ReachabilityOuterLeafRawPointer,
			frontier:     4096,
		})
	}
	if _, ok := seenDescs[deadFileID]; ok {
		t.Fatalf("deadFileID %d unexpectedly matches current segment", deadFileID)
	}
	descs = append(descs, stableContractDescriptor{
		generation:   uint64(deadFileID),
		kind:         rootpublication.ResourceOuterLeafLog,
		reachability: rootpublication.ReachabilityOuterLeafRawPointer,
		frontier:     4096,
	})
	baseWithDead := stableContractResourceSet(t, descs...)
	defer baseWithDead.Release()

	// Positive control: BEFORE MarkZombie(deadFileID), base reuse must succeed
	// and include both deadFileID and every current segment. This proves the
	// planner reaches the live-segment check rather than returning early on an
	// unknown current segment.
	delta := &valueLogRefDelta{outerLeafDependencyReuse: true}
	refsBefore, reusedBefore, err := d.planOuterLeafBaseDependencyReuseV1(baseWithDead, nil, d.meta, delta)
	if err != nil {
		t.Fatalf("planOuterLeafBaseDependencyReuseV1 (before zombie): %v", err)
	}
	if !reusedBefore {
		t.Fatalf("expected base reuse BEFORE MarkZombie(deadFileID=%d), got reused=false refs=%v", deadFileID, refsBefore)
	}
	if _, ok := refsBefore[deadFileID]; !ok {
		t.Fatalf("expected refsBefore=%v to include deadFileID=%d", refsBefore, deadFileID)
	}
	for _, seg := range currentSegments {
		if seg.FileID == 0 {
			continue
		}
		if _, ok := refsBefore[seg.FileID]; !ok {
			t.Fatalf("expected refsBefore=%v to include current segment %d", refsBefore, seg.FileID)
		}
	}

	// Hold a read snapshot so MarkZombie keeps the file registered as a zombie
	// (matching the live daemon state where a 0-live-page segment remains pinned
	// with IsZombie=true during subsequent writes).
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	snapClosed := false
	defer func() {
		if !snapClosed {
			_ = snap.Close()
		}
	}()
	if err := d.valueLogManager.MarkZombie(deadFileID); err != nil {
		t.Fatalf("MarkZombie(%d): %v", deadFileID, err)
	}

	// Negative assertion: AFTER MarkZombie(deadFileID), base reuse must decline
	// so the exact candidate scanner drops unreachable retired segments.
	refsAfter, reusedAfter, err := d.planOuterLeafBaseDependencyReuseV1(baseWithDead, nil, d.meta, delta)
	if err != nil {
		t.Fatalf("planOuterLeafBaseDependencyReuseV1 (after zombie): %v", err)
	}
	if reusedAfter || refsAfter != nil {
		t.Fatalf("expected planOuterLeafBaseDependencyReuseV1 to decline reuse when base contains zombied file %d, got reused=%t refs=%v", deadFileID, reusedAfter, refsAfter)
	}

	// End-to-end capture proof: captureDurableRootResourcesFromBaseV1 with
	// baseWithDead must fall back to the exact candidate scanner, succeed without
	// ErrUnresolvedResource, drop deadFileID, and retain the live current segment(s).
	scans := 0
	d.testScanCandidateExternalReferencesHook = func() { scans++ }
	captured, err := d.captureDurableRootResourcesFromBaseV1(
		d.idx.Load(), d.meta, delta, baseWithDead, nil,
		rootpublication.StableLogicalObligationRequirements{},
		rootpublication.StableLogicalObligationMutation{},
		rootpublication.StableLogicalObligationMutation{},
		rootpublication.StableResourceClosureWork{},
		nil, false, nil,
	)
	d.testScanCandidateExternalReferencesHook = nil
	if err != nil {
		t.Fatalf("captureDurableRootResourcesFromBaseV1 with zombied deadFileID=%d failed: %v", deadFileID, err)
	}
	defer captured.Release()
	if scans != 1 {
		t.Fatalf("expected exact candidate scan fallback count=1, got %d", scans)
	}
	diag, err := captured.AcquirePhysicalDiagnostics()
	if err != nil {
		t.Fatalf("AcquirePhysicalDiagnostics: %v", err)
	}
	foundLive := make(map[uint32]bool, len(currentSegments))
	for _, desc := range diag.Physical() {
		if desc.Kind != rootpublication.ResourceOuterLeafLog {
			continue
		}
		if desc.Generation == uint64(deadFileID) {
			_ = diag.CloseWithError()
			t.Fatalf("captured resources retained retired zombied deadFileID=%d", deadFileID)
		}
		foundLive[uint32(desc.Generation)] = true
	}
	if err := diag.CloseWithError(); err != nil {
		t.Fatalf("CloseWithError: %v", err)
	}
	for _, seg := range currentSegments {
		if seg.FileID != 0 && !foundLive[seg.FileID] {
			t.Fatalf("captured resources omitted live current segment %d (found=%v)", seg.FileID, foundLive)
		}
	}

	// Ordinary writes must continue to succeed while deadFileID is zombied.
	if err := d.Set([]byte("k/after-zombie"), val2); err != nil {
		t.Fatalf("Set after MarkZombie(deadFileID=%d) failed: %v", deadFileID, err)
	}
	if err := d.SetSync([]byte("k/after-zombie-sync"), val2); err != nil {
		t.Fatalf("SetSync after MarkZombie(deadFileID=%d) failed: %v", deadFileID, err)
	}
	snapClosed = true
	if err := snap.Close(); err != nil {
		t.Fatalf("snap.Close: %v", err)
	}

	// Conversely, if a genuinely live leaf generation is marked zombie, both
	// captureDurableRootResourcesFromBaseV1 and writes must fail closed with
	// ErrUnresolvedResource via the exact scan.
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
	if liveSnap == nil {
		t.Fatalf("AcquireSnapshot (live) returned nil")
	}
	defer func() { _ = liveSnap.Close() }()
	if err := d.valueLogManager.MarkZombie(liveFileID); err != nil {
		t.Fatalf("MarkZombie(live=%d): %v", liveFileID, err)
	}
	liveCaptured, liveErr := d.captureDurableRootResourcesFromBaseV1(
		d.idx.Load(), d.meta, delta, captured, nil,
		rootpublication.StableLogicalObligationRequirements{},
		rootpublication.StableLogicalObligationMutation{},
		rootpublication.StableLogicalObligationMutation{},
		rootpublication.StableResourceClosureWork{},
		nil, false, nil,
	)
	if liveCaptured != nil {
		liveCaptured.Release()
	}
	if !errors.Is(liveErr, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("expected captureDurableRootResourcesFromBaseV1 to fail closed with ErrUnresolvedResource for live zombie %d, got: %v", liveFileID, liveErr)
	}
	err = d.SetSync([]byte("k/after-live-zombie"), val2)
	if !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("expected ErrUnresolvedResource when live leaf file %d is zombie, got: %v", liveFileID, err)
	}
}
