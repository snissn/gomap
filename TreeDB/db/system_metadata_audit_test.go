package db_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func buildTestZstdDict(t *testing.T, seed string, id uint32) []byte {
	t.Helper()
	samples := make([][]byte, 32)
	for i := range samples {
		samples[i] = append(bytes.Repeat([]byte(seed+"-sample-payload-prefix-0123456789-"), 12), []byte(fmt.Sprintf("rec-%04d-val-%06d", i, i*41))...)
	}
	dictBytes, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       id,
		Contents: samples,
		History:  samples[0],
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil || len(dictBytes) == 0 {
		t.Fatalf("BuildDict(%s): len=%d err=%v", seed, len(dictBytes), err)
	}
	return dictBytes
}

func TestSystemMetadataAudit_EmptyDB(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	rep, err := d.SystemMetadataAudit(context.Background(), db.SystemMetadataAuditOptions{})
	if err != nil {
		t.Fatalf("SystemMetadataAudit: %v", err)
	}
	if rep.DictsTotal != 0 || rep.TemplatesTotal != 0 || rep.SegmentsScannedValueLog != 0 || rep.SegmentsScannedLeafLog != 0 {
		t.Fatalf("unexpected non-zero empty audit: %+v", rep)
	}
}

func TestSystemMetadataAudit_DictsTemplatesAndPointerSegments(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()

	dstore := dictdb.New(d)
	tstore := templatedb.NewBackend(d, templatedb.Config{})

	dictDeadBytes := buildTestZstdDict(t, "dead-dict-gen1", 11)
	dictLiveBytes := buildTestZstdDict(t, "live-dict-gen2", 22)
	dictCurrentBytes := buildTestZstdDict(t, "current-dict-gen3", 33)
	dictPtrDeadBytes := buildTestZstdDict(t, "ptr-dead-dict-gen4", 44)

	deadDictID, err := dstore.PublishDict(ctx, dictDeadBytes, 4, "default", true)
	if err != nil {
		t.Fatalf("PublishDict(dead): %v", err)
	}
	liveDictID, err := dstore.PublishDict(ctx, dictLiveBytes, 4, "default", true)
	if err != nil {
		t.Fatalf("PublishDict(live): %v", err)
	}
	// Supersede default with dictCurrentBytes so deadDictID and liveDictID are not current.
	currentDictID, err := dstore.PublishDict(ctx, dictCurrentBytes, 8, "default", true)
	if err != nil {
		t.Fatalf("PublishDict(current): %v", err)
	}
	// Store one dead dict via lane-254 ValuePtr.
	dstore.SetPointerThreshold(1)
	ptrDeadDictID, err := dstore.PutDictBytes(ctx, dictPtrDeadBytes)
	if err != nil {
		t.Fatalf("PutDictBytes(ptrDead): %v", err)
	}

	// Publish two templates: one live (written into value_vlog), one dead.
	tcfg := templ.NormalizeConfig(templ.Config{})
	liveDefBytes, err := templ.EncodeTemplateDef(templ.TemplateDef{
		Kind:    templ.TemplateAnchors,
		Anchors: [][]byte{[]byte("live-anchor-prefix-"), []byte("-live-anchor-suffix")},
	}, tcfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef(live): %v", err)
	}
	deadDefBytes, err := templ.EncodeTemplateDef(templ.TemplateDef{
		Kind:    templ.TemplateAnchors,
		Anchors: [][]byte{[]byte("dead-anchor-prefix-"), []byte("-dead-anchor-suffix")},
	}, tcfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef(dead): %v", err)
	}
	const sharedFP uint64 = 0x1111222233334444
	const deadOnlyFP uint64 = 0x5555666677778888
	liveTplID, err := tstore.PutTemplateDef(ctx, liveDefBytes, []uint64{sharedFP})
	if err != nil {
		t.Fatalf("PutTemplateDef(live): %v", err)
	}
	deadTplID, err := tstore.PutTemplateDef(ctx, deadDefBytes, []uint64{sharedFP, deadOnlyFP})
	if err != nil {
		t.Fatalf("PutTemplateDef(dead): %v", err)
	}

	// Write a sealed value_vlog segment using liveDictID + liveTplID (and rotate to seq 2 so seq 1 is sealed/cacheable).
	tplPayload, err := templ.EncodePayload(liveTplID, [][]byte{[]byte("gapA"), []byte("gapB"), []byte("gapC")})
	if err != nil {
		t.Fatalf("EncodePayload: %v", err)
	}
	vlogDir := db.ValueLogDirPath(dir)
	fileID1, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	path1 := filepath.Join(vlogDir, "value-l0-000001.log")
	w1, err := valuelog.NewWriter(path1, fileID1)
	if err != nil {
		t.Fatalf("NewWriter(1): %v", err)
	}
	if _, err := w1.AppendFrame(liveDictID, dictLiveBytes, []valuelog.Record{
		{RID: 1, Value: append(append([]byte(nil), tplPayload...), bytes.Repeat([]byte("live-dict-gen2-sample-payload-prefix-0123456789-"), 6)...)},
		{RID: 2, Value: bytes.Repeat([]byte("live-dict-gen2-sample-payload-prefix-0123456789-"), 12)},
	}); err != nil {
		t.Fatalf("AppendFrame: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("Close w1: %v", err)
	}
	if err := d.RegisterValueLogSegment(path1, fileID1); err != nil {
		t.Fatalf("RegisterValueLogSegment(1): %v", err)
	}
	// Create an active seq 2 segment so seq 1 is sealed (non-active) and exercises cache hits.
	fileID2, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	path2 := filepath.Join(vlogDir, "value-l0-000002.log")
	w2, err := valuelog.NewWriter(path2, fileID2)
	if err != nil {
		t.Fatalf("NewWriter(2): %v", err)
	}
	if _, err := w2.Append(0, nil, 3, []byte("plain-value")); err != nil {
		t.Fatalf("Append w2: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close w2: %v", err)
	}
	if err := d.RegisterValueLogSegment(path2, fileID2); err != nil {
		t.Fatalf("RegisterValueLogSegment(2): %v", err)
	}

	// 1) With default session grace (KeepRecentPublishedDicts=2), ptrDeadDictID and currentDictID are protected.
	repGrace, err := d.SystemMetadataAudit(ctx, db.SystemMetadataAuditOptions{})
	if err != nil {
		t.Fatalf("SystemMetadataAudit(grace): %v", err)
	}
	if repGrace.DictsTotal != 4 || repGrace.DictsLive != 1 || repGrace.DictsActiveCurrent != 1 || repGrace.DictsGraceProtected != 1 || repGrace.DictsReclaimable != 1 {
		t.Fatalf("unexpected grace dict breakdown: %+v", repGrace)
	}
	if len(repGrace.ReclaimableDictIDs) != 1 || repGrace.ReclaimableDictIDs[0] != deadDictID {
		t.Fatalf("ReclaimableDictIDs(grace)=%v want [%d]", repGrace.ReclaimableDictIDs, deadDictID)
	}
	// Both templates were published in this session (<= 16), so grace protects deadTplID.
	if repGrace.TemplatesTotal != 2 || repGrace.TemplatesLive != 1 || repGrace.TemplatesGraceProtected != 1 || repGrace.TemplatesReclaimable != 0 {
		t.Fatalf("unexpected grace template breakdown: %+v", repGrace)
	}

	// 2) With grace disabled (-1), both deadDictID & ptrDeadDictID and deadTplID are reclaimable,
	// and sealed segment 1 hits the segment ID cache on the second audit.
	repNoGrace, err := d.SystemMetadataAudit(ctx, db.SystemMetadataAuditOptions{
		KeepRecentPublishedDicts:     -1,
		KeepRecentPublishedTemplates: -1,
	})
	if err != nil {
		t.Fatalf("SystemMetadataAudit(no-grace): %v", err)
	}
	if repNoGrace.DictsTotal != 4 || repNoGrace.DictsLive != 1 || repNoGrace.DictsActiveCurrent != 1 || repNoGrace.DictsReclaimable != 2 {
		t.Fatalf("unexpected no-grace dict breakdown: %+v", repNoGrace)
	}
	if repNoGrace.PointerSegmentsReferenced != 1 || repNoGrace.PointerSegmentsOnlyReclaimable != 1 {
		t.Fatalf("pointer segments ref=%d onlyReclaimable=%d want 1,1", repNoGrace.PointerSegmentsReferenced, repNoGrace.PointerSegmentsOnlyReclaimable)
	}
	if repNoGrace.TemplatesTotal != 2 || repNoGrace.TemplatesLive != 1 || repNoGrace.TemplatesReclaimable != 1 {
		t.Fatalf("unexpected no-grace template breakdown: %+v", repNoGrace)
	}
	if len(repNoGrace.ReclaimableTemplateIDs) != 1 || repNoGrace.ReclaimableTemplateIDs[0] != deadTplID {
		t.Fatalf("ReclaimableTemplateIDs=%v want [%d]", repNoGrace.ReclaimableTemplateIDs, deadTplID)
	}
	if repNoGrace.TemplateFPKeysTotal != 2 || repNoGrace.TemplateFPKeysPrunable != 1 || repNoGrace.TemplateFPKeysDeletable != 1 {
		t.Fatalf("FP keys total=%d prunable=%d deletable=%d want 2,1,1",
			repNoGrace.TemplateFPKeysTotal, repNoGrace.TemplateFPKeysPrunable, repNoGrace.TemplateFPKeysDeletable)
	}
	if repNoGrace.SegmentsCacheHits < 1 {
		t.Fatalf("expected sealed segment cache hit on second audit, got %d", repNoGrace.SegmentsCacheHits)
	}

	// 3) Explicit ProtectedDictIDs / ProtectedTemplateIDs pin entries even with grace disabled.
	repPinned, err := d.SystemMetadataAudit(ctx, db.SystemMetadataAuditOptions{
		KeepRecentPublishedDicts:     -1,
		KeepRecentPublishedTemplates: -1,
		ProtectedDictIDs:             []uint64{ptrDeadDictID},
		ProtectedTemplateIDs:         []uint64{deadTplID},
	})
	if err != nil {
		t.Fatalf("SystemMetadataAudit(pinned): %v", err)
	}
	if repPinned.DictsReclaimable != 1 || repPinned.TemplatesReclaimable != 0 || repPinned.PointerSegmentsOnlyReclaimable != 0 {
		t.Fatalf("expected pins to protect ptrDeadDictID & deadTplID: %+v", repPinned)
	}
	_ = currentDictID

	// 4) Reopen read-only: session recent lists are empty, so default audit works cleanly in RO mode.
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ro, err := db.Open(db.Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open(ReadOnly): %v", err)
	}
	defer func() { _ = ro.Close() }()
	repRO, err := ro.SystemMetadataAudit(ctx, db.SystemMetadataAuditOptions{})
	if err != nil {
		t.Fatalf("SystemMetadataAudit(ReadOnly): %v", err)
	}
	if repRO.DictsTotal != 4 || repRO.DictsLive != 1 || repRO.DictsActiveCurrent != 1 || repRO.DictsReclaimable != 2 || repRO.TemplatesReclaimable != 1 {
		t.Fatalf("unexpected read-only audit: %+v", repRO)
	}
}

func TestSystemMetadataAudit_ProtectsStagedAndLeafVlogDict(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	ctx := context.Background()

	dstore := dictdb.New(d)
	leafDictBytes := buildTestZstdDict(t, "leaf-vlog-dict", 55)
	leafDictID, err := dstore.PutDictBytes(ctx, leafDictBytes)
	if err != nil {
		t.Fatalf("PutDictBytes(leaf): %v", err)
	}
	stagedDictBytes := buildTestZstdDict(t, "staged-dict", 66)
	stagedDictID, err := dstore.PutDictBytes(ctx, stagedDictBytes)
	if err != nil {
		t.Fatalf("PutDictBytes(staged): %v", err)
	}

	// Register a leaf_vlog segment compressed with leafDictID.
	leafDir := db.LeafLogDirPath(dir)
	leafFileID, err := valuelog.EncodeFileID(valuelog.ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatal(err)
	}
	leafPath := filepath.Join(leafDir, "leaf-l255-000001.log")
	lw, err := valuelog.NewWriter(leafPath, leafFileID)
	if err != nil {
		t.Fatalf("NewWriter(leaf): %v", err)
	}
	if _, err := lw.AppendFrame(leafDictID, leafDictBytes, []valuelog.Record{
		{RID: 1, Value: bytes.Repeat([]byte("leaf-vlog-dict-sample-payload-prefix-0123456789-"), 12)},
	}); err != nil {
		t.Fatalf("AppendFrame(leaf): %v", err)
	}
	if err := lw.Close(); err != nil {
		t.Fatalf("Close leaf writer: %v", err)
	}
	if err := d.RegisterValueLogSegment(leafPath, leafFileID); err != nil {
		t.Fatalf("RegisterValueLogSegment(leaf): %v", err)
	}

	// Stage a dict/current pointer for stagedDictID without committing yet.
	curVal := make([]byte, 8)
	binary.BigEndian.PutUint64(curVal, stagedDictID)
	if err := d.StageSystemOps([]db.SystemKVOp{{Key: []byte("dict/current/custom"), Value: curVal}}, nil); err != nil {
		t.Fatalf("StageSystemOps: %v", err)
	}

	rep, err := d.SystemMetadataAudit(ctx, db.SystemMetadataAuditOptions{
		KeepRecentPublishedDicts: -1,
	})
	if err != nil {
		t.Fatalf("SystemMetadataAudit: %v", err)
	}
	if rep.DictsTotal != 2 || rep.DictsLive != 1 || rep.DictsGraceProtected != 1 || rep.DictsReclaimable != 0 {
		t.Fatalf("expected leafDictID live and stagedDictID protected: %+v", rep)
	}
	if rep.SegmentsScannedLeafLog != 1 {
		t.Fatalf("expected 1 leaf_vlog segment scanned, got %d", rep.SegmentsScannedLeafLog)
	}
}

func TestSystemMetadataAudit_RejectsOversizedGraceWindow(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if _, err := d.SystemMetadataAudit(context.Background(), db.SystemMetadataAuditOptions{
		KeepRecentPublishedDicts: db.MaxKeepRecentPublishedSystemMetaIDs + 1,
	}); err == nil {
		t.Fatalf("expected error for oversized KeepRecentPublishedDicts")
	}
	if _, err := d.SystemMetadataAudit(context.Background(), db.SystemMetadataAuditOptions{
		KeepRecentPublishedTemplates: db.MaxKeepRecentPublishedSystemMetaIDs + 1,
	}); err == nil {
		t.Fatalf("expected error for oversized KeepRecentPublishedTemplates")
	}
}
