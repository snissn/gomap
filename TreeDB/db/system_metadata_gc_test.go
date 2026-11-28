package db_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func TestSystemMetadataGC_DeletesDeadDictsAndTemplatesPreservesLive(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()

	dstore := dictdb.New(d)
	tstore := templatedb.NewBackend(d, templatedb.Config{})

	// Dead chunked (> 2 KiB) dict with K + leafmode + hash keys.
	deadDictBytes := bytes.Repeat(buildTestZstdDict(t, "gc-dead-chunked", 101), 3)
	if len(deadDictBytes) <= db.SystemDictChunkSize {
		deadDictBytes = append(deadDictBytes, bytes.Repeat([]byte("chunk-pad-"), 300)...)
	}
	deadDictID, err := dstore.PublishDict(ctx, deadDictBytes, 8, "default", true)
	if err != nil {
		t.Fatalf("PublishDict(dead): %v", err)
	}
	if err := dstore.SetLeafPayloadMode(ctx, deadDictID, true); err != nil {
		t.Fatalf("SetLeafPayloadMode(dead): %v", err)
	}
	// Warm caches for deadDictID to verify eviction after GC.
	if _, err := d.LookupSystemDictBytes(deadDictID); err != nil {
		t.Fatalf("LookupSystemDictBytes(dead) pre-GC: %v", err)
	}

	// Live dict referenced by value_vlog + superseded by currentDict.
	liveDictBytes := buildTestZstdDict(t, "gc-live-vlog", 102)
	liveDictID, err := dstore.PublishDict(ctx, liveDictBytes, 4, "default", true)
	if err != nil {
		t.Fatalf("PublishDict(live): %v", err)
	}
	currentDictBytes := buildTestZstdDict(t, "gc-current", 103)
	currentDictID, err := dstore.PublishDict(ctx, currentDictBytes, 4, "default", true)
	if err != nil {
		t.Fatalf("PublishDict(current): %v", err)
	}

	// Templates: one live (written to value_vlog), one dead (chunked > 2 KiB via Mask base).
	tcfg := templ.NormalizeConfig(templ.Config{})
	liveDefBytes, err := templ.EncodeTemplateDef(templ.TemplateDef{
		Kind:    templ.TemplateAnchors,
		Anchors: [][]byte{[]byte("gc-live-anchor-a-"), []byte("-gc-live-anchor-b")},
	}, tcfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef(live): %v", err)
	}
	deadBase := bytes.Repeat([]byte("dead-chunked-template-base-0123456789abcdef"), 80) // > 3 KiB
	deadMask := make([]byte, (len(deadBase)+7)/8)
	deadMask[0] = 0x0f
	deadDefBytes, err := templ.EncodeTemplateDef(templ.TemplateDef{
		Kind: templ.TemplateMask,
		Base: deadBase,
		Mask: deadMask,
	}, tcfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef(dead): %v", err)
	}
	const sharedFP uint64 = 0xaaaa000011112222
	const deadOnlyFP uint64 = 0xbbbb000033334444
	liveTplID, err := tstore.PutTemplateDef(ctx, liveDefBytes, []uint64{sharedFP})
	if err != nil {
		t.Fatalf("PutTemplateDef(live): %v", err)
	}
	deadTplID, err := tstore.PutTemplateDef(ctx, deadDefBytes, []uint64{sharedFP, deadOnlyFP})
	if err != nil {
		t.Fatalf("PutTemplateDef(dead): %v", err)
	}
	if _, err := d.LookupSystemTemplateDef(deadTplID); err != nil {
		t.Fatalf("LookupSystemTemplateDef(dead) pre-GC: %v", err)
	}

	// Write a value_vlog segment using liveDictID and liveTplID.
	tplPayload, err := templ.EncodePayload(liveTplID, [][]byte{[]byte("g1"), []byte("g2"), []byte("g3")})
	if err != nil {
		t.Fatalf("EncodePayload: %v", err)
	}
	vlogDir := db.ValueLogDirPath(dir)
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	segPath := filepath.Join(vlogDir, "value-l0-000001.log")
	w, err := valuelog.NewWriter(segPath, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.AppendFrame(liveDictID, liveDictBytes, []valuelog.Record{
		{RID: 1, Value: append(append([]byte(nil), tplPayload...), bytes.Repeat([]byte("gc-live-vlog-sample-payload-prefix-0123456789-"), 8)...)},
	}); err != nil {
		t.Fatalf("AppendFrame: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	if err := d.RegisterValueLogSegment(segPath, fileID); err != nil {
		t.Fatalf("RegisterValueLogSegment: %v", err)
	}

	// DryRun mutates nothing.
	dryStats, err := d.SystemMetadataGC(ctx, db.SystemMetadataGCOptions{
		DryRun: true,
		Audit: db.SystemMetadataAuditOptions{
			KeepRecentPublishedDicts:     -1,
			KeepRecentPublishedTemplates: -1,
		},
	})
	if err != nil {
		t.Fatalf("SystemMetadataGC(DryRun): %v", err)
	}
	if !dryStats.DryRun || dryStats.OpsCommitted != 0 || dryStats.Audit.DictsReclaimable != 1 || dryStats.Audit.TemplatesReclaimable != 1 {
		t.Fatalf("unexpected DryRun stats: %+v", dryStats)
	}

	// Real sweep deletes deadDictID (plus chunks, k, leafmode, hash) and deadTplID (plus chunks, deadOnlyFP, prunes sharedFP).
	stats, err := d.SystemMetadataGC(ctx, db.SystemMetadataGCOptions{
		Audit: db.SystemMetadataAuditOptions{
			KeepRecentPublishedDicts:     -1,
			KeepRecentPublishedTemplates: -1,
		},
	})
	if err != nil {
		t.Fatalf("SystemMetadataGC: %v", err)
	}
	if stats.AbortedStale || stats.DictsDeleted != 1 || stats.TemplatesDeleted != 1 || stats.TemplateFPKeysUpdated != 1 || stats.TemplateFPKeysDeleted != 1 {
		t.Fatalf("unexpected GC stats: %+v", stats)
	}
	if stats.DictKeysDeleted < 5 {
		t.Fatalf("expected >=5 dict keys deleted (bytes+chunks+k+leafmode+hash), got %d", stats.DictKeysDeleted)
	}
	if stats.TemplateKeysDeleted < 3 {
		t.Fatalf("expected >=3 template keys deleted (def+chunks+fp), got %d", stats.TemplateKeysDeleted)
	}

	// Verify in-memory cache eviction: dead IDs now return missing errors.
	if _, err := d.LookupSystemDictBytes(deadDictID); !errors.Is(err, valuelog.ErrMissingDict) {
		t.Fatalf("LookupSystemDictBytes(dead) post-GC err=%v want ErrMissingDict", err)
	}
	if _, err := d.LookupSystemTemplateDef(deadTplID); !errors.Is(err, templ.ErrMissingTemplate) {
		t.Fatalf("LookupSystemTemplateDef(dead) post-GC err=%v want ErrMissingTemplate", err)
	}

	// Verify live + current dicts and live template remain readable and sharedFP was pruned cleanly.
	if got, err := d.LookupSystemDictBytes(liveDictID); err != nil || !bytes.Equal(got, liveDictBytes) {
		t.Fatalf("liveDictID lookup failed post-GC: err=%v len=%d", err, len(got))
	}
	if got, err := d.LookupSystemDictBytes(currentDictID); err != nil || !bytes.Equal(got, currentDictBytes) {
		t.Fatalf("currentDictID lookup failed post-GC: err=%v len=%d", err, len(got))
	}
	if got, err := d.LookupSystemTemplateDef(liveTplID); err != nil || !bytes.Equal(got, liveDefBytes) {
		t.Fatalf("liveTplID lookup failed post-GC: err=%v len=%d", err, len(got))
	}
	cands, err := tstore.GetCandidates(ctx, sharedFP, 10)
	if err != nil || len(cands) != 1 || cands[0].ID != liveTplID {
		t.Fatalf("sharedFP candidates post-GC=%v err=%v want [%d]", cands, err, liveTplID)
	}
	deadCands, err := tstore.GetCandidates(ctx, deadOnlyFP, 10)
	if err != nil || len(deadCands) != 0 {
		t.Fatalf("deadOnlyFP candidates post-GC=%v err=%v want empty", deadCands, err)
	}

	// Reopen and verify persistence + 0 remaining reclaimable entries.
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	if _, err := reopened.LookupSystemDictBytes(deadDictID); !errors.Is(err, valuelog.ErrMissingDict) {
		t.Fatalf("LookupSystemDictBytes(dead) post-reopen err=%v want ErrMissingDict", err)
	}
	if got, err := reopened.LookupSystemDictBytes(liveDictID); err != nil || !bytes.Equal(got, liveDictBytes) {
		t.Fatalf("liveDictID lookup post-reopen failed: err=%v", err)
	}
	if got, err := reopened.LookupSystemTemplateDef(liveTplID); err != nil || !bytes.Equal(got, liveDefBytes) {
		t.Fatalf("liveTplID lookup post-reopen failed: err=%v", err)
	}
	repAfter, err := reopened.SystemMetadataAudit(ctx, db.SystemMetadataAuditOptions{
		KeepRecentPublishedDicts:     -1,
		KeepRecentPublishedTemplates: -1,
	})
	if err != nil {
		t.Fatalf("SystemMetadataAudit post-reopen: %v", err)
	}
	if repAfter.DictsReclaimable != 0 || repAfter.TemplatesReclaimable != 0 || repAfter.TemplateFPKeysDeletable != 0 || repAfter.TemplateFPKeysPrunable != 0 {
		t.Fatalf("expected 0 reclaimable post-GC: %+v", repAfter)
	}
}

func TestSystemMetadataGC_ReadOnlyRejected(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ro, err := db.Open(db.Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open(ReadOnly): %v", err)
	}
	defer func() { _ = ro.Close() }()
	if _, err := ro.SystemMetadataGC(context.Background(), db.SystemMetadataGCOptions{}); !errors.Is(err, db.ErrReadOnly) {
		t.Fatalf("SystemMetadataGC(ReadOnly) err=%v want ErrReadOnly", err)
	}
}

func TestSystemMetadataGC_Lane254PointerSegmentReclaimedByValueLogGC(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()

	dstore := dictdb.New(d)
	dstore.SetPointerThreshold(1)

	// Store dead dict in lane-254 segment seq 1.
	deadDictBytes := buildTestZstdDict(t, "lane254-dead-seg1", 201)
	deadDictID, err := dstore.PutDictBytes(ctx, deadDictBytes)
	if err != nil {
		t.Fatalf("PutDictBytes(dead): %v", err)
	}
	// Close and reopen so the next lane-254 publish opens a fresh segment seq 2,
	// leaving seq 1 sealed/non-active (eligible for ValueLogGC once unreferenced).
	if err := d.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	d, err = db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Reopen 1: %v", err)
	}
	defer func() { _ = d.Close() }()

	dstore = dictdb.New(d)
	dstore.SetPointerThreshold(1)
	liveDictBytes := buildTestZstdDict(t, "lane254-live-seg2", 202)
	liveDictID, err := dstore.PublishDict(ctx, liveDictBytes, 4, "default", true)
	if err != nil {
		t.Fatalf("PublishDict(live): %v", err)
	}

	// Before SystemMetadataGC, seq 1 is still referenced by SystemRootPageID.
	gcBefore, err := d.ValueLogGC(ctx, db.ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC(before): %v", err)
	}
	if gcBefore.SegmentsDeleted != 0 {
		t.Fatalf("expected 0 lane-254 segments deleted before SystemMetadataGC, got %+v", gcBefore)
	}

	// Run SystemMetadataGC to delete deadDictID's pointer entry.
	metaStats, err := d.SystemMetadataGC(ctx, db.SystemMetadataGCOptions{
		Audit: db.SystemMetadataAuditOptions{KeepRecentPublishedDicts: -1},
	})
	if err != nil {
		t.Fatalf("SystemMetadataGC: %v", err)
	}
	if metaStats.DictsDeleted != 1 || metaStats.PointerEntriesDeleted != 1 {
		t.Fatalf("unexpected metaStats: %+v", metaStats)
	}
	if _, err := d.LookupSystemDictBytes(deadDictID); !errors.Is(err, valuelog.ErrMissingDict) {
		t.Fatalf("deadDictID still readable: err=%v", err)
	}

	// Advance the older dual-meta slot via a subsequent commit so neither
	// meta0 nor meta1 retains the pre-GC SystemRootPageID that still referenced
	// lane-254 seq 1.
	if err := d.SetSync([]byte("advance-dual-meta"), []byte("1")); err != nil {
		t.Fatalf("SetSync(advance-dual-meta): %v", err)
	}

	// Now ValueLogGC must reclaim lane-254 seq 1 while preserving seq 2 (liveDictID).
	gcAfter, err := d.ValueLogGC(ctx, db.ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC(after): %v", err)
	}
	if gcAfter.SegmentsDeleted != 1 {
		t.Fatalf("expected 1 lane-254 segment deleted after SystemMetadataGC + Checkpoint, got %+v", gcAfter)
	}
	if got, err := d.LookupSystemDictBytes(liveDictID); err != nil || !bytes.Equal(got, liveDictBytes) {
		t.Fatalf("liveDictID lookup failed after ValueLogGC: err=%v", err)
	}
}

func TestSystemMetadataGC_ConcurrentWritersRace(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	ctx := context.Background()
	dstore := dictdb.New(d)

	// Seed a couple of dead dicts.
	for i := 0; i < 4; i++ {
		if _, err := dstore.PutDictBytes(ctx, buildTestZstdDict(t, fmt.Sprintf("race-dead-%d", i), uint32(300+i))); err != nil {
			t.Fatalf("PutDictBytes(%d): %v", i, err)
		}
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	errCh := make(chan error, 4)

	// Concurrent user writer advancing CommitSeq.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; !stop.Load() && i < 60; i++ {
			if err := d.Set([]byte(fmt.Sprintf("k-%04d", i)), []byte(fmt.Sprintf("val-%04d", i))); err != nil {
				errCh <- fmt.Errorf("Set(%d): %w", i, err)
				return
			}
		}
	}()

	// Concurrent dict publisher.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; !stop.Load() && i < 6; i++ {
			if _, err := dstore.PublishDict(ctx, buildTestZstdDict(t, fmt.Sprintf("race-pub-%d", i), uint32(400+i)), 4, "default", true); err != nil {
				errCh <- fmt.Errorf("PublishDict(%d): %w", i, err)
				return
			}
		}
	}()

	// Run SystemMetadataGC concurrently several times (may succeed or safely abort stale).
	for i := 0; i < 6; i++ {
		if _, err := d.SystemMetadataGC(ctx, db.SystemMetadataGCOptions{}); err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("SystemMetadataGC(%d): %v", i, err)
		}
	}
	stop.Store(true)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// Current dict must still be readable!
	curID, err := dstore.GetCurrent(ctx)
	if err != nil || curID == 0 {
		t.Fatalf("GetCurrent post-race id=%d err=%v", curID, err)
	}
	if _, err := d.LookupSystemDictBytes(curID); err != nil {
		t.Fatalf("LookupSystemDictBytes(current=%d) post-race: %v", curID, err)
	}
}

func TestSystemMetadataGC_StagingSerializationAndDanglingReject(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	ctx := context.Background()
	dstore := dictdb.New(d)

	dict1ID, err := dstore.PutDictBytes(ctx, buildTestZstdDict(t, "stage-guard-1", 601))
	if err != nil {
		t.Fatalf("PutDictBytes(1): %v", err)
	}
	dict2ID, err := dstore.PutDictBytes(ctx, buildTestZstdDict(t, "stage-guard-2", 602))
	if err != nil {
		t.Fatalf("PutDictBytes(2): %v", err)
	}

	// Stage a dict/current marker for dict1ID; SystemMetadataGC must preserve dict1ID
	// while deleting unreferenced dict2ID.
	curVal1 := make([]byte, 8)
	binary.BigEndian.PutUint64(curVal1, dict1ID)
	if err := d.StageSystemOps([]db.SystemKVOp{{Key: []byte("dict/current"), Value: curVal1}}, nil); err != nil {
		t.Fatalf("StageSystemOps(dict1): %v", err)
	}

	stats, err := d.SystemMetadataGC(ctx, db.SystemMetadataGCOptions{
		Audit: db.SystemMetadataAuditOptions{KeepRecentPublishedDicts: -1},
	})
	if err != nil {
		t.Fatalf("SystemMetadataGC: %v", err)
	}
	if stats.DictsDeleted != 1 {
		t.Fatalf("expected exactly 1 dict deleted (dict2), got %+v", stats)
	}
	if _, err := d.LookupSystemDictBytes(dict1ID); err != nil {
		t.Fatalf("staged dict1ID=%d was unexpectedly deleted: %v", dict1ID, err)
	}
	if _, err := d.LookupSystemDictBytes(dict2ID); !errors.Is(err, valuelog.ErrMissingDict) {
		t.Fatalf("dict2ID=%d expected ErrMissingDict, got %v", dict2ID, err)
	}

	// Attempting to stage a dict/current marker referencing the just-deleted dict2ID
	// must fail closed with ErrMissingDict rather than queueing a dangling activation.
	curVal2 := make([]byte, 8)
	binary.BigEndian.PutUint64(curVal2, dict2ID)
	if err := d.StageSystemOps([]db.SystemKVOp{{Key: []byte("dict/current"), Value: curVal2}}, nil); !errors.Is(err, valuelog.ErrMissingDict) {
		t.Fatalf("StageSystemOps(deleted dict2) err=%v want ErrMissingDict", err)
	}
	// Direct ApplySystemOps (e.g. dictdb.SetCurrent / PublishDict after GC raced
	// past hash dedup) must also reject activating the deleted dict2ID atomically.
	if err := d.ApplySystemOps([]db.SystemKVOp{{Key: []byte("dict/current"), Value: curVal2}}, true); !errors.Is(err, valuelog.ErrMissingDict) {
		t.Fatalf("ApplySystemOps(deleted dict2) err=%v want ErrMissingDict", err)
	}

	// Stage ONLY a template/fp/* routing update referencing an existing template
	// (tpl1) outside the recent grace window. GC must preserve tpl1 while deleting
	// unreferenced tpl2, and subsequent SetSync must cleanly piggyback the staged
	// routing entry.
	tstore := templatedb.NewBackend(d, templatedb.Config{})
	tcfg := templ.NormalizeConfig(templ.Config{})
	tpl1Bytes, err := templ.EncodeTemplateDef(templ.TemplateDef{
		Kind:    templ.TemplateAnchors,
		Anchors: [][]byte{[]byte("stage-fp-1-anchor-a-"), []byte("-stage-fp-1-anchor-b")},
	}, tcfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef(1): %v", err)
	}
	tpl2Bytes, err := templ.EncodeTemplateDef(templ.TemplateDef{
		Kind:    templ.TemplateAnchors,
		Anchors: [][]byte{[]byte("stage-fp-2-anchor-a-"), []byte("-stage-fp-2-anchor-b")},
	}, tcfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef(2): %v", err)
	}
	tpl1ID, err := tstore.PutTemplateDef(ctx, tpl1Bytes, nil)
	if err != nil {
		t.Fatalf("PutTemplateDef(1): %v", err)
	}
	tpl2ID, err := tstore.PutTemplateDef(ctx, tpl2Bytes, nil)
	if err != nil {
		t.Fatalf("PutTemplateDef(2): %v", err)
	}
	encodeFPCand := func(id uint64) []byte {
		var buf []byte
		buf = binary.AppendUvarint(buf, id)
		buf = binary.AppendUvarint(buf, 32)
		return buf
	}
	fpKey := make([]byte, len("template/fp/")+8)
	copy(fpKey, "template/fp/")
	binary.BigEndian.PutUint64(fpKey[len("template/fp/"):], 0x1122334455667788)
	if err := d.StageSystemOps([]db.SystemKVOp{{Key: fpKey, Value: encodeFPCand(tpl1ID)}}, nil); err != nil {
		t.Fatalf("StageSystemOps(template/fp -> tpl1): %v", err)
	}
	stats, err = d.SystemMetadataGC(ctx, db.SystemMetadataGCOptions{
		Audit: db.SystemMetadataAuditOptions{
			KeepRecentPublishedDicts:     -1,
			KeepRecentPublishedTemplates: -1,
		},
	})
	if err != nil {
		t.Fatalf("SystemMetadataGC(tpl): %v", err)
	}
	if stats.TemplatesDeleted != 1 {
		t.Fatalf("expected exactly 1 template deleted (tpl2), got %+v", stats)
	}
	if err := d.SetSync([]byte("piggyback-fp"), []byte("ok")); err != nil {
		t.Fatalf("SetSync(piggyback-fp): %v", err)
	}
	if _, err := d.LookupSystemTemplateDef(tpl1ID); err != nil {
		t.Fatalf("staged-fp tpl1ID=%d was unexpectedly deleted: %v", tpl1ID, err)
	}
	if _, err := d.LookupSystemTemplateDef(tpl2ID); !errors.Is(err, templ.ErrMissingTemplate) {
		t.Fatalf("tpl2ID=%d expected ErrMissingTemplate, got %v", tpl2ID, err)
	}
	if err := d.StageSystemOps([]db.SystemKVOp{{Key: fpKey, Value: encodeFPCand(tpl2ID)}}, nil); !errors.Is(err, templ.ErrMissingTemplate) {
		t.Fatalf("StageSystemOps(deleted tpl2) err=%v want ErrMissingTemplate", err)
	}
	if err := d.ApplySystemOps([]db.SystemKVOp{{Key: fpKey, Value: encodeFPCand(tpl2ID)}}, true); !errors.Is(err, templ.ErrMissingTemplate) {
		t.Fatalf("ApplySystemOps(deleted tpl2) err=%v want ErrMissingTemplate", err)
	}
}

func TestSystemMetadataGC_DirectActivationWithLeafVlogSystemLeaves(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(db.Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
		FreshSystemPagerLeaves:     false,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	leafLog, err := db.NewStandaloneLeafPageLog(dir, db.StandaloneLeafPageLogOptions{})
	if err != nil {
		_ = d.Close()
		t.Fatalf("NewStandaloneLeafPageLog: %v", err)
	}
	d.SetLeafPageLog(leafLog)
	defer func() {
		_ = d.Close()
		_ = leafLog.Close()
	}()
	ctx := context.Background()
	dstore := dictdb.New(d)
	dictID, err := dstore.PutDictBytes(ctx, buildTestZstdDict(t, "leaf-vlog-sys-dict", 701))
	if err != nil {
		t.Fatalf("PutDictBytes: %v", err)
	}
	if err := dstore.SetCurrent(ctx, dictID); err != nil {
		t.Fatalf("SetCurrent with leaf_vlog system leaves: %v", err)
	}
}

func TestSystemMetadataGC_OnDurableCallbackCanStageWithoutDeadlock(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(db.Options{
		Dir:        dir,
		Durability: db.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	ctx := context.Background()
	dstore := dictdb.New(d)

	deadDictID, err := dstore.PutDictBytes(ctx, buildTestZstdDict(t, "cb-dead-dict", 801))
	if err != nil {
		t.Fatalf("PutDictBytes: %v", err)
	}
	_ = deadDictID

	// Stage a batch with an onDurable callback that calls StageSystemOps, then
	// flush unsynced so the callback waits for the next synchronous commit.
	var cbRan atomic.Bool
	var cbErr atomic.Value
	if err := d.StageSystemOps([]db.SystemKVOp{{Key: []byte("meta/pre-gc"), Value: []byte("1")}}, func() {
		if err := d.StageSystemOps([]db.SystemKVOp{{Key: []byte("meta/from-callback"), Value: []byte("ok")}}, nil); err != nil {
			cbErr.Store(err)
		}
		cbRan.Store(true)
	}); err != nil {
		t.Fatalf("StageSystemOps(pre-gc): %v", err)
	}
	if err := d.FlushStagedSystemOps(false); err != nil {
		t.Fatalf("FlushStagedSystemOps(false): %v", err)
	}

	// Synchronous SystemMetadataGC commits with sync=true, triggering the pending
	// onDurable callback. Because postWork runs after releasing systemMetaPublishMu,
	// staging from the callback must succeed without deadlocking.
	stats, err := d.SystemMetadataGC(ctx, db.SystemMetadataGCOptions{
		Audit: db.SystemMetadataAuditOptions{KeepRecentPublishedDicts: -1},
	})
	if err != nil {
		t.Fatalf("SystemMetadataGC: %v", err)
	}
	if stats.DictsDeleted != 1 {
		t.Fatalf("expected 1 dead dict deleted, got %+v", stats)
	}
	if !cbRan.Load() {
		t.Fatalf("expected pending onDurable callback to run on synchronous GC commit")
	}
	if v := cbErr.Load(); v != nil {
		t.Fatalf("onDurable callback StageSystemOps failed: %v", v)
	}
	if err := d.SetSync([]byte("flush-cb"), []byte("1")); err != nil {
		t.Fatalf("SetSync(flush-cb): %v", err)
	}
	got, err := d.GetSystem([]byte("meta/from-callback"))
	if err != nil || string(got) != "ok" {
		t.Fatalf("GetSystem(meta/from-callback) = %q, %v; want \"ok\", nil", got, err)
	}
}

func TestSystemMetadataGC_ConcurrentLookupDoesNotResurrectDeletedCache(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	ctx := context.Background()
	dstore := dictdb.New(d)
	tstore := templatedb.NewBackend(d, templatedb.Config{})
	tcfg := templ.NormalizeConfig(templ.Config{})

	dictID, err := dstore.PutDictBytes(ctx, buildTestZstdDict(t, "no-resurrect-dict", 802))
	if err != nil {
		t.Fatalf("PutDictBytes: %v", err)
	}
	tplBytes, err := templ.EncodeTemplateDef(templ.TemplateDef{
		Kind:    templ.TemplateAnchors,
		Anchors: [][]byte{[]byte("no-resurrect-anchor-a-"), []byte("-no-resurrect-anchor-b")},
	}, tcfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef: %v", err)
	}
	tplID, err := tstore.PutTemplateDef(ctx, tplBytes, nil)
	if err != nil {
		t.Fatalf("PutTemplateDef: %v", err)
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			_, _ = d.LookupSystemDictBytes(dictID)
			_, _ = d.LookupSystemTemplateDef(tplID)
		}
	}()

	stats, err := d.SystemMetadataGC(ctx, db.SystemMetadataGCOptions{
		Audit: db.SystemMetadataAuditOptions{
			KeepRecentPublishedDicts:     -1,
			KeepRecentPublishedTemplates: -1,
		},
	})
	stop.Store(true)
	wg.Wait()
	if err != nil {
		t.Fatalf("SystemMetadataGC: %v", err)
	}
	if stats.DictsDeleted != 1 || stats.TemplatesDeleted != 1 {
		t.Fatalf("expected 1 dict and 1 template deleted, got %+v", stats)
	}
	if _, err := d.LookupSystemDictBytes(dictID); !errors.Is(err, valuelog.ErrMissingDict) {
		t.Fatalf("post-GC LookupSystemDictBytes(%d) err=%v want ErrMissingDict (cache resurrected)", dictID, err)
	}
	if _, err := d.LookupSystemTemplateDef(tplID); !errors.Is(err, templ.ErrMissingTemplate) {
		t.Fatalf("post-GC LookupSystemTemplateDef(%d) err=%v want ErrMissingTemplate (cache resurrected)", tplID, err)
	}
}
