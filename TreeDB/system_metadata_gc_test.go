package treedb

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func buildPublicTestZstdDict(t *testing.T, seed string, id uint32) []byte {
	t.Helper()
	samples := make([][]byte, 32)
	for i := range samples {
		samples[i] = append(bytes.Repeat([]byte(seed+"-public-payload-prefix-0123456789-"), 12), []byte(fmt.Sprintf("r-%04d", i))...)
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

func TestPublicSystemMetadataGC_AndCompactStorageIntegration(t *testing.T) {
	for _, profile := range []Profile{ProfileNoWALFast, ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			testPublicSystemMetadataGCAndCompactStorage(t, profile)
		})
	}
}

func testPublicSystemMetadataGCAndCompactStorage(t *testing.T, profile Profile) {
	dir := t.TempDir()
	opts := OptionsFor(profile, dir)
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	ctx := context.Background()

	// Publish two dicts via dictdb directly against the backend; second supersedes first.
	dstore := dictdb.New(d.backend)
	deadDictID, err := dstore.PublishDict(ctx, buildPublicTestZstdDict(t, "pub-dead", 501), 4, "default", true)
	if err != nil {
		t.Fatalf("PublishDict(dead): %v", err)
	}
	currentDictID, err := dstore.PublishDict(ctx, buildPublicTestZstdDict(t, "pub-current", 502), 4, "default", true)
	if err != nil {
		t.Fatalf("PublishDict(current): %v", err)
	}

	// Stage a dead template via StageTemplateDef and verify onDurable fires on Checkpoint.
	tstore := templatedb.NewBackend(d.backend, templatedb.Config{})
	tcfg := templ.NormalizeConfig(templ.Config{})
	defBytes, err := templ.EncodeTemplateDef(templ.TemplateDef{
		Kind:    templ.TemplateAnchors,
		Anchors: [][]byte{[]byte("staged-anchor-one-"), []byte("-staged-anchor-two")},
	}, tcfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef: %v", err)
	}
	var durableFired atomic.Bool
	stagedTplID, err := tstore.StageTemplateDef(ctx, defBytes, []uint64{0x123456789abcdef0}, func() {
		durableFired.Store(true)
	})
	if err != nil || stagedTplID == 0 {
		t.Fatalf("StageTemplateDef: id=%d err=%v", stagedTplID, err)
	}
	if err := d.Set([]byte("user-key"), []byte("user-val")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if !durableFired.Load() {
		t.Fatalf("expected StageTemplateDef onDurable callback to fire on Checkpoint")
	}

	// Public SystemMetadataAudit should see deadDictID and stagedTplID as reclaimable when grace is disabled.
	rep, err := d.SystemMetadataAudit(ctx, SystemMetadataAuditOptions{
		KeepRecentPublishedDicts:     -1,
		KeepRecentPublishedTemplates: -1,
	})
	if err != nil {
		t.Fatalf("SystemMetadataAudit: %v", err)
	}
	if rep.DictsReclaimable != 1 || rep.TemplatesReclaimable != 1 {
		t.Fatalf("unexpected public audit: %+v", rep)
	}

	// Public SystemMetadataGC reclaims deadDictID and stagedTplID while keeping currentDictID.
	gcStats, err := d.SystemMetadataGC(ctx, SystemMetadataGCOptions{
		Audit: SystemMetadataAuditOptions{
			KeepRecentPublishedDicts:     -1,
			KeepRecentPublishedTemplates: -1,
		},
	})
	if err != nil {
		t.Fatalf("SystemMetadataGC: %v", err)
	}
	if gcStats.DictsDeleted != 1 || gcStats.TemplatesDeleted != 1 {
		t.Fatalf("unexpected public GC stats: %+v", gcStats)
	}
	if _, err := d.backend.LookupSystemDictBytes(currentDictID); err != nil {
		t.Fatalf("currentDictID=%d missing post-GC: %v", currentDictID, err)
	}
	_ = deadDictID

	// Publish another dead dict and verify CompactStorage runs system-metadata-gc automatically.
	if _, err := dstore.PutDictBytes(ctx, buildPublicTestZstdDict(t, "pub-compact-dead", 503)); err != nil {
		t.Fatalf("PutDictBytes(compact-dead): %v", err)
	}
	compactStats, err := d.CompactStorage(ctx, CompactStorageOptions{
		Mode: CompactStorageFull,
		SystemMetadataAudit: SystemMetadataAuditOptions{
			KeepRecentPublishedDicts:     -1,
			KeepRecentPublishedTemplates: -1,
		},
	})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if compactStats.SystemMetadataGC.DictsDeleted != 1 {
		t.Fatalf("expected CompactStorage to delete 1 dead dict, got %+v", compactStats.SystemMetadataGC)
	}
}
