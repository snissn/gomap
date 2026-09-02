package treedb_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func mainIndexPath(rootDir string) string {
	maindb := filepath.Join(rootDir, "maindb", "index.db")
	if _, err := os.Stat(maindb); err == nil {
		return maindb
	}
	return filepath.Join(rootDir, "index.db")
}

func readMainMeta(t *testing.T, rootDir string) rootpublication.DurableRootRecordV1 {
	t.Helper()
	indexPath := mainIndexPath(rootDir)
	p, err := pager.OpenReadOnly(indexPath, 256*1024)
	if err != nil {
		t.Fatalf("open pager: %v", err)
	}
	defer func() { _ = p.Close() }()

	readMetaAt := func(id uint64) (page.DurableMetaV1, bool) {
		data, err := p.Get(id)
		if err != nil {
			return page.DurableMetaV1{}, false
		}
		n := node.NewNodeView(data)
		if !n.VerifyChecksum() || n.Type() != page.PageTypeMeta {
			return page.DurableMetaV1{}, false
		}
		meta, err := page.DecodeDurableMetaV1(data[page.PageHeaderSize:])
		return meta, err == nil
	}

	m0, ok0 := readMetaAt(0)
	m1, ok1 := readMetaAt(1)
	var selected page.DurableMetaV1
	switch {
	case ok0 && ok1:
		if m1.CommitSeq > m0.CommitSeq {
			selected = m1
		} else {
			selected = m0
		}
	case ok0:
		selected = m0
	case ok1:
		selected = m1
	default:
		t.Fatalf("no valid durable-root meta pages found in %s", indexPath)
	}

	image, err := p.Get(selected.RootRecordPageID)
	if err != nil {
		t.Fatalf("read durable-root record page %d: %v", selected.RootRecordPageID, err)
	}
	record, err := rootpublication.DecodeDurableRootRecordV1(image, selected.RootRecordPageID, selected.RootRecordDigest)
	if err != nil {
		t.Fatalf("decode durable-root record page %d: %v", selected.RootRecordPageID, err)
	}
	return record
}

func requireMainRootLeafLogChildren(t *testing.T, rootDir string, rootID uint64) {
	t.Helper()
	indexPath := mainIndexPath(rootDir)
	p, err := pager.OpenReadOnly(indexPath, 256*1024)
	if err != nil {
		t.Fatalf("open pager: %v", err)
	}
	defer func() { _ = p.Close() }()
	data, err := p.Get(rootID)
	if err != nil {
		t.Fatalf("get root %d: %v", rootID, err)
	}
	n := node.NewNodeView(data)
	if n.Type() != page.PageTypeInternal {
		t.Fatalf("root %d type=%d want internal leaf-log root", rootID, n.Type())
	}
	found := false
	for i := uint16(0); i < n.Count(); i++ {
		ref, err := n.GetInternalChildRef(i)
		if err != nil {
			t.Fatalf("GetInternalChildRef(%d): %v", i, err)
		}
		if ref.Kind == page.ChildRefLeafLog {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("root %d has no leaf-log children", rootID)
	}
}

func TestVacuumIndexOnline_LeafPagesInValueLog_PreservesLeafRefWrites(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}

	dir := t.TempDir()
	opts := treedb.Options{
		Dir:                        dir,
		Durability:                 treedb.DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	}
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Small dataset: keep the tree as a single leaf so meta.UserRootPageID can
	// flip between a pager page-id (after vacuum) and a LeafRef (after a write).
	for i := 0; i < 16; i++ {
		key := []byte(fmt.Sprintf("k%03d", i))
		val := bytes.Repeat([]byte{byte(i)}, 32)
		if err := db.Set(key, val); err != nil {
			t.Fatalf("set %q: %v", string(key), err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	metaBefore := readMainMeta(t, dir)
	requireMainRootLeafLogChildren(t, dir, metaBefore.UserRootPageID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	// The published replacement must leave the live leaf-log root fully writable
	// and checkpointable.
	if err := db.Set([]byte("k010"), []byte("updated")); err != nil {
		t.Fatalf("set updated: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after vacuum: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	metaAfter := readMainMeta(t, dir)
	requireMainRootLeafLogChildren(t, dir, metaAfter.UserRootPageID)

	reopened, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, err := reopened.Get([]byte("k010"))
	if err != nil {
		_ = reopened.Close()
		t.Fatalf("get updated after reopen: %v", err)
	}
	if !bytes.Equal(got, []byte("updated")) {
		_ = reopened.Close()
		t.Fatalf("updated value after reopen=%q want updated", got)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened: %v", err)
	}
}

func TestVacuumIndexOffline_LeafPagesInValueLog_ReopenParity(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.Options{
		Dir:                        dir,
		Durability:                 treedb.DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 256; i++ {
		key := []byte(fmt.Sprintf("vac-off-%04d", i))
		val := bytes.Repeat([]byte{byte(i % 251)}, 64)
		if err := db.Set(key, val); err != nil {
			_ = db.Close()
			t.Fatalf("set %q: %v", string(key), err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mainDir := filepath.Dir(mainIndexPath(dir))
	leafPathsBefore, err := filepath.Glob(filepath.Join(mainDir, "leaf_vlog", "value-l*.log"))
	if err != nil {
		t.Fatalf("glob leaf_vlog before vacuum: %v", err)
	}
	if len(leafPathsBefore) == 0 {
		t.Fatalf("expected leaf_vlog files before vacuum")
	}

	if err := treedb.VacuumIndexOffline(treedb.Options{Dir: dir, KeepRecent: 1}); err != nil {
		t.Fatalf("VacuumIndexOffline: %v", err)
	}
	leafPathsAfter, err := filepath.Glob(filepath.Join(mainDir, "leaf_vlog", "value-l*.log"))
	if err != nil {
		t.Fatalf("glob leaf_vlog after vacuum: %v", err)
	}
	if len(leafPathsAfter) == 0 {
		t.Fatalf("expected leaf_vlog files after vacuum")
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	got, err := reopen.Get([]byte("vac-off-0010"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(got) != 64 {
		t.Fatalf("expected 64B value after reopen, got %dB", len(got))
	}
}

func TestOuterLeafPagesUseLeafVLogDir(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.Options{
		Dir:                        dir,
		Durability:                 treedb.DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	}
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 512; i++ {
		key := []byte(fmt.Sprintf("leaf-vlog-%04d", i))
		val := bytes.Repeat([]byte{byte(i % 251)}, 96)
		if err := db.Set(key, val); err != nil {
			t.Fatalf("set %q: %v", string(key), err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	leafPaths, err := filepath.Glob(filepath.Join(filepath.Dir(mainIndexPath(dir)), "leaf_vlog", "value-l*.log"))
	if err != nil {
		t.Fatalf("glob leaf_vlog: %v", err)
	}
	if len(leafPaths) == 0 {
		t.Fatalf("expected leaf_vlog segments")
	}
	nonEmpty := false
	for _, p := range leafPaths {
		info, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		if info.Size() > 0 {
			nonEmpty = true
			break
		}
	}
	if !nonEmpty {
		t.Fatalf("expected non-empty leaf_vlog segment, paths=%v", leafPaths)
	}
}

func TestValueLogRewriteOnline_LeafPagesInValueLog_ReopenParity(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.Options{
		Dir:                        dir,
		Durability:                 treedb.DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	}
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	values := map[string][]byte{
		"k1": bytes.Repeat([]byte("a"), 2*1024),
		"k2": bytes.Repeat([]byte("b"), 2*1024),
		"k3": bytes.Repeat([]byte("c"), 2*1024),
		"k4": bytes.Repeat([]byte("d"), 2*1024),
	}
	for k, v := range values {
		if err := db.Set([]byte(k), v); err != nil {
			_ = db.Close()
			t.Fatalf("set %s: %v", k, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint before rewrite: %v", err)
	}
	stats, err := db.ValueLogRewriteOnline(context.Background(), treedb.ValueLogRewriteOnlineOptions{
		BatchSize:     2,
		SyncEachBatch: true,
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		_ = db.Close()
		t.Fatalf("expected rewrite to copy records, stats=%+v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close after rewrite: %v", err)
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	for k, want := range values {
		got, err := reopen.Get([]byte(k))
		if err != nil {
			t.Fatalf("reopen get %s: %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("reopen mismatch key=%s got=%dB want=%dB", k, len(got), len(want))
		}
	}
}

func TestValueLogRewriteOffline_LeafPagesInValueLog_ReopenParity(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.PointerThreshold = 1
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := 0; i < 128; i++ {
		key := []byte(fmt.Sprintf("rew-off-%04d", i))
		val := bytes.Repeat([]byte{byte(i % 251)}, 2*1024)
		if err := db.Set(key, val); err != nil {
			_ = db.Close()
			t.Fatalf("set %q: %v", string(key), err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint before close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mainDir := filepath.Dir(mainIndexPath(dir))
	leafPathsBefore, err := filepath.Glob(filepath.Join(mainDir, "leaf_vlog", "value-l*.log"))
	if err != nil {
		t.Fatalf("glob leaf_vlog before rewrite: %v", err)
	}
	if len(leafPathsBefore) == 0 {
		t.Fatalf("expected leaf_vlog files before rewrite")
	}

	stats, err := treedb.ValueLogRewriteOffline(treedb.OptionsFor(treedb.ProfileNoWALFast, dir))
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected offline rewrite to copy records, stats=%+v", stats)
	}

	leafPathsAfter, err := filepath.Glob(filepath.Join(mainDir, "leaf_vlog", "value-l*.log"))
	if err != nil {
		t.Fatalf("glob leaf_vlog after rewrite: %v", err)
	}
	if len(leafPathsAfter) == 0 {
		t.Fatalf("expected leaf_vlog files after rewrite")
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	got, err := reopen.Get([]byte("rew-off-0010"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	want := bytes.Repeat([]byte{10}, 2*1024)
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch after reopen: got=%dB want=%dB", len(got), len(want))
	}
}

func TestValueLogRewriteOffline_LeafPagesInValueLog_PreservesLeafRefRoot_WhenValuesInline(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.PointerThreshold = 127 // keep small values inline
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Keep the tree small so the root is an internal page with explicit
	// leaf-log children.
	for i := 0; i < 16; i++ {
		key := []byte(fmt.Sprintf("rew-inline-%03d", i))
		val := bytes.Repeat([]byte{byte(i)}, 32)
		if err := db.Set(key, val); err != nil {
			_ = db.Close()
			t.Fatalf("set %q: %v", string(key), err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	metaBefore := readMainMeta(t, dir)
	requireMainRootLeafLogChildren(t, dir, metaBefore.UserRootPageID)

	stats, err := treedb.ValueLogRewriteOffline(treedb.OptionsFor(treedb.ProfileNoWALFast, dir))
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if stats.RecordsCopied == 0 || stats.BytesAfter == 0 {
		t.Fatalf("expected rewrite to copy leaf-page records, stats=%+v", stats)
	}

	metaAfter := readMainMeta(t, dir)
	requireMainRootLeafLogChildren(t, dir, metaAfter.UserRootPageID)

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	got, err := reopen.Get([]byte("rew-inline-010"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if len(got) != 32 {
		t.Fatalf("expected 32B value after reopen, got %dB", len(got))
	}
}

func TestLeafGenerationGC_BackendOpenWithoutFlag_PreservesLeafRefs(t *testing.T) {
	dir := t.TempDir()
	const (
		keyCount = 20000
		valSize  = 100
	)
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.DisableSideStores = true
	opts.IndexOuterLeavesInValueLog = true
	// Disable compression/dict/template so the only value-log reachability
	// comes from leaf-log child refs (mirrors the original data-loss report).
	opts.ValueLog.Compression = treedb.ValueLogCompressionOff
	opts.ValueLog.PointerThreshold = 127 // keep values inline (no value-log pointers)

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	rng := rand.New(rand.NewSource(1))
	scratch := make([]byte, valSize)
	for i := 0; i < keyCount; i++ {
		_, _ = rng.Read(scratch)
		value := append([]byte(nil), scratch...)
		key := []byte(fmt.Sprintf("gc-%08d", i))
		if err := db.Set(key, value); err != nil {
			_ = db.Close()
			t.Fatalf("set %q: %v", string(key), err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Open the backend directly without repeating IndexOuterLeavesInValueLog.
	// The backend open path must honor persisted format.json so split-log leaf
	// maintenance still sees live leaf generations after reopen.
	backend, err := treedbdb.Open(treedbdb.Options{
		Dir:             dir,
		ReadOnly:        false,
		Durability:      treedbdb.DurabilityWALOffRelaxed,
		ResolvedProfile: treedbdb.ProfileNoWALFast,
		ValueLog: treedbdb.ValueLogOptions{
			ReadIntegrity: treedbdb.IntegrityVerify,
		},
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	leafStats, err := backend.LeafGenerationGC(context.Background(), treedbdb.LeafGenerationGCOptions{DryRun: true})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("LeafGenerationGC: %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("backend close: %v", err)
	}
	if leafStats.GenerationsTotal == 0 {
		t.Fatalf("expected leaf generations to be visible on backend reopen; stats=%+v", leafStats)
	}
	if leafStats.GenerationsLive+leafStats.GenerationsWritable == 0 {
		t.Fatalf("expected live or writable leaf generations to remain visible; stats=%+v", leafStats)
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	got, err := reopen.Get([]byte("gc-00000000"))
	if err != nil {
		t.Fatalf("get after gc: %v", err)
	}
	if len(got) != valSize {
		t.Fatalf("expected %dB value after gc, got %dB", valSize, len(got))
	}
}
