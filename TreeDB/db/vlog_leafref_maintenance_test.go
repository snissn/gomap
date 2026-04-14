package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func waitForPathRemoval(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		_, err := os.Stat(path)
		if err == nil {
			if time.Now().After(deadline) {
				return fmt.Errorf("path still exists after %s: %s", timeout, path)
			}
			time.Sleep(25 * time.Millisecond)
			continue
		}
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
}

func TestValueLogGC_WithLeafPagesInValueLog_KeepsReferencedLeafSegments(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		_ = db.Close()
		t.Fatalf("mkdir wal: %v", err)
	}

	leafLog := newRewriteWriter(walDir, 0, 0, 16<<10)
	leafLog.blockCompression = false
	leafLog.blockCodec = valuelog.BlockCodecSnappy
	db.SetLeafPageLog(leafLog)

	defer func() {
		_ = leafLog.Close()
		_ = db.Close()
	}()

	// Inline values so that leaf pages are the only value-log references.
	value := bytes.Repeat([]byte("v"), 32)
	for i := 0; i < 512; i++ {
		key := []byte(fmt.Sprintf("k%06d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Set(%q): %v", key, err)
		}
	}

	referenced, err := db.referencedValueLogSegments(context.Background())
	if err != nil {
		t.Fatalf("referencedValueLogSegments: %v", err)
	}
	if len(referenced) == 0 {
		t.Fatalf("expected non-empty referenced value-log segments with leaf pages in vlog")
	}

	refPaths := make([]string, 0, len(referenced))
	for id := range referenced {
		refPaths = append(refPaths, db.valueLogManager.SegmentPath(id))
	}

	if _, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}

	for _, path := range refPaths {
		if path == "" {
			t.Fatalf("expected non-empty referenced segment path")
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("referenced segment removed unexpectedly: %s err=%v", path, err)
		}
	}
}

func TestValueLogRewriteOffline_PreservesLeafPagesInValueLogFormatConfig(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		_ = db.Close()
		t.Fatalf("mkdir wal: %v", err)
	}

	leafLog := newRewriteWriter(walDir, 0, 0, 0)
	leafLog.blockCompression = false
	leafLog.blockCodec = valuelog.BlockCodecSnappy
	db.SetLeafPageLog(leafLog)

	// Keep the tree as a single leaf so the root page ID itself is a leaf ref.
	value := bytes.Repeat([]byte("v"), 16)
	for i := 0; i < 32; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		if err := db.Set(key, value); err != nil {
			_ = leafLog.Close()
			_ = db.Close()
			t.Fatalf("Set(%q): %v", key, err)
		}
	}

	state := db.State()
	if state == nil {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("missing db state")
	}
	if _, ok := page.DecodeLeafRef(state.RootPageID); !ok {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("expected leaf-ref root page with leaf pages in vlog; root=%d", state.RootPageID)
	}

	if err := leafLog.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("leafLog close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}

	// Preserve leaf-page-in-vlog mode via format.json (Options should not need to
	// restate it here).
	if _, err := ValueLogRewriteOffline(Options{Dir: dir}); err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	state2 := reopen.State()
	if state2 == nil {
		t.Fatalf("missing state after rewrite reopen")
	}
	if _, ok := page.DecodeLeafRef(state2.RootPageID); !ok {
		t.Fatalf("expected leaf-ref root page after rewrite; root=%d", state2.RootPageID)
	}

	got, err := reopen.Get([]byte("k0000"))
	if err != nil {
		t.Fatalf("Get(k0000): %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("value mismatch after rewrite")
	}
}

func TestValueLogRewriteOnline_WithLeafPagesInValueLog_ReopenPreservesData(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	leafLog := newRewriteWriter(filepath.Join(dir, "value_vlog"), 0, 0, 64<<10)
	leafLog.blockCompression = false
	leafLog.blockCodec = valuelog.BlockCodecSnappy
	db.SetLeafPageLog(leafLog)

	ptrs := appendPointersInNewSegment(t, dir, 42, 1, 100_000, 8, func(i int) []byte {
		return bytes.Repeat([]byte{byte(i + 1)}, 1024)
	})
	b := db.NewBatch().(*Batch)
	for i := range ptrs {
		if err := b.SetPointer([]byte{byte('a' + i)}, ptrs[i]); err != nil {
			_ = leafLog.Close()
			_ = db.Close()
			t.Fatalf("SetPointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	if _, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:     2,
		SyncEachBatch: true,
	}); err != nil {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("leafLog close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}

	reopen, err := Open(Options{
		Dir:                        dir,
		ReadOnly:                   true,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	for i := range ptrs {
		key := []byte{byte('a' + i)}
		got, err := reopen.Get(key)
		if err != nil {
			t.Fatalf("get %q: %v", key, err)
		}
		want := bytes.Repeat([]byte{byte(i + 1)}, 1024)
		if !bytes.Equal(got, want) {
			t.Fatalf("value mismatch for %q", key)
		}
	}
}

func collectLeafRefs(t *testing.T, p *pager.Pager, rootID uint64) []uint64 {
	t.Helper()
	if p == nil || rootID == 0 {
		return nil
	}
	if _, ok := page.DecodeLeafRef(rootID); ok {
		return []uint64{rootID}
	}

	stack := make([]uint64, 0, 128)
	stack = append(stack, rootID)
	visited := make(map[uint64]struct{}, 1024)
	out := make([]uint64, 0, 1024)

	for len(stack) > 0 {
		pageID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := visited[pageID]; ok {
			continue
		}
		visited[pageID] = struct{}{}

		if _, ok := page.DecodeLeafRef(pageID); ok {
			out = append(out, pageID)
			continue
		}

		data, err := p.Get(pageID)
		if err != nil {
			t.Fatalf("pager.Get(%d): %v", pageID, err)
		}
		n := node.NewNodeView(data)
		if !n.VerifyChecksum() {
			t.Fatalf("checksum mismatch on page %d", pageID)
		}

		switch n.Type() {
		case page.PageTypeInternal:
			count := n.Count()
			for i := uint16(0); i < count; i++ {
				childID, err := n.GetInternalChildID(i)
				if err != nil {
					t.Fatalf("GetInternalChildID(%d,%d): %v", pageID, i, err)
				}
				stack = append(stack, childID)
			}
		case page.PageTypeLeaf:
			// no children
		default:
			t.Fatalf("unexpected page type %d at page %d", n.Type(), pageID)
		}
	}
	return out
}

func collectNestedLeafValueLogFileCounts(t *testing.T, reader interface {
	ReadUnsafe(ptr page.ValuePtr) ([]byte, error)
}, p *pager.Pager, rootID uint64, sourceSet map[uint32]struct{}) map[uint32]int {
	t.Helper()
	out := make(map[uint32]int, 128)
	if reader == nil || p == nil || rootID == 0 {
		return out
	}
	for _, id := range collectLeafRefs(t, p, rootID) {
		ptr, ok := page.DecodeLeafRef(id)
		if !ok {
			continue
		}
		if len(sourceSet) > 0 {
			if _, ok := sourceSet[ptr.ValueLogFileID()]; !ok {
				continue
			}
		}
		leafPage, err := reader.ReadUnsafe(ptr.ValuePtr())
		if err != nil {
			t.Fatalf("read leaf page file=%d offset=%d: %v", ptr.FileID, ptr.Offset, err)
		}
		if len(leafPage) != page.PageSize {
			t.Fatalf("leaf page size file=%d offset=%d got=%d want=%d", ptr.FileID, ptr.Offset, len(leafPage), page.PageSize)
		}
		leaf := node.NewNodeView(leafPage)
		if leaf.Type() != page.PageTypeLeaf {
			t.Fatalf("expected leaf page in value log file=%d offset=%d, got type=%d", ptr.FileID, ptr.Offset, leaf.Type())
		}
		if !leaf.VerifyChecksum() {
			t.Fatalf("leaf page checksum mismatch file=%d offset=%d", ptr.FileID, ptr.Offset)
		}
		for i := uint16(0); i < leaf.Count(); i++ {
			_, _, valPtr, flags, err := leaf.GetLeafEntryView(i)
			if err != nil {
				t.Fatalf("GetLeafEntryView(file=%d, idx=%d): %v", ptr.FileID, i, err)
			}
			if flags&node.FlagPointer == 0 || !page.IsValueLogFileID(valPtr.FileID) {
				continue
			}
			out[valPtr.FileID]++
		}
	}
	return out
}

func TestValueLogRewriteOnline_RewritesLeafRefsAndReclaimsSegments(t *testing.T) {
	dir := t.TempDir()

	var leafLog *rewriteWriter
	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if leafLog != nil {
			_ = leafLog.Close()
		}
		if db != nil {
			_ = db.Close()
		}
	}()

	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	leafLog = newRewriteWriter(walDir, 0, 0, 64<<10)
	leafLog.blockCompression = false
	leafLog.blockCodec = valuelog.BlockCodecSnappy
	db.SetLeafPageLog(leafLog)

	// Seed a tree with many leaves in a single commit so the current leaf refs
	// are spread across multiple value-log segments.
	const (
		keyCount = 8000
		valSize  = 32
	)
	b := db.NewBatch().(*Batch)
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("k%06d", i))
		val := bytes.Repeat([]byte{byte(i % 251)}, valSize)
		if err := b.Set(key, val); err != nil {
			t.Fatalf("seed set %q: %v", key, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	state := db.State()
	if state == nil {
		t.Fatalf("missing state")
	}
	refs := collectLeafRefs(t, db.Pager(), state.RootPageID)
	if len(refs) < 2 {
		t.Fatalf("expected multiple leaf refs, got %d", len(refs))
	}
	refsByFile := make(map[uint32][]uint64, 8)
	for _, id := range refs {
		ptr, ok := page.DecodeLeafRef(id)
		if !ok {
			continue
		}
		refsByFile[ptr.ValueLogFileID()] = append(refsByFile[ptr.ValueLogFileID()], id)
	}

	active := map[uint32]struct{}{}
	set := db.valueLogManager.CurrentSet()
	if set != nil {
		active = currentValueLogIDs(set)
		_ = db.valueLogManager.Release(set)
	}

	var targetID uint32
	for fileID, ids := range refsByFile {
		if len(ids) < 2 {
			continue
		}
		if _, ok := active[fileID]; ok {
			continue
		}
		targetID = fileID
		break
	}
	if targetID == 0 {
		for fileID, ids := range refsByFile {
			if len(ids) >= 2 {
				targetID = fileID
				break
			}
		}
	}
	if targetID == 0 {
		t.Fatalf("expected at least one value-log segment with multiple leaf refs")
	}
	targetPath := db.valueLogManager.SegmentPath(targetID)
	if _, err := os.Stat(targetPath); err != nil {
		t.Fatalf("expected segment %d to exist at %s: %v", targetID, targetPath, err)
	}

	// Move one leaf ref out of the target segment to ensure the segment contains
	// both live and stale leaf pages (GC alone can't reclaim it).
	moveID := refsByFile[targetID][0]
	movePtr, ok := page.DecodeLeafRef(moveID)
	if !ok {
		t.Fatalf("expected leafref id, got %d", moveID)
	}
	leafPage, err := db.valueLogManager.ReadUnsafe(movePtr.ValuePtr())
	if err != nil {
		t.Fatalf("read leaf page: %v", err)
	}
	leafNode := node.NewNodeView(leafPage)
	if leafNode.Type() != page.PageTypeLeaf {
		t.Fatalf("expected leaf page, got type=%d", leafNode.Type())
	}
	kView, vView, _, flags, err := leafNode.GetLeafEntryView(0)
	if err != nil {
		t.Fatalf("GetLeafEntryView: %v", err)
	}
	if flags&node.FlagPointer != 0 {
		t.Fatalf("expected inline seed values for touch, got pointer flags=0x%x", flags)
	}
	key := append([]byte(nil), kView...)
	val := append([]byte(nil), vView...)
	if err := db.Set(key, val); err != nil {
		t.Fatalf("touch set: %v", err)
	}

	// Sanity: the target segment should still be referenced before rewrite.
	state2 := db.State()
	if state2 == nil {
		t.Fatalf("missing state after touch")
	}
	refs2 := collectLeafRefs(t, db.Pager(), state2.RootPageID)
	inTarget := 0
	for _, id := range refs2 {
		ptr, ok := page.DecodeLeafRef(id)
		if ok && ptr.ValueLogFileID() == targetID {
			inTarget++
		}
	}
	if inTarget == 0 {
		t.Fatalf("expected target segment %d to still be referenced before rewrite", targetID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stats, err := db.ValueLogRewriteOnline(ctx, ValueLogRewriteOnlineOptions{
		BatchSize:     16,
		SyncEachBatch: true,
		SourceFileIDs: []uint32{targetID},
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected rewrite to copy leaf-page records, stats=%+v", stats)
	}

	// After rewrite, no leaf refs should point at the old segment.
	state3 := db.State()
	if state3 == nil {
		t.Fatalf("missing state after rewrite")
	}
	refs3 := collectLeafRefs(t, db.Pager(), state3.RootPageID)
	for _, id := range refs3 {
		ptr, ok := page.DecodeLeafRef(id)
		if ok && ptr.ValueLogFileID() == targetID {
			t.Fatalf("leaf ref still points at source segment %d after rewrite", targetID)
		}
	}
	waitTimeout := 2 * time.Second
	if runtime.GOOS == "windows" {
		waitTimeout = 5 * time.Second
	}
	if err := waitForPathRemoval(targetPath, waitTimeout); err != nil {
		t.Fatalf("expected source segment to be removed after rewrite: %v", err)
	}

	if err := leafLog.Close(); err != nil {
		t.Fatalf("leafLog close: %v", err)
	}
	leafLog = nil
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}
	db = nil

	reopen, err := Open(Options{
		Dir:                        dir,
		ReadOnly:                   true,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	got, err := reopen.Get([]byte("k000010"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	want := bytes.Repeat([]byte{10}, valSize)
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch after reopen: got=%x want=%x", got, want)
	}
}

func TestValueLogRewriteOnline_RewritesMultipleLeafRefSourceSegmentsAndReopensRW(t *testing.T) {
	dir := t.TempDir()

	var leafLog *rewriteWriter
	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if leafLog != nil {
			_ = leafLog.Close()
		}
		if db != nil {
			_ = db.Close()
		}
	}()

	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	leafLog = newRewriteWriter(walDir, 0, 0, 64<<10)
	leafLog.blockCompression = false
	leafLog.blockCodec = valuelog.BlockCodecSnappy
	db.SetLeafPageLog(leafLog)

	const (
		keyCount = 24000
		valSize  = 64
	)
	b := db.NewBatch().(*Batch)
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("k%06d", i))
		val := bytes.Repeat([]byte{byte(i % 251)}, valSize)
		if err := b.Set(key, val); err != nil {
			t.Fatalf("seed set %q: %v", key, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	state := db.State()
	if state == nil {
		t.Fatalf("missing state")
	}
	refs := collectLeafRefs(t, db.Pager(), state.RootPageID)
	if len(refs) < 8 {
		t.Fatalf("expected many leaf refs, got %d", len(refs))
	}
	refsByFile := make(map[uint32]int, 16)
	for _, id := range refs {
		ptr, ok := page.DecodeLeafRef(id)
		if !ok {
			continue
		}
		refsByFile[ptr.ValueLogFileID()]++
	}

	set := db.valueLogManager.CurrentSet()
	active := currentValueLogIDs(set)
	if set != nil {
		_ = db.valueLogManager.Release(set)
	}

	sourceIDs := make([]uint32, 0, 8)
	for fileID, count := range refsByFile {
		if count < 8 {
			continue
		}
		if _, ok := active[fileID]; ok {
			continue
		}
		sourceIDs = append(sourceIDs, fileID)
	}
	sort.Slice(sourceIDs, func(i, j int) bool { return sourceIDs[i] < sourceIDs[j] })
	if len(sourceIDs) < 4 {
		t.Fatalf("expected at least four non-active leafref source segments, got %d", len(sourceIDs))
	}
	sourceIDs = sourceIDs[:4]

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	stats, err := db.ValueLogRewriteOnline(ctx, ValueLogRewriteOnlineOptions{
		BatchSize:     32,
		SyncEachBatch: true,
		SourceFileIDs: sourceIDs,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected rewrite to copy records, stats=%+v", stats)
	}

	state2 := db.State()
	if state2 == nil {
		t.Fatalf("missing state after rewrite")
	}
	refs2 := collectLeafRefs(t, db.Pager(), state2.RootPageID)
	sourceSet := make(map[uint32]struct{}, len(sourceIDs))
	for _, id := range sourceIDs {
		sourceSet[id] = struct{}{}
	}
	for _, id := range refs2 {
		ptr, ok := page.DecodeLeafRef(id)
		if !ok {
			continue
		}
		if _, ok := sourceSet[ptr.ValueLogFileID()]; ok {
			t.Fatalf("leaf ref still points at rewritten source segment %d", ptr.ValueLogFileID())
		}
	}

	if err := leafLog.Close(); err != nil {
		t.Fatalf("leafLog close: %v", err)
	}
	leafLog = nil
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}
	db = nil

	reopen, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("reopen rw: %v", err)
	}
	defer reopen.Close()

	got, err := reopen.Get([]byte("k000100"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	want := bytes.Repeat([]byte{byte(100 % 251)}, valSize)
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch after reopen: got=%x want=%x", got, want)
	}
}

func TestValueLogRewriteOnline_PostRewriteWritesDoNotReintroduceLeafRefSources(t *testing.T) {
	dir := t.TempDir()

	var leafLog *rewriteWriter
	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if leafLog != nil {
			_ = leafLog.Close()
		}
		if db != nil {
			_ = db.Close()
		}
	}()

	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	leafLog = newRewriteWriter(walDir, 0, 0, 64<<10)
	leafLog.blockCompression = false
	leafLog.blockCodec = valuelog.BlockCodecSnappy
	db.SetLeafPageLog(leafLog)

	const (
		keyCount = 24000
		valSize  = 64
	)
	b := db.NewBatch().(*Batch)
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("k%06d", i))
		val := bytes.Repeat([]byte{byte(i % 251)}, valSize)
		if err := b.Set(key, val); err != nil {
			t.Fatalf("seed set %q: %v", key, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	state := db.State()
	if state == nil {
		t.Fatalf("missing state")
	}
	refs := collectLeafRefs(t, db.Pager(), state.RootPageID)
	refsByFile := make(map[uint32]int, 16)
	for _, id := range refs {
		ptr, ok := page.DecodeLeafRef(id)
		if !ok {
			continue
		}
		refsByFile[ptr.ValueLogFileID()]++
	}

	set := db.valueLogManager.CurrentSet()
	active := currentValueLogIDs(set)
	if set != nil {
		_ = db.valueLogManager.Release(set)
	}

	sourceIDs := make([]uint32, 0, 8)
	for fileID, count := range refsByFile {
		if count < 8 {
			continue
		}
		if _, ok := active[fileID]; ok {
			continue
		}
		sourceIDs = append(sourceIDs, fileID)
	}
	sort.Slice(sourceIDs, func(i, j int) bool { return sourceIDs[i] < sourceIDs[j] })
	if len(sourceIDs) < 4 {
		t.Fatalf("expected at least four non-active leafref source segments, got %d", len(sourceIDs))
	}
	sourceIDs = sourceIDs[:4]
	sourceSet := make(map[uint32]struct{}, len(sourceIDs))
	for _, id := range sourceIDs {
		sourceSet[id] = struct{}{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if _, err := db.ValueLogRewriteOnline(ctx, ValueLogRewriteOnlineOptions{
		BatchSize:     32,
		SyncEachBatch: true,
		SourceFileIDs: sourceIDs,
	}); err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}

	postRewriteRefs := collectLeafRefs(t, db.Pager(), db.State().RootPageID)
	for _, id := range postRewriteRefs {
		ptr, ok := page.DecodeLeafRef(id)
		if !ok {
			continue
		}
		if _, ok := sourceSet[ptr.ValueLogFileID()]; ok {
			t.Fatalf("leaf ref still points at rewritten source segment %d after rewrite", ptr.ValueLogFileID())
		}
	}

	post := db.NewBatch().(*Batch)
	for i := 0; i < 2048; i++ {
		key := []byte(fmt.Sprintf("post-%06d", i))
		val := bytes.Repeat([]byte{byte((i + 17) % 251)}, valSize)
		if err := post.Set(key, val); err != nil {
			t.Fatalf("post set %q: %v", key, err)
		}
	}
	if err := post.WriteSync(); err != nil {
		t.Fatalf("post write: %v", err)
	}
	_ = post.Close()

	postWriteRefs := collectLeafRefs(t, db.Pager(), db.State().RootPageID)
	for _, id := range postWriteRefs {
		ptr, ok := page.DecodeLeafRef(id)
		if !ok {
			continue
		}
		if _, ok := sourceSet[ptr.ValueLogFileID()]; ok {
			t.Fatalf("post-rewrite write reintroduced source segment %d", ptr.ValueLogFileID())
		}
	}

	if err := leafLog.Close(); err != nil {
		t.Fatalf("leafLog close: %v", err)
	}
	leafLog = nil
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}
	db = nil

	reopen, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("reopen rw: %v", err)
	}
	defer reopen.Close()

	got, err := reopen.Get([]byte("post-000042"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	want := bytes.Repeat([]byte{byte((42 + 17) % 251)}, valSize)
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch after reopen")
	}
}

func TestValueLogRewriteOnline_UnsyncedLeafRefRewriteRemainsReopenable(t *testing.T) {
	dir := t.TempDir()

	var leafLog *rewriteWriter
	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if leafLog != nil {
			_ = leafLog.Close()
		}
		if db != nil {
			_ = db.Close()
		}
	}()

	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	leafLog = newRewriteWriter(walDir, 0, 0, 64<<10)
	leafLog.blockCompression = false
	leafLog.blockCodec = valuelog.BlockCodecSnappy
	db.SetLeafPageLog(leafLog)

	const (
		keyCount = 24000
		valSize  = 64
	)
	b := db.NewBatch().(*Batch)
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("k%06d", i))
		val := bytes.Repeat([]byte{byte(i % 251)}, valSize)
		if err := b.Set(key, val); err != nil {
			t.Fatalf("seed set %q: %v", key, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	refs := collectLeafRefs(t, db.Pager(), db.State().RootPageID)
	refsByFile := make(map[uint32]int, 16)
	for _, id := range refs {
		ptr, ok := page.DecodeLeafRef(id)
		if !ok {
			continue
		}
		refsByFile[ptr.ValueLogFileID()]++
	}

	set := db.valueLogManager.CurrentSet()
	active := currentValueLogIDs(set)
	if set != nil {
		_ = db.valueLogManager.Release(set)
	}

	sourceIDs := make([]uint32, 0, 8)
	for fileID, count := range refsByFile {
		if count < 8 {
			continue
		}
		if _, ok := active[fileID]; ok {
			continue
		}
		sourceIDs = append(sourceIDs, fileID)
	}
	sort.Slice(sourceIDs, func(i, j int) bool { return sourceIDs[i] < sourceIDs[j] })
	if len(sourceIDs) < 4 {
		t.Fatalf("expected at least four non-active leafref source segments, got %d", len(sourceIDs))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if _, err := db.ValueLogRewriteOnline(ctx, ValueLogRewriteOnlineOptions{
		BatchSize:     32,
		SyncEachBatch: false,
		SourceFileIDs: sourceIDs[:4],
	}); err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}

	if err := leafLog.Close(); err != nil {
		t.Fatalf("leafLog close: %v", err)
	}
	leafLog = nil
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}
	db = nil

	reopen, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("reopen rw: %v", err)
	}
	defer reopen.Close()

	got, err := reopen.Get([]byte("k000100"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	want := bytes.Repeat([]byte{byte(100 % 251)}, valSize)
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch after reopen: got=%x want=%x", got, want)
	}
}

func TestValueLogRewriteOnline_UnsyncedRewriteThenVacuumRemainsReopenable(t *testing.T) {
	dir := t.TempDir()

	var leafLog *rewriteWriter
	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if leafLog != nil {
			_ = leafLog.Close()
		}
		if db != nil {
			_ = db.Close()
		}
	}()

	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	leafLog = newRewriteWriter(walDir, 0, 0, 64<<10)
	leafLog.blockCompression = false
	leafLog.blockCodec = valuelog.BlockCodecSnappy
	db.SetLeafPageLog(leafLog)

	const (
		keyCount = 24000
		valSize  = 64
	)
	b := db.NewBatch().(*Batch)
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("k%06d", i))
		val := bytes.Repeat([]byte{byte(i % 251)}, valSize)
		if err := b.Set(key, val); err != nil {
			t.Fatalf("seed set %q: %v", key, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	refs := collectLeafRefs(t, db.Pager(), db.State().RootPageID)
	refsByFile := make(map[uint32]int, 16)
	for _, id := range refs {
		ptr, ok := page.DecodeLeafRef(id)
		if !ok {
			continue
		}
		refsByFile[ptr.ValueLogFileID()]++
	}

	set := db.valueLogManager.CurrentSet()
	active := currentValueLogIDs(set)
	if set != nil {
		_ = db.valueLogManager.Release(set)
	}

	sourceIDs := make([]uint32, 0, 8)
	for fileID, count := range refsByFile {
		if count < 8 {
			continue
		}
		if _, ok := active[fileID]; ok {
			continue
		}
		sourceIDs = append(sourceIDs, fileID)
	}
	sort.Slice(sourceIDs, func(i, j int) bool { return sourceIDs[i] < sourceIDs[j] })
	if len(sourceIDs) < 4 {
		t.Fatalf("expected at least four non-active leafref source segments, got %d", len(sourceIDs))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	stats, err := db.ValueLogRewriteOnline(ctx, ValueLogRewriteOnlineOptions{
		BatchSize:     32,
		SyncEachBatch: false,
		SourceFileIDs: sourceIDs[:4],
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected rewrite copies")
	}
	if err := db.VacuumIndexOnline(context.Background()); err != nil {
		if errors.Is(err, ErrVacuumUnsupported) {
			t.Skipf("VacuumIndexOnline unsupported on this platform: %v", err)
		}
		t.Fatalf("VacuumIndexOnline: %v", err)
	}

	if err := leafLog.Close(); err != nil {
		t.Fatalf("leafLog close: %v", err)
	}
	leafLog = nil
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}
	db = nil

	reopen, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("reopen rw: %v", err)
	}
	defer reopen.Close()

	got, err := reopen.Get([]byte("k000100"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	want := bytes.Repeat([]byte{byte(100 % 251)}, valSize)
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch after reopen: got=%x want=%x", got, want)
	}
}

func TestValueLogRewriteOnline_WALOnLeafRefsPreserveNestedValueSegments(t *testing.T) {
	dir := t.TempDir()

	var leafLog *rewriteWriter
	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOnRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if leafLog != nil {
			_ = leafLog.Close()
		}
		if db != nil {
			_ = db.Close()
		}
	}()

	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	leafLog = newRewriteWriter(walDir, 0, 0, 64<<10)
	leafLog.blockCompression = false
	leafLog.blockCodec = valuelog.BlockCodecSnappy
	db.SetLeafPageLog(leafLog)

	const (
		seedBatches  = 12
		keysPerBatch = 4096
		valSize      = 1024
		valueLane    = 253
	)
	for batchNum := 0; batchNum < seedBatches; batchNum++ {
		ptrs := appendPointersInNewSegment(t, dir, valueLane, uint32(batchNum+1), uint64(batchNum*keysPerBatch+1), keysPerBatch, func(i int) []byte {
			return bytes.Repeat([]byte{byte((batchNum + i) % 251)}, valSize)
		})
		b := db.NewBatch().(*Batch)
		for i, ptr := range ptrs {
			key := []byte(fmt.Sprintf("seed-%02d-%05d", batchNum, i))
			if err := b.SetPointer(key, ptr); err != nil {
				t.Fatalf("seed set batch=%d key=%d: %v", batchNum, i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("seed write batch=%d: %v", batchNum, err)
		}
		_ = b.Close()
	}

	state := db.State()
	if state == nil {
		t.Fatalf("missing state")
	}
	refs := collectLeafRefs(t, db.Pager(), state.RootPageID)
	if len(refs) < 8 {
		t.Fatalf("expected many leaf refs, got %d", len(refs))
	}
	refsByFile := make(map[uint32]int, 32)
	for _, id := range refs {
		ptr, ok := page.DecodeLeafRef(id)
		if !ok {
			continue
		}
		refsByFile[ptr.ValueLogFileID()]++
	}

	set := db.valueLogManager.CurrentSet()
	active := currentValueLogIDs(set)
	if set != nil {
		_ = db.valueLogManager.Release(set)
	}

	sourceIDs := make([]uint32, 0, 8)
	for fileID, count := range refsByFile {
		if count < 8 {
			continue
		}
		if _, ok := active[fileID]; ok {
			continue
		}
		sourceIDs = append(sourceIDs, fileID)
	}
	sort.Slice(sourceIDs, func(i, j int) bool { return sourceIDs[i] < sourceIDs[j] })
	if len(sourceIDs) < 4 {
		t.Fatalf("expected at least four non-active leafref source segments, got %d", len(sourceIDs))
	}
	sourceIDs = sourceIDs[:4]
	sourceSet := make(map[uint32]struct{}, len(sourceIDs))
	for _, id := range sourceIDs {
		sourceSet[id] = struct{}{}
	}

	nestedCounts := collectNestedLeafValueLogFileCounts(t, db.valueLogManager, db.Pager(), state.RootPageID, sourceSet)
	if len(nestedCounts) == 0 {
		t.Fatalf("expected nested value-log pointers inside rewritten leaf pages")
	}
	nonActiveNested := make([]uint32, 0, len(nestedCounts))
	for fileID, count := range nestedCounts {
		if count == 0 {
			continue
		}
		if _, ok := active[fileID]; ok {
			continue
		}
		nonActiveNested = append(nonActiveNested, fileID)
	}
	sort.Slice(nonActiveNested, func(i, j int) bool { return nonActiveNested[i] < nonActiveNested[j] })
	if len(nonActiveNested) == 0 {
		t.Fatalf("expected non-active nested value-log segments referenced by source leaf pages")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	stats, err := db.ValueLogRewriteOnline(ctx, ValueLogRewriteOnlineOptions{
		BatchSize:     32,
		SyncEachBatch: true,
		SourceFileIDs: sourceIDs,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected rewrite copies, stats=%+v", stats)
	}

	for _, fileID := range nonActiveNested {
		lane, seq := valuelog.DecodeFileID(fileID)
		path := filepath.Join(walDir, fmt.Sprintf("value-l%d-%06d.log", lane, seq))
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("nested live value-log segment removed file=%d path=%s err=%v", fileID, path, err)
		}
	}

	if err := leafLog.Close(); err != nil {
		t.Fatalf("leafLog close: %v", err)
	}
	leafLog = nil
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}
	db = nil

	reopen, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOnRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("reopen rw: %v", err)
	}
	defer reopen.Close()

	got, err := reopen.Get([]byte("seed-03-00123"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	want := bytes.Repeat([]byte{byte((3 + 123) % 251)}, valSize)
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch after reopen")
	}
}
