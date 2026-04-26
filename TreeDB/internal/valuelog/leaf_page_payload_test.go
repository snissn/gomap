package valuelog

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func buildSparseLeafPageForPayloadTest(t *testing.T) []byte {
	t.Helper()
	buf := make([]byte, page.PageSize)
	b := node.NewBuilderWithOptions(buf, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	b.SetPageID(17)
	for i := 0; i < 4; i++ {
		if err := b.AddLeafEntry([]byte("celestia/outer/leaf/key/"+string(rune('a'+i))), []byte("value"), node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry(%d): %v", i, err)
		}
	}
	b.FinishNoNode()
	return buf
}

func compactLeafPayloadTestPath(t *testing.T, root string, seq uint32) string {
	t.Helper()
	leafDir := filepath.Join(root, compactLeafPagePayloadDirName)
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", leafDir, err)
	}
	return filepath.Join(leafDir, fmt.Sprintf("value-l%d-%06d.log", ReservedLeafLogLaneID, seq))
}

func newLeafPayloadTestManager(t *testing.T, dir, path string, fileID uint32) *Manager {
	t.Helper()
	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.RegisterSegment(path, fileID); err != nil {
		_ = mgr.Close()
		t.Fatalf("RegisterSegment(%q): %v", path, err)
	}
	return mgr
}

func requireLeafPagesLogicallyEqual(t *testing.T, want, got []byte) {
	t.Helper()
	wantNode := node.NewNodeView(want)
	gotNode := node.NewNodeView(got)
	if wantNode.Type() != gotNode.Type() {
		t.Fatalf("page types differ: got=%d want=%d", gotNode.Type(), wantNode.Type())
	}
	if wantNode.Count() != gotNode.Count() {
		t.Fatalf("page counts differ: got=%d want=%d", gotNode.Count(), wantNode.Count())
	}
	for i := uint16(0); i < wantNode.Count(); i++ {
		wantKey, wantVal, wantPtr, wantFlags, err := wantNode.GetLeafEntryView(i)
		if err != nil {
			t.Fatalf("want GetLeafEntryView(%d): %v", i, err)
		}
		gotKey, gotVal, gotPtr, gotFlags, err := gotNode.GetLeafEntryView(i)
		if err != nil {
			t.Fatalf("got GetLeafEntryView(%d): %v", i, err)
		}
		if !bytes.Equal(gotKey, wantKey) || !bytes.Equal(gotVal, wantVal) || gotPtr != wantPtr || gotFlags != wantFlags {
			t.Fatalf("leaf entry %d mismatch", i)
		}
	}
}

func TestMaybeCompactLeafLogPayload_SparseLeafRoundTrips(t *testing.T) {
	leaf := buildSparseLeafPageForPayloadTest(t)

	payload, compacted, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload: %v", err)
	}
	if !compacted {
		t.Fatalf("expected sparse leaf page to compact")
	}
	if len(payload) >= len(leaf) {
		t.Fatalf("payload len=%d want < %d", len(payload), len(leaf))
	}

	decoded, usedDst, decodedFlag, err := decodeCompactLeafLogPayloadTo(payload, make([]byte, 0, page.PageSize))
	if err != nil {
		t.Fatalf("decodeCompactLeafLogPayloadTo: %v", err)
	}
	if !decodedFlag {
		t.Fatalf("expected compact payload to decode")
	}
	if !usedDst {
		t.Fatalf("expected decode to reuse dst")
	}
	if len(decoded) != page.PageSize {
		t.Fatalf("decoded len=%d want %d", len(decoded), page.PageSize)
	}
	if !page.VerifyChecksumNonMutating(decoded) {
		t.Fatalf("decoded page checksum mismatch")
	}
	requireLeafPagesLogicallyEqual(t, leaf, decoded)
}

func TestMaybeCompactLeafLogPayload_DirtyFreeGapUsesCanonicalChecksum(t *testing.T) {
	leaf := buildSparseLeafPageForPayloadTest(t)
	prefixLen, suffixLen, err := node.LeafPageLiveBounds(leaf)
	if err != nil {
		t.Fatalf("LeafPageLiveBounds: %v", err)
	}
	suffixStart := len(leaf) - suffixLen
	if prefixLen >= suffixStart {
		t.Fatalf("leaf has no free gap: prefix=%d suffixStart=%d", prefixLen, suffixStart)
	}
	for i := prefixLen; i < suffixStart; i++ {
		leaf[i] = byte(0x80 + i%64)
	}

	payload, compacted, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload: %v", err)
	}
	if !compacted {
		t.Fatalf("expected sparse leaf page to compact")
	}
	decoded, _, decodedFlag, err := decodeCompactLeafLogPayloadTo(payload, nil)
	if err != nil {
		t.Fatalf("decodeCompactLeafLogPayloadTo: %v", err)
	}
	if !decodedFlag {
		t.Fatalf("expected compact payload to decode")
	}
	if !page.VerifyChecksumNonMutating(decoded) {
		t.Fatalf("decoded canonical page checksum mismatch")
	}
	requireLeafPagesLogicallyEqual(t, leaf, decoded)
}

func TestDecodeCompactLeafLogPayloadTo_AliasedDstRoundTrips(t *testing.T) {
	leaf := buildSparseLeafPageForPayloadTest(t)

	payload, compacted, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload: %v", err)
	}
	if !compacted {
		t.Fatalf("expected sparse leaf page to compact")
	}

	buf := make([]byte, 0, page.PageSize)
	buf = append(buf, payload...)
	decoded, usedDst, decodedFlag, err := decodeCompactLeafLogPayloadTo(buf, buf[:0])
	if err != nil {
		t.Fatalf("decodeCompactLeafLogPayloadTo: %v", err)
	}
	if !decodedFlag {
		t.Fatalf("expected compact payload to decode")
	}
	if !usedDst {
		t.Fatalf("expected decode to reuse aliased dst")
	}
	if len(decoded) != page.PageSize {
		t.Fatalf("decoded len=%d want %d", len(decoded), page.PageSize)
	}
	if !page.VerifyChecksumNonMutating(decoded) {
		t.Fatalf("decoded page checksum mismatch")
	}
	requireLeafPagesLogicallyEqual(t, leaf, decoded)
}

func TestDecodeCompactLeafLogPayloadTo_AliasedDstDoesNotAllocate(t *testing.T) {
	leaf := buildSparseLeafPageForPayloadTest(t)

	payload, compacted, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload: %v", err)
	}
	if !compacted {
		t.Fatalf("expected sparse leaf page to compact")
	}

	buf := make([]byte, page.PageSize)
	var decoded []byte
	allocs := testing.AllocsPerRun(100, func() {
		copy(buf, payload)
		out, usedDst, decodedFlag, err := decodeCompactLeafLogPayloadTo(buf[:len(payload)], buf[:0])
		if err != nil {
			t.Fatalf("decodeCompactLeafLogPayloadTo: %v", err)
		}
		if !decodedFlag {
			t.Fatalf("expected compact payload to decode")
		}
		if !usedDst {
			t.Fatalf("expected decode to reuse aliased dst")
		}
		decoded = out
	})
	if allocs != 0 {
		t.Fatalf("aliased decode allocations=%v want 0", allocs)
	}
	requireLeafPagesLogicallyEqual(t, leaf, decoded)
}

func TestMaybeCompactLeafLogPayload_PassthroughNonPagePayload(t *testing.T) {
	raw := []byte("not-a-leaf-page")
	payload, compacted, err := MaybeCompactLeafLogPayload(raw)
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload: %v", err)
	}
	if compacted {
		t.Fatalf("expected passthrough for non-page payload")
	}
	if !bytes.Equal(payload, raw) {
		t.Fatalf("payload changed for passthrough case")
	}
}

func TestManagerReadUnsafeTo_DecodesCompactLeafPayload(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := compactLeafPayloadTestPath(t, dir, 1)
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	leaf := buildSparseLeafPageForPayloadTest(t)
	payload, compacted, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload: %v", err)
	}
	if !compacted {
		t.Fatalf("expected sparse leaf page to compact")
	}
	ptr, err := w.Append(0, nil, 1, payload)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	mgr := newLeafPayloadTestManager(t, dir, path, fileID)
	defer func() { _ = mgr.Close() }()

	dst := make([]byte, 0, page.PageSize)
	got, usedDst, err := mgr.ReadUnsafeTo(ptr, dst)
	if err != nil {
		t.Fatalf("ReadUnsafeTo: %v", err)
	}
	if !usedDst {
		t.Fatalf("expected ReadUnsafeTo to reuse dst for compact leaf page")
	}
	if len(got) != page.PageSize {
		t.Fatalf("ReadUnsafeTo len=%d want %d", len(got), page.PageSize)
	}
	requireLeafPagesLogicallyEqual(t, leaf, got)
}

func TestManagerReadUnsafe_DecodesCompactLeafPayload(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := compactLeafPayloadTestPath(t, dir, 1)
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	leaf := buildSparseLeafPageForPayloadTest(t)
	payload, compacted, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload: %v", err)
	}
	if !compacted {
		t.Fatalf("expected sparse leaf page to compact")
	}
	ptr, err := w.Append(0, nil, 1, payload)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	mgr := newLeafPayloadTestManager(t, dir, path, fileID)
	defer func() { _ = mgr.Close() }()

	got, err := mgr.ReadUnsafe(ptr)
	if err != nil {
		t.Fatalf("ReadUnsafe: %v", err)
	}
	if len(got) != page.PageSize {
		t.Fatalf("ReadUnsafe len=%d want %d", len(got), page.PageSize)
	}
	requireLeafPagesLogicallyEqual(t, leaf, got)
}

func TestWriterAppendCompactLeafPage_BlockCompressionRoundTrips(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := compactLeafPayloadTestPath(t, dir, 1)
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	w.SetBlockCompression(BlockCodecSnappy, true)

	leaf := buildSparseLeafPageForPayloadTest(t)
	ptr, stats, compacted, err := w.AppendCompactLeafPage(1, leaf)
	if err != nil {
		t.Fatalf("AppendCompactLeafPage: %v", err)
	}
	if !compacted {
		t.Fatalf("expected sparse leaf page to compact")
	}
	if stats.RawPayloadBytes <= 0 {
		t.Fatalf("stats.RawPayloadBytes=%d want > 0", stats.RawPayloadBytes)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	mgr := newLeafPayloadTestManager(t, dir, path, fileID)
	defer func() { _ = mgr.Close() }()

	got, err := mgr.ReadUnsafe(ptr)
	if err != nil {
		t.Fatalf("ReadUnsafe: %v", err)
	}
	if len(got) != page.PageSize {
		t.Fatalf("ReadUnsafe len=%d want %d", len(got), page.PageSize)
	}
	requireLeafPagesLogicallyEqual(t, leaf, got)
}

func TestPrepareCompactLeafPageRecord_BufferedAppendRoundTrips(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := compactLeafPayloadTestPath(t, dir, 1)
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	leaf := buildSparseLeafPageForPayloadTest(t)
	raw, stats, compacted, ptrLength, err := PrepareCompactLeafPageRecord(nil, 1, leaf)
	if err != nil {
		t.Fatalf("PrepareCompactLeafPageRecord: %v", err)
	}
	if !compacted {
		t.Fatalf("expected sparse leaf page to compact")
	}
	if stats.RawPayloadBytes <= 0 || stats.RawPayloadBytes >= len(leaf) {
		t.Fatalf("stats.RawPayloadBytes=%d want compact payload smaller than %d", stats.RawPayloadBytes, len(leaf))
	}
	ptr, err := w.AppendRawRecordBuffered(raw, ptrLength)
	if err != nil {
		t.Fatalf("AppendRawRecordBuffered: %v", err)
	}
	if got := page.ValuePtrRecordLength(ptr); got == 0 || got != uint32(len(raw)-4) {
		t.Fatalf("record length hint=%d want %d", got, len(raw)-4)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	mgr := newLeafPayloadTestManager(t, dir, path, fileID)
	defer func() { _ = mgr.Close() }()

	got, err := mgr.ReadUnsafe(ptr)
	if err != nil {
		t.Fatalf("ReadUnsafe: %v", err)
	}
	if len(got) != page.PageSize {
		t.Fatalf("ReadUnsafe len=%d want %d", len(got), page.PageSize)
	}
	requireLeafPagesLogicallyEqual(t, leaf, got)
}

func TestSetReadUnsafe_DecodesCompactLeafPayload(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := compactLeafPayloadTestPath(t, dir, 1)
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	leaf := buildSparseLeafPageForPayloadTest(t)
	payload, compacted, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload: %v", err)
	}
	if !compacted {
		t.Fatalf("expected sparse leaf page to compact")
	}
	ptr, err := w.Append(0, nil, 1, payload)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	mgr := newLeafPayloadTestManager(t, dir, path, fileID)
	defer func() { _ = mgr.Close() }()

	set := mgr.CurrentSetNoRefresh()
	defer func() { _ = mgr.Release(set) }()
	got, err := set.ReadUnsafe(ptr)
	if err != nil {
		t.Fatalf("ReadUnsafe: %v", err)
	}
	if len(got) != page.PageSize {
		t.Fatalf("Set.ReadUnsafe len=%d want %d", len(got), page.PageSize)
	}
	requireLeafPagesLogicallyEqual(t, leaf, got)
}

func TestManagerReadUnsafeAppend_DecodesCompactLeafPayload(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := compactLeafPayloadTestPath(t, dir, 1)
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	leaf := buildSparseLeafPageForPayloadTest(t)
	payload, compacted, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload: %v", err)
	}
	if !compacted {
		t.Fatalf("expected sparse leaf page to compact")
	}
	ptr, err := w.Append(0, nil, 1, payload)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	mgr := newLeafPayloadTestManager(t, dir, path, fileID)
	defer func() { _ = mgr.Close() }()

	got, err := mgr.ReadUnsafeAppend(ptr, make([]byte, 0, page.PageSize))
	if err != nil {
		t.Fatalf("ReadUnsafeAppend: %v", err)
	}
	if len(got) != page.PageSize {
		t.Fatalf("ReadUnsafeAppend len=%d want %d", len(got), page.PageSize)
	}
	gotNode := node.NewNodeView(got)
	if typ := gotNode.Type(); typ != page.PageTypeLeaf {
		t.Fatalf("ReadUnsafeAppend page type=%d want=%d first16=%x", typ, page.PageTypeLeaf, got[:16])
	}
	requireLeafPagesLogicallyEqual(t, leaf, got)
}

func TestManagerReadUnsafeAppend_LeafCompactPayloadUsesMmapPath(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := compactLeafPayloadTestPath(t, dir, 1)
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	leaf := buildSparseLeafPageForPayloadTest(t)
	payload, compacted, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload: %v", err)
	}
	if !compacted {
		t.Fatalf("expected sparse leaf page to compact")
	}
	ptr, err := w.Append(0, nil, 1, payload)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	mgr := newLeafPayloadTestManager(t, dir, path, fileID)
	defer func() { _ = mgr.Close() }()

	f := mgr.files[fileID]
	if f == nil {
		t.Fatalf("manager missing file %d", fileID)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", path, err)
	}
	f.mmapData.Store(data)

	got, err := mgr.ReadUnsafeAppend(ptr, make([]byte, 0, page.PageSize))
	if err != nil {
		t.Fatalf("ReadUnsafeAppend: %v", err)
	}
	if len(got) != page.PageSize {
		t.Fatalf("ReadUnsafeAppend len=%d want %d", len(got), page.PageSize)
	}
	if fallbacks := f.mmapReadFallbackReadAt.Load(); fallbacks != 0 {
		t.Fatalf("mmapReadFallbackReadAt=%d want 0", fallbacks)
	}
	requireLeafPagesLogicallyEqual(t, leaf, got)
}

func TestManagerReadUnsafe_RegularLane255PathDoesNotDecodeCompactLeafPayload(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("value-l%d-%06d.log", ReservedLeafLogLaneID, 1))
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	leaf := buildSparseLeafPageForPayloadTest(t)
	payload, compacted, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload: %v", err)
	}
	if !compacted {
		t.Fatalf("expected sparse leaf page to compact")
	}
	ptr, err := w.Append(0, nil, 1, payload)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	got, err := mgr.ReadUnsafe(ptr)
	if err != nil {
		t.Fatalf("ReadUnsafe: %v", err)
	}
	if len(got) != len(payload) {
		t.Fatalf("ReadUnsafe len=%d want raw compact payload len=%d", len(got), len(payload))
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("ReadUnsafe unexpectedly decoded reserved-lane non-leaf-dir payload")
	}
}

func TestReadAtWithDictTo_CompactLeafPayloadDoesNotClaimDstWhenExpanded(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := compactLeafPayloadTestPath(t, dir, 1)
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	leaf := buildSparseLeafPageForPayloadTest(t)
	payload, compacted, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload: %v", err)
	}
	if !compacted {
		t.Fatalf("expected sparse leaf page to compact")
	}
	ptr, err := w.Append(0, nil, 1, payload)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open(%q): %v", path, err)
	}
	defer func() { _ = f.Close() }()

	dst := make([]byte, 0, len(payload))
	got, usedDst, err := ReadAtWithDictTo(f, ptr, true, nil, nil, nil, templ.DecodeOptions{}, dst)
	if err != nil {
		t.Fatalf("ReadAtWithDictTo: %v", err)
	}
	if usedDst {
		t.Fatalf("usedDst=true want false when compact leaf expansion allocates a full page")
	}
	if len(got) != page.PageSize {
		t.Fatalf("ReadAtWithDictTo len=%d want %d", len(got), page.PageSize)
	}
	requireLeafPagesLogicallyEqual(t, leaf, got)
}

func TestAppendMaybeDecodeLeafLogPayload_DecodesIntoPrefixBuffer(t *testing.T) {
	leaf := buildSparseLeafPageForPayloadTest(t)
	payload, compacted, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload: %v", err)
	}
	if !compacted {
		t.Fatalf("expected sparse leaf page to compact")
	}
	dst := make([]byte, 0, page.PageSize)
	dst = append(dst, payload...)
	got, err := appendMaybeDecodeLeafLogPayload(mustEncodeFileID(t, ReservedLeafLogLaneID, 1), filepath.Join("db", compactLeafPagePayloadDirName, "value-l255-000001.log"), dst[:0], dst)
	if err != nil {
		t.Fatalf("appendMaybeDecodeLeafLogPayload: %v", err)
	}
	if len(got) != page.PageSize {
		t.Fatalf("appendMaybeDecodeLeafLogPayload len=%d want %d", len(got), page.PageSize)
	}
	requireLeafPagesLogicallyEqual(t, leaf, got)
}
