package valuelog

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
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
	w, err := NewWriter(filepath.Join(dir, fmt.Sprintf("value-l%d-%06d.log", ReservedLeafLogLaneID, 1)), fileID)
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
	w, err := NewWriter(filepath.Join(dir, fmt.Sprintf("value-l%d-%06d.log", ReservedLeafLogLaneID, 1)), fileID)
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
	w, err := NewWriter(filepath.Join(dir, fmt.Sprintf("value-l%d-%06d.log", ReservedLeafLogLaneID, 1)), fileID)
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
	w, err := NewWriter(filepath.Join(dir, fmt.Sprintf("value-l%d-%06d.log", ReservedLeafLogLaneID, 1)), fileID)
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
	got, err := appendMaybeDecodeLeafLogPayload(mustEncodeFileID(t, ReservedLeafLogLaneID, 1), dst[:0], dst)
	if err != nil {
		t.Fatalf("appendMaybeDecodeLeafLogPayload: %v", err)
	}
	if len(got) != page.PageSize {
		t.Fatalf("appendMaybeDecodeLeafLogPayload len=%d want %d", len(got), page.PageSize)
	}
	requireLeafPagesLogicallyEqual(t, leaf, got)
}
