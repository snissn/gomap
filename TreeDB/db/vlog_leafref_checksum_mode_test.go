package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type leafPageChecksumModeReader struct {
	pageData            []byte
	readChecksumEnabled bool
}

func (r leafPageChecksumModeReader) Read(ptr page.ValuePtr) ([]byte, error) {
	return append([]byte(nil), r.pageData...), nil
}

func (r leafPageChecksumModeReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	return r.pageData, nil
}

func (r leafPageChecksumModeReader) ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error) {
	dst = append(dst[:0], r.pageData...)
	return dst, true, nil
}

func (r leafPageChecksumModeReader) ReadChecksumEnabled() bool {
	return r.readChecksumEnabled
}

func buildCorruptedLeafPageWithNestedPointer(t *testing.T, nested page.ValuePtr) []byte {
	t.Helper()
	buf := make([]byte, page.PageSize)
	n := node.NewNode(buf)
	n.SetType(page.PageTypeLeaf)
	n.SetPageID(7)
	if err := n.AddLeafEntry([]byte("k"), nil, node.FlagPointer, nested); err != nil {
		t.Fatalf("AddLeafEntry: %v", err)
	}
	n.UpdateChecksum()
	buf[8] ^= 0x80
	return buf
}

func TestCollectNestedLeafPageValueLogRefCounts_SkipsLeafChecksumWhenReaderDisabled(t *testing.T) {
	outer := page.ValuePtr{FileID: 1<<31 + 1, Offset: 64, Length: page.PageSize}
	nested := page.ValuePtr{FileID: 1<<31 + 2, Offset: 128, Length: 32}
	reader := leafPageChecksumModeReader{
		pageData:            buildCorruptedLeafPageWithNestedPointer(t, nested),
		readChecksumEnabled: false,
	}
	refs := map[uint32]uint64{}
	var scratch []byte
	if err := collectNestedLeafPageValueLogRefCounts(outer, reader, refs, &scratch, false); err != nil {
		t.Fatalf("collectNestedLeafPageValueLogRefCounts: %v", err)
	}
	if got := refs[nested.FileID]; got != 1 {
		t.Fatalf("nested refs[%d]=%d want 1", nested.FileID, got)
	}
}

func TestCollectNestedLeafPageValueLogRefCounts_VerifiesLeafChecksumWhenReaderEnabled(t *testing.T) {
	outer := page.ValuePtr{FileID: 1<<31 + 1, Offset: 64, Length: page.PageSize}
	nested := page.ValuePtr{FileID: 1<<31 + 2, Offset: 128, Length: 32}
	reader := leafPageChecksumModeReader{
		pageData:            buildCorruptedLeafPageWithNestedPointer(t, nested),
		readChecksumEnabled: true,
	}
	refs := map[uint32]uint64{}
	var scratch []byte
	if err := collectNestedLeafPageValueLogRefCounts(outer, reader, refs, &scratch, true); err == nil {
		t.Fatalf("expected checksum verification error")
	}
}
