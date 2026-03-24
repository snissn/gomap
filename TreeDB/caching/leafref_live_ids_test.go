package caching

import (
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type nestedLeafLiveIDsReader struct {
	pageData            []byte
	readChecksumEnabled bool
}

func (r nestedLeafLiveIDsReader) Read(ptr page.ValuePtr) ([]byte, error) {
	return append([]byte(nil), r.pageData...), nil
}

func (r nestedLeafLiveIDsReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	return r.pageData, nil
}

func (r nestedLeafLiveIDsReader) ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error) {
	dst = append(dst[:0], r.pageData...)
	return dst, true, nil
}

func (r nestedLeafLiveIDsReader) ReadChecksumEnabled() bool {
	return r.readChecksumEnabled
}

func buildCorruptedNestedLeafPage(t *testing.T, nested page.ValuePtr) []byte {
	t.Helper()
	buf := make([]byte, page.PageSize)
	n := node.NewNode(buf)
	n.SetType(page.PageTypeLeaf)
	n.SetPageID(11)
	if err := n.AddLeafEntry([]byte("k"), nil, node.FlagPointer, nested); err != nil {
		t.Fatalf("AddLeafEntry: %v", err)
	}
	n.UpdateChecksum()
	buf[8] ^= 0x40
	return buf
}

func TestCollectNestedLeafPageValueLogLiveIDs_SkipsLeafChecksumWhenReaderDisabled(t *testing.T) {
	outer := page.ValuePtr{FileID: 1<<31 + 1, Offset: 64, Length: page.PageSize}
	nested := page.ValuePtr{FileID: 1<<31 + 2, Offset: 128, Length: 32}
	reader := nestedLeafLiveIDsReader{
		pageData:            buildCorruptedNestedLeafPage(t, nested),
		readChecksumEnabled: false,
	}
	live := map[uint32]struct{}{}
	var scratch []byte
	if err := collectNestedLeafPageValueLogLiveIDs(context.Background(), outer, reader, live, &scratch, false); err != nil {
		t.Fatalf("collectNestedLeafPageValueLogLiveIDs: %v", err)
	}
	if _, ok := live[nested.FileID]; !ok {
		t.Fatalf("expected nested live id %d", nested.FileID)
	}
}

func TestCollectNestedLeafPageValueLogLiveIDs_VerifiesLeafChecksumWhenReaderEnabled(t *testing.T) {
	outer := page.ValuePtr{FileID: 1<<31 + 1, Offset: 64, Length: page.PageSize}
	nested := page.ValuePtr{FileID: 1<<31 + 2, Offset: 128, Length: 32}
	reader := nestedLeafLiveIDsReader{
		pageData:            buildCorruptedNestedLeafPage(t, nested),
		readChecksumEnabled: true,
	}
	live := map[uint32]struct{}{}
	var scratch []byte
	if err := collectNestedLeafPageValueLogLiveIDs(context.Background(), outer, reader, live, &scratch, true); err == nil {
		t.Fatalf("expected checksum verification error")
	}
}
