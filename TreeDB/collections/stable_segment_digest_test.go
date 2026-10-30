package collections

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"
)

// Compare with the established canonical byte layout on both sides of the
// fixed-buffer boundary. The digest identifies a segment, not an asset's
// contents or publication lineage.
func TestStableColumnSegmentDigestCanonical(t *testing.T) {
	golden := stableColumnSegmentDigest(ColumnAssetRef{})
	if got := hex.EncodeToString(golden[:]); got != "af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc" {
		t.Fatalf("empty namespace/file digest=%s", got)
	}
	for _, namespace := range []string{
		"", "column/assets", "目录/🗃", strings.Repeat("x", 503),
		strings.Repeat("x", 504), strings.Repeat("x", 505), strings.Repeat("x", 4096),
	} {
		for _, fileID := range []uint32{0, 1, 17, ^uint32(0)} {
			raw := make([]byte, 4+len(namespace)+4)
			binary.LittleEndian.PutUint32(raw[:4], uint32(len(namespace)))
			copy(raw[4:], namespace)
			binary.LittleEndian.PutUint32(raw[4+len(namespace):], fileID)
			want := sha256.Sum256(raw)
			ref := ColumnAssetRef{Namespace: namespace, FileID: fileID}
			if got := stableColumnSegmentDigest(ref); got != want {
				t.Fatalf("namespace bytes=%d file=%d: digest %x want %x", len(namespace), fileID, got, want)
			}
		}
	}
}
