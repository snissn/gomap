package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestPrimaryDirectoryRetainedDecodeMatchesCanonicalV6(t *testing.T) {
	base := PrimaryOperand{Ref: page.PageChildRef(2), Digest: sha256.Sum256([]byte("actual DATA"))}
	for _, count := range []int{0, 1, 17, 32, 49} {
		entries := make([]PrimaryDirectoryEntry, count)
		for i := range entries {
			entries[i] = PrimaryDirectoryEntry{Key: []byte(fmt.Sprintf("key-%02d", i)), Kind: PrimaryAbsence, Revision: page.EntryRevision(i + 1), ClassSlot: uint16(count - 1 - i)}
		}
		image := make([]byte, page.PageSize)
		if err := EncodePrimaryDirectory(image, 12, 1, base, entries); err != nil {
			t.Fatal(err)
		}
		check := func(image []byte) {
			t.Helper()
			expected, ordinaryErr := DecodePrimaryDirectory(image)
			var work PrimaryDirectoryDecodeWorkV6
			var ready bool
			var err error
			for turns := 0; turns < 100; turns++ {
				w := iterator.OrdinalScanWork{RecordLimit: 7, ByteLimit: 1 << 20}
				ready, err = work.Step(image, &w)
				if w.Records > 7 || w.Bytes > 1<<20 {
					t.Fatal("decode exceeded allowance")
				}
				if ready || err != nil {
					break
				}
			}
			if (err != nil) != (ordinaryErr != nil) || (!ready && err == nil) {
				t.Fatalf("count=%d canonical=%v retained=%v ready=%t", count, ordinaryErr, err, ready)
			}
			if err != nil {
				return
			}
			actual, ok := work.Result()
			if !ok || actual.Count() != expected.Count() {
				t.Fatal("retained canonical count")
			}
			for i := 0; i < count; i++ {
				a, _ := actual.Entry(i)
				e, _ := expected.Entry(i)
				if !bytes.Equal(a.Key, e.Key) || a.Operand != e.Operand || a.Revision != e.Revision || a.Kind != e.Kind || a.ClassSlot != e.ClassSlot {
					t.Fatal("retained canonical record differs")
				}
			}
		}
		check(image)
		// Resealed malformed bytes must fail the SAME complete format predicates.
		for _, off := range []int{16, 24, 28, 99, page.PageSize - 1} {
			bad := bytes.Clone(image)
			bad[off] ^= 1
			page.UpdateChecksum(bad)
			check(bad)
		}
		if count > 0 {
			for _, off := range []int{PrimaryDirectoryHeaderSize, PrimaryDirectoryRecordsOffset + 49, PrimaryDirectoryRecordsOffset + 53} {
				bad := bytes.Clone(image)
				bad[off] ^= 0x80
				page.UpdateChecksum(bad)
				check(bad)
			}
			bad := bytes.Clone(image)
			binary.LittleEndian.PutUint16(bad[PrimaryDirectoryRecordsOffset+49:], 0)
			page.UpdateChecksum(bad)
			check(bad)
		}
	}
}
