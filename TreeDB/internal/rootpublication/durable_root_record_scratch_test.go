package rootpublication

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/page"
)

// Retain the former allocating codec as an independent canonical-copy reference.
func formerRootRecordEncodingForTest(record DurableRootRecordV1, pageID uint64) ([]byte, [32]byte, error) {
	if err := record.validate(pageID); err != nil {
		return nil, [32]byte{}, err
	}
	image := make([]byte, page.PageSize)
	encodeDurableRootRecordFieldsV1(image, pageID, record)
	digest := formerRootRecordDigestForTest(image)
	copy(image[312:344], digest[:])
	page.UpdateChecksum(image)
	return image, digest, nil
}

func formerRootRecordDigestForTest(image []byte) [32]byte {
	canonical := append([]byte(nil), image...)
	clear(canonical[8:12])
	clear(canonical[312:344])
	return sha256.Sum256(canonical)
}

func rootRecordScratchFixture() DurableRootRecordV1 {
	return DurableRootRecordV1{
		CommitSeq: 12, DurableSeq: 11, UserRootPageID: 2, SystemRootPageID: 3, TotalPages: 34,
		MaxEntryRevision: 55, AppliedCommandLSN: 89, LastCommitHeight: 144,
		Freelist:          freelist.GenerationRefV1{HeaderPageID: 28, GenerationID: 12, CommitSeq: 12, HighWater: 33, Digest: [32]byte{8}},
		FreelistFreeCount: 5, FreelistRetiredCount: 6,
		Manifest:           DependencyManifestRefV1{FirstPageID: 30, ByteLength: 777, EntryCount: 4, PageCount: 2, Digest: [32]byte{7}},
		ParentRecordPageID: 20, ParentCommitSeq: 11, ParentRecordDigest: [32]byte{9}, MetaProjectionDigest: [32]byte{10},
	}
}

func TestDurableRootRecordEncodePageIntoMatchesFormerCodec(t *testing.T) {
	base := rootRecordScratchFixture()
	directory := base
	directory.Manifest = DependencyManifestRefV1{}
	directory.Directory = DependencyDirectoryRefV2{RootPageID: 30, PhysicalCount: 7, LogicalCount: 11}
	initial := base
	initial.ParentRecordPageID, initial.ParentCommitSeq, initial.ParentRecordDigest = 0, 0, [32]byte{}
	wide := base
	wide.MaxEntryRevision, wide.AppliedCommandLSN, wide.LastCommitHeight = ^uint64(0), ^uint64(0), ^uint64(0)
	for _, tc := range []struct {
		name   string
		record DurableRootRecordV1
	}{{"manifest", base}, {"directory", directory}, {"initial-parent", initial}, {"wide-fields", wide}} {
		t.Run(tc.name, func(t *testing.T) {
			want, wantDigest, err := formerRootRecordEncodingForTest(tc.record, 33)
			if err != nil {
				t.Fatal(err)
			}
			backing := bytes.Repeat([]byte{0xa5}, page.PageSize+2)
			dst := backing[1 : len(backing)-1]
			digest, err := tc.record.EncodePageInto(33, dst)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(dst, want) || digest != wantDigest || backing[0] != 0xa5 || backing[len(backing)-1] != 0xa5 {
				t.Fatal("caller target differs from former codec or overwrote neighboring bytes")
			}
			allocated, allocatedDigest, err := tc.record.EncodePage(33)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(allocated, want) || allocatedDigest != wantDigest {
				t.Fatal("allocating codec changed bytes/digest")
			}
			got, err := DecodeDurableRootRecordV1(dst, 33, digest)
			if err != nil || got != tc.record {
				t.Fatalf("round trip=%+v err=%v", got, err)
			}
			// Reuse the same target for a different record; the earlier independent
			// materialization image must remain intact.
			next := tc.record
			next.MaxEntryRevision ^= 1
			if _, err := next.EncodePageInto(33, dst); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(allocated, want) {
				t.Fatal("scratch reuse changed an independent image")
			}
		})
	}
}

func TestDurableRootRecordEncodePageIntoRefusalPreservesDestination(t *testing.T) {
	valid := rootRecordScratchFixture()
	invalid := valid
	invalid.Freelist.Digest = [32]byte{}
	for _, tc := range []struct {
		name   string
		record DurableRootRecordV1
		pageID uint64
		size   int
	}{{"short", valid, 33, page.PageSize - 1}, {"long", valid, 33, page.PageSize + 1}, {"nil", valid, 33, 0}, {"page-id", valid, 1, page.PageSize}, {"record", invalid, 33, page.PageSize}} {
		t.Run(tc.name, func(t *testing.T) {
			dst := bytes.Repeat([]byte{0x57}, tc.size)
			before := append([]byte(nil), dst...)
			digest, err := tc.record.EncodePageInto(tc.pageID, dst)
			if !errors.Is(err, ErrDurableRootRecordFormat) || digest != [32]byte{} || !bytes.Equal(dst, before) {
				t.Fatalf("refusal mutated destination/digest or changed error: %v", err)
			}
		})
	}
}

func TestDurableRootRecordDigestPreservesBorrowedInput(t *testing.T) {
	image, _, err := rootRecordScratchFixture().EncodePage(33)
	if err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int{0, 8, 11, 12, 31, 32, 128, 311, 312, 343, 344, 383, 384, page.PageSize - 1} {
		t.Run(fmt.Sprintf("byte-%d", offset), func(t *testing.T) {
			borrowed := append([]byte(nil), image...)
			borrowed[offset] ^= 1
			before := append([]byte(nil), borrowed...)
			got := durableRootRecordDigestV1(borrowed)
			want := formerRootRecordDigestForTest(borrowed)
			if got != want || !bytes.Equal(borrowed, before) {
				t.Fatal("digest differs or mutated borrowed input")
			}
		})
	}
}

func TestDurableRootRecordScratchCorruptionStillRejected(t *testing.T) {
	for _, tc := range []struct {
		name           string
		offset         int
		repairChecksum bool
		want           error
	}{{"checksum", 128, false, ErrDurableRootRecordChecksum}, {"bound-field", 128, true, ErrDurableRootRecordDigest}, {"embedded-digest", 312, true, ErrDurableRootRecordDigest}, {"reserved", 344, true, ErrDurableRootRecordFormat}, {"tail", page.PageSize - 1, true, ErrDurableRootRecordFormat}} {
		t.Run(tc.name, func(t *testing.T) {
			var image [page.PageSize]byte
			digest, err := rootRecordScratchFixture().EncodePageInto(33, image[:])
			if err != nil {
				t.Fatal(err)
			}
			image[tc.offset] ^= 1
			if tc.repairChecksum {
				page.UpdateChecksum(image[:])
			}
			before := image
			if _, err := DecodeDurableRootRecordV1(image[:], 33, digest); !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want %v", err, tc.want)
			}
			if image != before {
				t.Fatal("failed decode mutated borrowed image")
			}
		})
	}
}
