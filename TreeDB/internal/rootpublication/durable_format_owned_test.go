package rootpublication

import (
	"bytes"
	"encoding/binary"
	"github.com/snissn/gomap/TreeDB/freelist"
	"testing"
)

func TestOwnedRootRecordPhysicalVersionAndDigestNoAllocation(t *testing.T) {
	for _, owned := range []bool{false, true} {
		for _, directory := range []bool{false, true} {
			record := DurableRootRecordV1{OwnedLeafManifest: owned, CommitSeq: 1, DurableSeq: 1, UserRootPageID: 2, SystemRootPageID: 3, TotalPages: 9, Freelist: freelist.GenerationRefV1{HeaderPageID: 4, GenerationID: 1, CommitSeq: 1, HighWater: 9, Digest: [32]byte{1}}, MetaProjectionDigest: [32]byte{2}}
			if directory {
				record.Directory = DependencyDirectoryRefV2{RootPageID: 5}
			} else {
				record.Manifest = DependencyManifestRefV1{FirstPageID: 5, PageCount: 1, ByteLength: 16, Digest: [32]byte{3}}
			}
			image, digest, err := record.EncodePage(8)
			if err != nil {
				t.Fatal(err)
			}
			version := uint16(1)
			if directory {
				version = 2
			}
			if owned {
				version = 3
			}
			if binary.LittleEndian.Uint16(image[24:26]) != version {
				t.Fatal("unexpected physical root version")
			}
			retained := bytes.Clone(image)
			otherImage, _, err := record.EncodePage(7)
			if err != nil {
				t.Fatal(err)
			}
			otherImage[400] ^= 1
			if !bytes.Equal(image, retained) {
				t.Fatal("EncodePage image ownership was not retained")
			}
			decoded, err := DecodeDurableRootRecordV1(image, 8, digest)
			if err != nil || decoded != record {
				t.Fatalf("roundtrip: %+v %v", decoded, err)
			}
			if got, err := record.DigestPage(8); err != nil || got != digest {
				t.Fatalf("digest differs: %v", err)
			}
			if allocs := testing.AllocsPerRun(100, func() {
				if got, err := record.DigestPage(8); err != nil || got != digest {
					panic("digest")
				}
			}); allocs != 0 {
				t.Fatalf("DigestPage allocations=%g", allocs)
			}
		}
	}
}
