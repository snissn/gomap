package rootpublication

import (
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestDependencyManifestV1DeterministicMultiPageRoundTrip(t *testing.T) {
	entries := make([]DependencyManifestEntryV1, 0, 48)
	for i := 47; i >= 0; i-- {
		entries = append(entries, DependencyManifestEntryV1{
			Kind: ResourceValueLog, LogicalLane: "main", ResourceID: fmt.Sprintf("segment-%03d", i),
			DiagnosticPath: fmt.Sprintf("value_vlog/%03d-%s.vlog", i, string(make([]byte, 96))),
			Identity:       StableIdentity{Platform: "unix", VolumeID: 9, ObjectID: [16]byte{byte(i + 1)}, Generation: uint64(i + 1)},
			Generation:     uint64(i + 1), Frontier: DurableFrontier{Bytes: uint64((i + 1) * 4096)},
			Reachability: []ReachabilityField{ReachabilityValueLogPointer},
		})
	}
	manifest, err := NewDependencyManifestV1(entries)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.PageCount() < 2 {
		t.Fatalf("page count=%d want multi-page fixture", manifest.PageCount())
	}
	store := freelist.NewMemoryPageStoreV1()
	ref, err := manifest.Materialize(100, store)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadDependencyManifestV1(store, ref)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := loaded.Entries(), manifest.Entries(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("loaded manifest differs\ngot=%v\nwant=%v", got, want)
	}

	second := freelist.NewMemoryPageStoreV1()
	secondRef, err := loaded.Materialize(100, second)
	if err != nil {
		t.Fatal(err)
	}
	if ref != secondRef {
		t.Fatalf("reference changed across deterministic encode: %+v != %+v", ref, secondRef)
	}
	for id, first := range store.Pages {
		if got := second.Pages[id]; string(got) != string(first) {
			t.Fatalf("page %d encoding changed", id)
		}
	}

	delete(store.Pages, ref.FirstPageID+uint64(ref.PageCount)-1)
	if _, err := LoadDependencyManifestV1(store, ref); !errors.Is(err, ErrDependencyManifestFormat) {
		t.Fatalf("truncated manifest error=%v want %v", err, ErrDependencyManifestFormat)
	}
}

func TestDurableRootRecordV1RoundTripBindsMetaFreelistAndManifest(t *testing.T) {
	manifest := DependencyManifestRefV1{FirstPageID: 30, ByteLength: 777, EntryCount: 4, PageCount: 2, Digest: [32]byte{7}}
	freelistRef := freelist.GenerationRefV1{HeaderPageID: 28, GenerationID: 12, CommitSeq: 12, HighWater: 33, Digest: [32]byte{8}}
	want := DurableRootRecordV1{
		CommitSeq: 12, DurableSeq: 11, UserRootPageID: 2, SystemRootPageID: 3, TotalPages: 34,
		MaxEntryRevision: 55, AppliedCommandLSN: 89, LastCommitHeight: 144,
		Freelist: freelistRef, FreelistFreeCount: 5, FreelistRetiredCount: 6, Manifest: manifest,
		ParentRecordPageID: 20, ParentCommitSeq: 11, ParentRecordDigest: [32]byte{9}, MetaProjectionDigest: [32]byte{10},
	}
	image, digest, err := want.EncodePage(33)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeDurableRootRecordV1(image, 33, digest)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("round trip=%+v want %+v", got, want)
	}
	image[128] ^= 1
	page.UpdateChecksum(image)
	if _, err := DecodeDurableRootRecordV1(image, 33, digest); !errors.Is(err, ErrDurableRootRecordDigest) {
		t.Fatalf("record corruption error=%v want %v", err, ErrDurableRootRecordDigest)
	}
}
