package rootpublication

import (
	"bytes"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestDependencyDirectoryV2ExactIdentityAndCanonicalRecords(t *testing.T) {
	entry := DependencyManifestEntryV1{
		Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: "segment-1", DiagnosticPath: "columns/segment-1",
		Identity: StableIdentity{Platform: "unix", VolumeID: 1, ObjectID: [16]byte{2}, Generation: 1}, Generation: 1,
		Frontier: DurableFrontier{Bytes: 4096}, Reachability: []ReachabilityField{ReachabilityColumnManifest},
	}
	key := DependencyPhysicalKeyV2(entry)
	value, err := EncodeDependencyPhysicalV2(entry)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeDependencyPhysicalV2(key, value)
	if err != nil || got.Identity != entry.Identity || got.ResourceID != entry.ResourceID {
		t.Fatalf("descriptor: %+v %v", got, err)
	}
	rebound := entry
	rebound.Identity.ObjectID[0]++
	if !bytes.Equal(key, DependencyPhysicalKeyV2(rebound)) {
		t.Fatal("snapshot rebind renamed logical owner")
	}
	obligation := StableLogicalObligation{Class: "column", Kind: "chunk", Namespace: "docs", Generation: 1, PartID: 2, FileID: 3, Offset: 100, Length: 20, Checksum: 123, Reachability: ReachabilityColumnManifest, Digest: [32]byte{7}}
	logicalKey := DependencyLogicalKeyV2(obligation)
	logicalValue, err := EncodeDependencyLogicalV2(key, obligation)
	if err != nil {
		t.Fatal(err)
	}
	owner, decoded, err := DecodeDependencyLogicalV2(logicalKey, logicalValue)
	if err != nil || decoded != obligation || !bytes.Equal(owner, key) {
		t.Fatalf("logical record: %+v %v", decoded, err)
	}
	conflicting := obligation
	conflicting.Checksum++
	conflicting.Digest[0]++
	if !bytes.Equal(logicalKey, DependencyLogicalKeyV2(conflicting)) {
		t.Fatal("conflicting integrity metadata acquired a different identity")
	}
	for _, pair := range [][2][]byte{{logicalKey[:len(logicalKey)-1], logicalValue}, {logicalKey, logicalValue[:len(logicalValue)-1]}, {logicalKey, append(bytes.Clone(logicalValue), 0)}, {key, logicalValue}} {
		if _, _, err := DecodeDependencyLogicalV2(pair[0], pair[1]); err == nil {
			t.Fatal("malformed logical record accepted")
		}
	}
	entry.LogicalObligations = []StableLogicalObligation{obligation}
	if _, err := EncodeDependencyPhysicalV2(entry); err == nil {
		t.Fatal("physical encoder silently discarded logical authority")
	}
	if _, err := DecodeDependencyPhysicalV2(logicalKey, value); err == nil {
		t.Fatal("descriptor accepted wrong key")
	}
}

func TestDependencyDirectoryV2PinnedPointAndStreamingChecks(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.GrowTo(3); err != nil {
		t.Fatal(err)
	}
	entry := DependencyManifestEntryV1{Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: "one", DiagnosticPath: "columns/one", Identity: StableIdentity{Platform: "unix", ObjectID: [16]byte{1}, Generation: 1}, Generation: 1, Frontier: DurableFrontier{Bytes: 4096}, Reachability: []ReachabilityField{ReachabilityColumnManifest}}
	obligation := StableLogicalObligation{Class: "column", Kind: "chunk", Namespace: "docs", Generation: 1, PartID: 2, FileID: 1, Offset: 100, Length: 20, Checksum: 123, Reachability: ReachabilityColumnManifest, Digest: [32]byte{7}}
	physicalKey := DependencyPhysicalKeyV2(entry)
	physicalValue, err := EncodeDependencyPhysicalV2(entry)
	if err != nil {
		t.Fatal(err)
	}
	logicalKey := DependencyLogicalKeyV2(obligation)
	logicalValue, err := EncodeDependencyLogicalV2(physicalKey, obligation)
	if err != nil {
		t.Fatal(err)
	}
	image := make([]byte, page.PageSize)
	n := node.NewNode(image)
	n.SetType(page.PageTypeLeaf)
	n.SetPageID(2)
	if err := n.AddLeafEntry(physicalKey, physicalValue, node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatal(err)
	}
	if err := n.AddLeafEntry(logicalKey, logicalValue, node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatal(err)
	}
	n.UpdateChecksum()
	if err := p.Write(2, image); err != nil {
		t.Fatal(err)
	}
	releases := 0
	directory, err := NewDependencyDirectoryV2(p, DependencyDirectoryRefV2{RootPageID: 2, PhysicalCount: 1, LogicalCount: 1}, p.PageCount(), func() { releases++ })
	if err != nil {
		t.Fatal(err)
	}
	visited := 0
	if err := directory.Walk(func(_, _ []byte) error { visited++; return nil }); err != nil || visited != 2 {
		t.Fatalf("walk: visited=%d err=%v", visited, err)
	}
	owner, got, found, err := directory.LookupLogical(obligation)
	if err != nil || !found || got != obligation || !bytes.Equal(owner, physicalKey) {
		t.Fatalf("point proof: %+v %t %v", got, found, err)
	}
	missing := obligation
	missing.PartID++
	if _, _, found, err := directory.LookupLogical(missing); err != nil || found {
		t.Fatalf("missing proof: %t %v", found, err)
	}
	wrongCount, err := NewDependencyDirectoryV2(p, DependencyDirectoryRefV2{RootPageID: 2, PhysicalCount: 1, LogicalCount: 2}, p.PageCount(), func() {})
	if err != nil {
		t.Fatal(err)
	}
	if err := wrongCount.Walk(nil); !errors.Is(err, ErrDependencyManifestFormat) {
		t.Fatalf("count mismatch accepted: %v", err)
	}
	wrongCount.Release()
	if err := directory.Retain(); err != nil {
		t.Fatal(err)
	}
	directory.Release()
	if releases != 0 {
		t.Fatal("released index while reader was retained")
	}
	directory.Release()
	if releases != 1 {
		t.Fatalf("release count=%d", releases)
	}
	if _, _, _, err := directory.LookupLogical(obligation); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("released reader accepted: %v", err)
	}
}

func TestDurableRootRecordV2DirectoryOneOf(t *testing.T) {
	record := DurableRootRecordV1{
		CommitSeq: 12, DurableSeq: 11, UserRootPageID: 2, SystemRootPageID: 3, TotalPages: 34,
		Freelist:             freelist.GenerationRefV1{HeaderPageID: 28, GenerationID: 12, CommitSeq: 12, HighWater: 33, Digest: [32]byte{8}},
		Directory:            DependencyDirectoryRefV2{RootPageID: 30, PhysicalCount: 4, LogicalCount: 9000000},
		MetaProjectionDigest: [32]byte{10},
	}
	image, digest, err := record.EncodePage(33)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeDurableRootRecordV1(image, 33, digest)
	if err != nil || decoded != record || binary.LittleEndian.Uint16(image[24:26]) != 2 {
		t.Fatalf("directory roundtrip: %+v %v", decoded, err)
	}
	record.Manifest = DependencyManifestRefV1{FirstPageID: 30, ByteLength: 16, PageCount: 1, Digest: [32]byte{7}}
	if _, _, err := record.EncodePage(33); !errors.Is(err, ErrDurableRootRecordFormat) {
		t.Fatalf("mixed version accepted: %v", err)
	}
	// Even a correctly rechecksummed/rehashed mixed-format image is rejected.
	image[200] = 1
	digest = durableRootRecordDigestV1(image)
	copy(image[312:344], digest[:])
	page.UpdateChecksum(image)
	if _, err := DecodeDurableRootRecordV1(image, 33, digest); !errors.Is(err, ErrDurableRootRecordFormat) {
		t.Fatalf("mixed image accepted: %v", err)
	}
}

func TestDependencyDirectoryV2RejectsChildOutsideSelectedExtentAndCycles(t *testing.T) {
	for _, tc := range []struct {
		name   string
		child  uint64
		extent uint64
	}{
		{"beyond-selected-extent", 3, 3},
		{"cycle", 2, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if err := p.GrowTo(4); err != nil {
				t.Fatal(err)
			}
			image := make([]byte, page.PageSize)
			n := node.NewNode(image)
			n.SetType(page.PageTypeInternal)
			n.SetPageID(2)
			if err := n.AddInternalChild([]byte{}, tc.child); err != nil {
				t.Fatal(err)
			}
			n.UpdateChecksum()
			if err := p.Write(2, image); err != nil {
				t.Fatal(err)
			}
			leaf := make([]byte, page.PageSize)
			ln := node.NewNode(leaf)
			ln.SetType(page.PageTypeLeaf)
			ln.SetPageID(3)
			ln.UpdateChecksum()
			if err := p.Write(3, leaf); err != nil {
				t.Fatal(err)
			}
			directory, err := NewDependencyDirectoryV2(p, DependencyDirectoryRefV2{RootPageID: 2}, tc.extent, func() {})
			if err != nil {
				t.Fatal(err)
			}
			defer directory.Release()
			obligation := StableLogicalObligation{Class: "column", Kind: "chunk", Namespace: "docs", Generation: 1, PartID: 1, FileID: 1, Length: 1, Reachability: ReachabilityColumnManifest}
			if _, _, found, err := directory.LookupLogical(obligation); err == nil || found {
				t.Fatalf("corrupt lookup accepted: found=%v error=%v", found, err)
			}
			if err := directory.Walk(nil); err == nil {
				t.Fatal("corrupt directory walk accepted")
			}
		})
	}
}
