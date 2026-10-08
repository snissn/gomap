package rootpublication

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

func primaryRootTestLeaf(id uint64, key, value string, revision page.EntryRevision) []byte {
	image := make([]byte, page.PageSize)
	b := node.NewBuilderWithOptions(image, page.PageTypeLeaf, node.BuilderOptions{EntryRevisions: true})
	b.SetPageID(id)
	if e := b.AddLeafEntryWithRevision([]byte(key), []byte(value), 0, page.ValuePtr{}, revision); e != nil {
		panic(e)
	}
	b.FinishNoNode()
	return image
}
func TestPrimaryRootV5BindsActualDataAndCompleteBanks(t *testing.T) {
	data := freelist.NewMemoryPageStoreV1()
	base := primaryRootTestLeaf(2, "a", "old", 1)
	system := primaryRootTestLeaf(3, "system", "s", 1)
	data.Pages[2] = base
	data.Pages[3] = system
	g := freelist.MustNewFreelistGenerationV1(1, 64, nil, nil)
	txn := freelist.NewFreelistTxn(g, freelist.NewReservationLedger())
	candidate, e := txn.MaterializeCandidate(7, 7, freelist.CandidateIDV1{7}, data)
	if e != nil {
		t.Fatal(e)
	}
	generation := candidate.Generation()
	ref := generation.GenerationRef()
	banks := freelist.NewMemoryPageStoreV1()
	componentID := primaryarena.Namespace + 2
	component := primaryRootTestLeaf(componentID, "a", "new", 2)
	banks.Pages[2] = component
	dirID := primaryarena.Namespace + 6
	dir := make([]byte, page.PageSize)
	op := node.PrimaryOperand{Ref: page.PageChildRef(2), Digest: sha256.Sum256(base)}
	entry := node.PrimaryDirectoryEntry{Key: []byte("a"), Operand: node.PrimaryOperand{Ref: page.PageChildRef(componentID), Digest: sha256.Sum256(component)}, Revision: 2, Kind: node.PrimaryPut}
	if e = node.EncodePrimaryDirectory(dir, dirID, 1, op, []node.PrimaryDirectoryEntry{entry}); e != nil {
		t.Fatal(e)
	}
	banks.Pages[6] = dir
	v := DurablePrimaryRootRecordV5{Record: DurableRootRecordV1{CommitSeq: 9, DurableSeq: 9, UserRootPageID: dirID, SystemRootPageID: 3, TotalPages: ref.HighWater, Freelist: ref, FreelistFreeCount: generation.FreeCount(), FreelistRetiredCount: generation.RetiredCount(), Directory: DependencyDirectoryRefV2{RootPageID: primaryarena.Namespace + 8}, MetaProjectionDigest: [32]byte{8}}, Primary: PrimaryProjectionV5{ArenaUUID: [16]byte{1}, ArenaHighWater: 9, DirectoryDigest: sha256.Sum256(dir), BaseRootPageID: 2, BaseSequence: 1, DataCommitSeq: 7, BaseDigest: sha256.Sum256(base), SystemDigest: sha256.Sum256(system)}}
	recordID := primaryarena.Namespace + 7
	image, digest, e := v.EncodePage(recordID)
	if e != nil {
		t.Fatal(e)
	}
	scratch := bytes.Repeat([]byte{0x93}, page.PageSize)
	intoDigest, e := v.EncodePageInto(scratch, recordID)
	if e != nil || intoDigest != digest || !bytes.Equal(scratch, image) {
		t.Fatal("shared encoder differs from canonical V5 page", e)
	}
	for _, size := range []int{page.PageSize - 1, page.PageSize + 1} {
		refused := bytes.Repeat([]byte{0x73}, size)
		before := append([]byte(nil), refused...)
		if _, err := v.EncodePageInto(refused, recordID); !errors.Is(err, ErrDurableRootRecordFormat) || !bytes.Equal(refused, before) {
			t.Fatal("shape refusal changed scratch", err)
		}
	}
	invalid := v
	invalid.Primary.DataCommitSeq = 0
	before := append([]byte(nil), scratch...)
	if _, err := invalid.EncodePageInto(scratch, recordID); !errors.Is(err, ErrDurableRootRecordFormat) || !bytes.Equal(scratch, before) {
		t.Fatal("record refusal changed scratch", err)
	}
	got, e := DecodeDurablePrimaryRootRecordV5(image, recordID, digest)
	if e != nil || got != v {
		t.Fatalf("round trip: %v %+v", e, got)
	}
	if _, e = got.ValidatePhysicalProjectionV5(data, banks, ref.HighWater, 9, [16]byte{1}); e != nil {
		t.Fatal(e)
	}
	if _, e = DecodeDurableRootRecordV1(image, recordID, digest); !errors.Is(e, ErrDurableRootRecordFormat) {
		t.Fatalf("V1 accepted V5: %v", e)
	}
	// V1 continues rejecting mere sequence inequality even for ordinary IDs.
	legacy := v.Record
	legacy.UserRootPageID = 2
	legacy.Directory = DependencyDirectoryRefV2{}
	legacy.Manifest = DependencyManifestRefV1{FirstPageID: 60, ByteLength: 16, PageCount: 1, Digest: [32]byte{1}}
	if _, _, e = legacy.EncodePage(61); !errors.Is(e, ErrDurableRootRecordFormat) {
		t.Fatalf("V1 equality weakened: %v", e)
	}
	for _, change := range []struct {
		name  string
		apply func(*DurablePrimaryRootRecordV5)
	}{
		{"different actual base root", func(v *DurablePrimaryRootRecordV5) { v.Primary.BaseRootPageID = 3 }},
		{"different base generation", func(v *DurablePrimaryRootRecordV5) { v.Primary.BaseSequence++ }},
		{"different base bytes", func(v *DurablePrimaryRootRecordV5) { v.Primary.BaseDigest[0] ^= 1 }},
		{"different system bytes", func(v *DurablePrimaryRootRecordV5) { v.Primary.SystemDigest[0] ^= 1 }},
		{"different directory bytes", func(v *DurablePrimaryRootRecordV5) { v.Primary.DirectoryDigest[0] ^= 1 }},
		{"different DATA sequence", func(v *DurablePrimaryRootRecordV5) { v.Primary.DataCommitSeq++ }},
	} {
		t.Run(change.name, func(t *testing.T) {
			bad := v
			change.apply(&bad)
			if _, e := bad.ValidatePhysicalProjectionV5(data, banks, ref.HighWater, 9, [16]byte{1}); e == nil {
				t.Fatal("substituted physical authority accepted")
			}
		})
	}
	if _, e = v.ValidatePhysicalProjectionV5(data, banks, ref.HighWater, 9, [16]byte{2}); e == nil {
		t.Fatal("foreign arena identity accepted")
	}
	if _, e = v.ValidatePhysicalProjectionV5(data, banks, ref.HighWater, 8, [16]byte{1}); e == nil {
		t.Fatal("short arena extent accepted")
	}
	corrupt := append([]byte(nil), component...)
	corrupt[200] ^= 1
	page.UpdateChecksum(corrupt)
	banks.Pages[2] = corrupt
	if _, e = v.ValidatePhysicalProjectionV5(data, banks, ref.HighWater, 9, [16]byte{1}); e == nil {
		t.Fatal("CRC-resealed foreign component accepted")
	}
}
