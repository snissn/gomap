package freelist

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/page"
)

// This representation optimization preserves observable allocation semantics.
// The regression discriminator is physical amplification, alongside behavioral
// controls: one chunk must not require fourteen unary index pages.
func TestGenerationTopology5131PublicCandidatePageAmplification(t *testing.T) {
	store := NewMemoryPageStoreV1()
	base := MustNewFreelistGenerationV1(1, 1024, []uint64{300, 301, 302}, nil)
	txn := NewFreelistTxn(base, nil)
	if id, err := txn.Allocate(300); err != nil || id != 302 {
		t.Fatalf("allocate = %d, %v", id, err)
	}
	candidate, err := txn.MaterializeCandidate(2, 2, candidateIDFromString("patricia-public"), store)
	if err != nil {
		t.Fatal(err)
	}
	// Exactly one direct chunk, one reservation page, and one header.
	if got := len(candidate.DirtyPageIDs()); got != 3 {
		t.Fatalf("single-chunk candidate emitted %d pages; want 3", got)
	}
	for _, id := range candidate.DirtyPageIDs() {
		found := false
		for _, owned := range candidate.ReservationRecord().metadataPages() {
			if owned == id {
				found = true
			}
		}
		if !found {
			t.Fatalf("emitted page %d is outside candidate", id)
		}
	}
	loaded, err := LoadGenerationV1(store, candidate.Generation().GenerationRef())
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Allocatable(300) || !loaded.Allocatable(301) || loaded.Allocatable(302) || !base.Allocatable(302) {
		t.Fatal("allocation or held generation changed across reopen")
	}
}

func TestGenerationTopology5131SplitCollapseSparseHighBits(t *testing.T) {
	keys := []uint64{0, 1, 15, 16, 0x123456789abc, 1 << 55, (1 << 56) - 1}
	var root *stateNode
	for _, key := range keys {
		key := key
		prior := root
		root = mutateChunk(root, key, 0, func(c *stateChunk) { c.setFree(7, true) })
		if err := validateCanonicalStateV2(root, true); err != nil {
			t.Fatalf("insert %x: %v", key, err)
		}
		if prior != nil && lookupChunk(prior, key) != nil {
			t.Fatalf("old root gained %x", key)
		}
		for _, want := range keys {
			if want == key {
				break
			}
			if c := lookupChunk(root, want); c == nil || !c.isFree(7) {
				t.Fatalf("lost %x", want)
			}
		}
	}
	for _, key := range keys {
		root = mutateChunk(root, key, 0, func(c *stateChunk) { c.setFree(7, false) })
		if err := validateCanonicalStateV2(root, true); err != nil {
			t.Fatalf("delete %x: %v", key, err)
		}
		if lookupChunk(root, key) != nil {
			t.Fatalf("retained empty chunk %x", key)
		}
	}
	if root.freeCount != 0 || root.chunk != nil {
		t.Fatal("empty root is not canonical sentinel")
	}
}

func TestGenerationTopology5131PrivateAndPersistentUseSameShape(t *testing.T) {
	keys := []uint64{1, 2, 0x12345, 1 << 55}
	var persistent, private *stateNode
	for _, key := range keys {
		persistent = mutateChunk(persistent, key, 0, func(c *stateChunk) { c.retired[9] = 4 })
		private = mutatePrivateChunk(private, key, 0, func(c *stateChunk) { c.retired[9] = 4 })
	}
	for _, key := range keys {
		persistent = mutateChunk(persistent, key, 0, func(c *stateChunk) { c.retired[9] = 0; c.setFree(10, true) })
		private = mutatePrivateChunk(private, key, 0, func(c *stateChunk) { c.retired[9] = 0; c.setFree(10, true) })
	}
	var same func(*stateNode, *stateNode)
	same = func(a, b *stateNode) {
		if a == nil || b == nil {
			if a != b {
				t.Fatal("different shape")
			}
			return
		}
		if a.depth != b.depth || a.prefix != b.prefix || a.freeCount != b.freeCount || a.retiredCount != b.retiredCount ||
			a.minChunk != b.minChunk || a.maxChunk != b.maxChunk || (a.chunk == nil) != (b.chunk == nil) {
			t.Fatal("different shape/summary")
		}
		for i := range a.child {
			same(a.child[i], b.child[i])
		}
	}
	same(persistent, private)
	if err := validateCanonicalStateV2(private, true); err != nil {
		t.Fatal(err)
	}
}

func TestGenerationTopology5131DetachedEncodingCanonicalAndFailClosed(t *testing.T) {
	var root *stateNode
	for _, key := range []uint64{0x12340, 0x1234f} {
		root = mutateChunk(root, key, 0, func(c *stateChunk) { c.setFree(3, true) })
	}
	store := NewMemoryPageStoreV1()
	next := uint64(2)
	var emit func(*stateNode)
	emit = func(n *stateNode) {
		for _, child := range n.child {
			if child != nil {
				emit(child)
			}
		}
		n.pageID = next
		next++
		dst := bytes.Repeat([]byte{0x7f}, page.PageSize)
		if n.chunk != nil {
			n.chunk.pageID = n.pageID
			if err := encodeChunkPageInto(dst, n.pageID, 1, n.chunk); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(dst, encodeChunkPage(n.pageID, 1, n.chunk)) {
				t.Fatal("chunk encoder retains destination contents")
			}
		} else {
			if err := encodeIndexPageInto(dst, n.pageID, 1, n, 0); err != nil {
				t.Fatal(err)
			}
			want, err := encodeIndexPage(n.pageID, 1, n, 0)
			if err != nil || !bytes.Equal(dst, want) {
				t.Fatal("index encoder retains destination contents")
			}
		}
		n.checksum = binary.LittleEndian.Uint32(dst[8:12])
		if n.chunk != nil {
			n.chunk.checksum = n.checksum
		}
		store.Pages[n.pageID] = dst
	}
	emit(root)
	loaded, err := loadStateRefV2(store, root.pageID, 0, 1, true, true, next, make(map[uint64]struct{}), -1)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateCanonicalStateV2(loaded, true); err != nil {
		t.Fatal(err)
	}
	original := append([]byte(nil), store.Pages[root.pageID]...)
	for name, mutate := range map[string]func([]byte){
		"old-version":  func(b []byte) { binary.LittleEndian.PutUint16(b[24:26], 1) },
		"unary":        func(b []byte) { binary.LittleEndian.PutUint16(b[14:16], 1) },
		"wrong-prefix": func(b []byte) { b[64] ^= 1 },
		"invalid-kind": func(b []byte) { b[indexHeaderSize+1] = 2 },
		"nonzero-tail": func(b []byte) { b[len(b)-1] = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			b := append([]byte(nil), original...)
			mutate(b)
			page.UpdateChecksum(b)
			store.Pages[root.pageID] = b
			_, err := loadStateRefV2(store, root.pageID, 0, 1, true, true, next, make(map[uint64]struct{}), -1)
			if !errors.Is(err, ErrGenerationFormat) {
				t.Fatalf("corruption accepted: %v", err)
			}
		})
	}
}

func TestGenerationTopology5131ReservationDigestMatchesCanonicalCopy(t *testing.T) {
	record := ReservationRecordV1{GenerationID: 2, BaseID: 1, CandidateID: candidateIDFromString("digest")}
	for i := uint64(2); i < 400; i += 2 {
		record.Extents = append(record.Extents, ReservationExtentV1{StartPageID: i, Count: 1, Kind: ReservationReusedData})
	}
	pages, _, err := encodeReservationPages(1000, record)
	if err != nil {
		t.Fatal(err)
	}
	// Perturbing stored CRC/digest is ignored; changing a semantic byte is not.
	want := reservationDigest(pages)
	for _, b := range pages {
		b[8] ^= 1
		b[112] ^= 1
	}
	if reservationDigest(pages) != want {
		t.Fatal("digest includes canonical zero fields")
	}
	pages[0][176] ^= 1
	if reservationDigest(pages) == want {
		t.Fatal("digest omitted semantic entries")
	}
}

// Main's public snapshot classifier routes through the canonical lookup helper.
// A direct root leaf must distinguish its own key from another chunk key.
func TestGenerationTopology5131SnapshotDirectLeafClassification(t *testing.T) {
	g := MustNewFreelistGenerationV1(1, 1024, []uint64{2}, nil)
	for _, tc := range []struct {
		name   string
		id     uint64
		unused bool
	}{
		{name: "wrong-key", id: 300},
		{name: "matching-free", id: 2, unused: true},
		{name: "matching-live", id: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			unused, err := g.SnapshotPageUnusedV1(tc.id, 1)
			if err != nil || unused != tc.unused {
				t.Fatalf("classification=(%t,%v), want (%t,nil)", unused, err, tc.unused)
			}
			if err := g.Validate(); err != nil || !g.Allocatable(2) {
				t.Fatalf("classification changed generation: %v", err)
			}
		})
	}
}

// Direct-leaf routing may inspect its already admitted node summary, but must
// admit the chunk before inspecting any chunk fields or free/retired operands.
func TestGenerationTopology5131SnapshotDirectLeafWorkAdmission(t *testing.T) {
	g := MustNewFreelistGenerationV1(1, 1024, []uint64{2}, nil)
	nodeBytes := 2*uint64(unsafe.Sizeof(FreelistGenerationV1{})) + uint64(unsafe.Sizeof(stateNode{}))
	chunkBytes := uint64(unsafe.Sizeof(stateChunk{}))
	for _, tc := range []struct {
		name                   string
		id, records, bytes     uint64
		unused, ready          bool
		wantRecords, wantBytes uint64
	}{
		{name: "wrong-key-uses-node-summary", id: 300, records: 2, bytes: nodeBytes, ready: true, wantRecords: 2, wantBytes: nodeBytes},
		{name: "matching-key-record-underadmission", id: 2, records: 2, bytes: nodeBytes + chunkBytes, wantRecords: 2, wantBytes: nodeBytes},
		{name: "matching-key-byte-underadmission", id: 2, records: 3, bytes: nodeBytes + chunkBytes - 1, wantRecords: 2, wantBytes: nodeBytes},
		{name: "matching-key-exact", id: 2, records: 3, bytes: nodeBytes + chunkBytes, unused: true, ready: true, wantRecords: 3, wantBytes: nodeBytes + chunkBytes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			work := iterator.OrdinalScanWork{RecordLimit: tc.records, ByteLimit: tc.bytes}
			unused, ready, err := g.SnapshotPageUnusedWithWorkV1(tc.id, 1, &work)
			if err != nil || unused != tc.unused || ready != tc.ready {
				t.Fatalf("classification=(%t,%t,%v), want (%t,%t,nil)", unused, ready, err, tc.unused, tc.ready)
			}
			if work.Records != tc.wantRecords || work.Bytes != tc.wantBytes {
				t.Fatalf("work=(%d,%d), want (%d,%d)", work.Records, work.Bytes, tc.wantRecords, tc.wantBytes)
			}
			if err := g.Validate(); err != nil || !g.Allocatable(2) {
				t.Fatalf("read classification changed generation: %v", err)
			}
		})
	}
}
