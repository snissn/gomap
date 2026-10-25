package collections

import (
	"errors"
	"strings"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestTextV2RootFamilyClassifierBorrowed(t *testing.T) {
	collection, index := strings.Repeat("collection", 16), strings.Repeat("index", 16)
	roots := collectionTextV2RootNames(collection, index)
	for i, root := range roots {
		family, ok := textV2RootFamilyForName(collection, index, root)
		if !ok || family != textV2RootFamily(i+1) {
			t.Fatal(root, family, ok)
		}
		for _, invalid := range []string{root + "x", "x" + root, root[:len(root)-1], collection + "/other/" + index} {
			if _, ok := textV2RootFamilyForName(collection, index, invalid); ok {
				t.Fatal("accepted unrelated qualified root", invalid)
			}
		}
	}
	if got := testing.AllocsPerRun(100, func() {
		for _, root := range roots {
			if _, ok := textV2RootFamilyForName(collection, index, root); !ok {
				panic("missing family")
			}
		}
	}); got != 0 {
		t.Fatalf("root family classification allocated %g", got)
	}
}

func TestNativeTextServingLongIDAdmission(t *testing.T) {
	def := TextIndexDefinition{Fields: []TextIndexField{{Field: "body"}}}
	id := strings.Repeat("x", 64<<10)
	opts := TextSearchOptions{TopK: 10, textV2AllowedDocumentIDs: hybridScalarAllowSet{id: {}}}
	want := 2 * (memtable.COWAllocationCharge(uint64(len(id))) + memtable.COWAllocationCharge(uint64(len(id))+2))
	var admitted uint64
	catalog := nativeTextCatalog{admit: func(n uint64) error { admitted += n; return nil }}
	if err := catalog.admitSingleTermServing(def, opts); err != nil || admitted < want {
		t.Fatalf("long ID admission=%d want>=%d err=%v", admitted, want, err)
	}
	// A budget sufficient for headers alone must refuse before enabling the
	// specialized reader; neither traversal may allocate its ID-derived keys.
	refused := nativeTextCatalog{admit: func(n uint64) error {
		if n >= want {
			return memtable.ErrCOWCapacity
		}
		return nil
	}}
	if err := refused.admitSingleTermServing(def, opts); !errors.Is(err, memtable.ErrCOWCapacity) || refused.singleTermFields != 0 {
		t.Fatalf("long ID refusal fields=%d err=%v", refused.singleTermFields, err)
	}
}

func TestNativeTextServingRecordEnvelope(t *testing.T) {
	const count = 128
	norm := textV2NormBlockValue{BlockStart: 1, BlockSize: textV2DefaultNormBlockSize, FieldCount: 1}
	docmap := textV2DocMapBlockValue{BlockStart: 1, BlockSize: textV2DefaultDocMapBlockSize}
	postings := make([]textV2PostingBlockEntry, count)
	for i := uint64(0); i < count; i++ {
		norm.Entries = append(norm.Entries, textV2NormBlockEntry{Ordinal: i + 1, Generation: 1, FieldLengths: []uint32{1}})
		docmap.Entries = append(docmap.Entries, textV2DocMapEntry{Ordinal: i + 1, Generation: 1, DocumentID: []byte{byte(i + 1)}})
		postings[i] = textV2PostingBlockEntry{Ordinal: i + 1, Generation: 1, TermFrequency: 1, FieldFrequencies: []uint32{1}}
	}
	normRaw := encodeTextV2NormBlockValue(norm)
	normDecoded, err := decodeTextV2SearchNormBlock(normRaw)
	if err != nil {
		t.Fatal(err)
	}
	normCharge, err := nativeTextServingRecordCharge(textV2RootFamilyNormBlocks, false, uint64(len(normRaw)), 1)
	normBacking := memtable.COWAllocationCharge(uint64(cap(normDecoded.Entries))*uint64(unsafe.Sizeof(textV2SearchNormEntry{}))) + memtable.COWAllocationCharge(uint64(cap(normDecoded.FieldLengths))*4)
	if err != nil || normCharge < normBacking {
		t.Fatalf("norm charge=%d backing=%d err=%v", normCharge, normBacking, err)
	}
	docRaw := encodeTextV2DocMapBlockValue(docmap)
	docDecoded, err := decodeTextV2SearchDocMapBlock(docRaw)
	if err != nil {
		t.Fatal(err)
	}
	docCharge, err := nativeTextServingRecordCharge(textV2RootFamilyDocMap, false, uint64(len(docRaw)), 1)
	docBacking := memtable.COWAllocationCharge(uint64(cap(docDecoded.Entries)) * uint64(unsafe.Sizeof(textV2SearchDocMapEntry{})))
	if err != nil || docCharge < docBacking {
		t.Fatalf("docmap charge=%d backing=%d err=%v", docCharge, docBacking, err)
	}
	block, err := newTextV2PostingBlockValue(textV2PostingBlockKindSealed, postings, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	raw := encodeTextV2PostingBlockValue(block)
	scanner, err := newTextV2PostingBlockEntryScanner(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	postingCharge, err := nativeTextServingRecordCharge(textV2RootFamilyPostingBlocks, false, uint64(len(raw)), 1)
	want := memtable.COWAllocationCharge(uint64(cap(scanner.scratch))*4) + count*memtable.COWAllocationCharge(uint64(unsafe.Sizeof(textV2SearchCandidate{})))
	if err != nil || postingCharge < want {
		t.Fatalf("posting charge=%d backing=%d err=%v", postingCharge, want, err)
	}
	// This is a dimensional witness, not a performance claim: encoded bytes
	// no longer each receive all mutually exclusive decoder descriptor layouts.
	oldUnit := uint64(unsafe.Sizeof(textV2DocMapEntry{})) + uint64(unsafe.Sizeof(textV2NormBlockEntry{})) + uint64(unsafe.Sizeof(textV2PostingBlockEntry{})) + uint64(unsafe.Sizeof(textV2PositionFieldValue{})) + 4*uint64(unsafe.Sizeof([]byte{})) + 16
	old := memtable.COWAllocationCharge((memtable.COWAllocationCharge(uint64(len(raw))) + 1) * 2 * oldUnit)
	if postingCharge+memtable.COWAllocationCharge(uint64(len(raw))) >= old {
		t.Fatalf("posting bound did not remove summed envelope: %d >= %d", postingCharge, old)
	}
	for _, tc := range []struct {
		family textV2RootFamily
		fields uint64
	}{{textV2RootFamilyPostingBlocks, 1}, {textV2RootFamilyNormBlocks, 1}, {textV2RootFamilyDocMap, 1}, {textV2RootFamilyPositions, 1}} {
		if _, err := nativeTextServingRecordCharge(tc.family, false, ^uint64(0)-1, tc.fields); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("overflow family=%v err=%v", tc.family, err)
		}
	}
}

func TestNativeTextServingMalformedPreallocationEnvelope(t *testing.T) {
	// Norm/docmap codecs allow entryCount<=remaining+1 before make. These
	// malformed values reserve descriptors despite lacking complete entries.
	for _, family := range []textV2RootFamily{textV2RootFamilyNormBlocks, textV2RootFamilyDocMap} {
		var raw []byte
		if family == textV2RootFamilyNormBlocks {
			raw = []byte{textV2NormBlockValueVersion}
			raw = appendTextUvarint(raw, 1)
			raw = appendTextUvarint(raw, uint64(textV2DefaultNormBlockSize))
			raw = appendTextUvarint(raw, 1)
			raw = appendTextUvarint(raw, 16)
		} else {
			raw = []byte{textV2DocMapValueVersion}
			raw = appendTextUvarint(raw, 1)
			raw = appendTextUvarint(raw, uint64(textV2DefaultDocMapBlockSize))
			raw = appendTextUvarint(raw, 16)
		}
		raw = append(raw, make([]byte, 16)...)
		charge, err := nativeTextServingRecordCharge(family, false, uint64(len(raw)), 1)
		want := memtable.COWAllocationCharge(16 * uint64(unsafe.Sizeof(textV2SearchDocMapEntry{})))
		if err != nil || charge < want {
			t.Fatalf("malformed family=%v charge=%d want>=%d err=%v", family, charge, want, err)
		}
		if family == textV2RootFamilyNormBlocks {
			if _, err := decodeTextV2SearchNormBlock(raw); err == nil {
				t.Fatal("malformed norm accepted")
			}
		} else if _, err := decodeTextV2SearchDocMapBlock(raw); err == nil {
			t.Fatal("malformed docmap accepted")
		}
	}
	// A tiny posting header can admit maximum field scratch then fail while
	// reading summary lanes. Fixed scratch must be present even at b=1.
	charge, err := nativeTextServingRecordCharge(textV2RootFamilyPostingBlocks, false, 1, 1)
	want := memtable.COWAllocationCharge(3 * uint64(textV2PostingBlockMaxFieldCount) * 4)
	if err != nil || charge < want {
		t.Fatalf("early scanner charge=%d want>=%d err=%v", charge, want, err)
	}
}
