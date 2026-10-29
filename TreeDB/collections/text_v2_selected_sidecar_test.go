package collections

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"
)

func selectedSidecarOrdinals(ordinals ...uint64) *textV2SearchOrdinalAllowSet {
	s := &textV2SearchOrdinalAllowSet{ordinals: make(map[uint64]textV2SearchForwardIdentity)}
	for _, ordinal := range ordinals {
		s.ordinals[ordinal] = textV2SearchForwardIdentity{}
		s.sorted = append(s.sorted, ordinal)
	}
	slices.Sort(s.sorted)
	s.count = len(s.sorted)
	return s
}

func TestTextV2SelectedSidecarRetention(t *testing.T) {
	norm := textV2NormBlockValue{BlockStart: 1, BlockSize: 128, FieldCount: 2}
	docmap := textV2DocMapBlockValue{BlockStart: 1, BlockSize: 128}
	var ordinals []uint64
	for ordinal := uint64(1); ordinal <= 4096; ordinal++ {
		if ordinal%16 == 1 {
			ordinals = append(ordinals, ordinal)
		}
		if ordinal > 128 {
			continue
		}
		flags := byte(0)
		if ordinal == 2 {
			flags = textV2DocFlagTombstone
		}
		norm.Entries = append(norm.Entries, textV2NormBlockEntry{Ordinal: ordinal, Generation: 1, Flags: flags, FieldLengths: []uint32{3, 7}})
		docmap.Entries = append(docmap.Entries, textV2DocMapEntry{Ordinal: ordinal, Generation: 1, Flags: flags, DocumentID: []byte(fmt.Sprintf("d-%04d", ordinal))})
	}
	normRaw, docRaw := encodeTextV2NormBlockValue(norm), encodeTextV2DocMapBlockValue(docmap)
	strict := selectedSidecarOrdinals(ordinals...)
	all := selectedSidecarOrdinals(1)
	all.all = true // Includes historical ordinals absent from the explicit map.
	for _, tc := range []struct {
		name string
		set  *textV2SearchOrdinalAllowSet
		want int
	}{{"nil", nil, 128}, {"all", all, 128}, {"strict", strict, 8}, {"empty", selectedSidecarOrdinals(), 0}, {"other_block", selectedSidecarOrdinals(129), 0}} {
		t.Run(tc.name, func(t *testing.T) {
			n, err := decodeTextV2SearchNormBlockSelected(normRaw, tc.set)
			if err != nil {
				t.Fatal(err)
			}
			d, err := decodeTextV2SearchDocMapBlockSelected(docRaw, tc.set)
			if err != nil {
				t.Fatal(err)
			}
			if len(n.Entries) != tc.want || cap(n.Entries) != tc.want || len(n.FieldLengths) != 2*tc.want || cap(n.FieldLengths) != 2*tc.want || len(d.Entries) != tc.want || cap(d.Entries) != tc.want {
				t.Fatalf("retained entries/fields: norm=%d/%d fields=%d/%d doc=%d/%d want%d", len(n.Entries), cap(n.Entries), len(n.FieldLengths), cap(n.FieldLengths), len(d.Entries), cap(d.Entries), tc.want)
			}
			at := 0
			for i, entry := range norm.Entries {
				if !tc.set.contains(entry.Ordinal) {
					continue
				}
				got, doc := n.Entries[at], d.Entries[at]
				if got.Ordinal != entry.Ordinal || got.Generation != entry.Generation || got.Flags != entry.Flags || !reflect.DeepEqual(got.FieldLengths, entry.FieldLengths) || doc.Ordinal != entry.Ordinal || doc.Flags != entry.Flags || !bytes.Equal(doc.DocumentID, docmap.Entries[i].DocumentID) {
					t.Fatalf("selected record changed at%d: norm=%+v doc=%+v", at, got, doc)
				}
				at++
			}
		})
	}
	// Retained doc IDs borrow the original complete raw record. Selection does
	// not permit refunding or dropping that source backing early.
	d, err := decodeTextV2SearchDocMapBlockSelected(docRaw, strict)
	if err != nil {
		t.Fatal(err)
	}
	at := bytes.Index(docRaw, []byte("d-0001"))
	docRaw[at] = 'X'
	if d.Entries[0].DocumentID[0] != 'X' {
		t.Fatal("selected ID lost original source ownership")
	}
}

func TestTextV2SelectedSidecarValidatesExcludedEntries(t *testing.T) {
	selected := selectedSidecarOrdinals(1)
	norm := textV2NormBlockValue{BlockStart: 1, BlockSize: 128, FieldCount: 1, Entries: []textV2NormBlockEntry{
		{Ordinal: 1, Generation: 1, FieldLengths: []uint32{3}}, {Ordinal: 2, Generation: 1, FieldLengths: []uint32{7}},
	}}
	docmap := textV2DocMapBlockValue{BlockStart: 1, BlockSize: 128, Entries: []textV2DocMapEntry{
		{Ordinal: 1, Generation: 1, DocumentID: []byte("one")}, {Ordinal: 2, Generation: 1, DocumentID: []byte("two")},
	}}
	normCases, docCases := map[string][]byte{}, map[string][]byte{}
	for _, kind := range []string{"zero_generation", "flags", "outside", "duplicate"} {
		n, d := norm, docmap
		n.Entries, d.Entries = slices.Clone(norm.Entries), slices.Clone(docmap.Entries)
		switch kind {
		case "zero_generation":
			n.Entries[1].Generation, d.Entries[1].Generation = 0, 0
		case "flags":
			n.Entries[1].Flags, d.Entries[1].Flags = 2, 2
		case "outside":
			n.Entries[1].Ordinal, d.Entries[1].Ordinal = 129, 129
		case "duplicate":
			n.Entries[1].Ordinal, d.Entries[1].Ordinal = 1, 1
		}
		normCases[kind], docCases[kind] = encodeTextV2NormBlockValue(n), encodeTextV2DocMapBlockValue(d)
	}
	n, d := encodeTextV2NormBlockValue(norm), encodeTextV2DocMapBlockValue(docmap)
	normCases["truncated"], docCases["truncated"] = n[:len(n)-1], d[:len(d)-1]
	normCases["trailing"], docCases["trailing"] = append(slices.Clone(n), 0), append(slices.Clone(d), 0)
	normCases["field_overflow"] = appendTextUvarint(slices.Clone(n[:len(n)-1]), uint64(math.MaxUint32)+1)
	docmap.Entries[1].DocumentID = nil
	docCases["empty_id"] = encodeTextV2DocMapBlockValue(docmap)
	for name, raw := range normCases {
		t.Run("norm_"+name, func(t *testing.T) {
			_, full := decodeTextV2SearchNormBlock(raw)
			_, sparse := decodeTextV2SearchNormBlockSelected(raw, selected)
			if full == nil || sparse == nil || full.Error() != sparse.Error() {
				t.Fatalf("excluded corruption changed: full=%v sparse=%v", full, sparse)
			}
		})
	}
	for name, raw := range docCases {
		t.Run("doc_"+name, func(t *testing.T) {
			_, full := decodeTextV2SearchDocMapBlock(raw)
			_, sparse := decodeTextV2SearchDocMapBlockSelected(raw, selected)
			if full == nil || sparse == nil || full.Error() != sparse.Error() {
				t.Fatalf("excluded corruption changed: full=%v sparse=%v", full, sparse)
			}
		})
	}
	// A missing selected ordinal remains absent, never a synthetic tombstone.
	docmap.Entries = docmap.Entries[:1]
	gap, err := decodeTextV2SearchDocMapBlockSelected(encodeTextV2DocMapBlockValue(docmap), selectedSidecarOrdinals(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, found := gap.find(2); found {
		t.Fatal("missing selected ordinal was manufactured")
	}
}

func TestTextV2SelectedSidecarSparseDifferential(t *testing.T) {
	// Sparse source ordinals and allow-set gaps exercise both sides of the
	// ordered merge. The final block also exercises wide varints near uint64's
	// upper bound without changing the maintained overflow refusal.
	for _, start := range []uint64{1, 129, 1 << 32, math.MaxUint64 - 256} {
		norm := textV2NormBlockValue{BlockStart: start, BlockSize: 128, FieldCount: 2}
		docmap := textV2DocMapBlockValue{BlockStart: start, BlockSize: 128}
		var allowed []uint64
		for offset := uint64(0); offset < 128; offset++ {
			ordinal := start + offset
			if offset%5 == 0 || offset%11 == 0 {
				allowed = append(allowed, ordinal)
			}
			if offset%3 == 1 {
				continue
			}
			flags := byte(0)
			if offset%7 == 0 {
				flags = textV2DocFlagTombstone
			}
			norm.Entries = append(norm.Entries, textV2NormBlockEntry{Ordinal: ordinal, Generation: 1 + offset, Flags: flags, FieldLengths: []uint32{uint32(offset), uint32(offset * 2)}})
			docmap.Entries = append(docmap.Entries, textV2DocMapEntry{Ordinal: ordinal, Generation: 1 + offset, Flags: flags, DocumentID: []byte(fmt.Sprintf("doc-%d", ordinal))})
		}
		// Include a different block and absent source ordinals. Neither can
		// manufacture records or disturb the next block's immutable selection.
		allowed = append(allowed, start+128)
		set := selectedSidecarOrdinals(allowed...)
		before := slices.Clone(set.sorted)
		nraw, draw := encodeTextV2NormBlockValue(norm), encodeTextV2DocMapBlockValue(docmap)
		n, err := decodeTextV2SearchNormBlockSelected(nraw, set)
		if err != nil {
			t.Fatal(err)
		}
		d, err := decodeTextV2SearchDocMapBlockSelected(draw, set)
		if err != nil {
			t.Fatal(err)
		}
		var wantN []textV2SearchNormEntry
		var wantD []textV2SearchDocMapEntry
		for i, entry := range norm.Entries {
			if _, ok := set.ordinals[entry.Ordinal]; !ok {
				continue
			}
			wantN = append(wantN, textV2SearchNormEntry{Ordinal: entry.Ordinal, Generation: entry.Generation, Flags: entry.Flags, FieldLengths: entry.FieldLengths})
			doc := docmap.Entries[i]
			wantD = append(wantD, textV2SearchDocMapEntry{Ordinal: doc.Ordinal, Generation: doc.Generation, Flags: doc.Flags, DocumentID: doc.DocumentID})
		}
		if !reflect.DeepEqual(n.Entries, wantN) || !reflect.DeepEqual(d.Entries, wantD) || !slices.Equal(set.sorted, before) {
			t.Fatalf("sparse selected decode differs at block %d", start)
		}
		// Overflowing blocks retain the original full decoder's rejection;
		// selection must not turn excluded malformed entries into success.
		norm.BlockStart = math.MaxUint64 - 1
		norm.Entries = []textV2NormBlockEntry{{Ordinal: math.MaxUint64 - 1, Generation: 1, FieldLengths: []uint32{1, 1}}}
		raw := encodeTextV2NormBlockValue(norm)
		_, fullErr := decodeTextV2SearchNormBlock(raw)
		_, selectedErr := decodeTextV2SearchNormBlockSelected(raw, set)
		if fullErr == nil || selectedErr == nil || fullErr.Error() != selectedErr.Error() {
			t.Fatalf("overflow validation changed: full=%v selected=%v", fullErr, selectedErr)
		}
	}
}
