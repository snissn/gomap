package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

func primaryTestOperand(id uint64, key, value []byte, revision page.EntryRevision, tombstone bool) (PrimaryOperand, []byte) {
	image := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(image, page.PageTypeLeaf, BuilderOptions{EntryRevisions: true})
	b.SetPageID(id)
	flags := byte(0)
	if tombstone {
		flags = FlagTombstone
	}
	if err := b.AddLeafEntryWithRevision(key, value, flags, page.ValuePtr{}, revision); err != nil {
		panic(err)
	}
	b.FinishNoNode()
	return PrimaryOperand{Ref: page.PageChildRef(id), Digest: sha256.Sum256(image)}, image
}
func TestPrimaryDirectoryCanonicalEmptyKeyAndIntegrity(t *testing.T) {
	base, _ := primaryTestOperand(2, []byte("base"), []byte("base-value"), 1, false)
	empty, emptyImage := primaryTestOperand(3, []byte{}, []byte{}, 2, false)
	absent, absentImage := primaryTestOperand(4, []byte("a"), nil, 3, true)
	entries := []PrimaryDirectoryEntry{{Key: []byte{}, Operand: empty, Revision: 2, Kind: PrimaryPut}, {Key: []byte("a"), Operand: absent, Revision: 3, Kind: PrimaryAbsence, ClassSlot: 1}}
	image := make([]byte, page.PageSize)
	if err := EncodePrimaryDirectory(image, 5, 1, base, entries); err != nil {
		t.Fatal(err)
	}
	d, err := DecodePrimaryDirectory(image)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{nil, {}, []byte("a")} {
		if _, found := d.Search(key); !found {
			t.Fatalf("key %q missing", key)
		}
	}
	if _, found := d.Search([]byte("aa")); found {
		t.Fatal("prefix incorrectly matches")
	}
	if err := ValidatePrimaryComponent(entries[0], emptyImage); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePrimaryComponent(entries[1], absentImage); err != nil {
		t.Fatal(err)
	}
	changed := entries[1]
	changed.Revision++
	if !errors.Is(ValidatePrimaryComponent(changed, absentImage), ErrPrimaryDirectory) {
		t.Fatal("revision substitution accepted")
	}
	changed = entries[1]
	changed.Kind = PrimaryPut
	if !errors.Is(ValidatePrimaryComponent(changed, absentImage), ErrPrimaryDirectory) {
		t.Fatal("class substitution accepted")
	}
	for _, offset := range []int{16, 24, 28, 99, PrimaryDirectoryRecordsOffset + 53, page.PageSize - 1} {
		bad := append([]byte(nil), image...)
		bad[offset] ^= 1
		page.UpdateChecksum(bad)
		if _, err := DecodePrimaryDirectory(bad); !errors.Is(err, ErrPrimaryDirectory) {
			t.Fatalf("CRC-resealed malformed offset %d accepted", offset)
		}
	}
	bad := append([]byte(nil), image...)
	binary.LittleEndian.PutUint16(bad[PrimaryDirectoryRecordsOffset+49:], 0)
	page.UpdateChecksum(bad)
	if _, err := DecodePrimaryDirectory(bad); !errors.Is(err, ErrPrimaryDirectory) {
		t.Fatal("noncanonical heap accepted")
	}
}

func TestPrimaryDirectorySortedInsertionPreservesClassesAndRefusal(t *testing.T) {
	base, _ := primaryTestOperand(2, []byte("base"), []byte("value"), 1, false)
	entries := make([]PrimaryDirectoryEntry, 2)
	for i, key := range []string{"b", "d"} {
		op, _ := primaryTestOperand(uint64(10+i), []byte(key), []byte("value"), 2, false)
		entries[i] = PrimaryDirectoryEntry{Key: []byte(key), Operand: op, Revision: 2, Kind: PrimaryPut, ClassSlot: uint16(i)}
	}
	image := make([]byte, page.PageSize)
	if e := EncodePrimaryDirectory(image, 30, 1, base, entries); e != nil {
		t.Fatal(e)
	}
	view, e := DecodePrimaryDirectory(image)
	if e != nil {
		t.Fatal(e)
	}
	for _, key := range []string{"", "a", "c", "z"} {
		before := bytes.Clone(image)
		count := view.Count()
		op, _ := primaryTestOperand(uint64(40+count), []byte(key), nil, 3, true)
		entry := PrimaryDirectoryEntry{Key: []byte(key), Operand: op, Revision: 3, Kind: PrimaryAbsence, ClassSlot: uint16(count)}
		dst := bytes.Repeat([]byte{0xac}, page.PageSize)
		unchanged := bytes.Clone(dst)
		if _, ready, e := view.InsertPrimaryComponentWithVisit(dst, 60, entry, func(PrimaryDirectoryEntry) bool { return false }); e != nil || ready || !bytes.Equal(dst, unchanged) {
			t.Fatalf("refusal %q: ready=%v error=%v mutated=%v", key, ready, e, !bytes.Equal(dst, unchanged))
		}
		probes := 0
		next, ready, e := view.InsertPrimaryComponentWithVisit(dst, uint64(60+count), entry, func(PrimaryDirectoryEntry) bool { probes++; return true })
		if e != nil || !ready || probes == 0 {
			t.Fatalf("insert %q: ready=%v error=%v probes=%d", key, ready, e, probes)
		}
		decoded, e := DecodePrimaryDirectory(dst)
		if e != nil {
			t.Fatal(e)
		}
		if decoded.Count() != count+1 || next.Count() != decoded.Count() || !bytes.Equal(image, before) {
			t.Fatal("source/certificate changed")
		}
		oldEnd := PrimaryDirectoryRecordsOffset + count*PrimaryDirectoryEntrySize
		if !bytes.Equal(before[PrimaryDirectoryRecordsOffset:oldEnd], dst[PrimaryDirectoryRecordsOffset:oldEnd]) || !bytes.Equal(before[PrimaryDirectoryHeapOffset:view.heapEnd], dst[PrimaryDirectoryHeapOffset:view.heapEnd]) {
			t.Fatalf("insert %q relocated existing class/heap", key)
		}
		for i := 0; i < count; i++ {
			old, _ := view.ClassEntry(uint16(i))
			current, _ := decoded.ClassEntry(uint16(i))
			if !bytes.Equal(old.Key, current.Key) || old.Operand != current.Operand || old.Revision != current.Revision || old.Kind != current.Kind {
				t.Fatalf("class %d changed", i)
			}
		}
		got, found := decoded.Search([]byte(key))
		if !found || got.ClassSlot != uint16(count) || got.Kind != PrimaryAbsence {
			t.Fatalf("missing exact inserted %q", key)
		}
		image = dst
		view = decoded
	}
	failure := func(entry PrimaryDirectoryEntry, want error) {
		t.Helper()
		dst := bytes.Repeat([]byte{0xbc}, page.PageSize)
		before := bytes.Clone(dst)
		if _, ready, e := view.InsertPrimaryComponentWithVisit(dst, 90, entry, nil); ready || !errors.Is(e, want) || !bytes.Equal(dst, before) {
			t.Fatalf("failed insert ready=%v error=%v mutation=%v", ready, e, !bytes.Equal(dst, before))
		}
	}
	duplicate, _ := view.Search([]byte("b"))
	duplicate.ClassSlot = uint16(view.Count())
	failure(duplicate, ErrPrimaryDirectory)
	tooLarge := duplicate
	tooLarge.Key = bytes.Repeat([]byte{'x'}, page.PageSize)
	failure(tooLarge, ErrPrimaryDirectoryFull)
	entries = make([]PrimaryDirectoryEntry, PrimaryDirectoryMaxEntries)
	for i := range entries {
		entries[i] = duplicate
		entries[i].Key = []byte(fmt.Sprintf("k%02d", i))
		entries[i].ClassSlot = uint16(i)
	}
	full := make([]byte, page.PageSize)
	if e := EncodePrimaryDirectory(full, 95, 1, base, entries); e != nil {
		t.Fatal(e)
	}
	view, e = DecodePrimaryDirectory(full)
	if e != nil {
		t.Fatal(e)
	}
	duplicate.Key = []byte("after")
	duplicate.ClassSlot = PrimaryDirectoryMaxEntries
	failure(duplicate, ErrPrimaryDirectoryFull)
}

func TestPrimaryDirectoryInlineAbsenceIsCanonical(t *testing.T) {
	image := make([]byte, page.PageSize)
	base := PrimaryOperand{Ref: page.PageChildRef(2), Digest: [32]byte{1}}
	entries := []PrimaryDirectoryEntry{{Key: []byte{}, Revision: 3, Kind: PrimaryAbsence}, {Key: []byte("z"), Revision: 4, Kind: PrimaryAbsence, ClassSlot: 1}}
	if e := EncodePrimaryDirectory(image, 3, 1, base, entries); e != nil {
		t.Fatal(e)
	}
	view, e := DecodePrimaryDirectory(image)
	if e != nil {
		t.Fatal(e)
	}
	for _, key := range [][]byte{nil, []byte("z")} {
		entry, ok := view.Search(key)
		if !ok || !entry.InlineAbsence() {
			t.Fatal("inline absence acquired a locator")
		}
	}
	// A null put and partially null absence are noncanonical, even after CRC repair.
	for _, change := range []func([]byte){func(b []byte) { b[PrimaryDirectoryRecordsOffset+48] = byte(PrimaryPut) }, func(b []byte) { b[PrimaryDirectoryRecordsOffset+8] = 1 }} {
		bad := append([]byte(nil), image...)
		change(bad)
		page.UpdateChecksum(bad)
		if _, e := DecodePrimaryDirectory(bad); e == nil {
			t.Fatal("malformed inline absence accepted")
		}
	}
}
