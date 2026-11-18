package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func (idx *leafGenerationRecordLengthIndex) len() int {
	if idx == nil {
		return 0
	}
	return len(idx.offsets)
}

func (idx *leafGenerationRecordLengthIndex) lookup(offset uint64) (uint32, bool) {
	if idx == nil {
		return 0, false
	}
	i := sort.Search(len(idx.offsets), func(i int) bool {
		return uint64(idx.offsets[i]) >= offset
	})
	if i == len(idx.offsets) || uint64(idx.offsets[i]) != offset {
		return 0, false
	}
	return idx.lengths[i], true
}

func TestLeafGenerationRecordLengthIndex_NoteRawIsCopyOnWrite(t *testing.T) {
	db := &DB{}
	seed := &leafGenerationRecordLengthIndex{
		offsets: []uint32{4},
		lengths: []uint32{96},
	}
	db.storeLeafGenerationRecordLengthIndex(7, seed)

	before, ok := db.loadLeafGenerationRecordLengthIndex(7)
	if !ok || before == nil {
		t.Fatal("expected cached record-length index")
	}
	db.noteLeafGenerationRecordLengthRaw(7, 128, 144)

	after, ok := db.loadLeafGenerationRecordLengthIndex(7)
	if !ok || after == nil {
		t.Fatal("expected updated cached record-length index")
	}
	if got, ok := before.lookup(128); ok || got != 0 {
		t.Fatalf("stale reader lookup(128)=(%d,%v), want (0,false)", got, ok)
	}
	if got, ok := after.lookup(128); !ok || got != 144 {
		t.Fatalf("updated reader lookup(128)=(%d,%v), want (144,true)", got, ok)
	}
}

func TestLeafGenerationRecordLengthIndex_NoteRawAllowsOffsetZero(t *testing.T) {
	db := &DB{}
	db.noteLeafGenerationRecordLengthRaw(9, 0, 64)
	idx, ok := db.loadLeafGenerationRecordLengthIndex(9)
	if !ok || idx == nil {
		t.Fatal("expected record-length index for offset-zero entry")
	}
	if got, ok := idx.lookup(0); !ok || got != 64 {
		t.Fatalf("lookup(0)=(%d,%v), want (64,true)", got, ok)
	}
}

func TestLeafGenerationRecordLengthIndex_EmptyFilePublication(t *testing.T) {
	dir := t.TempDir()
	const rawID = 7
	db := &DB{dir: dir}
	source := leafGenerationFallbackPath(dir, rawID)
	if err := os.MkdirAll(filepath.Dir(source), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, nil, 0600); err != nil {
		t.Fatal(err)
	}
	path := leafGenerationRecordLengthIndexPath(dir, rawID)
	syncs := 0
	defer durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeDependencyFileSync && strings.HasPrefix(e.Path, path+".tmp.") {
			syncs++
		}
		return nil
	})()
	save := func(wantSyncs int) {
		t.Helper()
		syncs = 0
		sourceErr, indexErr := db.persistLeafGenerationRecordLengthIndex(rawID)
		if sourceErr != nil || indexErr != nil || syncs != wantSyncs {
			t.Errorf("persist: source=%v index=%v file syncs=%d, want %d", sourceErr, indexErr, syncs, wantSyncs)
		}
	}
	save(0)
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("fresh empty sidecar: Lstat error = %v, want absent", err)
	}
	db.noteLeafGenerationRecordLengthRaw(rawID, 4, 128)
	save(1)
	idx, found, err := loadLeafGenerationRecordLengthIndexFile(path, rawID)
	if got, ok := idx.lookup(4); err != nil || !found || !ok || got != 128 {
		t.Fatalf("nonempty sidecar: length=%d found=%t err=%v", got, found, err)
	}
	// An existing sidecar must be replaced even when a later table is empty.
	db.storeLeafGenerationRecordLengthIndex(rawID, &leafGenerationRecordLengthIndex{})
	save(1)
	idx, found, err = loadLeafGenerationRecordLengthIndexFile(path, rawID)
	if err != nil || !found || idx.len() != 0 {
		t.Fatalf("replacement sidecar: entries=%d found=%t err=%v", idx.len(), found, err)
	}
}

func TestLeafGenerationRecordLengthIndex_EmptyFileErrors(t *testing.T) {
	dir := t.TempDir()
	if err := saveLeafGenerationRecordLengthIndexFile(filepath.Join(dir, "missing", "index"), 7, nil); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("missing parent: %v", err)
	}
	if err := saveLeafGenerationRecordLengthIndexFile(filepath.Join(dir, "index"), 7, &leafGenerationRecordLengthIndex{lengths: []uint32{32}}); err == nil {
		t.Error("accepted a malformed zero-offset table")
	}
}

func TestLeafGenerationRecordLengthIndex_EmptyReplacesDanglingSymlink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index")
	target := filepath.Join(dir, "missing")
	if err := os.Symlink(target, path); err != nil {
		t.Skipf("cannot create a symlink: %v", err)
	}
	if err := saveLeafGenerationRecordLengthIndexFile(path, 7, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement followed symlink: %v", err)
	}
	idx, found, err := loadLeafGenerationRecordLengthIndexFile(path, 7)
	if err != nil || !found || idx.len() != 0 {
		t.Fatalf("replacement sidecar: entries=%d found=%t err=%v", idx.len(), found, err)
	}
}

// Decode persisted hints only to verify writer output in compatibility tests.
func loadLeafGenerationRecordLengthIndexFile(path string, rawFileID uint32) (*leafGenerationRecordLengthIndex, bool, error) {
	if path == "" || rawFileID == 0 {
		return nil, false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer func() { _ = f.Close() }()
	const headerSize = 16
	var header [headerSize]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, false, fmt.Errorf("leaf generation record-length index: decode %q: short header", path)
		}
		return nil, false, err
	}
	if string(header[:4]) != leafGenerationRecordLengthIndexMagic {
		return nil, false, fmt.Errorf("leaf generation record-length index: decode %q: bad magic", path)
	}
	version := binary.LittleEndian.Uint32(header[4:8])
	if version != leafGenerationRecordLengthIndexVersion {
		return nil, false, fmt.Errorf("leaf generation record-length index: decode %q: unsupported version %d", path, version)
	}
	gotRawFileID := binary.LittleEndian.Uint32(header[8:12])
	if gotRawFileID != rawFileID {
		return nil, false, fmt.Errorf("leaf generation record-length index: decode %q: raw file id %d != %d", path, gotRawFileID, rawFileID)
	}
	count := int(binary.LittleEndian.Uint32(header[12:16]))
	if count < 0 {
		return nil, false, fmt.Errorf("leaf generation record-length index: decode %q: invalid count %d", path, count)
	}
	if info, err := f.Stat(); err == nil {
		if info.Size() != int64(headerSize+count*8) {
			return nil, false, fmt.Errorf("leaf generation record-length index: decode %q: size mismatch", path)
		}
	}
	idx := &leafGenerationRecordLengthIndex{
		offsets: make([]uint32, count),
		lengths: make([]uint32, count),
	}
	if count == 0 {
		return idx, true, nil
	}
	entriesPerChunk := leafGenerationRecordLengthIndexChunkBytes / 8
	if entriesPerChunk <= 0 {
		entriesPerChunk = 1
	}
	buf := make([]byte, entriesPerChunk*8)
	prev := uint32(0)
	for out := 0; out < count; {
		chunkEntries := count - out
		if chunkEntries > entriesPerChunk {
			chunkEntries = entriesPerChunk
		}
		chunk := buf[:chunkEntries*8]
		if _, err := io.ReadFull(f, chunk); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, false, fmt.Errorf("leaf generation record-length index: decode %q: size mismatch", path)
			}
			return nil, false, err
		}
		for i := 0; i < chunkEntries; i++ {
			base := i * 8
			offset := binary.LittleEndian.Uint32(chunk[base : base+4])
			length := binary.LittleEndian.Uint32(chunk[base+4 : base+8])
			if length == 0 {
				return nil, false, fmt.Errorf("leaf generation record-length index: decode %q: zero length at entry %d", path, out+i)
			}
			if out+i > 0 && offset <= prev {
				return nil, false, fmt.Errorf("leaf generation record-length index: decode %q: offsets not strictly increasing", path)
			}
			prev = offset
			idx.offsets[out+i] = offset
			idx.lengths[out+i] = length
		}
		out += chunkEntries
	}
	return idx, true, nil
}
