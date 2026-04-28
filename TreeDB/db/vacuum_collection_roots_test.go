package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	vacuumTestCollectionRootKey = "collections/root/users/primary"
	vacuumTestDocumentKey       = "doc/u1"
	vacuumTestDocumentValue     = "document"
)

func TestVacuumIndexOffline_PreservesPointerBackedCollectionRootDescriptor(t *testing.T) {
	dir := t.TempDir()
	opts := vacuumPointerDescriptorOptions(dir)
	d := openVacuumPointerDescriptorFixture(t, opts)
	assertVacuumCollectionRootDescriptorPointerBacked(t, d, vacuumTestCollectionRootKey)
	verifyVacuumCollectionRootDescriptor(t, d, vacuumTestCollectionRootKey)
	if err := d.Close(); err != nil {
		t.Fatalf("close before vacuum: %v", err)
	}

	if err := VacuumIndexOffline(opts); err != nil {
		t.Fatalf("vacuum offline: %v", err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen after vacuum: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	verifyVacuumCollectionRootDescriptor(t, reopened, vacuumTestCollectionRootKey)
}

func TestVacuumIndexOnline_PreservesPointerBackedCollectionRootDescriptor(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}

	dir := t.TempDir()
	opts := vacuumPointerDescriptorOptions(dir)
	d := openVacuumPointerDescriptorFixture(t, opts)
	defer func() { _ = d.Close() }()
	assertVacuumCollectionRootDescriptorPointerBacked(t, d, vacuumTestCollectionRootKey)
	verifyVacuumCollectionRootDescriptor(t, d, vacuumTestCollectionRootKey)

	if err := d.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("vacuum online: %v", err)
	}
	verifyVacuumCollectionRootDescriptor(t, d, vacuumTestCollectionRootKey)
}

func vacuumPointerDescriptorOptions(dir string) Options {
	return Options{
		Dir:        dir,
		ChunkSize:  64 << 10,
		KeepRecent: 1,
		ValueLog: ValueLogOptions{
			ForcePointers:    true,
			PointerThreshold: 1,
		},
	}
}

func openVacuumPointerDescriptorFixture(t *testing.T, opts Options) *DB {
	t.Helper()
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("open fixture db: %v", err)
	}

	_, rootIDs, err := d.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          mustFrozenSystemMemtable(t, vacuumTestDocumentKey, vacuumTestDocumentValue).NewIterator(nil, nil),
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		ptr := appendCollectionRootDescriptorPointer(t, opts.Dir, rootIDs[0])
		return mustFrozenSystemPointerMemtable(t, vacuumTestCollectionRootKey, ptr).NewIterator(nil, nil), nil
	})
	if err != nil {
		_ = d.Close()
		t.Fatalf("publish fixture collection root: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		_ = d.Close()
		t.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("checkpoint fixture db: %v", err)
	}
	return d
}

func appendCollectionRootDescriptorPointer(t *testing.T, dir string, rootID uint64) page.ValuePtr {
	t.Helper()
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], rootID)
	return appendPointersInNewSegment(t, dir, 0, 91, 910_000, 1, func(int) []byte {
		return encoded[:]
	})[0]
}

func assertVacuumCollectionRootDescriptorPointerBacked(t *testing.T, d *DB, key string) {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil || snap.state == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	it, err := snap.IteratorAtRootWithOptions(snap.state.SystemRootPageID, []byte(key), nil, IteratorOptions{
		Mode: IteratorModePointerProjection,
	})
	if err != nil {
		t.Fatalf("descriptor iterator: %v", err)
	}
	defer func() { _ = it.Close() }()
	if !it.Valid() || !bytes.Equal(it.UnsafeKey(), []byte(key)) {
		t.Fatalf("descriptor %q not found", key)
	}
	_, _, flags := it.UnsafeEntry()
	if flags&node.FlagPointer == 0 {
		t.Fatalf("descriptor %q was not pointer-backed; flags=%#x", key, flags)
	}
}

func verifyVacuumCollectionRootDescriptor(t *testing.T, d *DB, key string) {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil || snap.state == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	descriptorValue, err := snap.GetAtRoot(snap.state.SystemRootPageID, []byte(key))
	if err != nil {
		t.Fatalf("read descriptor %q: %v", key, err)
	}
	if len(descriptorValue) != 8 {
		t.Fatalf("descriptor %q value length=%d want 8", key, len(descriptorValue))
	}
	rootID := binary.BigEndian.Uint64(descriptorValue)
	if rootID == 0 {
		t.Fatalf("descriptor %q has zero root", key)
	}
	entry, err := snap.GetEntryAtRoot(rootID, []byte(vacuumTestDocumentKey))
	if err != nil {
		t.Fatalf("read collection root document: %v", err)
	}
	if got := string(entry.Value); got != vacuumTestDocumentValue {
		t.Fatalf("document value=%q want %q", got, vacuumTestDocumentValue)
	}
}
