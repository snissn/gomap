package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	vacuumTestCollectionRootKey = "collections/root/users/primary"
	vacuumTestDocumentKey       = "doc/u1"
	vacuumTestDocumentValue     = "document"
)

func TestVacuumCollectCollectionEntriesFromRootWithLimits(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	_, _, err = d.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          mustFrozenSystemMemtable(t, "doc/u1", "document").NewIterator(nil, nil),
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenRawMemtable(t, vacuumTestCollectionRootKey, encodeCollectionRootDescriptorRootID(rootIDs[0])).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing snapshot")
	}
	defer func() { _ = snap.Close() }()
	root := snap.state.SystemRootPageID
	collect := func(entries int, bytes int64, roots int) ([]collectionEntry, error) {
		return vacuumCollectCollectionEntriesFromRootWithLimits(context.Background(), snap.idx.pager, &snap.reader, root, entries, bytes, roots)
	}
	exactBytes := int64(len(vacuumTestCollectionRootKey) + 8)
	got, err := collect(1, exactBytes, 1)
	if err != nil || len(got) != 1 || string(got[0].key) != vacuumTestCollectionRootKey || len(got[0].sourceRootIDs) != 1 {
		t.Fatalf("exact descriptor budget: entries=%v err=%v", got, err)
	}
	for _, tc := range []struct {
		name           string
		entries, roots int
		bytes          int64
	}{
		{name: "entry", entries: 0, bytes: exactBytes, roots: 1},
		{name: "bytes", entries: 1, bytes: exactBytes - 1, roots: 1},
		{name: "root IDs", entries: 1, bytes: exactBytes, roots: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := collect(tc.entries, tc.bytes, tc.roots); !errors.Is(err, ErrCollectionRootDescriptorBudget) {
				t.Fatalf("bounded descriptor scan error=%v, want budget rejection", err)
			}
		})
	}
}

func TestPreparedCollectionRootDescriptorBudgetRejectsPointerBeforeDecode(t *testing.T) {
	dir := t.TempDir()
	d := openVacuumPointerDescriptorFixture(t, vacuumPointerDescriptorOptions(dir))
	defer func() { _ = d.Close() }()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing snapshot")
	}
	defer func() { _ = snap.Close() }()
	root := snap.state.SystemRootPageID
	it, err := snap.IteratorAtRootWithOptions(root, []byte(vacuumTestCollectionRootKey), nil, IteratorOptions{Mode: IteratorModePointerProjection})
	if err != nil || !it.Valid() {
		t.Fatalf("pointer descriptor iterator: %v", err)
	}
	_, _, flags := it.UnsafeEntry()
	if flags&node.FlagPointer == 0 {
		t.Fatalf("descriptor flags=%x, want pointer", flags)
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.CheckPreparedCollectionRootDescriptorBudget(1, 1<<20, 1); !errors.Is(err, ErrCollectionRootDescriptorBudget) {
		t.Fatalf("pointer-backed prepared descriptor error=%v, want budget rejection", err)
	}
}

func TestPreparedCollectionRootDescriptorBudgetRejectsAliases(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = d.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          mustFrozenSystemMemtable(t, "doc/u1", "document").NewIterator(nil, nil),
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		encoded := encodeCollectionRootDescriptorRootID(rootIDs[0])
		return mustFrozenRawMemtable(t,
			"collections/root/users/primary", encoded,
			"collections/root/users/by-email", encoded,
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.CheckPreparedCollectionRootDescriptorBudget(2, 1<<20, 2); !errors.Is(err, ErrCollectionRootDescriptorBudget) {
		t.Fatalf("prepared aliased descriptor error=%v, want budget rejection", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(Options{Dir: dir, CommandWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSet, Key: []byte("witness"), Value: []byte("value"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := d.NewCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
	if err != nil {
		t.Fatal(err)
	}
	before := d.CommandWALNextLSN()
	_, _, err = d.PublishOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder(nil,
		func() error { return d.CheckPreparedCollectionRootDescriptorBudget(2, 1<<20, 2) }, intent,
		func(CommandWALPublishContext, []uint64) (iterator.UnsafeIterator, error) {
			t.Fatal("system builder ran after rejected preflight")
			return nil, nil
		})
	if !errors.Is(err, ErrCollectionRootDescriptorBudget) || intent.AssignedLSN() != 0 || d.CommandWALNextLSN() != before {
		t.Fatalf("aliased preflight error=%v assigned=%d next=%d, want rejected before LSN %d", err, intent.AssignedLSN(), d.CommandWALNextLSN(), before)
	}
	if err := d.commandWALPoisonedError(); err != nil {
		t.Fatalf("pre-WAL rejection poisoned handle: %v", err)
	}
}

func TestVacuumIndexOffline_PreservesCollectionRootFromPointerBackedDescriptor(t *testing.T) {
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

func TestVacuumIndexOnline_PreservesCollectionRootFromPointerBackedDescriptor(t *testing.T) {
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

func TestVacuumRewriteCollectionRootDescriptorsRewritesOverlayLists(t *testing.T) {
	key := []byte("collections/root-overlay/users/primary")
	rootIDs := []uint64{3, 2}
	descriptors := []vacuumCollectionRootDescriptor{
		{key: key, rootID: 3, rootIDs: rootIDs, rootIndex: 0},
		{key: key, rootID: 2, rootIDs: rootIDs, rootIndex: 1},
	}
	replacements, err := vacuumRewriteCollectionRootDescriptors(descriptors, func(descriptor vacuumCollectionRootDescriptor) (uint64, error) {
		return descriptor.rootID + 100, nil
	}, "test")
	if err != nil {
		t.Fatalf("rewrite descriptors: %v", err)
	}
	if len(replacements) != 1 {
		t.Fatalf("replacements=%d want 1", len(replacements))
	}
	if !bytes.Equal(replacements[0].key, key) {
		t.Fatalf("replacement key=%q want %q", replacements[0].key, key)
	}
	got, err := decodeCollectionRootDescriptorRootIDs(key, replacements[0].value, true)
	if err != nil {
		t.Fatalf("decode replacement: %v", err)
	}
	want := []uint64{103, 102}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replacement root ids=%v want %v", got, want)
	}
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
