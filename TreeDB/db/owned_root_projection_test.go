package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

type projectionTestCredit struct {
	scopedPointTestCredit
	refusal error
}

func (a *projectionTestCredit) ReserveStableMetadata(n uint64) error {
	if a.refusal != nil {
		return a.refusal
	}
	return a.scopedPointTestCredit.ReserveStableMetadata(n)
}

func TestOwnedRootProjectionRefusalPrecedesReadAndKeepsSnapshot(t *testing.T) {
	database := openOrderedRootSpanNativeTestDB(t, t.TempDir(), false, 0)
	defer database.Close()
	delta := newOrderedRootSpanNativeBatch(t, 128, "projection-refusal")
	root := publishOrderedRootSpanNativeBatch(t, database, 0, delta, OrderedRootStoragePagerLeaves)
	delta.Close()
	snap := database.AcquireSnapshot()
	defer snap.Close()
	before := snap.readState.Load()
	refusal := errors.New("exact projection credit refused")
	credit := &projectionTestCredit{refusal: refusal}
	records, err := snap.ReadOwnedInlineRecords(root, iterator.InlineRecordSelection{Prefixes: [8][]byte{[]byte("key-")}}, credit)
	if !errors.Is(err, refusal) || records != nil {
		t.Fatalf("denied construction exposed result: records=%d err=%v", len(records), err)
	}
	if snap.readState.Load() != before || snap.finalized.Load() || credit.refs != 0 || credit.bytes != 0 || len(snap.rootTrees) != 0 {
		t.Fatal("refused constructor changed captured lifetime/cache/debit")
	}
	// A failed owned read must leave the original exact root usable.
	entry, err := snap.GetEntryAtRoot(root, []byte("key-000030"))
	if err != nil || len(entry.Value) == 0 {
		t.Fatalf("refusal damaged original root: %v", err)
	}
}

func TestOwnedRootProjectionSameCapturedRootWithoutGenericCaches(t *testing.T) {
	for _, outer := range []bool{false, true} {
		t.Run(map[bool]string{false: "pager", true: "external-lz4"}[outer], func(t *testing.T) {
			dir := t.TempDir()
			database := openOrderedRootSpanNativeTestDB(t, dir, outer, 1)
			defer database.Close()
			policy := OrderedRootStoragePagerLeaves
			if outer {
				log, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionBlock, BlockCodec: ValueLogBlockLZ4})
				if err != nil {
					t.Fatal(err)
				}
				database.SetLeafPageLog(log)
				policy = OrderedRootStorageValueLogLeaves
			}
			delta := newOrderedRootSpanNativeBatch(t, 1024, "projection-owned")
			root := publishOrderedRootSpanNativeBatch(t, database, 0, delta, policy)
			delta.Close()
			snap := database.AcquireSnapshot()
			defer snap.Close()
			// Disable the captured generic resolver. An external-root read can only
			// succeed through the same exact File/pin + C13 native block decoder.
			snap.reader.vlogs = nil
			before := snap.readState.Load()
			credit := &projectionTestCredit{}
			records, err := snap.ReadOwnedInlineRecords(root, iterator.InlineRecordSelection{Prefixes: [8][]byte{[]byte("key-")}}, credit)
			if err != nil {
				t.Fatalf("same bounded decoder failed: %v", err)
			}
			if len(records) != 1024 || cap(records) != 1024 {
				t.Fatalf("copied records=%d cap=%d", len(records), cap(records))
			}
			if snap.readState.Load() != before || credit.refs != 0 || credit.bytes == 0 || len(snap.rootTrees) != 0 || len(snap.iterators) != 0 {
				t.Fatal("synchronous projection retained a reader/cache/creator")
			}
			for _, record := range records {
				if len(record.Key) == 0 || len(record.Value) == 0 {
					t.Fatal("owned copy lost inline bytes")
				}
			}
			// Only caller-owned bytes survived: moving another reader cannot alter the
			// copies, and mutating a copy cannot modify the actual captured root.
			key := string(records[30].Key)
			expected := string(records[30].Value)
			records[30].Value[0] ^= 0xff
			if !outer {
				entry, readErr := snap.GetEntryAtRoot(root, []byte(key))
				if readErr != nil || string(entry.Value) != expected {
					t.Fatalf("copy aliased pager backing: %v", readErr)
				}
			}
			iterator.ClearOwnedInlineRecords(records)
			// Restore ordinary ownership before ordinary Snapshot terminal cleanup.
			snap.reader.vlogs = snap.state.ValueLogSet
		})
	}
}
