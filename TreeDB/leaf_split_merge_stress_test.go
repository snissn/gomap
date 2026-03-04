package treedb

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
)

func be8Key(x uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], x)
	return buf[:]
}

func versionedValue(version uint32, key uint64, totalBytes int) []byte {
	if totalBytes < 16 {
		totalBytes = 16
	}
	v := make([]byte, totalBytes)
	binary.BigEndian.PutUint32(v[:4], version)
	binary.BigEndian.PutUint64(v[4:12], key)
	// Remaining bytes are deterministic filler so corruption is detectable.
	for i := 12; i < len(v); i++ {
		v[i] = byte(key>>uint((i-12)%8*8)) ^ byte(version)
	}
	return v
}

func decodeVersionedValue(t *testing.T, v []byte) (version uint32, key uint64) {
	t.Helper()
	if len(v) < 12 {
		t.Fatalf("value too small: %dB", len(v))
	}
	version = binary.BigEndian.Uint32(v[:4])
	key = binary.BigEndian.Uint64(v[4:12])
	return version, key
}

func verifyIteratorMatchesModel(t *testing.T, db *DB, model map[uint64]uint32) {
	t.Helper()

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	defer func() { _ = it.Close() }()

	var (
		prevKey uint64
		hasPrev bool
		seen    int
	)
	for it.Valid() {
		k := it.Key()
		if len(k) != 8 {
			t.Fatalf("iterator key len=%d want=8", len(k))
		}
		id := binary.BigEndian.Uint64(k)
		if hasPrev && id <= prevKey {
			t.Fatalf("iterator order violated: prev=%d cur=%d", prevKey, id)
		}
		prevKey, hasPrev = id, true

		wantVersion, ok := model[id]
		if !ok {
			t.Fatalf("iterator returned unexpected key=%d", id)
		}
		gotVal := it.Value()
		gotVersion, gotKey := decodeVersionedValue(t, gotVal)
		if gotKey != id {
			t.Fatalf("value key mismatch: iterKey=%d valKey=%d", id, gotKey)
		}
		if gotVersion != wantVersion {
			t.Fatalf("value version mismatch key=%d got=%d want=%d", id, gotVersion, wantVersion)
		}
		seen++
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if seen != len(model) {
		t.Fatalf("iterator count=%d want=%d", seen, len(model))
	}

	rev, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("ReverseIterator: %v", err)
	}
	defer func() { _ = rev.Close() }()
	hasPrev = false
	seen = 0
	for rev.Valid() {
		k := rev.Key()
		if len(k) != 8 {
			t.Fatalf("reverse iterator key len=%d want=8", len(k))
		}
		id := binary.BigEndian.Uint64(k)
		if hasPrev && id >= prevKey {
			t.Fatalf("reverse iterator order violated: prev=%d cur=%d", prevKey, id)
		}
		prevKey, hasPrev = id, true

		wantVersion, ok := model[id]
		if !ok {
			t.Fatalf("reverse iterator returned unexpected key=%d", id)
		}
		gotVal := rev.Value()
		gotVersion, gotKey := decodeVersionedValue(t, gotVal)
		if gotKey != id {
			t.Fatalf("reverse value key mismatch: iterKey=%d valKey=%d", id, gotKey)
		}
		if gotVersion != wantVersion {
			t.Fatalf("reverse value version mismatch key=%d got=%d want=%d", id, gotVersion, wantVersion)
		}
		seen++
		rev.Next()
	}
	if err := rev.Error(); err != nil {
		t.Fatalf("reverse iterator error: %v", err)
	}
	if seen != len(model) {
		t.Fatalf("reverse iterator count=%d want=%d", seen, len(model))
	}
}

func TestProfileFast_LeafSplitMerge_NoKeyLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy leaf split/merge stress test in -short mode")
	}

	// This test is intentionally heavier than typical unit tests because it is
	// meant to catch rare key-loss bugs in leaf split/merge and merge iteration.
	const (
		nKeys       = 20_000
		valueBytes  = 96
		churnRounds = 4
	)

	dir := t.TempDir()
	opts := OptionsFor(ProfileFast, dir)
	opts.FlushThreshold = 64 << 10
	opts.DisableBackgroundPrune = true
	opts.BackgroundIndexVacuumInterval = -1
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.PointerThreshold = 1
	opts.LeafFillTargetPPM = 600_000
	opts.InternalFillTargetPPM = 600_000

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	model := make(map[uint64]uint32, nKeys)

	for i := 0; i < nKeys; i++ {
		id := uint64(i)
		v := versionedValue(1, id, valueBytes)
		if err := db.Set(be8Key(id), v); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
		model[id] = 1
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint after initial ingest: %v", err)
	}
	verifyIteratorMatchesModel(t, db, model)

	rng := rand.New(rand.NewSource(1))
	for round := uint32(2); round < uint32(2+churnRounds); round++ {
		// Delete ~1/3 of keys (random) to drive underfull pages/coalescing.
		for i := 0; i < nKeys/3; i++ {
			id := uint64(rng.Intn(nKeys))
			if err := db.Delete(be8Key(id)); err != nil {
				t.Fatalf("Delete(%d): %v", id, err)
			}
			delete(model, id)
		}
		// Update ~1/2 of keys (random) to drive splits.
		for i := 0; i < nKeys/2; i++ {
			id := uint64(rng.Intn(nKeys))
			v := versionedValue(round, id, valueBytes)
			if err := db.Set(be8Key(id), v); err != nil {
				t.Fatalf("Set(%d): %v", id, err)
			}
			model[id] = round
		}

		if err := db.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint round %d: %v", round, err)
		}

		// Sample some point reads (existing + deleted).
		for i := 0; i < 256; i++ {
			id := uint64(rng.Intn(nKeys))
			val, err := db.Get(be8Key(id))
			wantVersion, exists := model[id]
			if !exists {
				if err != nil {
					t.Fatalf("Get(%d) unexpected error for missing key: %v", id, err)
				}
				if val != nil {
					t.Fatalf("Get(%d) unexpectedly returned value for missing key: %dB", id, len(val))
				}
				continue
			}
			if err != nil {
				t.Fatalf("Get(%d): %v", id, err)
			}
			if val == nil {
				t.Fatalf("Get(%d) returned nil value for existing key", id)
			}
			gotVersion, gotKey := decodeVersionedValue(t, val)
			if gotKey != id {
				t.Fatalf("Get(%d) value key mismatch: got=%d", id, gotKey)
			}
			if gotVersion != wantVersion {
				t.Fatalf("Get(%d) version mismatch: got=%d want=%d", id, gotVersion, wantVersion)
			}
		}

		verifyIteratorMatchesModel(t, db, model)
	}

	// Reopen should preserve everything.
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	verifyIteratorMatchesModel(t, db2, model)

	// Final point-read spot check across entire keyspace.
	for id, wantVersion := range model {
		val, err := db2.Get(be8Key(id))
		if err != nil {
			t.Fatalf("reopen Get(%d): %v", id, err)
		}
		gotVersion, gotKey := decodeVersionedValue(t, val)
		if gotKey != id || gotVersion != wantVersion || !bytes.Equal(val, versionedValue(wantVersion, id, valueBytes)) {
			t.Fatalf("reopen Get(%d) mismatch", id)
		}
	}
}
