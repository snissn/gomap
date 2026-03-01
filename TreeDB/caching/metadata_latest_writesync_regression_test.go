package caching_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func iavlStoreNodeKeyRegression(version uint64, nonce uint32) []byte {
	key := make([]byte, 13)
	key[0] = 's'
	binary.BigEndian.PutUint64(key[1:9], version)
	binary.BigEndian.PutUint32(key[9:13], nonce)
	return key
}

func latestValueBytesRegression(height uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, height)
	return buf
}

// Regression for issue #657 (bug A): metadata WriteSync may return while
// "s/latest" is not visible.
func TestRegression_MetadataLatestVisibleAfterWriteSync_Fast(t *testing.T) {
	dir := t.TempDir()

	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	// Match cosmos-db adapter behavior: do not force all values onto pointers.
	opts.ValueLog.ForcePointers = false
	if opts.MemtableMode == "" || opts.MemtableMode == "adaptive" {
		opts.MemtableMode = "adaptive:hash_sorted"
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	stores := []string{
		"acc", "authz", "bank", "blob", "capability",
		"circuit", "consensus", "distribution", "evidence", "feegrant",
		"gov", "hyperlane", "ibc", "icahost", "minfee",
		"mint", "packetfowardmiddleware", "params", "signal", "slashing",
		"staking", "transfer", "upgrade", "warp",
	}
	const height = uint64(9_993_000)
	const keysPerStore = 4096

	for si, storeName := range stores {
		b := db.NewBatch()
		for i := 0; i < keysPerStore; i++ {
			nonce := uint32((i % 4095) + 2)
			key := append([]byte("s/k:"+storeName+"/"), iavlStoreNodeKeyRegression(height, nonce)...)
			val := bytes.Repeat([]byte{byte((si % 251) + 1)}, 96)
			if err := b.Set(key, val); err != nil {
				_ = b.Close()
				t.Fatalf("store=%s nonce=%d set: %v", storeName, nonce, err)
			}
		}
		rootKey := append([]byte("s/k:"+storeName+"/"), iavlStoreNodeKeyRegression(height, 1)...)
		if err := b.Set(rootKey, bytes.Repeat([]byte{0xA5}, 73)); err != nil {
			_ = b.Close()
			t.Fatalf("store=%s root set: %v", storeName, err)
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("store=%s writesync: %v", storeName, err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("store=%s close: %v", storeName, err)
		}
	}

	latest := latestValueBytesRegression(height)
	commitInfoKey := []byte(fmt.Sprintf("s/%d", height))
	commitInfoValue := bytes.Repeat([]byte{0x7C}, 256)

	mb := db.NewBatch()
	if err := mb.Set(commitInfoKey, commitInfoValue); err != nil {
		_ = mb.Close()
		t.Fatalf("metadata set commit-info: %v", err)
	}
	if err := mb.Set([]byte("s/latest"), latest); err != nil {
		_ = mb.Close()
		t.Fatalf("metadata set s/latest: %v", err)
	}
	if err := mb.WriteSync(); err != nil {
		_ = mb.Close()
		t.Fatalf("metadata writesync: %v", err)
	}
	if err := mb.Close(); err != nil {
		t.Fatalf("metadata close: %v", err)
	}

	gotLatest, err := db.Get([]byte("s/latest"))
	if err != nil {
		t.Fatalf("get s/latest: %v", err)
	}
	if !bytes.Equal(latest, gotLatest) {
		t.Fatalf("s/latest should be visible immediately after WriteSync: got=%x want=%x", gotLatest, latest)
	}

	hasCommitInfo, err := db.Has(commitInfoKey)
	if err != nil {
		t.Fatalf("has commit-info: %v", err)
	}
	if !hasCommitInfo {
		t.Fatalf("s/<height> commit-info key should be visible immediately after WriteSync")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}

	db, err = treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db.Close() }()

	gotLatest, err = db.Get([]byte("s/latest"))
	if err != nil {
		t.Fatalf("get s/latest after reopen: %v", err)
	}
	if !bytes.Equal(latest, gotLatest) {
		t.Fatalf("s/latest should survive reopen: got=%x want=%x", gotLatest, latest)
	}
}
