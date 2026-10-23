package caching

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWOrderedIteratorReusesVerifiedLeafCache(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, entries := range []int{128, -1} {
			t.Run(fmt.Sprintf("compressed=%t/cache=%d", compressed, entries), func(t *testing.T) {
				dir := t.TempDir()
				backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true, LeafPageReadCacheEntries: entries})
				if err != nil {
					t.Fatal(err)
				}
				compression := vlogCompressionOff
				if compressed {
					compression = vlogCompressionBlock
				}
				db, err := Open(dir, backend, Options{
					DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_ordered", FlushThreshold: 1 << 30,
					IndexOuterLeavesInValueLog: true, ValueLogPointerThreshold: 256,
					ValueLogCompression: uint8(compression),
				})
				if err != nil {
					_ = backend.Close()
					t.Fatal(err)
				}
				defer db.Close()
				owner := backend.NewMVCCAdmission()
				defer owner.Close()
				want := make(map[string][]byte)
				for _, key := range []string{"", "\x00", "a\x00", "a\x00b", "\xff", "\xff\x00"} {
					value := []byte("binary-" + key)
					if err := db.Set([]byte(key), value); err != nil {
						t.Fatal(err)
					}
					want[key] = value
				}
				for key := 0; key < 32; key++ {
					for _, timestamp := range []uint64{1, 7} {
						physical := cowOrderedLifecycleKey(t, fmt.Sprintf("key-%02d\x00", key), timestamp)
						value := bytes.Repeat([]byte{byte(key + 1)}, 128)
						if key == 17 {
							value = bytes.Repeat(value, 64) // persistent pointer values
						}
						cowOrderedLifecycleSet(t, db, owner, physical, value)
						want[string(physical)] = value
					}
				}
				if err := db.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				old, err := db.acquireCOWSnapshotWithError()
				if err != nil {
					t.Fatal(err)
				}
				defer old.Close()
				beforeHits := cowLeafCacheCounter(t, backend, "hits")
				beforeStores := cowLeafCacheCounter(t, backend, "stores")
				check := func(snapshot *Snapshot, expected map[string][]byte) {
					t.Helper()
					keys := make([]string, 0, len(expected))
					for key := range expected {
						keys = append(keys, key)
					}
					sort.Strings(keys)
					for _, reverse := range []bool{false, true} {
						it, err := snapshot.Iterator(nil, nil)
						if err != nil {
							t.Fatal(err)
						}
						if reverse {
							_ = it.Close()
							it, err = snapshot.ReverseIterator(nil, nil)
						}
						if err != nil {
							t.Fatal(err)
						}
						for position := range keys {
							index := position
							if reverse {
								index = len(keys) - 1 - position
							}
							key := keys[index]
							if !it.Valid() || string(it.Key()) != key || !bytes.Equal(it.Value(), expected[key]) {
								t.Fatalf("reverse=%t position=%d key=%x want=%x err=%v", reverse, position, it.Key(), key, it.Error())
							}
							it.Next()
						}
						if it.Valid() || it.Error() != nil {
							t.Fatalf("iterator terminal valid=%t err=%v", it.Valid(), it.Error())
						}
						// Reposition repeatedly through the same logged leaves. Both
						// directions keep their original retained basis and domain.
						for _, index := range []int{len(keys) / 2, 1, len(keys) - 2, len(keys) / 2} {
							it.Seek([]byte(keys[index]))
							if !it.Valid() || string(it.Key()) != keys[index] || !bytes.Equal(it.Value(), expected[keys[index]]) || it.Error() != nil {
								t.Fatalf("reseek reverse=%t index=%d err=%v", reverse, index, it.Error())
							}
						}
						if err := it.Close(); err != nil {
							t.Fatal(err)
						}
					}
				}
				check(old, want)
				check(old, want)
				if hits := cowLeafCacheCounter(t, backend, "hits"); (entries > 0 && hits <= beforeHits) || (entries < 0 && hits != beforeHits) {
					t.Fatalf("iterator cache activation hits=%d before=%d entries=%d", hits, beforeHits, entries)
				}
				if stores := cowLeafCacheCounter(t, backend, "stores"); stores != beforeStores {
					t.Fatalf("iterator populated backend cache: %d -> %d", beforeStores, stores)
				}
				key := cowOrderedLifecycleKey(t, "key-17\x00", 7)
				newValue := bytes.Repeat([]byte("updated persistent pointer"), 512)
				cowOrderedLifecycleSet(t, db, owner, key, newValue)
				if err := db.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				current, err := db.acquireCOWSnapshotWithError()
				if err != nil {
					t.Fatal(err)
				}
				defer current.Close()
				if current.cowCut.basis == old.cowCut.basis {
					t.Fatal("checkpoint did not install a distinct captured basis")
				}
				check(old, want)
				want[string(key)] = newValue
				check(current, want)
			})
		}
	}
}

func TestCOWIteratorLeafCacheRetainsRecordVerification(t *testing.T) {
	db, _, _ := cowLeafCacheFixture(t, 128, false)
	snapshot, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	var keyScratch, leafScratch [page.PageSize]byte
	var pointer page.LeafLogPtr
	stop := errors.New("capture logged iterator leaf")
	_, err = snapshot.cowCut.basis.snapshot.GetEntryExactWithFixedScratch([]byte("a000"), keyScratch[:], leafScratch[:], func(ptr page.LeafLogPtr, _ []byte) ([]byte, error) {
		pointer = ptr
		return nil, stop
	})
	if !errors.Is(err, stop) || pointer.FileID == 0 {
		t.Fatalf("expected real logged leaf: %+v %v", pointer, err)
	}
	file, _, err := snapshot.cowCut.basis.snapshot.PinnedValueLogFile(pointer.ValueLogFileID())
	if err != nil {
		t.Fatal(err)
	}
	writer, err := os.OpenFile(file.Name(), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	check := func(wantCorrupt bool) {
		t.Helper()
		before := db.cow.budget.Stats()
		it, err := snapshot.Iterator([]byte("a000"), nil)
		if err == nil {
			err = it.Error()
			_ = it.Close()
		}
		if wantCorrupt && !errors.Is(err, valuelog.ErrCorrupt) {
			t.Fatalf("iterator accepted corrupt cached leaf: %v", err)
		}
		if !wantCorrupt && err != nil {
			t.Fatal(err)
		}
		after := db.cow.budget.Stats()
		if after.ExternalBytes != before.ExternalBytes || after.ExternalLeases != before.ExternalLeases || after.ReservedBytes != before.ReservedBytes {
			t.Fatalf("physical iterator did not drain after corrupt=%t: before=%+v after=%+v", wantCorrupt, before, after)
		}
	}
	for _, offset := range []int64{int64(pointer.Offset - 4), int64(pointer.Offset)} {
		var original [1]byte
		if _, err := writer.ReadAt(original[:], offset); err != nil {
			t.Fatal(err)
		}
		// Even a fatal assertion must leave the real file intact for the
		// snapshot and database cleanup paths.
		defer func(offset int64, original [1]byte) { _, _ = writer.WriteAt(original[:], offset) }(offset, original)
		if _, err := writer.WriteAt([]byte{original[0] ^ 1}, offset); err != nil {
			t.Fatal(err)
		}
		check(true)
		if _, err := writer.WriteAt(original[:], offset); err != nil {
			t.Fatal(err)
		}
		check(false) // upgrade only after the original full verified decode
	}
}
