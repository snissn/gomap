package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

var cowPublicContractProfiles = []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast}

func cowPublicContractOpen(t *testing.T, profile Profile, configure ...func(*Options)) (*DB, Options, *atomic.Bool) {
	t.Helper()
	opts := OptionsFor(profile, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.MemtableShards = 2
	opts.FlushThreshold = 1 << 30
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	for _, apply := range configure {
		apply(&opts)
	}
	database, err := Open(opts)
	if err != nil {
		t.Fatalf("public Open(%s, cow_btree): %v", profile, err)
	}
	blocked := &atomic.Bool{}
	t.Cleanup(func() {
		// A timed-out call still owns its locks. Avoid hiding the diagnostic
		// behind a second deadlock in cleanup; the isolated test process exits.
		if !blocked.Load() {
			_ = database.Close()
		}
	})
	return database, opts, blocked
}

func cowPublicContractComplete(t *testing.T, blocked *atomic.Bool, label string, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		blocked.Store(true)
		t.Fatalf("%s did not complete: possible lock reentry deadlock", label)
		return nil
	}
}

func cowPublicContractSnapshotValue(t *testing.T, snap Snapshot, key string, want []byte, found bool) EntryRevision {
	t.Helper()
	got, err := snap.Get([]byte(key))
	if !found {
		if !errors.Is(err, ErrKeyNotFound) || got != nil {
			t.Fatalf("snapshot missing %q = %q, %v", key, got, err)
		}
	} else if err != nil || !bytes.Equal(got, want) || (len(want) == 0 && got == nil) {
		t.Fatalf("snapshot Get(%q) = len %d, %v; want len %d", key, len(got), err, len(want))
	}
	versioned, revision, err := snap.GetVersioned([]byte(key))
	if found {
		if err != nil || !bytes.Equal(versioned, want) || revision == LegacyEntryRevision {
			t.Fatalf("snapshot GetVersioned(%q) = len %d, rev %d, %v", key, len(versioned), revision, err)
		}
	} else if !errors.Is(err, ErrKeyNotFound) || versioned != nil {
		t.Fatalf("snapshot versioned missing %q = %q, %v", key, versioned, err)
	}
	if has, err := snap.Has([]byte(key)); err != nil || has != found {
		t.Fatalf("snapshot Has(%q) = %t, %v; want %t", key, has, err, found)
	}
	return revision
}

func TestCOWPublicContractDirtyReadOwnership(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, _, blocked := cowPublicContractOpen(t, profile)
			want := bytes.Repeat([]byte("pointer-original/"), 1024)
			key, input := []byte("a"), bytes.Clone(want)
			if err := database.Set(key, input); err != nil {
				t.Fatal(err)
			}
			key[0], input[0] = 'z', 'X'
			for key, value := range map[string][]byte{"b": []byte("before-delete"), "empty": {}} {
				if err := database.Set([]byte(key), value); err != nil {
					t.Fatal(err)
				}
			}
			snap := database.AcquireSnapshot()
			if snap == nil {
				t.Fatal("public dirty snapshot unavailable")
			}
			defer snap.Close()
			originalRevision := cowPublicContractSnapshotValue(t, snap, "a", want, true)
			owned, err := snap.Get([]byte("a"))
			if err != nil {
				t.Fatal(err)
			}
			owned[0] ^= 1
			appended, err := snap.GetAppend([]byte("a"), []byte("prefix:"))
			if err != nil || !bytes.Equal(appended, append([]byte("prefix:"), want...)) {
				t.Fatalf("snapshot GetAppend len=%d err=%v", len(appended), err)
			}
			appended[len("prefix:")] ^= 1
			if err := database.Set([]byte("a"), []byte("intermediate")); err != nil {
				t.Fatal(err)
			}
			batch := database.NewBatch()
			if batch == nil {
				t.Fatal("public batch unavailable")
			}
			defer batch.Close()
			for _, op := range []func() error{
				func() error { return batch.Set([]byte("a"), []byte("after")) },
				func() error { return batch.Delete([]byte("b")) },
				func() error { return batch.Set([]byte("c"), []byte("new")) },
			} {
				if err := op(); err != nil {
					t.Fatal(err)
				}
			}
			if err := batch.Write(); err != nil {
				t.Fatal(err)
			}
			if revision := cowPublicContractSnapshotValue(t, snap, "a", want, true); revision != originalRevision {
				t.Fatalf("old cut revision changed: %d -> %d", originalRevision, revision)
			}
			cowPublicContractSnapshotValue(t, snap, "b", []byte("before-delete"), true)
			cowPublicContractSnapshotValue(t, snap, "c", nil, false)
			cowPublicContractSnapshotValue(t, snap, "empty", []byte{}, true)
			cowPublicContractSnapshotValue(t, snap, "missing", nil, false)
			live, revision, err := database.GetVersioned([]byte("a"))
			if err != nil || string(live) != "after" || revision <= originalRevision {
				t.Fatalf("live versioned value=%q revision=%d err=%v", live, revision, err)
			}
			live[0] ^= 1
			if got, err := database.Get([]byte("a")); err != nil || string(got) != "after" {
				t.Fatalf("live owned Get=%q err=%v", got, err)
			}
			if got, err := database.HasMany([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("empty"), []byte("missing")}); err != nil || !reflect.DeepEqual(got, []bool{true, false, true, true, false}) {
				t.Fatalf("live HasMany=%v err=%v", got, err)
			}
			if got, err := snap.HasPrefixes([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("emp"), []byte("absent/")}); err != nil || !reflect.DeepEqual(got, []bool{true, true, false, true, false}) {
				t.Fatalf("old HasPrefixes=%v err=%v", got, err)
			}
			keys := [][]byte{[]byte("a"), []byte("empty"), []byte("missing"), []byte("a")}
			calls := 0
			err = cowPublicContractComplete(t, blocked, "snapshot GetManyView callback reentry", func() error {
				return snap.GetManyView(keys, func(index int, key, value []byte, found bool) error {
					calls++
					if !bytes.Equal(key, keys[index]) || found != (index != 2) {
						return fmt.Errorf("view index %d key=%q found=%t", index, key, found)
					}
					if index == 0 || index == 3 {
						if !bytes.Equal(value, want) {
							return errors.New("view changed old pointer value")
						}
					}
					if index == 1 && (value == nil || len(value) != 0) {
						return errors.New("empty value lost presence")
					}
					if index == 2 && value != nil {
						return errors.New("missing view retained value")
					}
					if _, err := snap.Get([]byte("a")); err != nil {
						return err
					}
					return database.Set([]byte("later"), []byte("callback-write"))
				})
			})
			if err != nil || calls != len(keys) {
				t.Fatalf("snapshot view calls=%d err=%v", calls, err)
			}
			err = cowPublicContractComplete(t, blocked, "DB GetManyView callback reentry", func() error {
				return database.GetManyView([][]byte{[]byte("a")}, func(_ int, key, value []byte, found bool) error {
					if !found || string(value) != "after" {
						return errors.New("live view lost newer cut")
					}
					if _, err := database.Get(key); err != nil {
						return err
					}
					return database.Set([]byte("later"), []byte("live-callback-write"))
				})
			})
			if err != nil {
				t.Fatalf("DB view callback reentry: %v", err)
			}
			stop := errors.New("callback stop")
			for name, read := range map[string]func([][]byte, GetManyViewFunc) error{"DB": database.GetManyView, "snapshot": snap.GetManyView} {
				called := 0
				err := read([][]byte{[]byte("a"), []byte("empty")}, func(int, []byte, []byte, bool) error { called++; return stop })
				if !errors.Is(err, stop) || called != 1 {
					t.Fatalf("%s callback error=%v calls=%d", name, err, called)
				}
				if err := read(nil, func(int, []byte, []byte, bool) error { t.Error("empty input invoked callback"); return nil }); err != nil {
					t.Fatalf("%s empty view: %v", name, err)
				}
				if err := read(keys, nil); err == nil {
					t.Fatalf("%s accepted nil callback", name)
				}
			}
			it, err := snap.Iterator([]byte("a"), []byte("f"))
			if err != nil {
				t.Fatal(err)
			}
			defer it.Close()
			it.Seek([]byte("b"))
			if !it.Valid() || string(it.Key()) != "b" || string(it.Value()) != "before-delete" {
				t.Fatalf("old iterator Seek(b) key=%q value=%q error=%v", it.Key(), it.Value(), it.Error())
			}
			it.Seek(nil)
			var scan []string
			for ; it.Valid(); it.Next() {
				scan = append(scan, string(it.KeyCopy(nil)))
			}
			if err := it.Error(); err != nil || !reflect.DeepEqual(scan, []string{"a", "b", "empty"}) {
				t.Fatalf("old iterator keys=%v err=%v", scan, err)
			}
			err = cowPublicContractComplete(t, blocked, "snapshot Iterate callback reentry", func() error {
				return snap.Iterate([]byte("a"), []byte("f"), func(key, value []byte) error {
					got, err := snap.Get(key)
					if err != nil || !bytes.Equal(got, value) {
						return fmt.Errorf("iterate reentry %q: %v", key, err)
					}
					return database.Set([]byte("later"), []byte("iterate-callback-write"))
				})
			})
			if err != nil {
				t.Fatalf("snapshot Iterate reentry: %v", err)
			}
			if err := snap.Iterate(nil, nil, func([]byte, []byte) error { return stop }); !errors.Is(err, stop) {
				t.Fatalf("snapshot Iterate callback error=%v", err)
			}
			seekKey, seekValue, found, err := database.SeekGE([]byte("b"), []byte("f"))
			if err != nil || !found || string(seekKey) != "c" || string(seekValue) != "new" {
				t.Fatalf("live SeekGE=%q/%q found=%t err=%v", seekKey, seekValue, found, err)
			}
			seekKey[0], seekValue[0] = 'X', 'X'
			if key, value, found, err := database.SeekGE([]byte("b"), []byte("f")); err != nil || !found || string(key) != "c" || string(value) != "new" {
				t.Fatalf("SeekGE owned result=%q/%q found=%t err=%v", key, value, found, err)
			}
			if _, _, found, err := database.SeekGE([]byte("z"), nil); err != nil || found {
				t.Fatalf("empty seek found=%t err=%v", found, err)
			}
			if err := snap.Close(); err != nil {
				t.Fatal(err)
			}
			if it.Valid() || !errors.Is(it.Error(), ErrClosed) {
				t.Fatalf("closed snapshot retained valid iterator: %v", it.Error())
			}
			if _, err := snap.Get([]byte("a")); !errors.Is(err, ErrClosed) {
				t.Fatalf("closed snapshot Get=%v", err)
			}
			if err := snap.GetManyView(nil, func(int, []byte, []byte, bool) error { return nil }); !errors.Is(err, ErrClosed) {
				t.Fatalf("closed empty snapshot view=%v", err)
			}
		})
	}
}

func TestCOWPublicContractUnsupportedBeforeEffects(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, _, _ := cowPublicContractOpen(t, profile)
			if err := database.Set([]byte("keep"), []byte("original")); err != nil {
				t.Fatal(err)
			}
			before := database.Stats()
			callback := false
			for _, update := range []func([]byte, UpdateFunc) error{database.Update, database.UpdateSync} {
				err := update([]byte("keep"), func([]byte) (UpdateResult, error) { callback = true; return SetUpdate([]byte("wrong")), nil })
				if !errors.Is(err, caching.ErrCOWUnsupported) || callback {
					t.Fatalf("unsupported update invoked callback=%t err=%v", callback, err)
				}
			}
			for _, open := range []func() (*ConditionalTxn, error){database.NewConditionalTxn, database.NewConditionalTxnWithSnapshot} {
				if tx, err := open(); !errors.Is(err, ErrConditionalTxnUnsupported) || tx != nil {
					t.Fatalf("conditional txn=%v err=%v", tx, err)
				}
			}
			if err := database.DeleteRange([]byte("a"), []byte("z")); !errors.Is(err, caching.ErrCOWUnsupported) {
				t.Fatalf("DeleteRange=%v", err)
			}
			if it, err := database.ReverseIterator(nil, nil); !errors.Is(err, caching.ErrCOWUnsupported) || it != nil {
				t.Fatalf("reverse iterator=%v err=%v", it, err)
			}
			snap := database.AcquireSnapshot()
			if snap == nil {
				t.Fatal("snapshot unavailable")
			}
			defer snap.Close()
			if it, err := snap.ReverseIterator(nil, nil); !errors.Is(err, caching.ErrCOWUnsupported) || it != nil {
				t.Fatalf("snapshot reverse iterator=%v err=%v", it, err)
			}
			called := false
			if err := snap.ReverseIterate(nil, nil, func([]byte, []byte) error { called = true; return nil }); !errors.Is(err, caching.ErrCOWUnsupported) || called {
				t.Fatalf("unsupported reverse iteration invoked callback=%t err=%v", called, err)
			}
			batch := database.NewBatch()
			if batch == nil {
				t.Fatal("public batch unavailable")
			}
			defer batch.Close()
			if err := batch.Set([]byte("wrong-point"), []byte("must-not-appear")); err != nil {
				t.Fatal(err)
			}
			err := batch.DeleteRange([]byte("a"), []byte("z"))
			if err == nil {
				err = batch.Write()
			}
			if !errors.Is(err, caching.ErrCOWUnsupported) {
				t.Fatalf("mixed range batch=%v", err)
			}
			if got, err := database.Get([]byte("keep")); err != nil || string(got) != "original" {
				t.Fatalf("refusal changed prior value=%q err=%v", got, err)
			}
			if has, err := database.Has([]byte("wrong-point")); err != nil || has {
				t.Fatalf("mixed refusal published point=%t err=%v", has, err)
			}
			after := database.Stats()
			for _, key := range []string{"treedb.command_wal.max_lsn", "treedb.command_wal.bytes"} {
				if profile != ProfileNoWALFast && (before[key] == "" || after[key] != before[key]) {
					t.Fatalf("refusal changed WAL %s: %q -> %q", key, before[key], after[key])
				}
			}
		})
	}
}

func TestCOWPublicContractLargeSortedBatch(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, _, _ := cowPublicContractOpen(t, profile, func(opts *Options) {
				// The legacy streaming switch requires >=4096 entries,
				// >=1MiB, no journal, and inline values. Keep this command
				// above those historical thresholds; current persistent-value-log
				// gates may independently disable legacy streaming.
				opts.ValueLog.ForcePointers = false
				opts.ValueLog.PointerThreshold = 1 << 30
				// Admit this entire unsplit command under explicit finite test
				// limits, rather than relying on conservative default headroom.
				opts.COWMemtableLimits = DefaultCOWMemtableLimits()
				opts.COWMemtableLimits.MaxGenerationBytes = 512 << 20
				opts.COWMemtableLimits.MaxTotalBytes = 2 << 30
				opts.COWMemtableLimits.MaxRetiredBytes = 2 << 30
				opts.COWMemtableLimits.MaxInFlightBytes = 512 << 20
			})
			old := database.AcquireSnapshot()
			if old == nil {
				t.Fatal("opening snapshot unavailable")
			}
			defer old.Close()
			const count = 4096
			batch := database.NewBatchWithSize(count)
			if batch == nil {
				t.Fatal("public sized batch unavailable")
			}
			defer batch.Close()
			want := bytes.Repeat([]byte("v"), 512)
			for i := 0; i < count; i++ {
				if err := batch.Set([]byte(fmt.Sprintf("sorted/%04d", i)), want); err != nil {
					t.Fatal(err)
				}
			}
			if err := batch.Write(); err != nil {
				t.Fatalf("large sorted public batch: %v", err)
			}
			current := database.AcquireSnapshot()
			if current == nil {
				t.Fatal("batch snapshot unavailable")
			}
			defer current.Close()
			for i := 0; i < count; i++ {
				key := fmt.Sprintf("sorted/%04d", i)
				if has, err := old.Has([]byte(key)); err != nil || has {
					t.Fatalf("opening snapshot saw sorted key %q: %t, %v", key, has, err)
				}
				cowPublicContractSnapshotValue(t, current, key, want, true)
			}
		})
	}
}

func TestCOWPublicContractCheckpointCloseReopen(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, opts, blocked := cowPublicContractOpen(t, profile)
			if err := cowPublicContractComplete(t, blocked, "public SetSync", func() error { return database.SetSync([]byte("sync"), []byte("first")) }); err != nil {
				t.Fatal(err)
			}
			old := database.AcquireSnapshot()
			if old == nil {
				t.Fatal("sync snapshot unavailable")
			}
			defer old.Close()
			oldRevision := cowPublicContractSnapshotValue(t, old, "sync", []byte("first"), true)
			batch := database.NewBatch()
			if batch == nil {
				t.Fatal("public batch unavailable")
			}
			for _, op := range []func() error{
				func() error { return batch.Set([]byte("sync"), []byte("second")) },
				func() error { return batch.Set([]byte("deleted"), []byte("brief")) },
				func() error { return batch.Delete([]byte("deleted")) },
				func() error { return batch.Set([]byte("empty"), []byte{}) },
			} {
				if err := op(); err != nil {
					t.Fatal(err)
				}
			}
			if err := cowPublicContractComplete(t, blocked, "public WriteSync", batch.WriteSync); err != nil {
				t.Fatal(err)
			}
			if err := batch.Close(); err != nil {
				t.Fatal(err)
			}
			value, revision, err := database.GetVersioned([]byte("sync"))
			if err != nil || string(value) != "second" || revision <= oldRevision {
				t.Fatalf("batch value=%q revision=%d old=%d err=%v", value, revision, oldRevision, err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if got := cowPublicContractSnapshotValue(t, old, "sync", []byte("first"), true); got != oldRevision {
				t.Fatalf("checkpoint changed old revision=%d want=%d", got, oldRevision)
			}
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			if err := database.Set([]byte("close-only"), []byte("dirty-at-close")); err != nil {
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := database.Get([]byte("sync")); !errors.Is(err, ErrClosed) {
				t.Fatalf("closed DB Get=%v", err)
			}
			if err := database.GetManyView(nil, func(int, []byte, []byte, bool) error { return nil }); !errors.Is(err, ErrClosed) {
				t.Fatalf("closed DB empty GetManyView=%v", err)
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatalf("public reopen: %v", err)
			}
			defer reopened.Close()
			value, recoveredRevision, err := reopened.GetVersioned([]byte("sync"))
			if err != nil || string(value) != "second" || recoveredRevision != revision {
				t.Fatalf("reopened version=%q/%d want second/%d err=%v", value, recoveredRevision, revision, err)
			}
			for key, want := range map[string][]byte{"close-only": []byte("dirty-at-close"), "empty": {}} {
				got, err := reopened.Get([]byte(key))
				if err != nil || !bytes.Equal(got, want) || got == nil {
					t.Fatalf("reopened %q=%q err=%v", key, got, err)
				}
			}
			if has, err := reopened.Has([]byte("deleted")); err != nil || has {
				t.Fatalf("reopened tombstone found=%t err=%v", has, err)
			}
		})
	}
}

func TestCOWPublicContractInvalidOpenHasNoStorageEffect(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "absent-cow-db")
			opts := OptionsFor(profile, directory)
			opts.MemtableMode = "cow_btree"
			opts.MemtableShards = 2
			opts.COWMemtableLimits = DefaultCOWMemtableLimits()
			opts.COWMemtableLimits.MaxSources = 1
			if database, err := Open(opts); err == nil || database != nil {
				if database != nil {
					_ = database.Close()
				}
				t.Fatalf("invalid source/shard configuration Open=%v err=%v", database, err)
			}
			if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid configuration mutated storage: Stat=%v", err)
			}
		})
	}
}

func TestCOWPublicContractCapacityRefusalHasNoPointEffects(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, _, _ := cowPublicContractOpen(t, profile, func(opts *Options) {
				opts.ValueLog.ForcePointers = false
				opts.ValueLog.PointerThreshold = 1 << 30
				opts.COWMemtableLimits = DefaultCOWMemtableLimits()
				opts.COWMemtableLimits.MaxInFlightBytes = 1 << 20
			})
			if err := database.Set([]byte("keep"), []byte("original")); err != nil {
				t.Fatal(err)
			}
			before := database.Stats()
			command := database.NewBatchWithSize(256)
			if command == nil {
				t.Fatal("public batch unavailable")
			}
			defer command.Close()
			var refused error
			value := bytes.Repeat([]byte("v"), 16<<10)
			for i := 0; i < 256; i++ {
				if err := command.Set([]byte(fmt.Sprintf("refused/%04d", i)), value); err != nil {
					refused = err
					break
				}
			}
			if refused == nil {
				refused = command.Write()
			}
			if !errors.Is(refused, memtable.ErrCOWCapacity) {
				t.Fatalf("bounded command refusal=%v", refused)
			}
			// Pending owned operations retain admission until Close. Release
			// the rejected command before asking a new read to reserve scratch.
			if err := command.Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := database.Get([]byte("keep")); err != nil || string(got) != "original" {
				t.Fatalf("refusal changed seed=%q err=%v", got, err)
			}
			for i := 0; i < 256; i++ {
				if found, err := database.Has([]byte(fmt.Sprintf("refused/%04d", i))); err != nil || found {
					t.Fatalf("refusal published point %d found=%t err=%v", i, found, err)
				}
			}
			after := database.Stats()
			if profile != ProfileNoWALFast {
				for _, key := range []string{"treedb.command_wal.max_lsn", "treedb.command_wal.bytes"} {
					if before[key] == "" || after[key] != before[key] {
						t.Fatalf("capacity refusal changed WAL %s: %q -> %q", key, before[key], after[key])
					}
				}
			}
		})
	}
}
