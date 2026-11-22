package treedb

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWPublicCurrentValueLogMmap(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		t.Run(mode, func(t *testing.T) {
			for _, pointers := range []bool{false, true} {
				name := "leaf"
				if pointers {
					name = "value"
				}
				t.Run(name, func(t *testing.T) {
					opts := OptionsFor(ProfileNoWALFast, t.TempDir())
					opts.MemtableMode = mode
					opts.DisableBackgroundPrune = true
					opts.BackgroundCheckpointInterval = -1
					opts.BackgroundCheckpointIdleDuration = -1
					opts.BackgroundIndexVacuumInterval = -1
					opts.IndexOuterLeavesInValueLog = !pointers
					opts.ValueLog.ForcePointers = pointers
					want := bytes.Repeat([]byte("mapped read data"), 8)
					if pointers {
						want = bytes.Repeat(want, 32)
					}

					db, err := Open(opts)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := db.Close(); err != nil {
							t.Error(err)
						}
					})
					batch := db.NewBatch()
					t.Cleanup(func() {
						if err := batch.Close(); err != nil {
							t.Error(err)
						}
					})
					var key [8]byte
					for i := range 1024 {
						binary.BigEndian.PutUint64(key[:], uint64(i))
						if err := batch.Set(key[:], want); err != nil {
							t.Fatal(err)
						}
					}
					if err := batch.Write(); err != nil {
						t.Fatal(err)
					}
					if err := db.Checkpoint(); err != nil {
						t.Fatal(err)
					}
					binary.BigEndian.PutUint64(key[:], 17)
					if pointers {
						basis := db.backend.AcquireSnapshot()
						entry, err := basis.GetEntryExactWithFixedScratch(key[:], make([]byte, page.PageSize), make([]byte, page.PageSize), nil)
						closeErr := basis.Close()
						if err != nil || closeErr != nil || entry.Flags&node.FlagPointer == 0 {
							t.Fatalf("persisted value pointer absent: flags=%d, read=%v, close=%v", entry.Flags, err, closeErr)
						}
					}

					before := db.Stats()
					snap := db.AcquireSnapshot()
					t.Cleanup(func() {
						if err := snap.Close(); err != nil {
							t.Error(err)
						}
					})
					assertRead := func(read func([]byte, []byte) ([]byte, error), expected []byte) {
						t.Helper()
						dst := make([]byte, 1, 1+len(expected))
						dst[0] = 7
						got, err := read(key[:], dst)
						if err != nil || len(got) != len(expected)+1 || got[0] != 7 || !bytes.Equal(got[1:], expected) {
							t.Fatalf("GetAppend size=%d err=%v", len(got), err)
						}
					}
					for range 8 {
						assertRead(db.GetAppend, want)
						assertRead(snap.GetAppend, want)
					}
					after := db.Stats()
					counter := func(stats map[string]string, key string) uint64 {
						t.Helper()
						n, err := strconv.ParseUint(stats[key], 10, 64)
						if err != nil {
							t.Fatalf("stat %q: %v", key, err)
						}
						return n
					}
					delta := func(key string) uint64 {
						return counter(after, key) - counter(before, key)
					}
					hits := delta("treedb.vlog.mmap_read.hits") + delta("treedb.cache.vlog_mmap.read.hits")
					fallbacks := delta("treedb.vlog.mmap_read.fallback_readat") + delta("treedb.cache.vlog_mmap.read.fallback_readat")
					current := counter(after, "treedb.vlog.mmap_current_segments") + counter(after, "treedb.cache.vlog_mmap.current_segments")
					if runtime.GOOS == "windows" {
						if hits != 0 || fallbacks == 0 {
							t.Fatalf("expected positional fallback on Windows, hits=%d fallbacks=%d", hits, fallbacks)
						}
					} else if hits < 16 || fallbacks != 0 || current == 0 {
						t.Fatalf("expected mapped current-file reads, hits=%d fallbacks=%d current mappings=%d", hits, fallbacks, current)
					}

					replacement := bytes.Clone(want)
					replacement[0] ^= 1
					if err := db.Set(key[:], replacement); err != nil {
						t.Fatal(err)
					}
					if err := db.Checkpoint(); err != nil {
						t.Fatal(err)
					}
					assertRead(snap.GetAppend, want)
					assertRead(db.GetAppend, replacement)
					if err := snap.Close(); err != nil {
						t.Fatal(err)
					}
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
					reopened, err := Open(opts)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := reopened.Close(); err != nil {
							t.Error(err)
						}
					})
					assertRead(reopened.GetAppend, replacement)
				})
			}
		})
	}
}
