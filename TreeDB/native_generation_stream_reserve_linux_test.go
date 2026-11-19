//go:build linux

package treedb

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/sys/unix"
)

func TestNativeBSONGenerationReservesGrowingExt4Stream(t *testing.T) {
	opts := OptionsFor(ProfileCommandWALRelaxed, t.TempDir())
	dir, err := os.Open(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	ext4 := commitlog.IsExt4WALFilesystem(dir)
	if err := dir.Close(); err != nil {
		t.Fatal(err)
	}
	if !ext4 {
		t.Skip("requires an ext4 filesystem")
	}
	opts.FreshRelaxedStandaloneWALStream = true
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = DefaultCOWMemtableLimits()
	opts.COWMemtableLimits.MaxGenerationBytes = 8 << 20
	opts.COWMemtableLimits.MaxInFlightBytes = 512 << 20
	opts.COWMemtableLimits.MaxTotalBytes = 1 << 30
	opts.COWMemtableLimits.MaxRetiredBytes = 1 << 30
	opts.MaxWALBytes = 2 << 30
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.JournalCompression = false
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if db.Stats()["treedb.command_wal.writer.container"] != "stream" {
		t.Fatal("fixture did not select an ext4 stream")
	}
	backend, err := db.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	t.Cleanup(func() { _ = manager.Close() })
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := collections.NewNativeJSONManager(backend)
	if err != nil {
		t.Fatal(err)
	}
	id := []byte("row")
	doc, err := bson.Marshal(bson.D{{Key: "_id", Value: "row"}, {Key: "n", Value: int64(0)}, {Key: "pad", Value: make([]byte, 256<<10)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatchValidatedBSON([][]byte{id}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	held, err := reader.Capture("rows")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close() })

	type walPoint struct {
		path   string
		before unix.Stat_t
	}
	type receipt struct {
		before, after unix.Stat_t
		err           error
	}
	wal := filepath.Join(opts.Dir, "maindb", "wal")
	root := filepath.Join(opts.Dir, "maindb")
	var capture atomic.Pointer[walPoint]
	var latest atomic.Pointer[receipt]
	reserved := make(chan struct{}, 1)
	restore := sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.AfterDependencyFileSync && e.Resource == durabilitycut.ResourceCommandWAL &&
			e.LSN != 0 && e.StablePath != nil && filepath.Dir(e.Path) == wal {
			point := &walPoint{path: e.Path}
			if err := unix.Fstat(int(e.StablePath.Fd()), &point.before); err != nil {
				return err
			}
			if point.before.Size >= 64<<20 {
				capture.Store(point)
			}
		}
		if e.Point == durabilitycut.BeforeMetaWrite && e.Resource == durabilitycut.ResourceMeta && e.Root == root {
			if point := capture.Swap(nil); point != nil {
				r := &receipt{before: point.before}
				r.err = unix.Stat(point.path, &r.after)
				latest.Store(r)
				growth := r.after.Size - r.before.Size
				// Subtract live writes that advanced beyond the captured prefix.
				// Ext4 st_blocks is in 512-byte units.
				extra := (r.after.Blocks-r.before.Blocks)*512 - growth
				if r.err == nil && r.after.Ino == r.before.Ino && r.after.Dev == r.before.Dev &&
					growth >= 0 && extra >= 4<<20 && extra <= (8<<20)+r.before.Blksize {
					select {
					case reserved <- struct{}{}:
					default:
					}
				}
			}
		}
		return nil
	}))
	defer restore()

	// Grow the real ordinary native WAL and stop once a worker publication
	// demonstrates extra blocks between its pinned WAL sync and META.
	var last int64
	for ; last < 352 && len(reserved) == 0; last++ {
		typ, value, err := bson.MarshalValue(last + 1)
		if err != nil {
			t.Fatal(err)
		}
		results, ok, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]collections.BSONSetUpdateBatchItem{{
			DocumentID: id, Fields: []collections.BSONSetField{{Key: "n", Value: bson.RawValue{Type: typ, Value: value}}},
		}})
		if err != nil || !ok || len(results) != 1 || !results[0].Matched || !results[0].Modified {
			t.Fatalf("UPDATE %d failed: %+v %t %v", last+1, results, ok, err)
		}
	}
	select {
	case <-reserved:
	case <-time.After(5 * time.Second):
		t.Fatalf("no physical reserve beyond WAL EOF growth after %d native UPDATEs, last sync-to-META receipt=%+v", last, latest.Load())
	}
	restore()

	readValue := func(raw []byte, err error, want int64) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := bson.Raw(raw).Lookup("n").Int64OK(); !ok || got != want {
			t.Fatalf("BSON n=%d,%t want %d", got, ok, want)
		}
	}
	raw, err := held.Get(id)
	readValue(raw, err, 0)
	raw, err = reader.Get("rows", id)
	readValue(raw, err, last)
	if err := errors.Join(held.Close(), manager.Close()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(wal)
	if err != nil {
		t.Fatal(err)
	}
	empty := 0
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "commit-l0-") {
			continue
		}
		var stat unix.Stat_t
		if err := unix.Stat(filepath.Join(wal, entry.Name()), &stat); err != nil {
			t.Fatal(err)
		}
		if stat.Size == 0 {
			empty++
			if stat.Blocks != 0 {
				t.Fatalf("Close left %d allocated blocks on empty WAL %s", stat.Blocks, entry.Name())
			}
		}
	}
	if empty == 0 {
		t.Fatal("fixture did not retain an empty Close successor")
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	backend, err = reopened.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	reader, err = collections.NewNativeJSONManager(backend)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = reader.Get("rows", id)
	readValue(raw, err, last)
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}
}
