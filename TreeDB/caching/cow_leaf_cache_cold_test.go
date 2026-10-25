package caching

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// This public-path regression uses only APIs/counters present before allocating
// COW admission, so the identical test can run against the old implementation.
// A real reopen drops all write-origin cache backing before any COW-only reads.
func TestCOWColdLeafCacheReopenAdmission(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		for _, compressed := range []bool{false, true} {
			for _, entries := range []int{128, -1} {
				t.Run(fmt.Sprintf("mode=%s/compressed=%t/entries=%d", mode, compressed, entries), func(t *testing.T) {
					dir := t.TempDir()
					backendOpts := backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true, LeafPageReadCacheEntries: entries}
					compression := vlogCompressionOff
					if compressed {
						compression = vlogCompressionBlock
					}
					opts := Options{DisableWAL: true, AllowUnsafe: true, MemtableMode: mode, FlushThreshold: 1 << 30,
						IndexOuterLeavesInValueLog: true, ValueLogPointerThreshold: 256,
						ValueLogCompression: uint8(compression), ValueLogBlockCodec: 0}
					open := func() (*DB, *backenddb.DB) {
						t.Helper()
						backend, err := backenddb.Open(backendOpts)
						if err != nil {
							t.Fatal(err)
						}
						db, err := Open(dir, backend, opts)
						if err != nil {
							_ = backend.Close()
							t.Fatal(err)
						}
						t.Cleanup(func() {
							if !db.closing.Load() {
								if err := db.Close(); err != nil {
									t.Error(err)
								}
							}
						})
						return db, backend
					}
					db, _ := open()
					want := bytes.Repeat([]byte("cold-inline-leaf-value"), 4)
					for i := 0; i < 64; i++ {
						if err := db.Set([]byte(fmt.Sprintf("a%03d", i)), want); err != nil {
							t.Fatal(err)
						}
					}
					if err := db.Checkpoint(); err != nil {
						t.Fatal(err)
					}
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
					db, backend := open()
					if stores := cowLeafCacheCounter(t, backend, "stores"); stores != 0 {
						t.Fatalf("reopened fixture was not cold: stores=%d", stores)
					}
					// A failed full decode cannot acquire backing or trust a record.
					// Capturing its pointer stops before reading the physical leaf.
					if entries > 0 {
						snapshot, err := db.acquireCOWSnapshotWithError()
						if err != nil {
							t.Fatal(err)
						}
						defer snapshot.Close()
						basis := snapshot.cowCut.basis
						var keyScratch, leafScratch [page.PageSize]byte
						var ptr page.LeafLogPtr
						stop := errors.New("capture cold leaf")
						_, err = basis.snapshot.GetEntryExactWithFixedScratch([]byte("a000"), keyScratch[:], leafScratch[:], func(p page.LeafLogPtr, _ []byte) ([]byte, error) {
							ptr = p
							return nil, stop
						})
						if !errors.Is(err, stop) || ptr.FileID == 0 {
							t.Fatalf("fixture has no cold outer leaf: %+v %v", ptr, err)
						}
						file, _, err := basis.snapshot.PinnedValueLogFile(ptr.ValueLogFileID())
						if err != nil {
							t.Fatal(err)
						}
						writer, err := os.OpenFile(file.Name(), os.O_RDWR, 0)
						if err != nil {
							t.Fatal(err)
						}
						defer writer.Close()
						var crc [1]byte
						crcOffset := int64(ptr.Offset - 4)
						if _, err := writer.ReadAt(crc[:], crcOffset); err != nil {
							t.Fatal(err)
						}
						defer writer.WriteAt(crc[:], crcOffset)
						if _, err := writer.WriteAt([]byte{crc[0] ^ 1}, crcOffset); err != nil {
							t.Fatal(err)
						}
						if _, err := db.Get([]byte("a000")); !errors.Is(err, valuelog.ErrCorrupt) {
							t.Fatalf("cold corrupt record was admitted: %v", err)
						}
						if cowLeafCacheCounter(t, backend, "stores") != 0 {
							t.Fatal("failed cold decode populated cache")
						}
						if _, err := writer.WriteAt(crc[:], crcOffset); err != nil {
							t.Fatal(err)
						}
					}
					for i := 0; i < 4; i++ {
						got, err := db.Get([]byte("a000"))
						if err != nil || !bytes.Equal(got, want) {
							t.Fatalf("cold public read %d: %q %v", i, got, err)
						}
						got[0] ^= 1
					}
					stores := cowLeafCacheCounter(t, backend, "read_miss_admission_stores")
					hits := cowLeafCacheCounter(t, backend, "hits")
					if entries > 0 && (stores == 0 || hits == 0) {
						t.Fatalf("cold COW reads did not self-warm: admissions=%d hits=%d", stores, hits)
					}
					if entries < 0 && (stores != 0 || hits != 0) {
						t.Fatalf("disabled cache reported admission/hit: %d/%d", stores, hits)
					}
					// Owned output and iterator movement must retain the same values.
					if got, err := db.GetAppend([]byte("a000"), []byte("prefix")); err != nil || !bytes.Equal(got, append([]byte("prefix"), want...)) {
						t.Fatalf("owned append after admission: %q %v", got, err)
					}
					snapshot, err := db.acquireCOWSnapshotWithError()
					if err != nil {
						t.Fatal(err)
					}
					defer snapshot.Close()
					it, err := snapshot.Iterator([]byte("a000"), []byte("a001"))
					if err != nil {
						t.Fatal(err)
					}
					defer it.Close()
					if !it.Valid() || string(it.Key()) != "a000" || !bytes.Equal(it.Value(), want) {
						t.Fatal("iterator did not use the original logical cut")
					}
					it.Next()
					if it.Valid() || it.Error() != nil {
						t.Fatalf("iterator domain changed: %v", it.Error())
					}
				})
			}
		}
	}
}
