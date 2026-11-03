//go:build !windows

package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// Reopen a real dictionary producer: no decoded cache page, verification credit,
// decoder or dictionary definition is injected by the test.
func cowLeafMissFixture(t testing.TB, entries, keys int) (*DB, *backenddb.DB, []byte, page.LeafLogPtr) {
	return cowLeafMissFixtureWithLimits(t, entries, keys, memtable.COWLimits{})
}

func cowLeafMissFixtureWithLimits(t testing.TB, entries, keys int, limits memtable.COWLimits) (*DB, *backenddb.DB, []byte, page.LeafLogPtr) {
	t.Helper()
	original, want, ptr := cowSuccessorDictionaryLeafFixture(t)
	for i := 64; i < keys; i++ {
		if err := original.Set([]byte(fmt.Sprintf("a%03d", i)), want); err != nil {
			t.Fatal(err)
		}
	}
	if keys > 64 {
		if err := original.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	dir := filepath.Dir(original.dir)
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := dictdb.Open(filepath.Join(dir, "dictdb"), backenddb.Options{ChunkSize: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true, LeafPageReadCacheEntries: entries, ValueLog: backenddb.ValueLogOptions{DictLookup: func(id uint64) ([]byte, error) { return store.GetDictBytes(context.Background(), id) }}})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_ordered", COWMemtableLimits: limits, FlushThreshold: 1 << 30, IndexOuterLeavesInValueLog: true, ValueLogPointerThreshold: 256})
	if err != nil {
		_ = backend.Close()
		_ = store.Close()
		t.Fatal(err)
	}
	db.SetDictStore(store)
	t.Cleanup(func() {
		if !db.closing.Load() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	return db, backend, want, ptr
}

type cowLeafMissOutcome struct {
	key, value []byte
	found      bool
	err        error
}

// Holding only the existing decoder mutex lets all independent public read cuts
// perform real inspection and their first cache miss before any decoder runs.
func cowQueuedLeafReads(t *testing.T, db *DB, n int) []cowLeafMissOutcome {
	t.Helper()
	db.cow.physicalMu.Lock()
	var once sync.Once
	release := func() { once.Do(db.cow.physicalMu.Unlock) }
	done := make(chan cowLeafMissOutcome, n)
	received := 0
	t.Cleanup(func() {
		release()
		timeout := time.NewTimer(5 * time.Second)
		defer timeout.Stop()
		for received < n {
			select {
			case <-done:
				received++
			case <-timeout.C:
				t.Error("queued public leaf reads failed to join after release")
				return
			}
		}
	})
	for range n {
		go func() {
			s, err := db.AcquireMVCCReadCut()
			if err != nil {
				done <- cowLeafMissOutcome{err: err}
				return
			}
			k, v, found, err := s.SeekGEVersionRange([]byte("a000"), []byte("a001"))
			err = errors.Join(err, s.Close())
			done <- cowLeafMissOutcome{k, v, found, err}
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	var stacks []byte
	for {
		stacks = make([]byte, 1<<20)
		stacks = stacks[:runtime.Stack(stacks, true)]
		queued := 0
		for _, stack := range strings.Split(string(stacks), "\n\n") {
			if strings.Contains(stack, "readPhysicalLeafWithDictionary") && strings.Contains(stack, "[sync.Mutex.Lock]") {
				queued++
			}
		}
		if queued == n {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d public cuts reached shared decoder mutex\n%s", queued, n, stacks)
		}
		time.Sleep(time.Millisecond)
	}
	release()
	out := make([]cowLeafMissOutcome, 0, n)
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for received < n {
		select {
		case r := <-done:
			received++
			out = append(out, r)
		case <-timeout.C:
			t.Fatal("public leaf reads did not complete after mutex release")
		}
	}
	return out
}

func TestCOWVerifiedLeafMissCoalescesPublicCuts(t *testing.T) {
	for _, entries := range []int{128, -1} {
		t.Run(fmt.Sprint(entries), func(t *testing.T) {
			db, backend, want, _ := cowLeafMissFixture(t, entries, 64)
			if cowLeafCacheCounter(t, backend, "stores") != 0 {
				t.Fatal("fixture is not cold")
			}
			const readers = 8
			for _, r := range cowQueuedLeafReads(t, db, readers) {
				if r.err != nil || !r.found || string(r.key) != "a000" || !bytes.Equal(r.value, want) {
					t.Fatalf("read=%+v", r)
				}
				r.value[0] ^= 1
			}
			hits := cowLeafCacheCounter(t, backend, "hits")
			if entries > 0 {
				// Existing repeated-miss probation permits the first two real decodes.
				// Every remaining waiter must reuse that actually admitted verified page.
				if hits < readers-2 {
					t.Fatalf("queued misses redundantly decoded: hits=%d want>=%d", hits, readers-2)
				}
				if stores := cowLeafCacheCounter(t, backend, "read_miss_admission_stores"); stores != 1 {
					t.Fatalf("admitted pages=%d want 1", stores)
				}
			} else if hits != 0 || cowLeafCacheCounter(t, backend, "stores") != 0 {
				t.Fatal("disabled cache acquired an entry")
			}
			// Original caller results are owned, and closing all cuts refunds their views.
			s, err := db.AcquireMVCCReadCut()
			if err != nil {
				t.Fatal(err)
			}
			_, value, found, err := s.SeekGEVersionRange([]byte("a000"), []byte("a001"))
			if err != nil || !found || !bytes.Equal(value, want) {
				t.Fatalf("warm read %q/%t/%v", value, found, err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if got := db.COWMemoryStats().Views; got != 0 {
				t.Fatalf("views leaked: %d", got)
			}
		})
	}
}

func TestCOWVerifiedLeafMissFailureDoesNotPublish(t *testing.T) {
	db, backend, _, ptr := cowLeafMissFixture(t, 128, 64)
	file, _, err := db.cow.cut.basis.snapshot.PinnedValueLogFile(ptr.ValueLogFileID())
	if err != nil {
		t.Fatal(err)
	}
	writer, err := os.OpenFile(file.Name(), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	var saved [1]byte
	offset := int64(ptr.Offset - 4)
	if _, err := writer.ReadAt(saved[:], offset); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := writer.WriteAt(saved[:], offset); err != nil {
			t.Error(err)
		}
	}()
	if _, err := writer.WriteAt([]byte{saved[0] ^ 1}, offset); err != nil {
		t.Fatal(err)
	}
	for _, r := range cowQueuedLeafReads(t, db, 4) {
		if !errors.Is(r.err, valuelog.ErrCorrupt) {
			t.Fatalf("failed CRC read: %+v", r)
		}
	}
	if cowLeafCacheCounter(t, backend, "hits") != 0 || cowLeafCacheCounter(t, backend, "stores") != 0 {
		t.Fatal("failed decode published a verified page")
	}
	if _, err := writer.WriteAt(saved[:], offset); err != nil {
		t.Fatal(err)
	}
	// A decode error must release the mutex and leave the original owner usable.
	s, err := db.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	_, _, found, readErr := s.SeekGEVersionRange([]byte("a000"), []byte("a001"))
	if err := errors.Join(readErr, s.Close()); err != nil || !found {
		t.Fatalf("read after repair: found=%t err=%v", found, err)
	}
	if got := db.COWMemoryStats().Views; got != 0 {
		t.Fatalf("failed read views leaked: %d", got)
	}
}

// Sequential warm and cache-disabled guards include the public cut lifetime.
// Distinct reads cycle across many leaves. The one-entry setting resolves to
// the maintained minimum cache and forces repeated capacity misses.
func BenchmarkCOWVerifiedLeafMissLifecycle(b *testing.B) {
	for _, entries := range []int{128, -1, 1} {
		for _, distinct := range []bool{false, true} {
			b.Run(fmt.Sprintf("entries=%d/distinct=%t", entries, distinct), func(b *testing.B) {
				keys := 64
				if distinct {
					keys = 1024
				}
				db, backend, want, _ := cowLeafMissFixture(b, entries, keys)
				for i := 0; i < 128; i++ {
					s, err := db.AcquireMVCCReadCut()
					if err != nil {
						b.Fatal(err)
					}
					_, _, _, err = s.SeekGEVersionRange([]byte(fmt.Sprintf("a%03d", (i*32)%keys)), []byte("b"))
					err = errors.Join(err, s.Close())
					if err != nil {
						b.Fatal(err)
					}
				}
				hits := func() uint64 {
					v, err := strconv.ParseUint(backend.Stats()["treedb.process.read_path.outer_leaf.cache.hits"], 10, 64)
					if err != nil {
						b.Fatal(err)
					}
					return v
				}
				beforeHits := hits()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					k := 0
					if distinct {
						k = (i * 32) % keys
					}
					s, err := db.AcquireMVCCReadCut()
					if err != nil {
						b.Fatal(err)
					}
					_, v, found, err := s.SeekGEVersionRange([]byte(fmt.Sprintf("a%03d", k)), []byte("b"))
					err = errors.Join(err, s.Close())
					if err != nil || !found || !bytes.Equal(v, want) {
						b.Fatalf("read %t/%v", found, err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(hits()-beforeHits)/float64(b.N), "cache-hits/op")
			})
		}
	}
}
