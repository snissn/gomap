//go:build mvcc_native_prune && treedb_test

package mvcc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// This optional harness deliberately requires the bounded API. Pre-M7 tagged
// builds must fail compilation; there is no unbounded fallback.
type nativePruneCase struct {
	Name, Order    string
	Profile        treedb.Profile
	History, Extra int
}

func nativePruneCases(smoke bool) []nativePruneCase {
	if smoke {
		return []nativePruneCase{{"smoke/ascending/8", "ascending", treedb.ProfileCommandWALDurable, 8, 0}}
	}
	var cases []nativePruneCase
	for _, order := range []string{"shuffled", "ascending"} {
		for _, n := range []int{512, 1024} {
			cases = append(cases, nativePruneCase{fmt.Sprintf("ordered/%s/%d", order, n), order, treedb.ProfileCommandWALDurable, n, 0})
		}
	}
	for _, p := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileNoWALFast} {
		for _, n := range []int{512, 4096} {
			cases = append(cases, nativePruneCase{fmt.Sprintf("queued/%s/%d", p, n), "pairwise", p, 1024, n})
		}
	}
	return cases
}

type nativePruneResult struct {
	Case                                                                           string         `json:"case"`
	Profile                                                                        treedb.Profile `json:"profile"`
	Order                                                                          string         `json:"order"`
	History                                                                        int            `json:"history"`
	Extra                                                                          int            `json:"extra"`
	Calls, Records, Bytes, Visited, SetupRecords, CleanupRecords, Deletes, Batches uint64
	MaxRecords, MaxBytes                                                           uint64
	SetupNS, ColdOpenNS, PassNS, MaxQuantumNS, ACKReopenNS, CursorCloseNS, CloseNS int64
	AllocBytes, Allocs                                                             uint64
	Complete, Oracle, CursorClosed, FirstACKReopened                               bool
	AllocationScope                                                                string
	Error                                                                          string
}

// BenchmarkNativePruneFixedQ runs one fresh complete pass per process. Setup,
// cold open, cursor close and final DB Close have separate wall-time receipts.
func BenchmarkNativePruneFixedQ(b *testing.B) {
	selected := os.Getenv("MVCC_NATIVE_CASE")
	for _, c := range nativePruneCases(os.Getenv("MVCC_NATIVE_SMOKE") == "1") {
		if selected != "" && selected != c.Name {
			continue
		}
		b.Run(c.Name, func(b *testing.B) { benchmarkNativePrune(b, c) })
	}
}

func benchmarkNativePrune(b *testing.B, c nativePruneCase) {
	if b.N != 1 {
		b.Fatal("native evidence requires -benchtime=1x, one fresh process per subcase")
	}
	b.StopTimer()
	r := nativePruneResult{Case: c.Name, Profile: c.Profile, Order: c.Order, History: c.History, Extra: c.Extra, AllocationScope: "combined_process_during_pass"}
	out := os.Getenv("MVCC_NATIVE_RESULT")
	if out == "" {
		b.Fatal("MVCC_NATIVE_RESULT must name a new result file")
	}
	defer func() {
		data, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			b.Error(err)
			return
		}
		f, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			b.Error(err)
			return
		}
		_, err = f.Write(append(data, '\n'))
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			b.Errorf("result: %v %v", err, closeErr)
		}
	}()
	fail := func(err error) { r.Error = err.Error(); b.Fatal(err) }
	dir := filepath.Join(b.TempDir(), "db")
	options := treedb.OptionsFor(c.Profile, dir)
	if c.Extra != 0 {
		options.IndexOuterLeavesInValueLog = true
		options.ValueLog.PointerThreshold = 1
	}
	start := time.Now()
	db, err := treedb.Open(options)
	if err != nil {
		fail(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	store := New(db)
	key := []byte("ordered")
	futureKey := append([]byte("future/"), bytes.Repeat([]byte("k"), 900)...)
	payload := bytes.Repeat([]byte("old-pointer"), 64)
	if c.Extra == 0 {
		for i := 1; i <= c.History; i++ {
			ts := uint64(i)
			if c.Order == "shuffled" {
				ts = uint64(((i * 2053) % c.History) + 1)
			}
			if err := store.CommitAt(ts, []Mutation{{Key: key, Value: []byte("v")}}, CommitRelaxed); err != nil {
				fail(err)
			}
		}
		if err := store.AdvanceDiscardFloor(uint64(c.History), CommitRelaxed); err != nil {
			fail(err)
		}
	} else {
		key = []byte("gone")
		batch := db.NewBatchWithSize(c.History)
		for ts := uint64(1); ts <= uint64(c.History); ts++ {
			physical, err := mvcckey.Encode(key, ts)
			if err != nil {
				fail(err)
			}
			record := append([]byte{recordValueV1}, payload...)
			if ts == uint64(c.History) {
				record = []byte{recordTombstoneV1}
			}
			if err := batch.Set(physical, record); err != nil {
				fail(err)
			}
		}
		if err := batch.Write(); err != nil {
			fail(err)
		}
		if err := batch.Close(); err != nil {
			fail(err)
		}
		if err := store.AdvanceDiscardFloor(uint64(c.History), CommitDurable); err != nil {
			fail(err)
		}
		if err := db.Checkpoint(); err != nil {
			fail(err)
		}
		if err := db.Close(); err != nil {
			fail(err)
		}
		cold := time.Now()
		db, err = treedb.Open(options)
		r.ColdOpenNS = time.Since(cold).Nanoseconds()
		if err != nil {
			fail(err)
		}
		release, err := db.HoldQueuedSourcesForTest()
		if err != nil {
			fail(err)
		}
		defer release()
		oldKey, err := mvcckey.Encode(key, uint64(c.History-1))
		if err != nil {
			fail(err)
		}
		oldValue := append([]byte{recordValueV1}, payload...)
		if err := db.Set(oldKey, oldValue); err != nil {
			fail(err)
		}
		oldReader := db.AcquireSnapshot()
		if oldReader == nil {
			fail(fmt.Errorf("missing queued old reader"))
		}
		defer oldReader.Close()
		var readers []treedb.Snapshot
		defer func() {
			for _, reader := range readers {
				_ = reader.Close()
			}
		}()
		for i := 0; i < c.Extra; i++ {
			ts := uint64(c.History + (i/2)*2 + 2 - i%2)
			physical, err := mvcckey.Encode(futureKey, ts)
			if err != nil {
				fail(err)
			}
			if err := db.Set(physical, []byte{recordValueV1, 'v'}); err != nil {
				fail(err)
			}
			if (i+1)%512 == 0 && i+1 < c.Extra {
				reader := db.AcquireSnapshot()
				if reader == nil {
					fail(fmt.Errorf("missing queued reader"))
				}
				readers = append(readers, reader)
			}
		}
		old, err := oldReader.Get(oldKey)
		if err != nil || !bytes.Equal(old, oldValue) {
			fail(fmt.Errorf("old pointer oracle: %v", err))
		}
		if err := oldReader.Close(); err != nil {
			fail(err)
		}
		for _, reader := range readers {
			if err := reader.Close(); err != nil {
				fail(err)
			}
		}
		readers = nil
		queue, err := strconv.ParseUint(db.Stats()["treedb.cache.queue_len"], 10, 64)
		if err != nil || queue == 0 {
			fail(fmt.Errorf("queued fixture lost: %d %v", queue, err))
		}
		store = New(db) // Leave the reopened floor cold, as in the original fixture.
		release()
	}
	r.SetupNS = time.Since(start).Nanoseconds()
	capCalls := (c.History + c.Extra) * 16
	var cursor *PruneCursor
	defer func() {
		if cursor != nil {
			_ = cursor.Close()
		}
	}()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	b.ReportAllocs()
	b.StartTimer()
	pass := time.Now()
	for q := 0; q < capCalls; q++ {
		quantum := time.Now()
		stats, err := store.PruneVersions(PruneOptions{Mode: CommitDurable, BatchSize: 1, WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
		elapsed := time.Since(quantum).Nanoseconds()
		r.Calls++
		r.Records += stats.WorkRecords
		r.Bytes += stats.WorkBytes
		r.Visited += stats.Visited
		r.SetupRecords += stats.SetupRecords
		r.CleanupRecords += stats.CleanupRecords
		r.Deletes += stats.Pruned
		r.Batches += stats.Batches
		r.MaxQuantumNS = max(r.MaxQuantumNS, elapsed)
		r.MaxRecords = max(r.MaxRecords, stats.WorkRecords)
		r.MaxBytes = max(r.MaxBytes, stats.WorkBytes)
		if stats.WorkRecords > 32 || stats.WorkBytes > 1<<20 {
			fail(fmt.Errorf("unadmitted quantum %d", q))
		}
		if err != nil {
			fail(fmt.Errorf("quantum %d: %w", q, err))
		}
		cursor = stats.Cursor
		if c.Extra != 0 && stats.Pruned != 0 && !r.FirstACKReopened {
			reopen := time.Now()
			if err := db.Close(); err != nil {
				fail(err)
			}
			db, err = treedb.Open(options)
			if err != nil {
				fail(err)
			}
			store = New(db)
			cursor = nil
			r.FirstACKReopened = true
			r.ACKReopenNS = time.Since(reopen).Nanoseconds()
			continue
		}
		if stats.Complete {
			r.Complete = true
			break
		}
		if cursor == nil {
			fail(fmt.Errorf("missing continuation at quantum %d", q))
		}
	}
	r.PassNS = time.Since(pass).Nanoseconds()
	b.StopTimer()
	runtime.ReadMemStats(&after)
	r.AllocBytes = after.TotalAlloc - before.TotalAlloc
	r.Allocs = after.Mallocs - before.Mallocs
	if !r.Complete {
		fail(fmt.Errorf("original %d-call cap exhausted", capCalls))
	}
	want := uint64(c.History - 1)
	if c.Extra != 0 {
		want = uint64(c.History)
	}
	if r.Deletes != want || r.Batches != want {
		fail(fmt.Errorf("ACK counts deletes=%d batches=%d want=%d", r.Deletes, r.Batches, want))
	}
	if c.Extra == 0 && r.Records > uint64(c.History)*512 {
		fail(fmt.Errorf("original charged-record cap exceeded"))
	}
	result, err := store.GetAt(key, uint64(c.History+1))
	if err != nil {
		fail(err)
	}
	if c.Extra == 0 {
		if result.State != Present || result.Timestamp != uint64(c.History) || !bytes.Equal(result.Value, []byte("v")) {
			fail(fmt.Errorf("anchor oracle failed"))
		}
	} else {
		if !r.FirstACKReopened || result.State != Absent {
			fail(fmt.Errorf("anchor retirement/reopen oracle failed"))
		}
		for i := 1; i <= c.Extra; i++ {
			result, err := store.GetAt(futureKey, uint64(c.History+i))
			if err != nil || result.State != Present || result.Timestamp != uint64(c.History+i) || !bytes.Equal(result.Value, []byte("v")) {
				fail(fmt.Errorf("future oracle %d: %v", i, err))
			}
		}
	}
	it, err := store.IterateVersions(VersionIteratorOptions{})
	if err != nil {
		fail(err)
	}
	var remaining uint64
	for it.Valid() {
		entry := it.Entry()
		if c.Extra == 0 {
			if !bytes.Equal(entry.Key, key) || entry.Timestamp != uint64(c.History) || entry.State != Present || !bytes.Equal(entry.Value, []byte("v")) {
				_ = it.Close()
				fail(fmt.Errorf("physical anchor oracle failed"))
			}
		} else if !bytes.Equal(entry.Key, futureKey) || entry.Timestamp <= uint64(c.History) || entry.Timestamp > uint64(c.History+c.Extra) || entry.State != Present || !bytes.Equal(entry.Value, []byte("v")) {
			_ = it.Close()
			fail(fmt.Errorf("physical future oracle failed"))
		}
		remaining++
		it.Next()
	}
	iterErr := it.Error()
	closeErr := it.Close()
	wantRemaining := uint64(1)
	if c.Extra != 0 {
		wantRemaining = uint64(c.Extra)
	}
	if iterErr != nil || closeErr != nil || remaining != wantRemaining {
		fail(fmt.Errorf("remaining=%d want=%d iterator=%v close=%v", remaining, wantRemaining, iterErr, closeErr))
	}
	r.Oracle = true
	closeStart := time.Now()
	if cursor != nil {
		if err := cursor.Close(); err != nil {
			fail(err)
		}
		cursor = nil
	}
	r.CursorCloseNS = time.Since(closeStart).Nanoseconds()
	r.CursorClosed = true
	closeStart = time.Now()
	if err := db.Close(); err != nil {
		fail(err)
	}
	db = nil
	r.CloseNS = time.Since(closeStart).Nanoseconds()
	b.ReportMetric(float64(r.Calls), "calls/pass")
	b.ReportMetric(float64(r.MaxQuantumNS), "max_quantum_ns")
}
