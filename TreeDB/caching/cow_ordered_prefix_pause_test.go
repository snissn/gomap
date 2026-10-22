package caching

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type cowOrderedPrefixVersion struct {
	key      []byte
	value    []byte
	revision page.EntryRevision
}

func cowOrderedPrefixWrite(db *DB, owner *mvccadmission.Capability, versions []cowOrderedPrefixVersion) error {
	b := db.NewBatchWithSize(len(versions))
	var err error
	for _, version := range versions {
		_, timestamp, decodeErr := mvcckey.Decode(version.key)
		if decodeErr != nil {
			err = decodeErr
			break
		}
		if err = b.setWithMVCCInput(version.key, version.value, version.revision, owner.Input(version.key, timestamp, 0)); err != nil {
			break
		}
	}
	if err == nil {
		err = b.Write()
	}
	return errors.Join(err, b.Close())
}

func cowOrderedPrefixRequire(t *testing.T, snapshot *Snapshot, versions map[string]cowOrderedPrefixVersion) map[string]node.LeafEntry {
	t.Helper()
	entries := make(map[string]node.LeafEntry, len(versions))
	for key, want := range versions {
		entry, err := snapshot.GetEntryExact([]byte(key))
		if err != nil || !bytes.Equal(entry.Key, want.key) || entry.Revision != want.revision {
			t.Fatalf("physical projection key=%x entry=%+v want revision=%d error=%v", key, entry, want.revision, err)
		}
		logical, timestamp, err := mvcckey.Decode(entry.Key)
		wantLogical, wantTimestamp, wantErr := mvcckey.Decode(want.key)
		if err != nil || wantErr != nil || timestamp != wantTimestamp || !bytes.Equal(logical, wantLogical) {
			t.Fatalf("logical timestamp projection key=%x timestamp=%d want=%d error=%v", key, timestamp, wantTimestamp, err)
		}
		value, revision, err := snapshot.GetVersioned(want.key)
		if err != nil || !bytes.Equal(value, want.value) || revision != want.revision {
			t.Fatalf("versioned projection key=%x value=%x revision=%d want=%x/%d error=%v", key, value, revision, want.value, want.revision, err)
		}
		if entry.Flags != node.FlagInline && entry.Flags != node.FlagPointer {
			t.Fatalf("logical version became a physical erase or lost native flags: %+v", entry)
		}
		entries[key] = entry
	}
	it, err := snapshot.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	seen := make(map[string]bool, len(versions))
	var previous []byte
	for ; it.Valid(); it.Next() {
		key := bytes.Clone(it.Key())
		want, found := versions[string(key)]
		if !found || seen[string(key)] || (previous != nil && bytes.Compare(previous, key) >= 0) || !bytes.Equal(it.Value(), want.value) {
			t.Fatalf("physical scan lost exact ordered history: key=%x found=%t duplicate=%t", key, found, seen[string(key)])
		}
		seen[string(key)] = true
		previous = key
	}
	if err := it.Error(); err != nil || len(seen) != len(versions) {
		t.Fatalf("physical history scan rows=%d want=%d error=%v", len(seen), len(versions), err)
	}
	return entries
}

func cowOrderedPrefixBackendRequire(t *testing.T, backend *backenddb.DB, versions map[string]cowOrderedPrefixVersion) {
	t.Helper()
	snapshot := backend.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("backend metadata snapshot unavailable")
	}
	defer snapshot.Close()
	for key, want := range versions {
		entry, err := snapshot.GetEntryExact(want.key)
		if err != nil || !bytes.Equal(entry.Key, want.key) || entry.Revision != want.revision || entry.Flags&node.FlagTombstone != 0 {
			t.Fatalf("backend physical metadata key=%x entry=%+v want revision=%d error=%v", key, entry, want.revision, err)
		}
		value, revision, err := snapshot.GetVersioned(want.key)
		if err != nil || !bytes.Equal(value, want.value) || revision != want.revision {
			t.Fatalf("backend versioned history key=%x revision=%d want=%d error=%v", key, revision, want.revision, err)
		}
	}
}

func TestCOWOrderedPausedPrivatePrefixPreservesMultiTimestampTail(t *testing.T) {
	db, owner := cowOrderedLifecycleFixture(t)
	key := func(logical string, timestamp uint64) []byte { return cowOrderedLifecycleKey(t, logical, timestamp) }
	const logical = "shared\x00binary"
	k50, k75, k100 := key(logical, 50), key(logical, 75), key(logical, 100)
	kHigh, kLater, kFull := key(logical, 1<<63+150), key(logical, 1<<63+200), key(logical, ^uint64(0)-1)
	other, late := key("other", 70), key("tail-only", 1)
	const revisionBase = page.EntryRevision(1 << 40)
	version := func(k []byte, text string, revision uint64) cowOrderedPrefixVersion {
		return cowOrderedPrefixVersion{key: k, value: append([]byte{1}, bytes.Repeat([]byte(text), 1024)...), revision: revisionBase + page.EntryRevision(revision)}
	}
	base := []cowOrderedPrefixVersion{version(k50, "base50", 1), version(k100, "base100", 2), version(other, "base-other", 3)}
	if err := cowOrderedPrefixWrite(db, owner, base); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	backend := &cowChunkPauseBackend{cowAcceptedReportBackend: &cowAcceptedReportBackend{DB: db.backend.(*backenddb.DB)}, paused: make(chan struct{}), resume: make(chan struct{})}
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(backend.resume) }) }
	defer resume()
	db.backend = backend
	db.flushBackendMaxEntries = 1
	first := []cowOrderedPrefixVersion{version(k50, "prefix50", 11), version(k75, "backfill75", 12), version(k100, "prefix100-first", 13)}
	if err := cowOrderedPrefixWrite(db, owner, first); err != nil {
		t.Fatal(err)
	}
	if err := db.rolloverCOWForFlush(); err != nil {
		t.Fatal(err)
	}
	second := []cowOrderedPrefixVersion{version(k100, "prefix100-second", 21), version(kHigh, "prefix-high", 22), {key: other, value: []byte{2}, revision: revisionBase + 23}}
	if err := cowOrderedPrefixWrite(db, owner, second); err != nil {
		t.Fatal(err)
	}
	if err := db.rolloverCOWForFlush(); err != nil {
		t.Fatal(err)
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	wantOld := make(map[string]cowOrderedPrefixVersion)
	for _, group := range [][]cowOrderedPrefixVersion{base, first, second} {
		for _, entry := range group {
			wantOld[string(entry.key)] = entry
		}
	}
	metadata := cowOrderedPrefixRequire(t, old, wantOld)
	covered := append([]cowTable(nil), old.cowCut.frozen...)
	if len(covered) < 2 {
		t.Fatal("fixture did not capture multiple ordered frozen sources")
	}
	orderedVersions := 0
	for _, table := range covered {
		_, versions := table.root.OrderedCounts()
		orderedVersions += versions
	}
	if orderedVersions < len(wantOld) {
		t.Fatalf("qualified multi-timestamp prefix did not activate: versions=%d", orderedVersions)
	}
	before := backend.State().CommitSeq
	done := make(chan error, 1)
	go func() {
		db.flushMu.Lock()
		defer db.flushMu.Unlock()
		done <- db.flushCOWFrozen(true, nil)
	}()
	finished := false
	defer func() {
		resume()
		if !finished {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("paused prefix cleanup did not finish")
			}
		}
	}()
	select {
	case <-backend.paused:
	case err := <-done:
		finished = true
		t.Fatalf("prefix completed before an intermediate chunk pause: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("ordered streamed chunk did not pause")
	}
	if backend.State().CommitSeq != before || backend.publications.Load() != 0 {
		t.Fatal("private intermediate ordered chunk published a backend root")
	}
	baseMap := make(map[string]cowOrderedPrefixVersion, len(base))
	for _, entry := range base {
		baseMap[string(entry.key)] = entry
	}
	cowOrderedPrefixBackendRequire(t, backend.DB, baseMap)
	for _, entry := range [][]byte{k75, kHigh} {
		if found, err := backend.Has(entry); err != nil || found {
			t.Fatalf("intermediate chunk exposed prefix-only key=%x found=%t error=%v", entry, found, err)
		}
	}
	frozenTail := []cowOrderedPrefixVersion{version(k100, "tail-frozen100", 31), version(kLater, "tail-later", 32), version(late, "late-frozen", 33)}
	if err := cowOrderedPrefixWrite(db, owner, frozenTail); err != nil {
		t.Fatal(err)
	}
	if err := db.rolloverCOWForFlush(); err != nil {
		t.Fatal(err)
	}
	mutableTail := []cowOrderedPrefixVersion{version(k100, "tail-mutable100", 41), version(kFull, "tail-full-width", 42)}
	if err := cowOrderedPrefixWrite(db, owner, mutableTail); err != nil {
		t.Fatal(err)
	}
	tailSources := append([]cowTable(nil), db.cow.cut.frozen[len(covered):]...)
	if len(tailSources) == 0 || !db.SupportsLogicalMVCC() {
		t.Fatal("new frozen tail did not retain the ordered successor")
	}
	again := cowOrderedPrefixRequire(t, old, wantOld)
	for key, entry := range metadata {
		now := again[key]
		if !bytes.Equal(now.Key, entry.Key) || !bytes.Equal(now.Value, entry.Value) || now.Flags != entry.Flags || now.ValuePtr != entry.ValuePtr || now.Revision != entry.Revision {
			t.Fatalf("paused old-cut native metadata changed for %x", key)
		}
	}
	if backend.State().CommitSeq != before || backend.publications.Load() != 0 {
		t.Fatal("concurrent tail commits published an intermediate prefix")
	}
	resume()
	select {
	case err := <-done:
		finished = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ordered prefix handoff did not finish")
	}
	if backend.publications.Load() != 1 || backend.State().CommitSeq != before+1 {
		t.Fatal("ordered captured prefix did not publish exactly once")
	}
	if len(db.cow.cut.frozen) != len(tailSources) {
		t.Fatal("handoff did not replace exactly the captured source prefix")
	}
	for index, source := range tailSources {
		if db.cow.cut.frozen[index].root != source.root {
			t.Fatal("handoff replaced a newer frozen source")
		}
	}
	wantCurrent := make(map[string]cowOrderedPrefixVersion)
	for _, group := range [][]cowOrderedPrefixVersion{base, first, second, frozenTail, mutableTail} {
		for _, entry := range group {
			wantCurrent[string(entry.key)] = entry
		}
	}
	current, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	cowOrderedPrefixRequire(t, current, wantCurrent)
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	cowOrderedPrefixRequire(t, old, wantOld)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if backend.publications.Load() != 2 || backend.State().CommitSeq != before+2 || len(db.cow.cut.frozen) != 0 || db.cow.handoff != nil {
		t.Fatal("final tail drain replayed the covered prefix or lost handoff progress")
	}
	cowOrderedPrefixBackendRequire(t, backend.DB, wantCurrent)
	cowOrderedPrefixRequire(t, old, wantOld)
}

func TestCOWOrderedHeldCutPressureRefusesBeforeAcceptAndReleaseProgresses(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend"), DisableBackgroundPrune: true, ValueLog: backenddb.ValueLogOptions{PointerThreshold: 1 << 30}})
	if err != nil {
		t.Fatal(err)
	}
	limits := memtable.DefaultCOWLimits()
	limits.MaxTotalBytes, limits.MaxRetiredBytes, limits.MaxGenerationBytes = 64<<20, 64<<20, 8<<20
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_ordered", MemtableShards: 4,
		COWMemtableLimits: limits, FlushThreshold: 1 << 30, ValueLogPointerThreshold: 1 << 30, ValueLogCompression: uint8(vlogCompressionOff)})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	defer db.Close()
	owner := backend.NewMVCCAdmission()
	if owner == nil || !db.SupportsLogicalMVCC() {
		t.Fatal("ordered Store route did not activate")
	}
	defer owner.Close()
	var entries []cowOrderedPrefixVersion
	var counts [4]int
	for candidate := 0; len(entries) < 128 && candidate < 4096; candidate++ {
		logical := fmt.Sprintf("pressure-history/%04d", candidate)
		key := cowOrderedLifecycleKey(t, logical, 17)
		shard := db.shardIndex(key)
		if counts[shard] == 32 {
			continue
		}
		for _, timestamp := range []uint64{17, 1<<63 + 31} {
			entries = append(entries, cowOrderedPrefixVersion{key: cowOrderedLifecycleKey(t, logical, timestamp), value: append([]byte{1}, bytes.Repeat([]byte("a"), 1024)...), revision: page.EntryRevision(100 + timestamp%32)})
			counts[shard]++
		}
	}
	if len(entries) != 128 {
		t.Fatal("could not construct balanced ordered histories")
	}
	for round := range 4 {
		for index := range entries {
			entries[index].revision = page.EntryRevision(200 + round)
		}
		if err := cowOrderedPrefixWrite(db, owner, entries); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.rolloverCOWForFlush(); err != nil {
		t.Fatal(err)
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	wantOld := make(map[string]cowOrderedPrefixVersion, len(entries))
	for _, entry := range entries {
		wantOld[string(entry.key)] = entry
	}
	cowOrderedPrefixRequire(t, old, wantOld)
	b := db.NewBatchWithSize(len(entries))
	defer b.Close()
	groups := make([][]memtable.COWMutation, 4)
	for _, entry := range entries {
		_, timestamp, _ := mvcckey.Decode(entry.key)
		value := append([]byte{1}, bytes.Repeat([]byte("z"), 1024)...)
		if err := b.setWithMVCCInput(entry.key, value, 999, owner.Input(entry.key, timestamp, 0)); err != nil {
			t.Fatal(err)
		}
		shard := db.shardIndex(entry.key)
		groups[shard] = append(groups[shard], memtable.COWMutation{Key: entry.key, Value: value, Flags: node.FlagInline, MVCC: true, Timestamp: timestamp, Revision: 999})
	}
	var total, largest uint64
	for index, group := range groups {
		charge, err := db.cow.writers[index].Estimate(group, memtable.COWPrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		total += charge.Total()
		largest = max(largest, charge.Total())
	}
	stats := db.COWMemoryStats()
	// Use actual retained charge and the original per-shard estimator to
	// expose aggregate refusal. The legitimate pressure lease never changes
	// limits and stays held across release/retry.
	headroom := max(largest+8192, total-min(total/2, stats.HistoryBytes/2))
	if headroom >= total || headroom >= stats.HistoryBytes || stats.TotalBytes+headroom >= limits.MaxTotalBytes {
		t.Fatalf("invalid finite ordered pressure setup total=%d largest=%d headroom=%d stats=%+v", total, largest, headroom, stats)
	}
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := db.cow.budget.AcquireExternal(limits.MaxTotalBytes - stats.TotalBytes - overhead - headroom)
	if err != nil {
		t.Fatal(err)
	}
	defer pressure.Close()
	sequence, checkpoints := db.cow.cut.logicalSeq, db.checkpointRuns.Load()
	err = b.Write()
	if !errors.Is(err, memtable.ErrCOWCapacity) || b.COWReceipt().Assigned || b.COWReceipt().LogicalInstalled || db.cow.cut.logicalSeq != sequence {
		t.Fatalf("retained history pressure accepted/refinalized group: receipt=%+v seq=%d error=%v", b.COWReceipt(), db.cow.cut.logicalSeq, err)
	}
	if db.checkpointRuns.Load() != checkpoints+1 {
		t.Fatal("ordered aggregate pressure did not join one original checkpoint")
	}
	cowOrderedPrefixRequire(t, old, wantOld)
	beforeRelease := db.COWMemoryStats()
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	afterRelease := db.COWMemoryStats()
	if afterRelease.HistoryBytes >= beforeRelease.HistoryBytes || afterRelease.TotalBytes >= beforeRelease.TotalBytes {
		t.Fatalf("last ordered cut did not release its original cohort: before=%+v after=%+v", beforeRelease, afterRelease)
	}
	if err := b.Write(); err != nil || b.COWReceipt().Assigned || !b.COWReceipt().LogicalInstalled || db.cow.cut.logicalSeq != sequence+1 {
		t.Fatalf("same unaccepted batch did not progress after release: receipt=%+v seq=%d error=%v", b.COWReceipt(), db.cow.cut.logicalSeq, err)
	}
	if db.cow.budget.Limits() != limits {
		t.Fatal("pressure progress changed original finite limits")
	}
	pressure.Close()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	wantRetry := make(map[string]cowOrderedPrefixVersion, len(entries))
	for _, entry := range entries {
		entry.value = append([]byte{1}, bytes.Repeat([]byte("z"), 1024)...)
		entry.revision = 999
		wantRetry[string(entry.key)] = entry
	}
	cowOrderedPrefixBackendRequire(t, backend, wantRetry)
}
