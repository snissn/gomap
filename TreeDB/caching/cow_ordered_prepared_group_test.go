package caching

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type cowOrderedPreparedVersion struct {
	key      []byte
	value    []byte
	revision page.EntryRevision
}

func cowOrderedPreparedGroup(t *testing.T, db *DB, owner *mvccadmission.Capability, versions []cowOrderedPreparedVersion) *Batch {
	t.Helper()
	b := db.NewBatchWithSize(len(versions))
	t.Cleanup(func() { _ = b.Close() })
	for _, version := range versions {
		// Derive the timestamp through the same test codec as ordinary Store
		// input; the native revision is intentionally an independent identity.
		_, timestamp, err := mvcckey.Decode(version.key)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.setWithMVCCInput(version.key, version.value, version.revision, owner.Input(version.key, timestamp, 0)); err != nil {
			t.Fatal(err)
		}
	}
	return b
}

func cowOrderedPreparedRequire(t *testing.T, snapshot *Snapshot, want []cowOrderedPreparedVersion, absent ...[]byte) {
	t.Helper()
	for _, version := range want {
		value, revision, err := snapshot.GetVersioned(version.key)
		if err != nil || !bytes.Equal(value, version.value) || revision != version.revision {
			t.Fatalf("captured version %x: value=%x want=%x revision=%d want=%d error=%v", version.key, value, version.value, revision, version.revision, err)
		}
	}
	for _, key := range absent {
		value, err := snapshot.Get(key)
		if !errors.Is(err, tree.ErrKeyNotFound) || value != nil {
			t.Fatalf("private version %x escaped: value=%x error=%v", key, value, err)
		}
	}
	iterator, err := snapshot.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.Close()
	count := 0
	for iterator.Valid() {
		count++
		iterator.Next()
	}
	if err := iterator.Error(); err != nil || count != len(want) {
		t.Fatalf("captured complete group: records=%d want=%d error=%v", count, len(want), err)
	}
}

func TestCOWOrderedUnorderedGroupKeepsDuplicateWinnerAndEntryOrder(t *testing.T) {
	db, owner := cowOrderedLifecycleFixture(t)
	last := cowOrderedLifecycleKey(t, "z-sort", 90)
	var first []byte
	for i := 0; i < 100; i++ {
		candidate := cowOrderedLifecycleKey(t, fmt.Sprintf("a-sort-%d", i), 50)
		if db.shardIndex(candidate) == db.shardIndex(last) {
			first = candidate
			break
		}
	}
	if first == nil {
		t.Fatal("no same-shard peer")
	}
	group := []cowOrderedPreparedVersion{
		{last, []byte{1, 'h'}, 900},
		{first, []byte{1, 'x'}, 1000},
		{cowOrderedLifecycleKey(t, "z-sort", 10), []byte{1, 'l'}, 2},
		{first, []byte{1, 'w'}, 3}, // Last entry wins despite its lower revision.
		{cowOrderedLifecycleKey(t, "z-sort", 50), []byte{2}, 7},
	}
	b := cowOrderedPreparedGroup(t, db, owner, group)
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
	if len(b.entries) != len(group) {
		t.Fatalf("original producer entry count=%d want=%d", len(b.entries), len(group))
	}
	for i, entry := range b.entries {
		if !bytes.Equal(entry.Key, group[i].key) || !bytes.Equal(entry.Value, group[i].value) || entry.Revision != group[i].revision {
			t.Fatalf("original producer entry %d reordered or changed", i)
		}
	}
	current := db.AcquireSnapshot()
	defer current.Close()
	cowOrderedPreparedRequire(t, current, []cowOrderedPreparedVersion{group[0], group[2], group[3], group[4]})
	logical, physical := 0, 0
	for _, shard := range current.cowCut.shards {
		keys, versions := shard.root.OrderedCounts()
		logical += keys
		physical += versions
	}
	if logical != 2 || physical != 4 {
		t.Fatalf("ordered postimage: logical=%d physical=%d", logical, physical)
	}
}

// Exercise an actual qualified batch, not a manually assembled raw COW root.
// Both logical keys and all their timestamps remain private until the one cut
// swap. Native revisions need not increase with application time or publication.
func TestCOWOrderedPreparedGroupCapturesCompleteCutsAndExplicitRevisions(t *testing.T) {
	db, owner := cowOrderedLifecycleFixture(t)
	first := "prepared-a"
	second := ""
	for i := 0; i < 100; i++ {
		candidate := fmt.Sprintf("prepared-peer-%d", i)
		if db.shardIndex(cowOrderedLifecycleKey(t, first, 50)) != db.shardIndex(cowOrderedLifecycleKey(t, candidate, 50)) {
			second = candidate
			break
		}
	}
	if second == "" {
		t.Fatal("fixture failed to cover two distinct ordered shards")
	}
	seed := []cowOrderedPreparedVersion{
		{cowOrderedLifecycleKey(t, first, 50), []byte{1, 'a'}, 900},
		{cowOrderedLifecycleKey(t, second, 50), []byte{1, 'b'}, 901},
	}
	seedBatch := cowOrderedPreparedGroup(t, db, owner, seed)
	if err := seedBatch.Write(); err != nil {
		t.Fatal(err)
	}
	old := db.AcquireSnapshot()
	defer old.Close()
	opening := old.cowCut
	group := []cowOrderedPreparedVersion{
		{cowOrderedLifecycleKey(t, first, 10), []byte{1, 'l'}, 1<<63 + 7},
		{seed[0].key, []byte{1, 'r'}, 5},
		{cowOrderedLifecycleKey(t, first, 100), []byte{2}, 3},
		{seed[1].key, []byte{1, 'p'}, 2},
		{cowOrderedLifecycleKey(t, second, 110), []byte{1}, 7},
	}
	b := cowOrderedPreparedGroup(t, db, owner, group)
	ready := make(chan *cowBatchPreparation, 1)
	resume := make(chan struct{})
	var resumeOnce sync.Once
	unpause := func() { resumeOnce.Do(func() { close(resume) }) }
	done := make(chan error, 1)
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Root == db.dir && event.Point == durabilitycut.AfterCOWCanonicalPreparation {
			ready <- b.cowState.prepared
			<-resume
		}
		return nil
	})
	defer restore()
	finished := false
	defer func() {
		unpause()
		if !finished {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("paused ordered writer did not finish cleanup")
			}
		}
	}()
	go func() { done <- b.Write() }()
	var prepared *cowBatchPreparation
	select {
	case prepared = <-ready:
	case err := <-done:
		finished = true
		t.Fatalf("write returned before canonical pause: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("write did not reach the canonical preparation boundary")
	}
	if prepared == nil || prepared.cut == nil || prepared.cut.opening != opening || prepared.cut.next == opening {
		t.Fatal("canonical preparation lacks a private successor of the captured cut")
	}
	logicalKeys, versions := 0, 0
	for _, shard := range prepared.cut.next.shards {
		keys, records := shard.root.OrderedCounts()
		logicalKeys += keys
		versions += records
	}
	if logicalKeys != 2 || versions != 5 {
		t.Fatalf("qualified group missed the ordered primary: logical=%d versions=%d", logicalKeys, versions)
	}
	// Snapshot capture and reads must finish while preparation still owns the
	// writer gate. They cannot wait for, or see a partial subset of, this group.
	during := db.AcquireSnapshot()
	defer during.Close()
	if during.cowCut != opening {
		t.Fatal("private canonical successor became the current read cut")
	}
	absent := [][]byte{group[0].key, group[2].key, group[4].key}
	cowOrderedPreparedRequire(t, old, seed, absent...)
	cowOrderedPreparedRequire(t, during, seed, absent...)
	unpause()
	select {
	case err := <-done:
		finished = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("prepared group did not finish publication")
	}
	current := db.AcquireSnapshot()
	defer current.Close()
	if current.cowCut == opening || current.cowCut.logicalSeq != opening.logicalSeq+1 {
		t.Fatal("group did not install exactly one new complete publication cut")
	}
	cowOrderedPreparedRequire(t, current, group)
	cowOrderedPreparedRequire(t, old, seed, absent...)
	cowOrderedPreparedRequire(t, during, seed, absent...)
}

func TestCOWOrderedPreparedGroupRefusalPreservesCutAndTransientCredits(t *testing.T) {
	db, owner := cowOrderedLifecycleFixture(t)
	seed := []cowOrderedPreparedVersion{{cowOrderedLifecycleKey(t, "refused-history", 50), []byte{1, 'a'}, 400}}
	seedBatch := cowOrderedPreparedGroup(t, db, owner, seed)
	if err := seedBatch.Write(); err != nil {
		t.Fatal(err)
	}
	if err := seedBatch.Close(); err != nil {
		t.Fatal(err)
	}
	old := db.AcquireSnapshot()
	defer old.Close()
	cowOrderedPreparedRequire(t, old, seed)
	opening := old.cowCut
	group := []cowOrderedPreparedVersion{
		{seed[0].key, []byte{1, 'r'}, 7},
		{cowOrderedLifecycleKey(t, "refused-history", 10), []byte{1, 'l'}, 800},
		{cowOrderedLifecycleKey(t, "refused-history", 90), []byte{2}, 3},
		{cowOrderedLifecycleKey(t, "refused-peer", 60), []byte{1, 'p'}, 2},
	}
	baseline := db.cow.budget.Stats()
	b := cowOrderedPreparedGroup(t, db, owner, group)
	refused := errors.New("refuse qualified ordered canonical finalizer")
	preparedCount := 0
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Root == db.dir && event.Point == durabilitycut.AfterCOWCanonicalPreparation {
			preparedCount++
			return refused
		}
		return nil
	})
	err := b.Write()
	restore()
	if !errors.Is(err, refused) || preparedCount != 1 || b.cowState.prepared != nil || b.cowState.accepted.Valid() {
		t.Fatalf("canonical refusal failed to cancel preparation: error=%v preparations=%d pending=%t accepted=%t", err, preparedCount, b.cowState.prepared != nil, b.cowState.accepted.Valid())
	}
	if db.cow.cut != opening {
		t.Fatal("refused ordered group replaced the original cut")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	after := db.cow.budget.Stats()
	// Generation-local copied metadata may stay conservatively charged. The
	// preparation's independent scratch, cut, and pointer leases must not.
	if after.ExternalBytes != baseline.ExternalBytes || after.ExternalLeases != baseline.ExternalLeases || after.ReservedBytes != baseline.ReservedBytes || after.Views != baseline.Views {
		t.Fatalf("refused group retained transient admission: before=%+v after=%+v", baseline, after)
	}
	absent := [][]byte{group[1].key, group[2].key, group[3].key}
	cowOrderedPreparedRequire(t, old, seed, absent...)
	retry := cowOrderedPreparedGroup(t, db, owner, group)
	if err := retry.Write(); err != nil {
		t.Fatalf("canceled group was not retryable under the same original authority: %v", err)
	}
	current := db.AcquireSnapshot()
	defer current.Close()
	cowOrderedPreparedRequire(t, current, group)
	cowOrderedPreparedRequire(t, old, seed, absent...)
}
