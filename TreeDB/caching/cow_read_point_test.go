package caching

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func cowPointReadFixture(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend"), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{
		DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_ordered", MemtableShards: 2,
		FlushThreshold: 1 << 30, IndexOuterLeavesInValueLog: true,
		ValueLogPointerThreshold: 256, ValueLogCompression: uint8(vlogCompressionOff),
	})
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
	return db
}

// Admitted point reads preserve values, revisions, and caller ownership across
// publication and checkpoint. Inline reads borrow the retained cut; their
// distinct view-admission behavior is covered by the pressure controls.
func TestCOWPublicPointReadOwnedRevisionAndCuts(t *testing.T) {
	db := cowPointReadFixture(t)
	keys := [][]byte{nil, {}, {0}, {'a', 0, 'b'}, {255, 0}, []byte("pointer"), []byte("gone")}
	values := [][]byte{nil, {}, []byte("zero"), bytes.Repeat([]byte{3}, 128), []byte("binary"), bytes.Repeat([]byte("pointer-value"), 512), []byte("deleted")}
	batch := db.NewBatch()
	for i, key := range keys {
		if err := batch.SetWithRevision(key, values[i], page.EntryRevision(20+i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	check := func() {
		t.Helper()
		for i, key := range keys {
			if i == len(keys)-1 {
				continue
			}
			wantRevision := page.EntryRevision(20 + i)
			if len(key) == 0 { // nil and empty denote the same raw key.
				wantRevision = 21
			}
			got, revision, err := db.GetVersioned(key)
			if err != nil || !bytes.Equal(got, values[i]) || got == nil || revision != wantRevision {
				t.Fatalf("GetVersioned(%x)=%x/%d/%v", key, got, revision, err)
			}
			owned := append([]byte(nil), got...)
			prefix := []byte("prefix:")
			dst := make([]byte, len(prefix), len(prefix)+len(got))
			copy(dst, prefix)
			out, rev, err := db.GetVersionedAppend(key, dst)
			if err != nil || rev != revision || !bytes.Equal(out, append(prefix, values[i]...)) {
				t.Fatalf("GetVersionedAppend(%x)=%x/%d/%v", key, out, rev, err)
			}
			if len(got) > 0 {
				got[0] ^= 255
				out[len(prefix)] ^= 127
			}
			again, err := db.Get(key)
			if err != nil || !bytes.Equal(again, owned) {
				t.Fatalf("caller output aliased scratch for %x: %v", key, err)
			}
		}
		for _, key := range [][]byte{[]byte("missing"), []byte("gone")} {
			got, err := db.Get(key)
			if got != nil || err != nil {
				t.Fatalf("Get miss=%x/%v", got, err)
			}
			dst := []byte("keep")
			out, err := db.GetAppend(key, dst)
			if !errors.Is(err, tree.ErrKeyNotFound) || !bytes.Equal(out, dst) {
				t.Fatalf("GetAppend miss=%q/%v", out, err)
			}
		}
	}
	if err := db.Delete([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	check() // dirty inline/pointer and tombstone
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	check() // backend inline/pointer and physical miss
	if err := db.Set([]byte("pointer"), []byte("new inline")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got, err := old.Get([]byte("pointer")); err != nil || !bytes.Equal(got, values[5]) {
		t.Fatalf("held cut changed: %d/%v", len(got), err)
	}
	if got, err := db.Get([]byte("pointer")); err != nil || string(got) != "new inline" {
		t.Fatalf("current cut: %q/%v", got, err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GetAppend(nil, []byte("keep")); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("post-Close GetAppend: %v", err)
	}
	if stats := db.COWMemoryStats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 || stats.Views != 0 {
		t.Fatalf("final read owners not drained: %+v", stats)
	}
}

func TestCOWPublicPointReadFiniteAdmissionAndReuse(t *testing.T) {
	db := cowPointReadFixture(t)
	if err := db.Set([]byte("inline"), bytes.Repeat([]byte{7}, 128)); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	budget := db.cow.budget
	before := budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 4, 132)
	copy(dst, "keep")
	out, err := db.GetAppend([]byte("inline"), dst)
	if !errors.Is(err, memtable.ErrCOWCapacity) || !bytes.Equal(out, dst) {
		t.Fatalf("cold scratch must refuse before allocation: %q/%v", out, err)
	}
	pressure.Close()
	if _, err := db.GetAppend([]byte("inline"), dst); err != nil {
		t.Fatalf("retry after capacity release: %v", err)
	}
	before = budget.Stats()
	for i := 0; i < 100; i++ {
		if out, err := db.GetAppend([]byte("inline"), dst); err != nil || len(out) != 132 {
			t.Fatalf("reuse: %d/%v", len(out), err)
		}
	}
	after := budget.Stats()
	if after.TotalBytes != before.TotalBytes || after.ExternalLeases != before.ExternalLeases || after.Views != before.Views {
		t.Fatalf("point reads retained per-operation scratch: before=%+v after=%+v", before, after)
	}
}

func TestCOWPublicPointReadConcurrentPublicationAndClose(t *testing.T) {
	db := cowPointReadFixture(t)
	key := []byte("value")
	if err := db.Set(key, bytes.Repeat([]byte{1}, 128)); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errorsSeen := make(chan error, 5)
	start := make(chan struct{})
	for reader := 0; reader < 4; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			dst := make([]byte, 0, 4096)
			for i := 0; i < 150; i++ {
				got, err := db.GetAppend(key, dst)
				if errors.Is(err, backenddb.ErrClosed) {
					return
				}
				if err != nil || (len(got) != 128 && len(got) != 4096) || !bytes.Equal(got, bytes.Repeat(got[:1], len(got))) {
					errorsSeen <- fmt.Errorf("partial publication: len=%d err=%v", len(got), err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 12; i++ {
			size := 128
			if i%2 != 0 {
				size = 4096 // exercise the original pointer materializer as well
			}
			if err := db.Set(key, bytes.Repeat([]byte{byte(i + 2)}, size)); err != nil {
				errorsSeen <- err
				return
			}
			if err := db.Checkpoint(); err != nil {
				errorsSeen <- err
				return
			}
		}
		if err := db.Close(); err != nil {
			errorsSeen <- err
		}
	}()
	close(start)
	wg.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Error(err)
	}
}

// Queue DB Close behind an admitted backend point. Releasing traversal forces
// the pointer case into same-cut Snapshot construction while Close is already
// waiting; recursively entering the cache RLock here would deadlock.
func TestCOWPublicPointReadQueuedClose(t *testing.T) {
	for _, size := range []int{128, 4096} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			db := cowPointReadFixture(t)
			value := bytes.Repeat([]byte{7}, size)
			if err := db.Set([]byte("point"), value); err != nil {
				t.Fatal(err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			basis := db.cow.cut.basis
			basis.readMu.Lock()
			locked := true
			defer func() {
				if locked {
					basis.readMu.Unlock()
				}
			}()
			read := make(chan error, 1)
			go func() {
				got, err := db.GetAppend([]byte("point"), nil)
				if err == nil && !bytes.Equal(got, value) {
					err = errors.New("Close changed admitted output")
				}
				read <- err
			}()
			deadline := time.Now().Add(5 * time.Second)
			for db.cow.readMu.TryLock() {
				db.cow.readMu.Unlock()
				if time.Now().After(deadline) {
					t.Fatal("point did not enter cache read gate")
				}
				runtime.Gosched()
			}
			closed := make(chan error, 1)
			go func() { closed <- db.Close() }()
			for db.cow.readMu.TryRLock() {
				db.cow.readMu.RUnlock()
				if time.Now().After(deadline) {
					t.Fatal("Close did not queue behind point")
				}
				runtime.Gosched()
			}
			basis.readMu.Unlock()
			locked = false
			cowWait(t, read)
			cowWait(t, closed)
			if stats := db.COWMemoryStats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Fatalf("point/Close retained resources: %+v", stats)
			}
		})
	}
}

func cowPointReadForTest(db *DB, api string, key, dst []byte) ([]byte, page.EntryRevision, error) {
	switch api {
	case "Get":
		value, err := db.Get(key)
		return value, 0, err
	case "GetAppend":
		value, err := db.GetAppend(key, dst)
		return value, 0, err
	case "GetVersioned":
		return db.GetVersioned(key)
	case "GetVersionedAppend":
		return db.GetVersionedAppend(key, dst)
	default:
		panic("unknown point API")
	}
}

// Pointer and dictionary fallbacks still construct an admitted Snapshot on the
// same retained cut. Exhausting view slots must refuse, without consuming dst.
func cowAssertPointReadViewRefusal(t *testing.T, db *DB, key []byte) {
	t.Helper()
	release := cowPinViewFrontier(t, db.cow)
	defer release()
	views := db.COWMemoryStats().Views
	for _, api := range []string{"Get", "GetAppend", "GetVersioned", "GetVersionedAppend"} {
		t.Run(api, func(t *testing.T) {
			c := db.cow
			c.cutMu.Lock()
			cut, refs := c.cut, c.cut.refs
			c.cutMu.Unlock()
			before := c.budget.Stats()
			dst := []byte("keep")
			got, _, err := cowPointReadForTest(db, api, key, dst)
			if !errors.Is(err, memtable.ErrCOWCapacity) {
				t.Fatalf("fallback bypassed view admission: %v", err)
			}
			if (api == "GetAppend" || api == "GetVersionedAppend") && !bytes.Equal(got, dst) {
				t.Fatalf("refusal consumed caller prefix: %q", got)
			}
			if db.COWMemoryStats().Views != views {
				t.Fatal("fallback refusal changed view ownership")
			}
			c.cutMu.Lock()
			sameCut, afterRefs := c.cut == cut, cut.refs
			c.cutMu.Unlock()
			after := c.budget.Stats()
			after.PeakBytes = before.PeakBytes
			if !sameCut || afterRefs != refs || after != before {
				t.Fatalf("refused fallback leaked its exact-cut reference/admission: refs=%d->%d before=%+v after=%+v", refs, afterRefs, before, after)
			}
		})
	}
}

func TestCOWPublicPointReadPointerPreservesViewAdmission(t *testing.T) {
	db := cowPointReadFixture(t)
	key := []byte("pointer")
	want := bytes.Repeat([]byte("p"), 4096)
	if err := db.Set(key, want); err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"cached", "basis"} {
		t.Run(phase, func(t *testing.T) {
			if phase == "basis" {
				if err := db.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "cached" {
				_, _, flags, _, found := db.cow.cut.domains[db.shardIndex(key)].getEntryWithRevision(key)
				if !found || flags&node.FlagPointer == 0 {
					t.Fatal("fixture did not retain an actual cached pointer")
				}
			} else {
				entry, err := db.cow.cut.basis.snapshot.GetEntryExact(key)
				if err != nil || entry.Flags&node.FlagPointer == 0 {
					t.Fatalf("fixture did not persist an actual pointer: %+v/%v", entry, err)
				}
			}
			cowAssertPointReadViewRefusal(t, db, key)
			if got, err := db.Get(key); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("fallback after releasing views: %d/%v", len(got), err)
			}
		})
	}
}

// Measure the original complete-cut materializer's real retained charge before
// applying pressure. The public DB call must fit that same budget, without also
// retaining a fresh opportunistic basis traversal throughout the handoff.
func cowAssertFreshPointHandoffRefundsBeforeLocalAdmission(t *testing.T, db *DB, key, want []byte, pointer bool) {
	t.Helper()
	if got, err := db.Get(key); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("warm original shared codec: len=%d err=%v", len(got), err)
	}
	basis, budget := db.cow.cut.basis, db.cow.budget
	basis.readMu.Lock()
	basis.reader.close()
	basis.reader = nil
	basis.readMu.Unlock()
	var entry node.LeafEntry
	if pointer {
		var err error
		entry, err = basis.snapshot.GetEntryExact(key)
		if err != nil || entry.Flags&node.FlagPointer == 0 {
			t.Fatalf("fixture requires an actual physical pointer: entry=%+v err=%v", entry, err)
		}
	}
	before := budget.Stats()
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	if pointer {
		s.cowReadMu.Lock()
		got, _, err = s.cowValueFromEntryLocked(entry)
		s.cowReadMu.Unlock()
	} else {
		got, err = s.Get(key)
	}
	if err != nil || !bytes.Equal(got, want) || s.cowReader == nil {
		_ = s.Close()
		t.Fatalf("original cut materializer: len=%d err=%v", len(got), err)
	}
	localCharge := budget.Stats().ReservedBytes - before.ReservedBytes
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if localCharge == 0 || budget.Stats() != before || basis.reader != nil {
		t.Fatalf("measurement must refund the original local owner: before=%+v after=%+v", before, budget.Stats())
	}
	leaseCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - localCharge - leaseCharge)
	if err != nil {
		t.Fatal(err)
	}
	defer pressure.Close()
	pressured := budget.Stats()
	for _, api := range []string{"Get", "GetAppend", "GetVersioned", "GetVersionedAppend"} {
		dst := []byte("keep:")
		got, _, err := cowPointReadForTest(db, api, key, dst)
		expected := want
		if api == "GetAppend" || api == "GetVersionedAppend" {
			expected = append(append([]byte(nil), dst...), want...)
		}
		if err != nil || !bytes.Equal(got, expected) {
			t.Fatalf("%s fresh handoff exceeded original local budget: len=%d err=%v", api, len(got), err)
		}
		after := budget.Stats()
		// PeakBytes is a cumulative high-water counter, not live ownership.
		// Preserve exact equality for every current byte/lease/view counter.
		after.PeakBytes = pressured.PeakBytes
		if basis.reader != nil || after != pressured {
			t.Fatalf("%s retained opportunistic scratch or leaked local ownership: before=%+v after=%+v", api, pressured, budget.Stats())
		}
	}
	pressure.Close()
	if got, err := db.GetAppend(key, nil); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("unpressured handoff: len=%d err=%v", len(got), err)
	}
	owner, retained := basis.reader, budget.Stats()
	if owner == nil || owner.point == nil {
		t.Fatal("successful point handoff discarded reusable traversal")
	}
	for _, api := range []string{"Get", "GetAppend", "GetVersioned", "GetVersionedAppend"} {
		got, _, err := cowPointReadForTest(db, api, key, nil)
		after := budget.Stats()
		after.PeakBytes = retained.PeakBytes
		if err != nil || !bytes.Equal(got, want) || basis.reader != owner || after != retained {
			t.Fatalf("%s warm handoff changed owner/admission: err=%v before=%+v after=%+v", api, err, retained, after)
		}
	}
}

func TestCOWPublicPointFreshPointerRefundsBeforeLocalAdmission(t *testing.T) {
	db := cowPointReadFixture(t)
	want := bytes.Repeat([]byte("physical-pointer-handoff"), 512)
	if err := db.Set([]byte("pointer"), want); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	cowAssertFreshPointHandoffRefundsBeforeLocalAdmission(t, db, []byte("pointer"), want, true)
}

func TestCOWPublicPointReadMissingKeepsWarmTraversal(t *testing.T) {
	db := cowPointReadFixture(t)
	if err := db.Set([]byte("present"), []byte("inline")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	key, dst := []byte("missing"), []byte("keep:")
	got, revision, err := db.GetVersionedAppend(key, dst)
	if !errors.Is(err, tree.ErrKeyNotFound) || !bytes.Equal(got, dst) || revision != page.LegacyEntryRevision {
		t.Fatalf("physical miss changed API contract: out=%q revision=%d err=%v", got, revision, err)
	}
	basis, budget := db.cow.cut.basis, db.cow.budget
	owner, retained := basis.reader, budget.Stats()
	if owner == nil || owner.point == nil {
		t.Fatal("completed physical miss discarded reusable traversal")
	}
	for i := 0; i < 3; i++ {
		for _, api := range []string{"Get", "GetAppend", "GetVersioned", "GetVersionedAppend"} {
			got, revision, err := cowPointReadForTest(db, api, key, dst)
			if api == "Get" || api == "GetVersioned" {
				if err != nil || got != nil {
					t.Fatalf("%s normalized miss changed: out=%q err=%v", api, got, err)
				}
			} else if !errors.Is(err, tree.ErrKeyNotFound) || !bytes.Equal(got, dst) {
				t.Fatalf("%s miss consumed prefix: out=%q err=%v", api, got, err)
			}
			after := budget.Stats()
			after.PeakBytes = retained.PeakBytes
			if revision != page.LegacyEntryRevision || basis.reader != owner || after != retained {
				t.Fatalf("%s repeated miss changed revision/owner/admission: revision=%d before=%+v after=%+v", api, revision, retained, after)
			}
		}
	}
}

func TestCOWFreshReadOwnerRefundMatchesIdentity(t *testing.T) {
	db := cowPointReadFixture(t)
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	basis, budget := s.cowCut.basis, db.cow.budget
	basis.readMu.Lock()
	first, err := basis.physicalWorkspaceLocked(budget, db.cow.readLimits())
	if err != nil {
		basis.readMu.Unlock()
		t.Fatal(err)
	}
	stale := cowReadHandoffLocked(&basis.readMu, &basis.reader, true, true, nil)
	// Another read has already refunded the first owner and admitted a new
	// one. The old token must not evict that replacement, even under pressure.
	basis.reader = nil
	first.close()
	replacement, err := basis.physicalWorkspaceLocked(budget, db.cow.readLimits())
	basis.readMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	before := budget.Stats()
	if stale.refund() || stale.refund() || basis.reader != replacement || budget.Stats() != before {
		t.Fatal("stale token changed another owner or authorized a retry")
	}
	basis.readMu.Lock()
	warm := cowReadHandoffLocked(&basis.readMu, &basis.reader, false, true, nil)
	basis.readMu.Unlock()
	if warm.refund() || basis.reader != replacement {
		t.Fatal("warm owner must never be refundable by a handoff token")
	}
}
