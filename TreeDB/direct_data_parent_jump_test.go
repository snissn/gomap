package treedb_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tree "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
)

// Backend-root writes, unlike cached Set calls, each advance the actual visible
// CommitSeq. Hold one DATA seal after preparation so two accepted suffix roots
// must share the next seal, then overwrite its parent's original meta slot.
func TestDATAParentRetentionAcrossCommitJump(t *testing.T) {
	opts := directDATAOptions(directDATAFixtureDir(t, "parent-jump"))
	backend, cleanup, err := tree.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cleanup != nil {
			if err := cleanup(); err != nil {
				t.Errorf("original backend cleanup: %v", err)
			}
		}
	})
	if backend.HasNativeSemanticBridge() {
		t.Fatal("backend-root fixture unexpectedly activated a logical COW owner")
	}
	oldValue := bytes.Repeat([]byte("old-pointer/"), 64)
	newValue := bytes.Repeat([]byte("new-pointer/"), 64)
	// Reuse the exported pointer producer from TestBackendFactoryRolesLegacyLifecycle.
	// Backend Set intentionally refuses oversized inline values; it does not append them.
	ptrs, err := backend.AppendValueLogValues([][]byte{oldValue})
	if err != nil || len(ptrs) != 1 {
		t.Fatalf("append original pointer: %v %v", ptrs, err)
	}
	oldPtr := ptrs[0]
	write := func(want uint64, puts map[string][]byte, pointer *page.ValuePtr, deletes ...string) {
		t.Helper()
		done := make(chan error, 1)
		go func() {
			done <- func() (result error) {
				batch := backend.NewBatch()
				defer func() { result = errors.Join(result, batch.Close()) }()
				for _, key := range []string{"a", "b", "c", "d"} {
					if value, ok := puts[key]; ok {
						if err := batch.Set([]byte(key), value); err != nil {
							return err
						}
					}
				}
				if pointer != nil {
					pointerBatch, ok := batch.(interface {
						SetPointer([]byte, page.ValuePtr) error
					})
					if !ok {
						return errors.New("backend batch missing exported SetPointer")
					}
					if err := pointerBatch.SetPointer([]byte("b"), *pointer); err != nil {
						return err
					}
				}
				for _, key := range deletes {
					if err := batch.Delete([]byte(key)); err != nil {
						return err
					}
				}
				return batch.Write()
			}()
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("ordinary backend batch want commit %d: %v", want, err)
			}
		case <-time.After(30 * time.Second):
			state, ok := backend.StateToken()
			t.Fatalf("backend batch blocked before commit %d: state=%+v valid=%t", want, state, ok)
		}
		state, ok := backend.StateToken()
		if !ok || state.CommitSeq != want {
			t.Fatalf("actual accepted commit: %+v valid=%t want=%d", state, ok, want)
		}
		t.Logf("accepted physical producer state: %+v", state)
	}
	initial, ok := backend.StateToken()
	if !ok {
		t.Fatal("missing original backend state")
	}
	write(initial.CommitSeq+1, map[string][]byte{"a": []byte("old-inline"), "c": []byte("delete-later")}, &oldPtr)
	if err := backend.Checkpoint(); err != nil {
		t.Fatal("seed checkpoint", err)
	}
	base, ok := backend.StateToken()
	if !ok {
		t.Fatal("missing seed state")
	}
	c := base.CommitSeq
	seedSlots := directDATAReadSlots(t, opts.Dir)
	if seedSlots[0].meta.CommitSeq != c && seedSlots[1].meta.CommitSeq != c {
		t.Fatalf("seed accepted state not sealed: state=%+v slots=%+v", base, seedSlots)
	}
	held := backend.AcquireSnapshot()
	if held == nil {
		t.Fatal("missing original held snapshot")
	}
	t.Cleanup(func() { _ = held.Close() })
	old := map[string][]byte{"a": []byte("old-inline"), "b": oldValue, "c": []byte("delete-later")}
	directDATAValues(t, held.Get, old)
	entry, err := held.GetEntryExact([]byte("b"))
	if err != nil || entry.ValuePtr != oldPtr || oldPtr.FileID == 0 {
		t.Fatalf("actual held persistent pointer: %+v want=%+v error=%v", entry, oldPtr, err)
	}
	// Append the replacement through the same original public producer before
	// pausing META; the suffix will publish this exact pointer, not append under the hook.
	ptrs, err = backend.AppendValueLogValues([][]byte{newValue})
	if err != nil || len(ptrs) != 1 {
		t.Fatalf("append suffix pointer: %v %v", ptrs, err)
	}
	newPtr := ptrs[0]
	reached, release := make(chan struct{}), make(chan struct{})
	var armed atomic.Bool
	var releaseOnce sync.Once
	resume := func() { releaseOnce.Do(func() { close(release) }) }
	var mu sync.Mutex
	var events []durabilitycut.Event
	root, path := filepath.Join(opts.Dir, "maindb"), filepath.Join(opts.Dir, "maindb", "index.db")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Root != root || event.Path != path || event.Resource != durabilitycut.ResourceMeta {
			return nil
		}
		switch event.Point {
		case durabilitycut.BeforeMetaWrite, durabilitycut.AfterMetaWrite, durabilitycut.BeforeMetaSync, durabilitycut.AfterMetaSync:
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
		}
		if event.Point == durabilitycut.BeforeMetaWrite && armed.CompareAndSwap(false, true) {
			close(reached)
			select {
			case <-release:
			case <-time.After(30 * time.Second):
				return fmt.Errorf("DATA parent-jump first META pause timed out: %s", path)
			}
		}
		return nil
	})
	t.Cleanup(func() { resume(); restore() })
	write(c+1, map[string][]byte{"a": []byte("first-inline"), "d": []byte("first-insert")}, nil)
	checkpoint := make(chan error, 1)
	go func() { checkpoint <- backend.Checkpoint() }()
	completed := false
	t.Cleanup(func() {
		resume()
		if !completed {
			select {
			case err := <-checkpoint:
				if err != nil {
					t.Errorf("paused original checkpoint cleanup: %v", err)
				}
			case <-time.After(30 * time.Second):
				t.Error("paused checkpoint cleanup did not complete")
			}
		}
	})
	select {
	case <-reached:
	case err := <-checkpoint:
		completed = true
		t.Fatalf("checkpoint returned before DATA META rendezvous: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("first DATA META rendezvous not reached")
	}
	write(c+2, nil, &newPtr)
	write(c+3, nil, nil, "c")
	if slots := directDATAReadSlots(t, opts.Dir); slots != seedSlots {
		t.Fatalf("paused first seal already changed a meta slot: before=%+v after=%+v", seedSlots, slots)
	}
	resume()
	select {
	case err := <-checkpoint:
		completed = true
		if err != nil {
			t.Fatal("first paused checkpoint", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("first paused checkpoint did not finish")
	}
	if err := backend.Checkpoint(); err != nil {
		t.Fatal("seal accepted suffix", err)
	}
	jumpSlots := directDATAReadSlots(t, opts.Dir)
	find := func(slots [2]directDATASlot, sequence uint64) int {
		t.Helper()
		for i := range slots {
			if slots[i].meta.CommitSeq == sequence {
				return i
			}
		}
		t.Fatalf("actual seal sequence %d missing from slots %+v", sequence, slots)
		return -1
	}
	firstSlot, jumpSlot := find(jumpSlots, c+1), find(jumpSlots, c+3)
	first, jump := jumpSlots[firstSlot], jumpSlots[jumpSlot]
	if firstSlot == jumpSlot || jump.record.CommitSeq-first.record.CommitSeq != 2 || jump.record.ParentRecordPageID != first.meta.RootRecordPageID || jump.record.ParentRecordDigest != first.meta.RootRecordDigest || jump.record.ParentCommitSeq != first.record.CommitSeq {
		t.Fatalf("not an exact jump with retained immediate parent: first=%+v jump=%+v", first, jump)
	}
	directDATAValues(t, held.Get, old)
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	write(c+4, map[string][]byte{"a": []byte("latest-inline"), "d": []byte("latest-insert")}, nil)
	if err := backend.Checkpoint(); err != nil {
		t.Fatal("overwrite original parent meta slot", err)
	}
	finalSlots := directDATAReadSlots(t, opts.Dir)
	latestSlot := find(finalSlots, c+4)
	if latestSlot != firstSlot || finalSlots[jumpSlot] != jump || finalSlots[latestSlot].record.ParentRecordPageID != jump.meta.RootRecordPageID || finalSlots[latestSlot].record.ParentRecordDigest != jump.meta.RootRecordDigest {
		t.Fatalf("third seal lost surviving jump or exact parent: jump=%+v final=%+v", jumpSlots, finalSlots)
	}
	// directDATAReadSlots decoded C+1 through C+3's exact parent digest above,
	// after C+1 ceased owning either meta slot and after the old reader released.
	// Read the original persisted freelist through its exported decoder. Merely
	// decoding an unreused parent would not prove that its retirement is safe.
	// This is the same boundary classifier used by the physical snapshot reuse
	// control, applied to the actual nonconsecutive public producer above.
	latestView := backend.AcquireSnapshot()
	if latestView == nil {
		t.Fatal("missing latest snapshot while reading immutable freelist")
	}
	generation, err := freelist.LoadGenerationV1(latestView.Pager(), finalSlots[latestSlot].record.Freelist)
	if err != nil {
		_ = latestView.Close()
		t.Fatal("decode final persisted freelist", err)
	}
	parentPage := first.meta.RootRecordPageID
	atSuccessor, atErr := generation.SnapshotPageUnusedV1(parentPage, c+3)
	afterSuccessor, afterErr := generation.SnapshotPageUnusedV1(parentPage, c+4)
	allocatable := generation.Allocatable(parentPage)
	if err := errors.Join(atErr, afterErr, latestView.Close()); err != nil {
		t.Fatal("classify persisted off-slot parent", err)
	}
	if allocatable || atSuccessor || !afterSuccessor {
		t.Fatalf("persisted parent retirement not bound to actual successor: parent=%d successor=%d free=%t at=%t after=%t", parentPage, c+3, allocatable, atSuccessor, afterSuccessor)
	}
	mu.Lock()
	authority := append([]durabilitycut.Event(nil), events...)
	mu.Unlock()
	if len(authority) != 12 {
		t.Fatalf("expected exactly three four-event DATA seals: %+v", authority)
	}
	order := []durabilitycut.Point{durabilitycut.BeforeMetaWrite, durabilitycut.AfterMetaWrite, durabilitycut.BeforeMetaSync, durabilitycut.AfterMetaSync}
	for i, event := range authority {
		if event.Point != order[i%4] {
			t.Fatalf("DATA authority event %d not bound to alternating exact slots: %+v", i, event)
		}
		if i%4 < 2 && (event.Offset != int64([]int{firstSlot, jumpSlot, latestSlot}[i/4])*int64(page.PageSize) || event.Length != int64(page.PageSize)) {
			t.Fatalf("DATA meta write event %d not bound to exact slot: %+v", i, event)
		}
	}
	jumpValues := map[string][]byte{"a": []byte("first-inline"), "b": newValue, "d": []byte("first-insert")}
	latestValues := map[string][]byte{"a": []byte("latest-inline"), "b": newValue, "d": []byte("latest-insert")}
	directDATAValues(t, backend.Get, latestValues)
	for _, key := range []string{"treedb.command_wal.append.count_total", "treedb.command_wal.file_sync.calls_total"} {
		if got := backend.Stats()[key]; got != "0" {
			t.Fatalf("NoWAL ordinary ACK used WAL: %s=%q", key, got)
		}
	}
	closeOriginal := cleanup
	cleanup = nil
	if err := closeOriginal(); err != nil {
		t.Fatal("original backend close", err)
	}
	if slots := directDATAReadSlots(t, opts.Dir); slots != finalSlots {
		t.Fatalf("Close changed three-seal authority: before=%+v after=%+v", finalSlots, slots)
	}
	latest, err := tree.Open(directDATAReadOnlyOptions(opts))
	if err != nil {
		t.Fatal("latest public DATA reopen", err)
	}
	directDATAValues(t, latest.Get, latestValues)
	if err := latest.Close(); err != nil {
		t.Fatal(err)
	}
	// Preserve the original newest META bytes, then damage that slot in place.
	// Recovery binds dependencies to their original physical namespace; copying
	// those files would create a different identity rather than a fallback image.
	f, err := os.OpenFile(filepath.Join(opts.Dir, "maindb", "index.db"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	originalMeta := make([]byte, page.PageSize)
	if _, err := f.ReadAt(originalMeta, int64(latestSlot)*int64(page.PageSize)); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	metaEvidence := filepath.Join(directDATAFixtureDir(t, "original-newest-meta"), "newest-meta.page")
	if err := os.WriteFile(metaEvidence, originalMeta, 0600); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	var corrupt [1]byte
	offset := int64(latestSlot)*int64(page.PageSize) + int64(page.PageHeaderSize)
	_, readErr := f.ReadAt(corrupt[:], offset)
	corrupt[0] ^= 1
	_, writeErr := f.WriteAt(corrupt[:], offset)
	if err := errors.Join(readErr, writeErr, f.Sync(), f.Close()); err != nil {
		t.Fatal(err)
	}
	fallbackOpts := directDATAReadOnlyOptions(opts)
	fallback, err := tree.Open(fallbackOpts)
	if err != nil {
		t.Fatal("surviving jump public fallback reopen", err)
	}
	defer fallback.Close()
	if got := fallback.Stats()["treedb.durable_root.commit_seq"]; got != strconv.FormatUint(c+3, 10) {
		t.Fatalf("fallback selected commit %q want exact jump %d", got, c+3)
	}
	directDATAValues(t, fallback.Get, jumpValues)
	t.Logf("actual three DATA seals: base=%d first=%d jump=%d latest=%d slots=%d/%d/%d; off-slot parent=%d; backend-root/no native bridge; NoWAL volatile ACK, explicit checkpoint seals", c, c+1, c+3, c+4, firstSlot, jumpSlot, latestSlot, first.meta.RootRecordPageID)
}
