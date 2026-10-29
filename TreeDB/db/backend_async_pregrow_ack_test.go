package db

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Backend scope: an allocator-owned setup proves actual async file extension
// before one command-WAL pointer batch WriteSync ACK. This does not exercise
// public COW/MVCC completion ordering, growth concurrent with ACK, or
// growth-failure recovery.
func TestBackendAsyncPregrowExtensionThenCommandWALStableACK(t *testing.T) {
	const childEnv = "GOMAP_BACKEND_PREGROW_ACK_CHILD"
	if os.Getenv(childEnv) != "1" {
		// This contains a stranded worker/allocator failure; it is not a latency
		// requirement for SetSync or recovery.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBackendAsyncPregrowExtensionThenCommandWALStableACK$", "-test.timeout=25s", "-test.v")
		cmd.Env = append(os.Environ(), childEnv+"=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("backend async pre-grow child: %v\n%s", err, out)
		}
		return
	}

	const chunkSize int64 = 256 << 10
	opts := Options{
		Dir:                    t.TempDir(),
		ChunkSize:              chunkSize,
		CommandWAL:             true,
		Durability:             DurabilityDurable,
		DisableBackgroundPrune: true,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
			Compression:      ValueLogCompressionOff,
			ReadIntegrity:    IntegrityVerify,
		},
	}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if !database.CommandWALEnabled() {
		t.Fatal("command WAL did not activate")
	}
	if !database.HasValueLogAppender() {
		t.Fatal("command WAL persistent value-log appender unavailable")
	}
	// The backend Set/SetSync API accepts inline values only. Forced pointers
	// require the installed persistent appender plus an ordinary pointer batch;
	// WriteSync assigns its native revision and publishes its command-WAL LSN.
	newPointerBatch := func(key, value []byte) (*Batch, page.ValuePtr) {
		t.Helper()
		ptrs, err := database.AppendValueLogValues([][]byte{value})
		if err != nil {
			t.Fatal(err)
		}
		if len(ptrs) != 1 || !page.IsValueLogFileID(ptrs[0].FileID) || ptrs[0].Length == 0 {
			t.Fatalf("persistent appender pointers=%+v", ptrs)
		}
		batch := database.NewBatch().(*Batch)
		t.Cleanup(func() {
			if err := batch.Close(); err != nil {
				t.Error(err)
			}
		})
		if err := batch.SetPointer(key, ptrs[0]); err != nil {
			t.Fatal(err)
		}
		return batch, ptrs[0]
	}
	anchor := []byte("pregrow-anchor")
	baselineBatch, _ := newPointerBatch(anchor, []byte("baseline"))
	if err := baselineBatch.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err := baselineBatch.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	baseline := database.State()
	if baseline == nil {
		t.Fatal("baseline state unavailable")
	}
	snapshot := database.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("growth snapshot unavailable")
	}
	defer snapshot.Close()
	p := snapshot.Pager()
	if p == nil {
		t.Fatal("growth pager unavailable")
	}
	fileSize := func() int64 {
		t.Helper()
		var size int64
		if err := p.WithStableResourceFile(func(file *os.File) error {
			info, err := file.Stat()
			if err == nil {
				size = info.Size()
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return size
	}
	pagesPerChunk := uint64(chunkSize / int64(page.PageSize))
	targetPages := pagesPerChunk/2 + 1
	var fixturePages uint64
	// Baseline Checkpoint has drained the publisher. Retain its index owner and
	// exclude ordinary writer admission while reserving through that index's
	// actual allocator. Unlike raw pager.Alloc, AllocAppend advances COW ledger
	// ownership and transaction high-water together with the pager's count.
	func() {
		database.teardownMu.RLock()
		defer database.teardownMu.RUnlock()
		database.writeMu.Lock()
		defer database.writeMu.Unlock()
		if err := database.checkWriteAdmissionLocked(); err != nil {
			t.Fatal(err)
		}
		idx := database.idx.Load()
		if idx == nil || idx != snapshot.idx || idx.pager != p || idx.allocator == nil {
			t.Fatal("growth setup lost its retained current index owner")
		}
		if got := fileSize(); got > chunkSize || p.PageCount() >= targetPages {
			t.Fatalf("baseline is not below async trigger: bytes=%d pages=%d target=%d", got, p.PageCount(), targetPages)
		}
		baselinePages := p.PageCount()
		reserved := make([]uint64, 0, pagesPerChunk)
		for n := uint64(0); p.PageCount() < targetPages && n < pagesPerChunk; n++ {
			// A ledger reservation can make append skip IDs. Check its upper
			// bound before allocation so this setup cannot grow past chunk one.
			profile := idx.allocator.COWPrepareProfileV1()
			if !profile.Valid || profile.HighWater >= pagesPerChunk || profile.LedgerHighestReservedEnd >= pagesPerChunk {
				t.Fatalf("allocator append authority exceeds fixture budget: %+v", profile)
			}
			id, err := idx.allocator.AllocAppend()
			if err != nil {
				t.Fatal(err)
			}
			reserved = append(reserved, id)
		}
		fixturePages = p.PageCount()
		if fixturePages < targetPages || fixturePages > pagesPerChunk {
			t.Fatalf("allocator setup escaped first chunk: pages=%d target=%d max=%d", fixturePages, targetPages, pagesPerChunk)
		}
		if err := idx.allocator.ValidateCOWPhysicalTailV1(baselinePages); err != nil {
			t.Fatalf("allocator no longer owns its physical logical extent: %v", err)
		}
		// AllocAppend uses ordinary pager.Truncate with nil work. Each logical
		// reservation fits in the first chunk; no policy-extra growth is used.
		// Thus only the async grower can extend this file to a second chunk.
		// Wait for the state change, bounded only by the child watchdog above;
		// there is no separate scheduler or pre-grow latency requirement.
		for fileSize() < 2*chunkSize {
			time.Sleep(5 * time.Millisecond)
		}
		if got := fileSize(); got != 2*chunkSize {
			t.Fatalf("async extension bytes=%d want=%d", got, 2*chunkSize)
		}
		// Join any in-flight worker through its allocation mutex. This does not
		// claim that successful file extension proves second-mmap installation:
		// Truncate precedes mapping, and Alloc(0) needs only the first chunk.
		if start, err := p.Alloc(0); err != nil || start != fixturePages || p.PageCount() != fixturePages {
			t.Fatalf("join growth: start=%d pages=%d want=%d err=%v", start, p.PageCount(), fixturePages, err)
		}
		// The fixture never links these reservations into a tree. Retire them
		// through the same allocator so the next real publication owns their
		// conservative reclamation state rather than leaking uninitialized pages.
		if err := idx.allocator.RetireCOWV1(reserved, baseline.CommitSeq+1); err != nil {
			t.Fatal(err)
		}
	}()
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}

	var observationMu sync.Mutex
	active := true
	events, commandSyncs, rootSyncs := 0, 0, 0
	restoreObserver := durabilitycut.Install(func(event durabilitycut.Event) error {
		observationMu.Lock()
		defer observationMu.Unlock()
		if !active {
			return nil
		}
		// The bounded child runs only this DB. Retain the full oracle operation
		// for every event, including namespace, metadata and staging effects.
		if err := model.Observe(opts.Dir, event); err != nil {
			return err
		}
		events++
		if event.Point == durabilitycut.AfterDependencyFileSync && event.Resource == durabilitycut.ResourceCommandWAL {
			commandSyncs++
		}
		if event.Point == durabilitycut.AfterMetaSync || event.Point == durabilitycut.AfterIndexDataSync {
			rootSyncs++
		}
		return nil
	})
	var restoreOnce sync.Once
	restore := func() { restoreOnce.Do(restoreObserver) }
	defer restore()
	key := []byte("ack-after-async-extension")
	value := bytes.Repeat([]byte("persistent-after-pregrow/"), 128)
	ackBatch, appendedPointer := newPointerBatch(key, value)
	if err := ackBatch.WriteSync(); err != nil {
		t.Fatal(err)
	}
	// Freeze on ACK before State, Get, snapshot acquisition, Checkpoint or Close.
	observationMu.Lock()
	active = false
	stable := model.Clone()
	observed, synced, sealed := events, commandSyncs, rootSyncs
	observationMu.Unlock()
	restore()
	if observed == 0 || synced == 0 {
		t.Fatalf("ACK lacks observed command-file sync: events=%d syncs=%d", observed, synced)
	}
	if idx := database.idx.Load(); idx == nil || idx != snapshot.idx || idx.pager != p {
		t.Fatal("ACK did not use the retained pre-grown index generation")
	}
	ack := database.State()
	if ack == nil || ack.AppliedCommandLSN <= baseline.AppliedCommandLSN {
		t.Fatalf("ACK did not advance accepted command: baseline=%+v ack=%+v", baseline, ack)
	}
	got, revision, err := database.GetVersioned(key)
	if err != nil || revision == page.LegacyEntryRevision || !bytes.Equal(got, value) {
		t.Fatalf("source ACK value/revision: revision=%d equal=%t err=%v", revision, bytes.Equal(got, value), err)
	}
	physical := database.AcquireSnapshot()
	if physical == nil {
		t.Fatal("ACK physical snapshot unavailable")
	}
	entry, entryErr := physical.GetEntryExact(key)
	closeErr := physical.Close()
	if entryErr != nil || closeErr != nil || entry.Revision != revision || entry.Flags&node.FlagPointer == 0 || entry.ValuePtr != appendedPointer || !page.IsValueLogFileID(entry.ValuePtr.FileID) || entry.ValuePtr.Length == 0 {
		t.Fatalf("ACK did not activate persistent pointer: entry=%+v errors=%v/%v", entry, entryErr, closeErr)
	}

	crashDir := t.TempDir()
	if err := stable.MaterializeStable(crashDir); err != nil {
		t.Fatal(err)
	}
	releaseIdentities, err := stable.InstallStableIdentityOverrides(crashDir)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseIdentities()
	reopenOpts := opts
	reopenOpts.Dir = crashDir
	reopened, err := Open(reopenOpts)
	if err != nil {
		t.Fatalf("backend Open rejected stable ACK image: %v", err)
	}
	defer reopened.Close()
	got, recoveredRevision, err := reopened.GetVersioned(key)
	if err != nil || recoveredRevision != revision || !bytes.Equal(got, value) {
		t.Fatalf("stable ACK value/revision: revision=%d want=%d equal=%t err=%v", recoveredRevision, revision, bytes.Equal(got, value), err)
	}
	recoveredPhysical := reopened.AcquireSnapshot()
	if recoveredPhysical == nil {
		t.Fatal("stable ACK physical snapshot unavailable")
	}
	recoveredEntry, recoveredEntryErr := recoveredPhysical.GetEntryExact(key)
	recoveredCloseErr := recoveredPhysical.Close()
	if recoveredEntryErr != nil || recoveredCloseErr != nil || recoveredEntry.Revision != revision || recoveredEntry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(recoveredEntry.ValuePtr.FileID) || recoveredEntry.ValuePtr.Length == 0 {
		t.Fatalf("stable ACK persistent pointer/revision: entry=%+v want_revision=%d errors=%v/%v", recoveredEntry, revision, recoveredEntryErr, recoveredCloseErr)
	}
	if state := reopened.State(); state == nil || state.AppliedCommandLSN < ack.AppliedCommandLSN {
		t.Fatalf("stable ACK frontier regressed: recovered=%+v ack=%+v", state, ack)
	}
	if got, err := reopened.Get(anchor); err != nil || string(got) != "baseline" {
		t.Fatalf("stable baseline value=%q err=%v", got, err)
	}
	// Backend ACK may also seal DATA/root. This establishes stable-ACK recovery,
	// not necessarily WAL-only replay. No root-sync event is filtered or hidden.
	t.Logf("backend async extension then ACK: pages=%d events=%d command_syncs=%d root_syncs=%d baseline_lsn=%d ack_lsn=%d", fixturePages, observed, synced, sealed, baseline.AppliedCommandLSN, ack.AppliedCommandLSN)
}
