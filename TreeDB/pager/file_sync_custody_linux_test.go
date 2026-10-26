//go:build linux

package pager

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// Hold only the first actual file fence. Cleanup always releases and joins it,
// including a predecessor whose mapping lock prevents the asserted mutation.
func holdFirstFileFence(t *testing.T, call func() error, failure error) (*os.File, func() error) {
	t.Helper()
	original := syncPageFileFn
	entered := make(chan *os.File, 1)
	release := make(chan struct{})
	finished := make(chan error, 1)
	var first, released, joined sync.Once
	var result error
	syncPageFileFn = func(file *os.File) error {
		selected := false
		first.Do(func() { selected = true })
		if selected {
			entered <- file
			<-release
			if failure != nil {
				return failure
			}
		}
		return original(file)
	}
	finish := func() error {
		released.Do(func() { close(release) })
		joined.Do(func() {
			select {
			case result = <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("file fence did not join after release")
			}
		})
		return result
	}
	t.Cleanup(func() { _ = finish(); syncPageFileFn = original })
	go func() { finished <- call() }()
	select {
	case file := <-entered:
		return file, finish
	case <-time.After(5 * time.Second):
		_ = finish()
		t.Fatal("file fence was not reached")
		return nil, finish
	}
}

func joinPagerMutation(t *testing.T, result <-chan error, finish func() error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		// Release first, then join the predecessor's blocked mutation before
		// reporting the progress failure. No goroutine or mmap alias escapes.
		_ = finish()
		select {
		case <-result:
		case <-time.After(5 * time.Second):
			t.Fatal("pager mutation did not join after fence release")
		}
		t.Fatal("pager mutation blocked behind file-only fence")
		return nil
	}
}

func TestFileSyncCustodyMutationProgressAndRedirty(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		var injected error
		if fail {
			name = "failure"
			injected = errors.New("held file fence failed")
		}
		t.Run(name, func(t *testing.T) {
			chunkSize := syncPagesTestChunkSize(16)
			p, err := Open(filepath.Join(t.TempDir(), "index.db"), chunkSize)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			p.QuiesceBackgroundGrowth()
			initialPages := int(chunkSize/page.PageSize) + 1
			if _, err := p.Alloc(initialPages); err != nil {
				t.Fatal(err)
			}
			if err := p.Sync(); err != nil {
				t.Fatal(err)
			}
			old, newer := bytes.Repeat([]byte{0x31}, page.PageSize), bytes.Repeat([]byte{0x72}, page.PageSize)
			if err := p.Write(0, old); err != nil {
				t.Fatal(err)
			}
			owned, finish := holdFirstFileFence(t, p.SyncIndexData, injected)
			defer finish()
			p.mu.RLock()
			detached := p.dirtyChunks.count == 0
			p.mu.RUnlock()
			if !detached {
				t.Fatal("file fence began before original dirty snapshot detached")
			}
			mutated := make(chan error, 1)
			go func() {
				id, err := p.Alloc(1) // Existing represented second chunk, not just a count probe.
				if err == nil {
					err = p.Write(0, newer) // Re-dirty the selected chunk after detachment.
				}
				if err == nil {
					err = p.Write(id, newer) // Write the newly allocated page as well.
				}
				mutated <- err
			}()
			if err := joinPagerMutation(t, mutated, finish); err != nil {
				t.Fatal(err)
			}
			assertIndependentSyncFile(t, owned, p.file)
			if err := finish(); !errors.Is(err, injected) {
				t.Fatalf("file fence=%v want %v", err, injected)
			}
			if _, err := owned.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("call-owned descriptor survived return: %v", err)
			}
			if !p.dirtyChunks.contains(0) || !p.dirtyChunks.contains(1) || p.dirtyChunks.count != 2 {
				t.Fatal("completed/failed old snapshot erased same-chunk or distinct newer dirt")
			}
			if err := p.SyncIndexData(); err != nil {
				t.Fatal(err)
			}
			if p.dirtyChunks.count != 0 {
				t.Fatal("joined retry did not consume its own dirty snapshot")
			}
			got := make([]byte, page.PageSize)
			for _, id := range []uint64{0, uint64(initialPages)} {
				if _, err := p.file.ReadAt(got, int64(id)*page.PageSize); err != nil || !bytes.Equal(got, newer) {
					t.Fatalf("post-fence page%d bytes changed: %v", id, err)
				}
			}
		})
	}
}

func TestFileSyncCustodyCapturedExtentAndMonotoneCompletion(t *testing.T) {
	for _, variant := range []string{"ordinary", "retained-wrapper", "newer-completes-first"} {
		t.Run(variant, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "index.db")
			p, err := Open(path, syncPagesTestChunkSize(16))
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			p.QuiesceBackgroundGrowth()
			if _, err := p.Alloc(1); err != nil {
				t.Fatal(err)
			}
			info, err := p.file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			captured := info.Size()
			call := p.SyncIndexData
			if variant == "retained-wrapper" {
				stable, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer stable.Close()
				call = func() error { return p.SyncIndexDataWithStableFileAtPath(stable, path) }
			}
			_, finish := holdFirstFileFence(t, call, nil)
			defer finish()
			grew := make(chan error, 1)
			go func() { grew <- p.GrowTo(uint64(captured/page.PageSize) + 1) }()
			if err := joinPagerMutation(t, grew, finish); err != nil {
				t.Fatal(err)
			}
			info, err = p.file.Stat()
			if err != nil || info.Size() <= captured {
				t.Fatalf("growth did not expand exact file: %v", err)
			}
			want := captured
			if variant == "newer-completes-first" {
				if err := p.SyncIndexData(); err != nil {
					t.Fatal(err)
				}
				want = info.Size()
			}
			if err := finish(); err != nil {
				t.Fatal(err)
			}
			if got := p.durableFileSize.Load(); got != want {
				t.Fatalf("covered prefix=%d want%d; post-barrier growth is not a claim", got, want)
			}
		})
	}
}

func TestFileSyncCustodyRefusalAndInvalidTargetsKeepDirtySnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	p, err := Open(path, syncPagesTestChunkSize(16))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.QuiesceBackgroundGrowth()
	if _, err := p.Alloc(1); err != nil {
		t.Fatal(err)
	}
	if err := p.Write(0, make([]byte, page.PageSize)); err != nil {
		t.Fatal(err)
	}
	revision := p.dirtyChunks.revision
	calls := 0
	restore := durabilitycut.Install(func(e durabilitycut.Event) error { calls++; return nil })
	defer restore()
	original := syncPageFileFn
	syncPageFileFn = func(*os.File) error { calls++; return errors.New("unadmitted fence") }
	defer func() { syncPageFileFn = original }()
	// This was the old native snapshot's complete record grant (one dirty
	// word/chunk/worker). The new descriptor custody must be admitted too.
	work := &iterator.OrdinalScanWork{RecordLimit: 15, ByteLimit: 1 << 20}
	ok, err := p.SyncIndexDataWithStableFileWithWork(p.file, work)
	var tooLarge *iterator.OrdinalUnitTooLarge
	if ok || !errors.As(err, &tooLarge) || tooLarge.Records != 23 || work.Records != 0 {
		t.Fatalf("descriptor work admission=%t %v records%d", ok, err, work.Records)
	}
	closed, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	foreign, err := os.CreateTemp(t.TempDir(), "foreign")
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	for _, target := range []*os.File{nil, closed, foreign} {
		if err := p.SyncIndexDataWithStableFile(target); err == nil {
			t.Fatal("invalid target entered file barrier")
		}
		if calls != 0 || p.dirtyChunks.revision != revision || !p.dirtyChunks.contains(0) {
			t.Fatal("refusal/duplicate/identity failure detached dirty state or emitted I/O")
		}
	}
}

func TestFileSyncCustodyCloseAndPathReplacement(t *testing.T) {
	for _, variant := range []string{"ordinary", "retained", "retained-failure"} {
		fail := variant == "retained-failure"
		var injected error
		if fail {
			injected = errors.New("file fence failed after original Close")
		}
		t.Run(variant, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "index.db")
			p, err := Open(path, syncPagesTestChunkSize(16))
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			p.QuiesceBackgroundGrowth()
			if _, err := p.Alloc(1); err != nil {
				t.Fatal(err)
			}
			if err := p.Write(0, bytes.Repeat([]byte{0x53}, page.PageSize)); err != nil {
				t.Fatal(err)
			}
			stable, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer stable.Close()
			retry, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer retry.Close()
			var observed *os.File
			before, after := 0, 0
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource != durabilitycut.ResourceIndex || e.Path != path {
					return nil
				}
				if e.Point == durabilitycut.BeforeIndexDataSync {
					observed = e.StablePath
					before++
				} else if e.Point == durabilitycut.AfterIndexDataSync {
					if observed != e.StablePath {
						return errors.New("before/after switched descriptor authority")
					}
					after++
				}
				got, err := e.StablePath.Stat()
				want, wantErr := retry.Stat()
				if err != nil || wantErr != nil || !os.SameFile(got, want) || e.StablePath == stable || e.StablePath == retry {
					return errors.New("observation lost independent exact file custody")
				}
				return nil
			})
			defer restore()
			call := p.SyncIndexData
			if variant != "ordinary" {
				call = func() error { return p.SyncIndexDataWithStableFileAtPath(stable, path) }
			}
			owned, finish := holdFirstFileFence(t, call, injected)
			defer finish()
			if err := os.Rename(path, path+".retired"); err != nil {
				t.Fatal(err)
			}
			foreign := []byte("replacement must never supply fence authority")
			if err := os.WriteFile(path, foreign, 0600); err != nil {
				t.Fatal(err)
			}
			if err := stable.Close(); err != nil {
				t.Fatal(err)
			}
			closed := make(chan error, 1)
			go func() { closed <- p.Close() }()
			if err := joinPagerMutation(t, closed, finish); err != nil {
				t.Fatal(err)
			}
			if _, err := owned.Stat(); err != nil {
				t.Fatalf("Close consumed the outstanding call's descriptor: %v", err)
			}
			if err := finish(); !errors.Is(err, injected) {
				t.Fatalf("fence after Close=%v want%v", err, injected)
			}
			if _, err := owned.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("returned descriptor not released: %v", err)
			}
			if p.dirtyChunks.words != nil || p.dirtyChunks.count != 0 {
				t.Fatal("failure resurrected mappings retired by Close")
			}
			if err := p.SyncIndexDataWithStableFileAtPath(retry, path); err != nil {
				t.Fatal(err)
			}
			wantAfter := 2
			if fail {
				wantAfter = 1
			}
			if before != 2 || after != wantAfter {
				t.Fatalf("owned barrier observations=%d/%d", before, after)
			}
			if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, foreign) {
				t.Fatalf("rebound diagnostic path was accessed: %q %v", got, err)
			}
			if err := p.Sync(); err == nil {
				t.Fatal("ordinary Sync after Close borrowed retained authority")
			}
		})
	}
}

func TestFileSyncCustodyObserverAndCleanupFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	p, err := Open(path, syncPagesTestChunkSize(16))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.QuiesceBackgroundGrowth()
	if _, err := p.Alloc(1); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("observer/fence rejected")
	for _, point := range []durabilitycut.Point{durabilitycut.BeforeIndexDataSync, durabilitycut.AfterIndexDataSync} {
		var owned *os.File
		restore := durabilitycut.Install(func(e durabilitycut.Event) error {
			if e.Point == point {
				owned = e.StablePath
				if err := p.Write(0, bytes.Repeat([]byte{0x64}, page.PageSize)); err != nil {
					return err
				}
				return injected
			}
			return nil
		})
		err := p.SyncIndexData()
		restore()
		if !errors.Is(err, injected) || !p.dirtyChunks.contains(0) {
			t.Fatalf("observer error lost retry union: %v", err)
		}
		if _, err := owned.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("observer-error duplicate leaked: %v", err)
		}
	}
	original := syncPageFileFn
	syncPageFileFn = func(file *os.File) error {
		if err := file.Close(); err != nil {
			return err
		}
		return injected
	}
	err = p.SyncIndexData()
	syncPageFileFn = original
	if !errors.Is(err, injected) || !errors.Is(err, os.ErrClosed) || !p.dirtyChunks.contains(0) {
		t.Fatalf("cleanup failure masked original failure or dirty union: %v", err)
	}
	if err := p.SyncIndexData(); err != nil {
		t.Fatal(err)
	}
}
