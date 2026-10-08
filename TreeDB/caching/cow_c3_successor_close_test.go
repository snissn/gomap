package caching

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// This wrapper pauses a real captured source lower bound before the tombstone
// merge fallback. It changes no production admission or publication hook.
type c3PausedSuccessor struct {
	memtable.Table
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (s *c3PausedSuccessor) SeekGE(start, end []byte) ([]byte, []byte, page.ValuePtr, byte, page.EntryRevision, bool) {
	key, value, ptr, flags, revision, found := s.Table.(memtable.SuccessorTable).SeekGE(start, end)
	s.once.Do(func() { close(s.entered); <-s.resume })
	return key, value, ptr, flags, revision, found
}

func TestC3COWSuccessorFallbackCompletesBeforeQueuedDBClose(t *testing.T) {
	for _, disk := range []bool{false, true} {
		for _, pointer := range []bool{false, true} {
			t.Run(fmt.Sprintf("disk=%t/pointer=%t", disk, pointer), func(t *testing.T) {
				// Own the directory explicitly: failure cleanup must join the admitted read
				// and actual DB Close before releasing the snapshot or deleting storage.
				dir, err := os.MkdirTemp("", "cow-c3-successor-close-")
				if err != nil {
					t.Fatal(err)
				}
				backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend")})
				if err != nil {
					_ = os.RemoveAll(dir)
					t.Fatal(err)
				}
				threshold := 1 << 30
				if pointer {
					threshold = 1
				}
				db, err := Open(dir, backend, Options{
					DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30,
					MemtableShards: 2, ValueLogPointerThreshold: threshold,
					ValueLogCompression: uint8(vlogCompressionOff),
				})
				if err != nil {
					_ = backend.Close()
					_ = os.RemoveAll(dir)
					t.Fatal(err)
				}
				c, err := newCOWCache(backend, 2, memtable.DefaultCOWLimits())
				if err != nil {
					_ = db.Close()
					_ = os.RemoveAll(dir)
					t.Fatal(err)
				}
				db.cow = c
				var snapshot *Snapshot
				entered, resume := make(chan struct{}), make(chan struct{})
				readDone, closeDone := make(chan struct{}), make(chan struct{})
				var resumeOnce sync.Once
				var readErr, closeErr error
				readStarted, closeStarted := false, false
				release := func() { resumeOnce.Do(func() { close(resume) }) }
				join := func(done <-chan struct{}, deadline time.Time) bool {
					select {
					case <-done:
						return true
					case <-time.After(time.Until(deadline)):
						return false
					}
				}
				t.Cleanup(func() {
					release()
					deadline := time.Now().Add(10 * time.Second)
					joined := true
					if readStarted && !join(readDone, deadline) {
						t.Errorf("read did not join; retaining %s", dir)
						joined = false
					}
					if closeStarted && !join(closeDone, deadline) {
						t.Errorf("Close did not join; retaining %s", dir)
						joined = false
					}
					if !joined {
						return
					}
					if snapshot != nil {
						if err := snapshot.Close(); err != nil {
							t.Error(err)
						}
					}
					if !closeStarted {
						if err := db.Close(); err != nil {
							t.Error(err)
						}
					}
					if err := os.RemoveAll(dir); err != nil {
						t.Error(err)
					}
				})
				value := bytes.Repeat([]byte("captured-pointer"), 8)
				if err := db.Set([]byte("a"), []byte("deleted")); err != nil {
					t.Fatal(err)
				}
				if err := db.Set([]byte("b"), value); err != nil {
					t.Fatal(err)
				}
				if err := db.Delete([]byte("a")); err != nil {
					t.Fatal(err)
				}
				if disk {
					if err := db.Checkpoint(); err != nil {
						t.Fatal(err)
					}
				}
				snapshot, err = db.AcquireMVCCReadCut()
				if err != nil {
					t.Fatal(err)
				}
				empty, err := snapshot.cowCut.basis.snapshot.OwnedUserRootEmpty()
				if err != nil || empty == disk {
					t.Fatalf("basis empty=%t err=%v", empty, err)
				}
				if !disk {
					sources := append([]memtable.Table(nil), snapshot.rootIterator.immutables...)
					wrapped := false
					for i, source := range sources {
						_, _, flags, found := source.GetEntry([]byte("a"))
						if found && flags&node.FlagTombstone != 0 {
							sources[i] = &c3PausedSuccessor{Table: source, entered: entered, resume: resume}
							wrapped = true
							break
						}
					}
					if !wrapped {
						t.Fatal("captured tombstone source missing")
					}
					snapshot.rootIterator.immutables = sources
				}
				readStarted = true
				go func() {
					defer close(readDone)
					var key, got []byte
					var found bool
					if disk {
						// The disk route uses the same production fallback helper. Holding its
						// real outer admission gives a deterministic pre-construction seam
						// without a permanent production pause hook.
						if readErr = snapshot.beginRead(); readErr != nil {
							return
						}
						defer snapshot.endRead()
						close(entered)
						<-resume
						key, got, found, readErr = snapshot.cowSeekGEMergeAdmitted([]byte("a"), []byte("z"))
					} else {
						key, got, found, readErr = snapshot.SeekGEVersionRange([]byte("a"), []byte("z"))
					}
					if readErr == nil && (!found || string(key) != "b" || !bytes.Equal(got, value)) {
						readErr = fmt.Errorf("captured successor key=%q found=%t value bytes=%d", key, found, len(got))
					}
				}()
				deadline := time.Now().Add(10 * time.Second)
				if !join(entered, deadline) {
					t.Fatal("successor did not reach admitted pause")
				}
				closeStarted = true
				go func() { defer close(closeDone); closeErr = db.Close() }()
				// With one admitted reader and no other storage writer, failed TryRLock
				// proves actual Close queued its exclusive gate. Launch alone proves none.
				for c.readMu.TryRLock() {
					c.readMu.RUnlock()
					if time.Now().After(deadline) {
						t.Fatal("actual Close did not queue its read gate")
					}
					runtime.Gosched()
				}
				release()
				if !join(readDone, deadline) {
					t.Fatal("successor deadlocked behind queued Close")
				}
				if !join(closeDone, deadline) {
					t.Fatal("actual Close did not join after successor")
				}
				if readErr != nil || closeErr != nil {
					t.Fatalf("read=%v Close=%v", readErr, closeErr)
				}
				if _, _, _, err := snapshot.SeekGEVersionRange(nil, nil); err != backenddb.ErrClosed {
					t.Fatalf("late successor=%v", err)
				}
				if err := snapshot.Close(); err != nil {
					t.Fatal(err)
				}
				snapshot = nil
				stats := c.budget.Stats()
				if c.activeCuts.Load() != 0 || stats.ReservedBytes != 0 {
					t.Fatalf("Close retained cuts=%d bytes=%d", c.activeCuts.Load(), stats.ReservedBytes)
				}
			})
		}
	}
}
