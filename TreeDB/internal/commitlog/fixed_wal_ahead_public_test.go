package commitlog

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Only predecessor APIs are used in this file. Project this file alone onto
// 36ff for the causal RED: it has no unpublished initialization while the
// current segment still accepts commands. No performance threshold is tested.
func TestFixedWALAheadPublicAppendProgress(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	dir := t.TempDir()
	j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	oldSync := fixedWALInitializeSync
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	fixedWALInitializeSync = func(file *os.File) error {
		if strings.Contains(filepath.Base(file.Name()), ".wal-ahead-") {
			once.Do(func() { close(entered) })
			<-release
		}
		return oldSync(file)
	}
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer func() { unblock(); _ = j.Close(); fixedWALInitializeSync = oldSync }()
	frame := fixedWALLifecycleFrame(t, 256<<10)
	count := 0
	// The next public append observes flushed half occupancy and starts work.
	for count < 10 {
		if _, err = j.AppendCommand(frame); err != nil {
			t.Fatal(err)
		}
		count++
		if err = j.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no unpublished ahead reserve before pressure rotation")
	}
	if j.Path() != filepath.Join(dir, CommandSegmentName(0, 1)) {
		t.Fatal("preparation published a successor")
	}
	if _, err = os.Stat(filepath.Join(dir, CommandSegmentName(0, 2))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canonical successor before adoption: %v", err)
	}
	progress := make(chan error, 1)
	go func() {
		_, e := j.AppendCommand(fixedWALTestBarrier())
		if e == nil {
			e = j.Flush()
		}
		progress <- e
	}()
	select {
	case err = <-progress:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		unblock()
		<-progress
		t.Fatal("foreground append waited on private reserve sync")
	}
	count++
	closing := make(chan error, 1)
	go func() { closing <- j.Close() }()
	select {
	case err = <-closing:
		t.Fatalf("Close did not retain in-flight worker custody: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	select {
	case err = <-closing:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not join released worker")
	}
	fixedWALInitializeSync = oldSync
	frames, err := ScanCommandFramesV2(filepath.Join(dir, CommandSegmentName(0, 1)), Options{})
	if err != nil || len(frames) != count {
		t.Fatalf("accepted prefix: frames=%d want=%d err=%v", len(frames), count, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".wal-ahead-") {
			t.Fatal("unused reserve survived joined Close")
		}
	}
	reopened, err := openFixedTestCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.NextLSN() != uint64(count+1) {
		t.Fatal("reopen lost accepted prefix")
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
