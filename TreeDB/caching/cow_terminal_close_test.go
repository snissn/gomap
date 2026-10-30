package caching

import (
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// Pause the original physical reader destructor, rather than introducing a
// shutdown hook. Close must join its actual resource disposal when no cut is held.
type cowTerminalCloseDecoder struct {
	entered chan struct{}
	resume  <-chan struct{}
}

func (*cowTerminalCloseDecoder) Decode(valuelog.FrameHeader, []byte, []byte) ([]byte, error) {
	panic("terminal-close decoder is only a destruction observer")
}

func (d *cowTerminalCloseDecoder) Close() {
	close(d.entered)
	<-d.resume
}

func TestCOWCloseJoinsTerminalPhysicalReaderRefund(t *testing.T) {
	db := cowPointReadFixture(t)
	c := db.cow
	entered, resume := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	started := false
	defer func() {
		release()
		if started {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("Close did not join after releasing original decoder")
			}
		}
	}()
	c.physicalMu.Lock()
	w, err := c.physicalWorkspaceLocked(c.readLimits())
	if err == nil {
		var slot int
		slot, err = w.addCodecSlot()
		if err == nil {
			w.codecs[slot] = &cowReadCodec{decoder: &cowTerminalCloseDecoder{entered: entered, resume: resume}}
		}
	}
	c.physicalMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	started = true
	go func() { done <- db.Close() }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("original physical reader destructor did not run")
	}
	select {
	case err := <-done:
		started = false
		t.Fatalf("Close returned before terminal physical reader refund: err=%v resources=%+v", err, db.COWMemoryStats())
	case <-time.After(100 * time.Millisecond):
	}
	release()
	select {
	case err := <-done:
		started = false
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not finish original physical reader disposal")
	}
	if stats := db.COWMemoryStats(); stats.TotalBytes != 0 || stats.Sources != 0 || stats.ExternalLeases != 0 || stats.Views != 0 || stats.Generations != 0 {
		t.Fatalf("terminal resources: %+v", stats)
	}
}
