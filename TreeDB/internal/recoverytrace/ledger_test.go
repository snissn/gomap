package recoverytrace

import (
	"strings"
	"sync"
	"testing"
)

func TestLedgerBoundedOwnershipAndOverflow(t *testing.T) {
	if New(0) != nil || New(MaxRecords+1) != nil {
		t.Fatal("invalid capacity accepted")
	}
	ledger := New(2)
	text := Text(strings.Repeat("x", LabelBytes+1))
	if !text.Truncated || len(text.String()) != LabelBytes {
		t.Fatal("label truncation was hidden")
	}
	first := ledger.Record(Event{Stage: PhysicalSync, Outcome: Started, Path: text, LSN: 7})
	ledger.End(PhysicalSync, first, true)
	if ledger.Record(Event{Stage: Fixture}) != 0 {
		t.Fatal("overflow returned a usable correlation")
	}
	start, ok := ledger.Read(0)
	if !ok || start.Sequence != 1 || start.LSN != 7 || !start.Path.Truncated {
		t.Fatal("lost first owned record")
	}
	end, ok := ledger.Read(1)
	if !ok || end.Parent != first || end.Outcome != ReturnedError {
		t.Fatal("lost original error outcome")
	}
	if ledger.Attempted() != 3 || ledger.Dropped() != 1 || ledger.Capacity() != 2 {
		t.Fatal("overflow accounting changed")
	}
	if _, ok := ledger.Read(2); ok {
		t.Fatal("read outside fixed storage")
	}
}

func TestLedgerSingleInstallationAndRestore(t *testing.T) {
	ledger, other := New(2), New(2)
	restore, ok := Install(ledger)
	if !ok {
		t.Fatal("install refused")
	}
	defer restore()
	if Active() != ledger {
		t.Fatal("wrong active ledger")
	}
	if _, ok := Install(other); ok {
		t.Fatal("overlapping install accepted")
	}
	restore()
	if Active() != nil {
		t.Fatal("restore retained installation")
	}
	if _, ok := Install(ledger); ok {
		t.Fatal("reused original installation")
	}
	replacement := New(2)
	restoreReplacement, ok := Install(replacement)
	if !ok {
		t.Fatal("fresh replacement refused")
	}
	defer restoreReplacement()
	restore()
	if Active() != replacement {
		t.Fatal("old restore removed replacement")
	}
	// A captured original producer can finish without retaining any installed
	// authority; the original ledger remains an ordinary Go object.
	ledger.Record(Event{Stage: PhysicalSync, Outcome: Returned})
	if _, ok := ledger.Read(0); !ok {
		t.Fatal("late bounded producer lost")
	}
}

func TestLedgerConcurrentPublicationAndRead(t *testing.T) {
	const writers, each = 8, 16
	ledger := New(writers * each)
	var workers sync.WaitGroup
	start, done := make(chan struct{}), make(chan struct{})
	for worker := 0; worker < writers; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			<-start
			for n := 0; n < each; n++ {
				ledger.Record(Event{Stage: ReplayFrame, LSN: uint64(worker*each + n + 1)})
			}
		}(worker)
	}
	go func() { workers.Wait(); close(done) }()
	close(start)
	read := func() {
		for index := 0; index < ledger.Capacity(); index++ {
			event, ready := ledger.Read(index)
			if ready && (event.Sequence != uint64(index+1) || event.LSN == 0) {
				t.Fatal("partially published event")
			}
		}
	}
	for {
		select {
		case <-done:
			read()
			if ledger.Attempted() != writers*each || ledger.Dropped() != 0 {
				t.Fatal("lost publication")
			}
			return
		default:
			read()
		}
	}
}
