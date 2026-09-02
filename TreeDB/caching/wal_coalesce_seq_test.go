package caching

import "testing"

func TestNextWALCoalesceSeqResetsAfterGlobalCommitSeqAdvance(t *testing.T) {
	var db DB
	var l lane

	l.walMu.Lock()
	seq := db.nextWALCoalesceSeqLocked(&l)
	if seq == 0 {
		l.walMu.Unlock()
		t.Fatalf("initial coalesce seq is zero")
	}
	if got := db.nextWALCoalesceSeqLocked(&l); got != seq {
		l.walMu.Unlock()
		t.Fatalf("same uninterrupted lane run got seq=%d want %d", got, seq)
	}
	l.walMu.Unlock()

	// Simulate any other lane or explicit batch issuing a global commit fence.
	otherSeq := db.nextCommitSeq.Add(1)
	if otherSeq <= seq {
		t.Fatalf("simulated global seq=%d want > %d", otherSeq, seq)
	}

	l.walMu.Lock()
	defer l.walMu.Unlock()
	if got := db.nextWALCoalesceSeqLocked(&l); got <= otherSeq {
		t.Fatalf("coalesce seq after global advance=%d want > %d", got, otherSeq)
	}
}
