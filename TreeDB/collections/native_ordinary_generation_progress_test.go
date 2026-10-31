package collections

import (
	"bytes"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// This is a success regression, distinct from the retained historical
// expected-refusal characterization. No checkpoint or capacity retry is allowed
// inside the public load. The small fixed requests leave scheduling headroom;
// this is not a guarantee that arbitrary requests fit the current generation.
func TestNativeOrdinaryGenerationProgress(t *testing.T) {
	const rows = 128
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.COWMemtableLimits = treedb.DefaultCOWMemtableLimits()
	opts.COWMemtableLimits.MaxGenerationBytes = 2 << 20
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	ownerOpen := true
	t.Cleanup(func() {
		if ownerOpen {
			if err := owner.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	managerOpen := true
	t.Cleanup(func() {
		if managerOpen {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if !manager.UsesNativeCommandWALApply() {
		t.Fatal("fixture did not select native command WAL owner")
	}
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatal(err)
	}
	c, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	doc := append(append([]byte(`{"payload":"`), bytes.Repeat([]byte("a"), 32<<10)...), []byte(`"}`)...)
	var held *NativeJSONRead
	// Capture the first actual ACK's immutable read for the whole load. Retaining
	// an old generation does not license reclaiming it during maintenance.
	t.Cleanup(func() {
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	var firstBasis backenddb.StateToken
	var failed []byte
	var acknowledged [][]byte
	for i := 0; i < rows; i++ {
		id := []byte(fmt.Sprintf("row-%06d", i))
		before, err := c.nativeJSON.db.ObserveNativeProducer()
		if err != nil {
			t.Fatal(err)
		}
		next := backend.CommandWALNextLSN()
		gotID, insertErr := c.Insert(id, doc)
		if insertErr != nil {
			t.Errorf("public progress stopped after %d/%d ACKs: id=%q err=%v", len(acknowledged), rows, id, insertErr)
			if backenddb.CommitPublicationAccepted(insertErr) {
				t.Fatal("failed request has accepted/uncertain publication; cannot classify it as an absent row")
			}
			failed = bytes.Clone(id)
			after, observeErr := c.nativeJSON.db.ObserveNativeProducer()
			if observeErr != nil || after.LogicalSequence != before.LogicalSequence || backend.CommandWALNextLSN() != next || len(gotID) != 0 {
				t.Fatalf("refusal changed logical/WAL state: before=%+v after=%+v next=%d observed=%d id=%q err=%v", before, after, next, backend.CommandWALNextLSN(), gotID, observeErr)
			}
			break // Preserve the real failure; never retry it toward GREEN.
		}
		if !bytes.Equal(gotID, id) {
			t.Fatalf("returned ID=%q want=%q", gotID, id)
		}
		acknowledged = append(acknowledged, bytes.Clone(id))
		if held == nil {
			held, err = c.nativeJSON.Capture(c.name)
			if err != nil {
				t.Fatal(err)
			}
			var valid bool
			firstBasis, valid = held.capture.Snapshot().StateToken()
			if !valid {
				t.Fatal("first ACK has no physical basis")
			}
		}
	}
	if len(acknowledged) != rows {
		t.Errorf("acknowledged=%d want=%d", len(acknowledged), rows)
	}
	if held == nil {
		t.Fatal("no acknowledged row available for lifecycle verification")
	}
	current, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	lastBasis, valid := current.capture.Snapshot().StateToken()
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if !valid || lastBasis.CommitSeq <= firstBasis.CommitSeq {
		t.Errorf("no automatic physical progress: first=%+v last=%+v", firstBasis, lastBasis)
	}
	verify := func() {
		t.Helper()
		for _, id := range acknowledged {
			got, err := c.Get(id)
			if err != nil || !bytes.Equal(got, doc) {
				t.Fatalf("ACK %q differs: bytes=%d err=%v", id, len(got), err)
			}
		}
		if len(failed) != 0 {
			if got, err := c.Get(failed); err != nil || got != nil {
				t.Fatalf("unaccepted row %q visible: bytes=%d err=%v", failed, len(got), err)
			}
		}
	}
	verify()
	if got, err := held.Get(acknowledged[0]); err != nil || !bytes.Equal(got, doc) {
		t.Fatalf("held ACK changed across ordinary load: bytes=%d err=%v", len(got), err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if got, err := held.Get(acknowledged[0]); err != nil || !bytes.Equal(got, doc) {
		t.Fatalf("held ACK changed across checkpoint: bytes=%d err=%v", len(got), err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	managerOpen = false
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	ownerOpen = false
	owner, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	ownerOpen = true
	backend, err = owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = NewCollectionManager(backend)
	managerOpen = true
	if !manager.UsesNativeCommandWALApply() {
		t.Fatal("reopen did not select native command WAL owner")
	}
	c, err = manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	verify()
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	managerOpen = false
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	ownerOpen = false
	t.Logf("exact lifecycle verified for %d ACKs; failed ID=%q; automatic basis %d -> %d", len(acknowledged), failed, firstBasis.CommitSeq, lastBasis.CommitSeq)
}
