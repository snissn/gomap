package mvcc

import (
	"bytes"
	"errors"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// This is a previsibility safety control, not the native success gate. The
// retained route must never ACK a selected history row at its missing seam.
func TestNativeMVCCRealBackendRouteRefusalPreservesCustodyV6(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.IndexOuterLeavesInValueLog = false
	opts.ValueLog.PointerThreshold = 2048
	opts.BackgroundCheckpointInterval = -1
	d, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	s := New(d)
	key := []byte("actual-selected-anchor")
	for ts := uint64(1); ts <= 33; ts++ {
		if err := s.CommitAt(ts, []Mutation{{Key: key, Value: []byte{byte(ts)}}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.AdvanceDiscardFloor(32, CommitDurable); err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s = New(d)
	oldKey, _ := mvcckey.Encode(key, 1)
	oldValue, err := d.Get(oldKey)
	if err != nil {
		t.Fatal(err)
	}
	oldReader := d.AcquireSnapshot()
	if oldReader == nil {
		t.Fatal("actual old reader missing")
	}
	defer oldReader.Close()
	var c *PruneCursor
	defer func() {
		if c != nil {
			if err := c.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	var lastPhase uint8 = 255
	for calls := 0; calls < 10000; calls++ {
		stats, err := s.PruneVersions(PruneOptions{Mode: CommitDurable, BatchSize: 1, WorkRecords: 32, WorkBytes: 1 << 20, Cursor: c})
		c = stats.Cursor
		if stats.WorkRecords > 32 || stats.WorkBytes > 1<<20 || stats.Pruned != 0 || stats.Complete {
			t.Fatalf("unqualified route acknowledged work: %+v err=%v", stats, err)
		}
		if c == nil {
			t.Fatalf("missing original cursor: %v", err)
		}
		if c.phase != lastPhase {
			t.Logf("call=%d phase=%d work=%d/%d selected=%d anchor=%d requestFloor=%d", calls, c.phase, stats.WorkRecords, stats.WorkBytes, len(c.batchEntries), c.anchorSize, c.request.Floor())
			lastPhase = c.phase
		}
		if err == nil {
			continue
		}
		t.Logf("first actual route refusal: calls=%d phase=%d error=%v", calls+1, c.phase, err)
		if !errors.Is(err, rootpublication.ErrNativeMVCCRootWorkRequiredV6) {
			t.Fatal(err)
		}
		if c.phase != 16 || c.preparation == nil || len(c.batchEntries) != 1 || !c.haveAnchor || c.request.Floor() != 32 {
			t.Fatalf("refused before real selected+anchor producer: phase=%d", c.phase)
		}
		if got, e := d.Get(oldKey); e != nil || !bytes.Equal(got, oldValue) {
			t.Fatalf("refused output became visible: %x %v", got, e)
		}
		if got, e := oldReader.Get(oldKey); e != nil || !bytes.Equal(got, oldValue) {
			t.Fatalf("old reader changed: %x %v", got, e)
		}
		if err := s.CommitAt(34, []Mutation{{Key: []byte("newer-real-writer"), Value: []byte("durable")}}, CommitDurable); err != nil {
			t.Fatalf("foreground writer after retained refusal: %v", err)
		}
		return
	}
	t.Fatalf("real route did not reach finite seam: phase=%d", c.phase)
}
