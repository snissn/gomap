package db

import (
	"errors"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func preAppendTestBatch(t *testing.T, key, value string) *batch.Batch {
	t.Helper()
	b := batch.NewBoundedRetainingLargeEntries(nil, -1, 1)
	if err := b.Set([]byte(key), []byte(value)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestOrderedRootPreAppendContextRefusalBurnsNoIdentity(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	intent := mustRawKVCommandWALIntent(t, d, "cmd/refused", "value")
	before := d.commandJournal.NextLSN()
	state := d.State()
	refused := errors.New("actual exact context cannot be prepared")
	installed := false
	_, _, err = d.PublishOrderedRootDeltaBatchGroupWithPreAppendContextAndPreparedLimits(nil, nil, intent,
		func(ctx OrderedRootPreAppendContext) (OrderedRootPreAppendPlan, error) {
			if ctx.AppliedCommandLSN != before || intent.AssignedLSN() != 0 {
				t.Fatal("preparation consumed identity")
			}
			return OrderedRootPreAppendPlan{}, refused
		},
		func(CommandWALPublishContext) ([]OrderedRootDeltaBatchPublishInput, error) {
			installed = true
			return nil, nil
		},
		func(CommandWALPublishContext, []uint64) (iterator.UnsafeIterator, error) {
			t.Fatal("system effects after refused preparation")
			return nil, nil
		}, nil)
	if !errors.Is(err, refused) || installed || intent.AssignedLSN() != 0 || d.commandJournal.NextLSN() != before || d.State().CommitSeq != state.CommitSeq {
		t.Fatal("refusal changed command/root authority", err)
	}
}

func TestOrderedRootPreAppendContextUsesExactIdentityAndOneApply(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	intent := mustRawKVCommandWALIntent(t, d, "cmd/exact", "value")
	contextDelta := preAppendTestBatch(t, "manifest/exact", "owned bytes")
	systemKeys := preAppendTestBatch(t, "system/exact", "late value")
	prepared, installed := 0, 0
	var projected uint64
	system, roots, err := d.PublishOrderedRootDeltaBatchGroupWithPreAppendContextAndPreparedLimits(nil, nil, intent,
		func(ctx OrderedRootPreAppendContext) (OrderedRootPreAppendPlan, error) {
			prepared++
			projected = ctx.AppliedCommandLSN
			if intent.AssignedLSN() != 0 {
				t.Fatal("already appended")
			}
			return OrderedRootPreAppendPlan{ContextDeltas: []OrderedRootDeltaBatchPublishInput{{Delta: contextDelta}}, SystemDelta: systemKeys}, nil
		},
		func(ctx CommandWALPublishContext) ([]OrderedRootDeltaBatchPublishInput, error) {
			installed++
			if ctx.AppliedCommandLSN != projected || intent.AssignedLSN() != projected {
				t.Fatal("append changed projected LSN")
			}
			return []OrderedRootDeltaBatchPublishInput{{Delta: contextDelta}}, nil
		},
		func(ctx CommandWALPublishContext, ids []uint64) (iterator.UnsafeIterator, error) {
			if len(ids) != 1 || ids[0] == 0 {
				t.Fatal("actual context root missing")
			}
			m := mustFrozenSystemMemtable(t, "system/exact", strconv.FormatUint(ids[0], 10))
			return m.NewIterator(nil, nil), nil
		}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if prepared != 1 || installed != 1 || system == 0 || len(roots) != 1 || d.State().AppliedCommandLSN != projected {
		t.Fatal("packet was not one serial publication", prepared, installed, roots)
	}
	snap := d.AcquireSnapshot()
	defer snap.Close()
	if got, err := snap.GetAtRoot(roots[0], []byte("manifest/exact")); err != nil || string(got) != "owned bytes" {
		t.Fatal("installed context content differs", string(got), err)
	}
}

func TestOrderedRootPreAppendContextRejectsInstalledReferenceMismatchBeforeApply(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	intent := mustRawKVCommandWALIntent(t, d, "cmd/mismatch", "value")
	planned := preAppendTestBatch(t, "manifest/exact", "file1 offset0")
	actual := preAppendTestBatch(t, "manifest/exact", "file2 offset8")
	system := preAppendTestBatch(t, "system/exact", "late")
	before := d.State()
	systemBuilt := false
	_, _, err = d.PublishOrderedRootDeltaBatchGroupWithPreAppendContextAndPreparedLimits(nil, nil, intent,
		func(OrderedRootPreAppendContext) (OrderedRootPreAppendPlan, error) {
			return OrderedRootPreAppendPlan{ContextDeltas: []OrderedRootDeltaBatchPublishInput{{Delta: planned}}, SystemDelta: system}, nil
		},
		func(CommandWALPublishContext) ([]OrderedRootDeltaBatchPublishInput, error) {
			return []OrderedRootDeltaBatchPublishInput{{Delta: actual}}, nil
		},
		func(CommandWALPublishContext, []uint64) (iterator.UnsafeIterator, error) {
			systemBuilt = true
			return nil, nil
		}, nil)
	if !errors.Is(err, ErrOrderedRootPreAppendMismatch) || intent.AssignedLSN() == 0 || systemBuilt || d.State().CommitSeq != before.CommitSeq || d.State().RootPageID != before.RootPageID {
		t.Fatal("mismatched installed ref reached Apply/publication", err)
	}
	if err := d.CheckCommandWALPublishReady(); err == nil {
		t.Fatal("post-append mismatch did not poison retry")
	}
}
