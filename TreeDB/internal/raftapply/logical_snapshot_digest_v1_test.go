package raftapply

import (
	"context"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func TestLogicalSnapshotDigestV1MatchesCanonicalDigestAcrossLayouts(t *testing.T) {
	var expected LogicalDigestV1
	for _, outer := range []bool{false, true} {
		t.Run(map[bool]string{false: "inline-leaves", true: "outer-leaves"}[outer], func(t *testing.T) {
			database := openApplyHarnessDBWithOptions(t, t.TempDir(), backenddb.Options{IndexOuterLeavesInValueLog: outer})
			defer database.Close()
			create := deterministicCreateCollectionEntry(t, "users", "snapshot:create", testCreateCollectionMetaOptions{})
			insert := deterministicInsertBatchEntry(t, "users", "snapshot:insert", nativewire.DocumentFormatJSON,
				[][]byte{[]byte("z"), []byte("a"), []byte("é"), []byte("a-longer-key")},
				[][]byte{[]byte(`{"value":3}`), []byte(`{"value":1}`), []byte(`{"value":4}`), []byte(`{"value":2}`)})
			applyCreateSequence(t, database, create, insert)
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			want, err := LogicalDigestV1ForDB(database, LogicalDigestOptionsV1{})
			if err != nil {
				t.Fatal(err)
			}
			got, err := LogicalDigestV1ForSnapshotDB(database, LogicalDigestOptionsV1{})
			if err != nil || got != want {
				t.Fatalf("streamed=%s canonical=%s err=%v", got.Hex(), want.Hex(), err)
			}
			if expected != (LogicalDigestV1{}) && expected != got {
				t.Fatal("physical layout changed snapshot digest")
			}
			// Two admission checks precede the count scan; four IDs precede the
			// hash scan. Cancel after processing one real ID in either pass.
			for _, cancelAt := range []int{4, 8} {
				ctx, cancel := context.WithCancel(context.Background())
				probe := &snapshotDigestCancelContext{Context: ctx, cancel: cancel, cancelAt: cancelAt}
				_, err := LogicalDigestV1ForSnapshotDBContext(probe, database, LogicalDigestOptionsV1{})
				cancel()
				if !errors.Is(err, context.Canceled) || probe.calls < cancelAt {
					t.Fatalf("cancel at check %d: calls=%d err=%v", cancelAt, probe.calls, err)
				}
			}
			expected = got
		})
	}
}

// The digest checks cancellation synchronously. A counting context deterministically
// interrupts nonempty scans without timing races or production test hooks.
type snapshotDigestCancelContext struct {
	context.Context
	cancel          context.CancelFunc
	cancelAt, calls int
}

func (c *snapshotDigestCancelContext) Err() error {
	c.calls++
	if c.calls == c.cancelAt {
		c.cancel()
	}
	return c.Context.Err()
}
