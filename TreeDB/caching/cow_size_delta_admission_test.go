package caching

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWSizeDeltaOverflowCancelsBeforeCommandAppend(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		t.Run(fmt.Sprintf("ordered_%t", ordered), func(t *testing.T) {
			backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			c, err := newCOWCacheMode(backend, 2, memtable.DefaultCOWLimits(), ordered)
			if err != nil {
				_ = backend.Close()
				t.Fatal(err)
			}
			t.Cleanup(func() { c.close(); _ = backend.Close() })
			groups := make([][]memtable.COWMutation, 2)
			for i := range groups {
				key := []byte(fmt.Sprintf("key-%d", i))
				if ordered {
					key, err = mvcckey.Encode(key, 7)
					if err != nil {
						t.Fatal(err)
					}
				}
				groups[i] = []memtable.COWMutation{{Key: key, Value: []byte("value"), Flags: node.FlagInline, MVCC: ordered, Timestamp: 7}}
			}
			// Corrupt only the host size scalar to reach the otherwise physically
			// impossible signed boundary. The second shard must cancel the first
			// shard's already concrete preparation and both successor promises.
			opening, refs := c.cut, c.cut.refs
			opening.shards[1].size = math.MaxInt64
			before, maintenance := c.budget.Stats(), c.maintenance.Stats()
			nextLSN := backend.CommandWALNextLSN()
			var prepared *cowPreparedCut
			finalized, scanned := false, false
			c.writerMu.Lock()
			lsn, err := backend.AppendRawKVCommandWALOrderedEntryScanWithHintPreparedFinalizedAndMode(func() error {
				var prepareErr error
				prepared, prepareErr = c.prepare(groups, make([]memtable.COWPrepareOptions, 2))
				return prepareErr
			}, func([]byte, func(page.ValuePtr) (uint64, bool)) error {
				finalized = true
				return nil
			}, func(func(batch.Entry) error) error {
				scanned = true
				return nil
			}, 2, backenddb.RawKVCommandWALAppendRelaxed)
			c.writerMu.Unlock()
			if prepared == nil || !prepared.consumed || !errors.Is(err, memtable.ErrCOWCapacity) || lsn != 0 || finalized || scanned || backend.CommandWALNextLSN() != nextLSN {
				t.Fatalf("overflow escaped private preparation: p=%p lsn=%d err=%v finalizer=%t scan=%t", prepared, lsn, err, finalized, scanned)
			}
			if err := prepared.drainCancelled(); err != nil {
				t.Fatal(err)
			}
			for _, pair := range [][2]memtable.COWStats{{before, c.budget.Stats()}, {maintenance, c.maintenance.Stats()}} {
				want, got := pair[0], pair[1]
				got.PeakBytes = want.PeakBytes
				if got != want {
					t.Fatalf("overflow retained private credits: before=%+v after=%+v", want, got)
				}
			}
			if c.cut != opening || opening.refs != refs {
				t.Fatal("overflow replaced or retained opening cut")
			}
			for _, group := range groups {
				for _, shard := range opening.shards {
					if _, exists := shard.root.Get(group[0].Key); exists {
						t.Fatal("overflow exposed a private postimage")
					}
				}
			}
			opening.shards[1].size = 0
			c.writerMu.Lock()
			retry, err := c.prepare(groups, make([]memtable.COWPrepareOptions, 2))
			if err != nil {
				c.writerMu.Unlock()
				t.Fatal("overflow left the original writer pending", err)
			}
			retired := retry.publish()
			c.writerMu.Unlock()
			if retired != nil {
				retired.drain()
			}
			for i, group := range groups {
				if got := c.cut.shards[i].size; got != int64(len(group[0].Key)+len(group[0].Value)) {
					t.Fatalf("resumed shard=%d size=%d", i, got)
				}
			}
		})
	}
}
