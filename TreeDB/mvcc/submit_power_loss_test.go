package mvcc

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
)

// Retain only observed stable bytes/names, before Close can checkpoint the
// acknowledged group. Recover through public Open, including external values
// and WAL rotation. This is a modeled power-loss witness, not a clean reopen.
func TestSubmitCommitGroupAtPointerStableImagePublicReopen(t *testing.T) {
	for _, size := range []int{4096, 65 << 10} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
			opts.MemtableMode = "cow_ordered"
			opts.MemtableShards = 4
			opts.FlushThreshold = 1 << 30
			opts.DisableSideStores = true
			opts.DisableBackgroundPrune = true
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.BackgroundIndexVacuumInterval = -1
			opts.MaxWALBytes = -1
			opts.ValueLog.PointerThreshold = 1
			opts.ValueLog.ForcePointers = true
			opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff
			if size > 4096 {
				opts.CommandWALSegmentTargetBytes = 1
			}
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			model, err := powerlossoracle.Capture(opts.Dir)
			if err != nil {
				t.Fatal(err)
			}
			var modelMu sync.Mutex
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				modelMu.Lock()
				defer modelMu.Unlock()
				return model.Observe(opts.Dir, e)
			})
			var restoreOnce sync.Once
			stop := func() { restoreOnce.Do(restore) }
			defer stop()
			s := New(db)
			var futures []*treedb.CommitFuture
			for ts := uint64(1); ts <= 3; ts++ {
				f, err := s.SubmitCommitGroupAt([]CommitGroup{{Timestamp: ts, Mutations: []Mutation{
					{Key: []byte("k"), Value: bytes.Repeat([]byte{byte(ts)}, size)},
					{Key: []byte("pair"), Value: bytes.Repeat([]byte{byte(ts + 10)}, size)},
				}}}, CommitDurable)
				if err != nil {
					t.Fatal(err)
				}
				futures = append(futures, f)
			}
			for _, f := range futures {
				if err := f.Wait(); err != nil {
					t.Fatal(err)
				}
			}
			stop()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			result, reopened, closeReopened, err := powerlossreopen.Stable(model, opts, false)
			if closeReopened != nil {
				defer closeReopened()
			}
			if err != nil || result.Rejected || reopened == nil {
				t.Fatalf("stable public Open rejected acknowledged group: %+v %v", result, err)
			}
			recovered := New(reopened)
			for ts := uint64(1); ts <= 3; ts++ {
				for _, key := range []string{"k", "pair"} {
					got, err := recovered.GetAt([]byte(key), ts)
					v := byte(ts)
					if key == "pair" {
						v += 10
					}
					if err != nil || got.State != Present || got.Timestamp != ts || !bytes.Equal(got.Value, bytes.Repeat([]byte{v}, size)) {
						t.Fatalf("stable recovered %s@%d: state=%v ts=%d bytes=%d err=%v", key, ts, got.State, got.Timestamp, len(got.Value), err)
					}
				}
			}
		})
	}
}
