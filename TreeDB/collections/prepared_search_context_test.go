package collections

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPreparedSearchBuilderBarrierCancellation(t *testing.T) {
	for _, mode := range []VectorIndexQueryMode{VectorIndexQueryModeExact, VectorIndexQueryModeQuantizedOnly} {
		for _, route := range []string{"warm", "buffered"} {
			t.Run(string(mode)+"/"+route, func(t *testing.T) {
				rows := []columnGraphRebuildInputRowV2A{{id: "doc-a", vector: []float32{1, 0, 0}}, {id: "doc-b", vector: []float32{0, 1, 0}}}
				_, db, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
				defer db.Close()
				if _, err := col.RebuildVectorIndex(def.Name); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := col.CloseVectorIndexPreparedSearchCache(); err != nil {
						t.Error(err)
					}
				}()
				opts := VectorIndexSearchOptions{IndexName: def.Name, Query: rows[0].vector, QueryMode: mode, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}
				if mode == VectorIndexQueryModeQuantizedOnly {
					opts.QuantizedIndexName = def.QuantizedIndexes[0].Name
				}
				invoke := func(opts VectorIndexSearchOptions) error {
					if route == "warm" {
						_, err := col.WarmVectorIndexPreparedSearch(opts)
						return err
					}
					var buffer VectorIndexSearchBuffer
					response, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
					if err == nil && (len(response.Results) != 1 || string(response.Results[0].ID) != "doc-a") {
						return errors.New("healthy buffered retry returned wrong result")
					}
					return err
				}
				root, err := canonicalVectorPartitionStorageRootV1(db.Dir())
				if err != nil {
					t.Fatal(err)
				}
				held, release := make(chan struct{}), make(chan struct{})
				holder := make(chan error, 1)
				go func() {
					holder <- WithVectorPartitionStorageBarrierV1(root, func() error { close(held); <-release; return nil })
				}()
				<-held
				var releaseOnce sync.Once
				defer releaseOnce.Do(func() { close(release) })
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				opts.Context = ctx
				done := make(chan error, 1)
				go func() { done <- invoke(opts) }()
				deadline := time.NewTimer(5 * time.Second)
				defer deadline.Stop()
				tick := time.NewTicker(time.Millisecond)
				defer tick.Stop()
				for {
					vectorPartitionStorageBarriersV1.Lock()
					entry := vectorPartitionStorageBarriersV1.entries[root]
					waiting := entry != nil && entry.refs == 2
					vectorPartitionStorageBarriersV1.Unlock()
					if waiting {
						break
					}
					select {
					case err := <-done:
						t.Fatalf("builder exited before barrier: %v", err)
					case <-deadline.C:
						t.Fatal("builder did not reach held barrier")
					case <-tick.C:
					}
				}
				cancel()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Errorf("canceled builder: %v", err)
					}
				case <-time.After(time.Second):
					t.Error("canceled public builder remained blocked on storage barrier")
					releaseOnce.Do(func() { close(release) })
					<-done
				}
				releaseOnce.Do(func() { close(release) })
				if err := <-holder; err != nil {
					t.Fatal(err)
				}
				state := col.collectionVectorIndexPreparedSearchCacheSnapshot()
				if state.Entries != 0 || state.BuildingEntries != 0 || state.ActiveHandles != 0 {
					t.Fatalf("canceled builder retained cache resources: %+v", state)
				}
				// Nil context retains the legacy successful caller behavior.
				opts.Context = nil
				if err := invoke(opts); err != nil {
					t.Fatalf("healthy retry: %v", err)
				}
				state = col.collectionVectorIndexPreparedSearchCacheSnapshot()
				if state.Entries != 1 || state.BuildingEntries != 0 || state.ActiveHandles == 0 {
					t.Fatalf("healthy retry cache: %+v", state)
				}
			})
		}
	}
}
