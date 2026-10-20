package caching

import (
	"errors"
	"fmt"
	"testing"
)

// Automatic streaming is currently disabled by the value-log selectors. Keep
// the retained backend-batch state covered without claiming public activation.
func TestBatchWALOffBypassSyncBoundaryAndReuse(t *testing.T) {
	for _, legacyBackend := range []bool{false, true} {
		for _, relaxed := range []bool{false, true} {
			t.Run(fmt.Sprintf("legacy_backend_%t/relaxed_%t", legacyBackend, relaxed), func(t *testing.T) {
				mock := NewMockBackend()
				backend := &checkpointBoundaryBackend{BackendDB: mock}
				cache, err := Open(t.TempDir(), backend, Options{
					DisableWAL: true, RelaxedSync: relaxed, AllowUnsafe: true, FlushThreshold: 1,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer cache.Close()
				b := cache.NewBatch()
				defer b.Close()
				for i, syncWrite := range []bool{false, true, false} {
					if legacyBackend {
						b.backend = backend.NewBatch()
					}
					if err := b.Set([]byte(fmt.Sprintf("key/%d", i)), []byte("value")); err != nil {
						t.Fatal(err)
					}
					if syncWrite {
						err = b.WriteSync()
					} else {
						err = b.Write()
					}
					if err != nil {
						t.Fatal(err)
					}
					wantBoundaries := 0
					if i > 0 {
						wantBoundaries = 1
					}
					if backend.calls != wantBoundaries {
						t.Fatalf("checkpoint calls after write %d=%d want %d", i, backend.calls, wantBoundaries)
					}
					if mock.writeCalls != i+1 || mock.writeSyncs != 0 {
						t.Fatalf("serialized backend writes=%d sync writes=%d want %d/0; WAL-off sync must use unlocked checkpoint", mock.writeCalls, mock.writeSyncs, i+1)
					}
					if b.backend != nil || len(b.entries) != 0 || b.size != 0 {
						t.Fatal("successful write did not reset the batch")
					}
					b.Reset()
				}
				if err := b.Close(); err != nil {
					t.Fatal(err)
				}
				if err := b.WriteSync(); !errors.Is(err, ErrBatchClosed) {
					t.Fatalf("closed WriteSync=%v want ErrBatchClosed", err)
				}
			})
		}
	}
}

func TestBatchWALOffBypassSyncPropagatesWriteAndBoundaryErrors(t *testing.T) {
	for _, legacyBackend := range []bool{false, true} {
		for _, writeFailure := range []bool{false, true} {
			t.Run(fmt.Sprintf("legacy_backend_%t/write_failure_%t", legacyBackend, writeFailure), func(t *testing.T) {
				want := errors.New("injected write or checkpoint failure")
				mock := NewMockBackend()
				backend := &checkpointBoundaryBackend{BackendDB: mock}
				cache, err := Open(t.TempDir(), backend, Options{
					DisableWAL: true, RelaxedSync: true, AllowUnsafe: true, FlushThreshold: 1,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					mock.SetWriteErr(nil)
					backend.err = nil
					_ = cache.Close()
				}()
				b := cache.NewBatch()
				defer b.Close()
				if legacyBackend {
					b.backend = backend.NewBatch()
					b.batchRange.add([]byte("key"))
				}
				if err := b.Set([]byte("key"), []byte("value")); err != nil {
					t.Fatal(err)
				}
				wantBoundaries := 1
				if writeFailure {
					mock.SetWriteErr(want)
					wantBoundaries = 0
				} else {
					backend.err = want
				}
				if err := b.WriteSync(); !errors.Is(err, want) {
					t.Fatalf("WriteSync=%v want %v", err, want)
				}
				if backend.calls != wantBoundaries {
					t.Fatalf("checkpoint calls=%d want %d", backend.calls, wantBoundaries)
				}
				// Preserve the callers' original error/reset distinction: retained
				// backend streams close/reset after write, while bypass keeps input.
				if legacyBackend {
					if b.backend != nil || b.size != 0 {
						t.Fatal("failed legacy backend write did not close/reset")
					}
				} else if b.size == 0 || len(b.entries) == 0 {
					t.Fatal("failed bypass discarded its input")
				}
				if !writeFailure {
					// Publication succeeded before checkpoint failed. Its committed
					// range must remain visible to iterator range pruning.
					cache.mu.Lock()
					cache.backendRangeKnown = true
					cache.mu.Unlock()
					it, err := cache.Iterator([]byte("key"), []byte("kez"))
					if err != nil {
						t.Fatal(err)
					}
					defer it.Close()
					if !it.Valid() || string(it.Key()) != "key" || string(it.Value()) != "value" {
						t.Fatal("checkpoint error hid the already-published key from iteration")
					}
				}
			})
		}
	}
}
