package treedb

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCOWCloseKeepsSystemRootMutationFenceThroughFinalCheckpoint(t *testing.T) {
	for iter := 0; iter < 5; iter++ {
		opts := OptionsFor(ProfileCommandWALRelaxed, t.TempDir())
		opts.MemtableMode = "cow_btree"
		opts.BackgroundCheckpointInterval = -1
		opts.BackgroundCheckpointIdleDuration = -1

		database, err := Open(opts)
		if err != nil {
			t.Fatalf("iter %d open: %v", iter, err)
		}
		for i := 0; i < 64; i++ {
			if err := database.Set([]byte(fmt.Sprintf("k-%03d", i)), []byte("payload-value-for-final-checkpoint")); err != nil {
				_ = database.Close()
				t.Fatalf("iter %d set: %v", iter, err)
			}
		}

		// Concurrently mutate SystemRootPageID while Close() runs its final
		// command-WAL checkpoint and fallback convergence. Before keeping
		// SetSystemRootMutationFence installed through cached.Close(), an
		// unfenced ApplySystemOps between CheckpointDeferringCommandWALCleanup
		// and refreshPublicCommandWALCheckpointFallback diverged SystemRootPageID
		// from the COW basis and failed Close with ErrRootPublicationBasisMismatch.
		backend := database.backend
		var stop atomic.Bool
		started := make(chan struct{})
		var startOnce sync.Once
		errCh := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := 0; !stop.Load(); seq++ {
				opErr := backend.ApplySystemOps([]backenddb.SystemKVOp{{
					Key:   []byte(fmt.Sprintf("dict/test/%d", seq)),
					Value: []byte("system-metadata-value"),
				}}, false)
				startOnce.Do(func() { close(started) })
				if opErr != nil {
					if errors.Is(opErr, backenddb.ErrClosed) {
						return
					}
					select {
					case errCh <- opErr:
					default:
					}
					return
				}
			}
		}()

		<-started
		closeErr := database.Close()
		stop.Store(true)
		wg.Wait()

		if closeErr != nil {
			t.Fatalf("iter %d Close failed: %v", iter, closeErr)
		}
		select {
		case opErr := <-errCh:
			t.Fatalf("iter %d ApplySystemOps failed unexpectedly: %v", iter, opErr)
		default:
		}
	}
}
