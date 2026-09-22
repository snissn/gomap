package embedding

import (
	"context"
	"fmt"
	"runtime"
	"sync"
)

// RunBatch executes unit(i) for every i in [0,n) on a worker pool bounded by
// GOMAXPROCS, the collection write domain's existing parallelism limit. It is
// the shared orchestration behind EmbedBatch implementations.
//
// Contracts:
//   - Every index is attempted unless cancellation or an earlier error stops
//     the pool first; unit may be called concurrently and must be safe for it.
//   - The first error (or context cancellation) fails the whole batch: the
//     returned error wraps the original with the failing position, so callers
//     can never mistake partial progress for success.
//   - All workers join before RunBatch returns; cancellation leaves no
//     goroutines behind.
func RunBatch(ctx context.Context, n int, unit func(ctx context.Context, i int) error) error {
	if n < 0 {
		return fmt.Errorf("embedding: negative batch size %d", n)
	}
	if n == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("embedding: batch canceled before start: %w", err)
	}
	workers := runtime.GOMAXPROCS(0)
	if workers > n {
		workers = n
	}
	var (
		mu       sync.Mutex // guards cursor and batchErr
		cursor   int
		wg       sync.WaitGroup
		batchErr error
	)
	fail := func(i int, err error) {
		mu.Lock()
		if batchErr == nil {
			batchErr = fmt.Errorf("embedding: batch failed at position %d: %w", i, err)
		}
		mu.Unlock()
	}
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				i := cursor
				cursor++
				failed := batchErr != nil
				mu.Unlock()
				if i >= n || failed || ctx.Err() != nil {
					return
				}
				if err := unit(ctx, i); err != nil {
					fail(i, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if ctx.Err() != nil && batchErr == nil {
		return fmt.Errorf("embedding: batch canceled mid-flight: %w", ctx.Err())
	}
	return batchErr
}
