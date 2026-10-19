package caching

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

// The real named backend operation runs once; only its return is held so Close
// can revoke new admission while the original maintenance flush fence is live.
type cowClosingRewriteBackend struct {
	*backenddb.DB
	cache                  *DB
	entered, resume        chan struct{}
	report                 error
	calls, closingCaptures atomic.Int32
}

func (b *cowClosingRewriteBackend) ValueLogRewriteOnlineForPrivateCOW(owner *transactionowner.Owner, ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error) {
	b.calls.Add(1)
	stats, err := b.DB.ValueLogRewriteOnlineForPrivateCOW(owner, ctx, opts)
	if err != nil {
		return stats, err
	}
	close(b.entered)
	<-b.resume
	return stats, b.report
}
func (b *cowClosingRewriteBackend) AcquireSnapshotWithAllocationAdmission(admit func(backenddb.SnapshotAllocationSizes) error) (*backenddb.Snapshot, error) {
	snap, err := b.DB.AcquireSnapshotWithAllocationAdmission(admit)
	if err == nil && b.cache.closing.Load() && b.cache.maintenanceActive.Load() {
		b.closingCaptures.Add(1)
	}
	return snap, err
}

func TestCOWEquivalentRewriteCompletesThroughPendingClose(t *testing.T) {
	for _, reported := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "reported_error"}[reported], func(t *testing.T) {
			cache, _ := nativeBridgeFixture(t, false)
			budget := cache.cow.budget
			if err := cache.Set([]byte("pointer"), []byte("original persistent value")); err != nil {
				t.Fatal(err)
			}
			if err := cache.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			backend := &cowClosingRewriteBackend{DB: cache.backend.(*backenddb.DB), cache: cache, entered: make(chan struct{}), resume: make(chan struct{})}
			if reported {
				backend.report = errors.New("original rewrite report after completion")
			}
			cache.backend = backend
			rewriteDone, closeDone := make(chan error, 1), make(chan error, 1)
			go func() {
				_, err := cache.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{})
				rewriteDone <- err
			}()
			released := false
			defer func() {
				if !released {
					close(backend.resume)
				}
			}()
			select {
			case <-backend.entered:
			case err := <-rewriteDone:
				t.Fatalf("rewrite admission: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("rewrite did not enter")
			}
			go func() { closeDone <- cache.Close() }()
			deadline := time.Now().Add(5 * time.Second)
			for !cache.closing.Load() && time.Now().Before(deadline) {
				runtime.Gosched()
			}
			if !cache.closing.Load() {
				t.Fatal("Close did not revoke new admission")
			}
			select {
			case err := <-closeDone:
				t.Fatalf("Close crossed live maintenance fence: %v", err)
			default:
			}
			close(backend.resume)
			released = true
			select {
			case err := <-rewriteDone:
				if !errors.Is(err, backend.report) {
					t.Fatalf("original rewrite error=%v want %v", err, backend.report)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("rewrite completion blocked by Close")
			}
			select {
			case err := <-closeDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not finish")
			}
			if backend.calls.Load() != 1 || backend.closingCaptures.Load() == 0 {
				t.Fatalf("completion calls=%d closing basis captures=%d", backend.calls.Load(), backend.closingCaptures.Load())
			}
			if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ReservedBytes != 0 {
				t.Fatalf("final original budget: %+v", stats)
			}
		})
	}
}
