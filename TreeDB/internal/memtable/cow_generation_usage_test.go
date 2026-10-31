package memtable

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func cowGenerationUsageFixture(t *testing.T) (*COWBudget, *COWRoot) {
	t.Helper()
	b, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	r := nativeSourceTestRoot(t, b, 128)
	t.Cleanup(func() {
		b.Close()
		cowTestRelease(r)
		if stats := b.Stats(); stats.TotalBytes != 0 || stats.Sources != 0 || stats.Generations != 0 {
			t.Errorf("observation retained an owner or charge: %+v", stats)
		}
	})
	return b, r
}

func TestCOWGenerationUsageIncludesNativeMetadataWithoutRetention(t *testing.T) {
	b, root := cowGenerationUsageFixture(t)
	before := b.Stats()
	if before.ExternalBytes == 0 || before.HistoryBytes == 0 {
		t.Fatalf("fixture lacks actual native source metadata/history: %+v", before)
	}
	// The only external allocation in this fixture is the native source cohort.
	// It is accounted separately from HistoryBytes globally but must consume
	// this generation's limit along with the ordinary COW history.
	want := COWGenerationUsage{
		HistoryBytes:       before.HistoryBytes + before.ExternalBytes,
		MaxGenerationBytes: b.Limits().MaxGenerationBytes,
	}
	root.mu.Lock()
	refs, tree, generation := root.refs, root.tree, root.generation
	root.mu.Unlock()
	b.mu.Lock()
	generationRefs, activeWriters := generation.refs, generation.activeWriters
	b.mu.Unlock()
	for i := 0; i < 100; i++ {
		got, err := root.GenerationUsage()
		if err != nil || got != want {
			t.Fatalf("observation %d: got=%+v want=%+v err=%v", i, got, want, err)
		}
	}
	if after := b.Stats(); after != before {
		t.Fatalf("observation changed accounting: before=%+v after=%+v", before, after)
	}
	root.mu.Lock()
	rootChanged := root.refs != refs || root.tree != tree || root.generation != generation
	root.mu.Unlock()
	b.mu.Lock()
	generationChanged := generation.refs != generationRefs || generation.activeWriters != activeWriters
	b.mu.Unlock()
	if rootChanged || generationChanged {
		t.Fatal("observation changed root or generation ownership")
	}
	if got, ok := root.Get([]byte("key")); !ok || got.Value != "value" {
		t.Fatalf("observation changed the immutable row: %+v found=%v", got, ok)
	}
	// Fixture cleanup drops the sole original root and requires full refund;
	// a hidden retained edge cannot pass merely by preserving visible bytes.
}

func TestCOWGenerationUsageIncludesSameGenerationPendingAndCancel(t *testing.T) {
	b, root := cowGenerationUsageFixture(t)
	fork, err := ForkCOWWriter(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cowTestClose(fork) })
	before, err := root.GenerationUsage()
	if err != nil {
		t.Fatal(err)
	}
	beforeStats := b.Stats()
	prepared, err := fork.Prepare([]COWMutation{{Key: []byte("key"), Value: []byte("unpublished")}}, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if prepared != nil {
			retirement := prepared.Cancel()
			retirement.Drain()
		}
	})
	charge := prepared.Charge().Total()
	if charge == 0 || charge <= prepared.Charge().History() {
		t.Fatal("fixture lacks a real preparation including temporary reservation")
	}
	got, err := root.GenerationUsage()
	if err != nil || got.HistoryBytes != before.HistoryBytes || got.PendingBytes != charge || got.MaxGenerationBytes != before.MaxGenerationBytes {
		t.Fatalf("original root missed fork reservation: before=%+v pending=%+v charge=%d err=%v", before, got, charge, err)
	}
	// Global reservations also include the existing native source's external
	// lease. Only the new Prepare charge belongs to this generation's pending
	// observation; retain that original global reservation in the comparison.
	if stats := b.Stats(); stats.ReservedBytes != beforeStats.ReservedBytes+charge || stats.TotalBytes != beforeStats.TotalBytes+charge || stats.HistoryBytes != beforeStats.HistoryBytes || stats.ExternalBytes != beforeStats.ExternalBytes {
		t.Fatalf("observation differs from actual admitted reservation: before=%+v after=%+v charge=%d", beforeStats, stats, charge)
	}
	if row, ok := root.Get([]byte("key")); !ok || row.Value != "value" {
		t.Fatalf("private preparation changed the observed root: %+v found=%v", row, ok)
	}
	retirement := prepared.Cancel()
	prepared = nil
	retirement.Drain()
	after, err := root.GenerationUsage()
	if err != nil || after != before {
		t.Fatalf("cancel did not clear pending usage: before=%+v after=%+v err=%v", before, after, err)
	}
	if stats := b.Stats(); stats.TotalBytes != beforeStats.TotalBytes || stats.ReservedBytes != beforeStats.ReservedBytes || stats.HistoryBytes != beforeStats.HistoryBytes || stats.ExternalBytes != beforeStats.ExternalBytes {
		t.Fatalf("cancel failed to refund original reservation: before=%+v after=%+v", beforeStats, stats)
	}
}

func TestCOWGenerationUsageRejectsClosedLifetime(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		var root *COWRoot
		if got, err := root.GenerationUsage(); !errors.Is(err, ErrCOWClosed) || got != (COWGenerationUsage{}) {
			t.Fatalf("nil observation=%+v err=%v", got, err)
		}
	})
	for _, closedBudget := range []bool{false, true} {
		name := "released-root"
		if closedBudget {
			name = "closed-budget-owned-root"
		}
		t.Run(name, func(t *testing.T) {
			b, err := NewCOWBudget(DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			root := nativeSourceTestRoot(t, b, 128)
			if closedBudget {
				b.Close()
			} else {
				cowTestRelease(root)
			}
			before := b.Stats()
			got, observeErr := root.GenerationUsage()
			if !errors.Is(observeErr, ErrCOWClosed) || got != (COWGenerationUsage{}) {
				t.Errorf("invalid lifetime observation=%+v err=%v", got, observeErr)
			}
			if after := b.Stats(); after != before {
				t.Errorf("failed observation changed accounting: before=%+v after=%+v", before, after)
			}
			if closedBudget {
				cowTestRelease(root)
			} else {
				b.Close()
			}
			if stats := b.Stats(); stats.TotalBytes != 0 {
				t.Errorf("closed lifetime leaked: %+v", stats)
			}
		})
	}
}

func TestCOWGenerationUsageConcurrentForkPreparation(t *testing.T) {
	b, root := cowGenerationUsageFixture(t)
	fork, err := ForkCOWWriter(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cowTestClose(fork) })
	baseline, err := root.GenerationUsage()
	if err != nil {
		t.Fatal(err)
	}
	beforeStats := b.Stats()
	entries := []COWMutation{{Key: []byte("key"), Value: []byte("private")}}
	estimate, err := fork.Estimate(entries, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	reservation := estimate.Total()
	// Measure the real pending state once, then race the same preparation shape
	// with independent observers. The owned immutable root remains live until
	// all goroutines join; no observer relies on a borrowed root after release.
	prepared, err := fork.Prepare(entries, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pending := prepared.Charge().Total()
	if got, err := root.GenerationUsage(); err != nil || got.PendingBytes != pending {
		retirement := prepared.Cancel()
		retirement.Drain()
		t.Fatalf("pending setup: got=%+v want=%d err=%v", got, pending, err)
	}
	retirement := prepared.Cancel()
	prepared = nil
	retirement.Drain()
	const observers = 3
	start := make(chan struct{})
	errorsFound := make(chan error, observers+1)
	var joined sync.WaitGroup
	joined.Add(observers + 1)
	for observer := 0; observer < observers; observer++ {
		go func() {
			defer joined.Done()
			<-start
			for i := 0; i < 512; i++ {
				got, err := root.GenerationUsage()
				// An observer may run before node reconciliation refunds the
				// unused part of the original conservative reservation.
				if err != nil || got.HistoryBytes != baseline.HistoryBytes || got.MaxGenerationBytes != baseline.MaxGenerationBytes || got.PendingBytes > reservation {
					errorsFound <- fmt.Errorf("concurrent observation=%+v baseline=%+v reservation=%d err=%v", got, baseline, reservation, err)
					return
				}
			}
		}()
	}
	go func() {
		defer joined.Done()
		<-start
		for i := 0; i < 256; i++ {
			p, err := fork.Prepare(entries, COWPrepareOptions{})
			if err != nil {
				errorsFound <- fmt.Errorf("concurrent prepare %d: %w", i, err)
				return
			}
			retirement := p.Cancel()
			p = nil
			retirement.Drain()
		}
	}()
	close(start)
	joined.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
	if got, err := root.GenerationUsage(); err != nil || got != baseline {
		t.Errorf("final observation=%+v want=%+v err=%v", got, baseline, err)
	}
	if stats := b.Stats(); stats.TotalBytes != beforeStats.TotalBytes || stats.ReservedBytes != beforeStats.ReservedBytes || stats.HistoryBytes != beforeStats.HistoryBytes || stats.ExternalBytes != beforeStats.ExternalBytes {
		t.Errorf("concurrent cancellation/observation leaked: before=%+v after=%+v", beforeStats, stats)
	}
	if got, ok := root.Get([]byte("key")); !ok || got.Value != "value" {
		t.Errorf("concurrent private preparation changed root: %+v found=%v", got, ok)
	}
}
