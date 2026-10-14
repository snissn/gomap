package rootpublication

import (
	"context"
	"crypto/sha256"
	"errors"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This isolates the original physical constructor Finish/recovery terminal
// boundary. Public caller controls separately exercise its publication route.
func TestPrimaryFinishContainingRefundJoinsOriginalCall(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		name := "finish"
		if recovery {
			name = "recovery"
		}
		t.Run(name, func(t *testing.T) {
			a, e := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
			if e != nil {
				t.Fatal(e)
			}
			defer a.Close()
			original, ready, e := NewPrimaryConstructionTransactionV6(a, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
			if !ready || e != nil {
				t.Fatal(ready, e)
			}
			txn := original.(*DurableRootTransaction)
			set, e := NewStableResourceSetBuilder().Freeze()
			if e != nil {
				t.Fatal(e)
			}
			set.owner.Store(uint32(ResourceOwnerCoordinator))
			txn.constructionV6.resources = set
			txn.owner = ResourceOwnerCoordinator
			txn.phase = durableRootFinishing
			budget, e := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
			if e != nil {
				t.Fatal(e)
			}
			observed, locked, lateState := false, false, false
			wrapper := &containingUnlockBudget{actual: budget, observe: func() {
				if txn.constructionV6 == nil {
					observed = true
					lateState = txn.owner != ResourceOwnerReleased || txn.phase != durableRootConsumed
					if !txn.mu.TryLock() {
						locked = true
					} else {
						txn.mu.Unlock()
					}
				}
			}}
			capture, e := a.MetadataOwner().Enroll(wrapper)
			if e != nil {
				t.Fatal(e)
			}
			if recovery {
				txn.owner = ResourceOwnerRecovery
				set.owner.Store(uint32(ResourceOwnerRecovery))
				if e = txn.prepareFromRecovery(); e != nil {
					t.Fatal(e)
				}
				if e = set.releaseFrom(ResourceOwnerRecovery); e != nil {
					t.Fatal(e)
				}
				node, err := txn.releaseFromRecovery()
				e = err
				if e == nil {
					completePrimaryTerminalsV6(&node)
				}
			} else {
				node, err := txn.finishOrdinaryPrimaryV6(&iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}, nil)
				e = err
				if e == nil {
					completePrimaryTerminalsV6(&node)
				}
			}
			if e != nil {
				t.Fatal(e)
			}
			if !observed || locked || lateState {
				t.Fatalf("actual containing refund: observed=%t locked=%t state-written-after-refund=%t", observed, locked, lateState)
			}
			capture.Close()
			budget.Close()
		})
	}
}

// This uses the maintained constructor, immutable DATA certificate, candidate,
// Enqueue and startup WaitThrough path. The retained-owner lease pauses the
// actual containing refund, after physical Finish and callback scope return.
func TestPrimaryFinishAccountingBeforeAckAndStop(t *testing.T) {
	for _, stop := range []bool{false, true} {
		name := "ack"
		if stop {
			name = "stop"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			a, e := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
			if e != nil {
				t.Fatal(e)
			}
			defer a.Close()
			w := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
			original, ready, e := NewPrimaryConstructionTransactionV6(a, w)
			if !ready || e != nil {
				t.Fatal(ready, e)
			}
			txn := original.(*DurableRootTransaction)
			ref := txn.PrimaryConstructionReferenceV6()
			data := freelist.NewMemoryPageStoreV1()
			base, system := primaryRootTestLeaf(2, "a", "old", 1), primaryRootTestLeaf(3, "system", "s", 1)
			data.Pages[2], data.Pages[3] = base, system
			generation := freelist.MustNewFreelistGenerationV1(1, 4, nil, nil)
			image := make([]byte, page.PageSize)
			if e = node.EncodePrimaryDirectory(image, ref.PageID, 1, node.PrimaryOperand{Ref: page.PageChildRef(2), Digest: sha256.Sum256(base)}, nil); e != nil {
				t.Fatal(e)
			}
			if ready, e = txn.SealPrimaryConstructionV6(image, [1]*primaryarena.Group{}, w); !ready || e != nil {
				t.Fatal(ready, e)
			}
			frontier := NewFrontier(1, ref.PageID, 3, 0, 1)
			prepared, ready, e := NewPreparedPrimaryReadRootCertificateV6(txn.PrimaryConstructionCertificateV6(), generation, nil, frontier, data, nil, w)
			if !ready || e != nil {
				t.Fatal(ready, e)
			}
			set, e := NewStableResourceSetBuilder().Freeze()
			if e != nil {
				t.Fatal(e)
			}
			noop := func(DurableRootCallbackInput) error { return nil }
			if ready, e = txn.BindPrimaryVisibilityV6(DurableRootTransactionSpec{Lineage: DurableRootLineageID{1}, Sequence: 1, PreparedPrimary: prepared, Activate: noop, Consume: noop, Abort: noop, Fail: func(DurableRootCallbackInput, error) error { return nil }}, set, w); !ready || e != nil {
				t.Fatal(ready, e)
			}
			defer func() {
				if ok, e := a.Drop(ref, w); !ok || e != nil {
					t.Errorf("state edge: %t %v", ok, e)
				}
			}()
			candidate, e := NewPreparedRootCandidate(CandidateSpec{Frontier: frontier, TotalPages: 4, ResourceSet: set, DurableRoot: txn})
			if e != nil {
				t.Fatal(e)
			}
			c, e := New(Options{StartupSynchronous: true, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
				return PublishResult{Outcome: PublishSucceeded}
			})})
			if e != nil {
				t.Fatal(e)
			}
			budget, e := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
			if e != nil {
				t.Fatal(e)
			}
			held, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			var observed, violation atomic.Bool
			wrapper := &containingUnlockBudget{actual: budget, observe: func() {
				if txn.constructionV6 != nil || !observed.CompareAndSwap(false, true) {
					return
				}
				if txn.owner != ResourceOwnerReleased || txn.phase != durableRootConsumed {
					violation.Store(true)
				}
				if !txn.mu.TryLock() {
					violation.Store(true)
				} else {
					txn.mu.Unlock()
				}
				if !c.mu.TryLock() {
					violation.Store(true)
				} else {
					if c.durable.commitSeq != 0 || !c.publishing || c.activeAttempt.id != 0 || c.retryAttempt.id != 0 || len(c.pending) != 0 || c.activeDispatch != 1 {
						violation.Store(true)
					}
					c.mu.Unlock()
				}
				close(held)
				<-release
			}}
			capture, e := a.MetadataOwner().Enroll(wrapper)
			if e != nil {
				t.Fatal(e)
			}
			defer func() { unblock(); capture.Close(); budget.Close() }()
			if e = c.Enqueue(ctx, candidate); e != nil {
				t.Fatal(e)
			}
			waited := make(chan error, 1)
			go func() { waited <- c.WaitThrough(ctx, 1) }()
			select {
			case <-held:
			case <-ctx.Done():
				t.Fatal("containing refund was not reached")
			}
			if violation.Load() || c.Stats().DurableCommitSeq != 0 {
				t.Fatal("terminal refund retained an original lock/alias or advanced ACK")
			}
			if e = c.CompleteStartup(); !errors.Is(e, ErrPublisherProtocol) {
				t.Fatalf("startup accepted active accounting: %v", e)
			}
			select {
			case e = <-waited:
				t.Fatalf("WaitThrough returned before terminal accounting: %v", e)
			default:
			}
			stopped := make(chan error, 1)
			if stop {
				go func() { stopped <- c.Stop(ctx) }()
				select {
				case <-c.ctx.Done():
				case <-ctx.Done():
					t.Fatal("Stop did not enter")
				}
				select {
				case e = <-stopped:
					t.Fatalf("Stop returned before foreground terminal accounting: %v", e)
				case <-time.After(25 * time.Millisecond):
				}
			}
			unblock()
			select {
			case e = <-waited:
			case <-ctx.Done():
				t.Fatal("WaitThrough did not join")
			}
			if !stop && e != nil {
				t.Fatal(e)
			}
			if stop && e != nil && !errors.Is(e, ErrPublicationStopped) && !errors.Is(e, ErrClosed) {
				t.Fatal(e)
			}
			if c.Stats().DurableCommitSeq != 1 {
				t.Fatal("physical and accounting completion did not advance ACK")
			}
			if stop {
				select {
				case e = <-stopped:
				case <-ctx.Done():
					t.Fatal("Stop did not join")
				}
				if e != nil {
					t.Fatal(e)
				}
			} else {
				if e = c.Stop(ctx); e != nil {
					t.Fatal(e)
				}
			}
		})
	}
}
