package db

import (
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"testing"
)

type nativeSealRefundObserver struct {
	lease *memtable.COWExternalLease
	check func()
	calls int
}

func (o *nativeSealRefundObserver) Close() {
	o.calls++
	o.check()
	o.lease.Close()
}

func TestNativeSealOriginalCellRefundFollowsLastCallerAndQueueAlias(t *testing.T) {
	for _, order := range []string{"unattached", "caller-first", "queue-first"} {
		t.Run(order, func(t *testing.T) {
			budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			before := budget.Stats().ExternalBytes
			intent := &CommandWALIntent{}
			if err := intent.PrepareSealedReceiptWithAdmission(budget); err != nil {
				t.Fatal(err)
			}
			cell := intent.inner.sealedReceipt
			original := cell.lease.(*memtable.COWExternalLease)
			observer := &nativeSealRefundObserver{lease: original}
			observer.check = func() {
				if intent.inner.sealedReceipt != nil || cell.lease != nil || cell.refs.Load() != 0 || cell.receipt.Valid() {
					t.Fatal("refunded original request before aliases detached")
				}
				if !cell.mu.TryLock() {
					t.Fatal("refund retained original request mutex")
				}
				cell.mu.Unlock()
			}
			cell.lease = observer
			charged := budget.Stats().ExternalBytes
			if charged <= before {
				t.Fatal("request cell was not preadmitted")
			}
			var queue *NativeSealReceipt
			if order != "unattached" {
				queue, err = intent.TakeNativeSealReceipt()
				if err != nil || queue != cell {
					t.Fatalf("changed original queue cell: %v", err)
				}
				if _, err = intent.TakeNativeSealReceipt(); err == nil {
					t.Fatal("original request transferred twice")
				}
			}
			releaseQueue := func() {
				refund := queue.ReleaseNativeSeal()
				queue = nil
				if refund != nil {
					refund.Close()
				}
			}
			if order == "queue-first" {
				releaseQueue()
				if budget.Stats().ExternalBytes != charged || observer.calls != 0 {
					t.Fatal("queue refunded live original caller")
				}
			}
			intent.ReleaseNativeSealedReceipt()
			intent.ReleaseNativeSealedReceipt()
			if order == "caller-first" {
				if budget.Stats().ExternalBytes != charged || observer.calls != 0 {
					t.Fatal("caller refunded live original queue")
				}
				releaseQueue()
			}
			if observer.calls != 1 || budget.Stats().ExternalBytes != before {
				t.Fatal("original request accounting not completed exactly once")
			}
		})
	}
}
