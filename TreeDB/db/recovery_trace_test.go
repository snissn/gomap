package db

import (
	"errors"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/recoverytrace"
)

func TestRecoveryLedgerStoragePhaseErrorsPreserveOriginalOrder(t *testing.T) {
	for _, fail := range []string{"", "materialize", "index", "install"} {
		t.Run(fail, func(t *testing.T) {
			sentinel := errors.New("original producer failure")
			var order []string
			step := func(name string) error {
				order = append(order, name)
				if fail == name {
					return sentinel
				}
				return nil
			}
			ledger := recoverytrace.New(8)
			restore, ok := recoverytrace.Install(ledger)
			if !ok {
				t.Fatal("diagnostic ledger overlap")
			}
			defer restore()
			mutated, err := executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
				materialize:      func() error { return step("materialize") },
				syncIndex:        func() error { return step("index") },
				installAuthority: func() (bool, error) { return true, step("install") },
			})
			want := []string{"materialize", "index", "install"}
			if fail == "materialize" {
				want = want[:1]
			}
			if fail == "index" {
				want = want[:2]
			}
			if !reflect.DeepEqual(order, want) || mutated != (len(want) == 3) {
				t.Fatalf("original phase order/custody changed: %v mutated=%t", order, mutated)
			}
			if (fail == "" && err != nil) || (fail != "" && !errors.Is(err, sentinel)) {
				t.Fatalf("original error identity changed: %v", err)
			}
			wantCount := uint64(4)
			if fail == "materialize" {
				wantCount = 2
			}
			if ledger.Attempted() != wantCount || ledger.Dropped() != 0 {
				t.Fatal("wrong phase count")
			}
			for index := 0; index < int(wantCount); index += 2 {
				start, ready := ledger.Read(index)
				end, ended := ledger.Read(index + 1)
				if !ready || !ended || end.Parent != start.Sequence {
					t.Fatal("lost original phase pair")
				}
				expected := recoverytrace.Returned
				if (fail == "materialize" && index == 0) || (fail == "index" && index == 2) {
					expected = recoverytrace.ReturnedError
				}
				if end.Outcome != expected {
					t.Fatal("wrong original phase outcome")
				}
			}
		})
	}
}
