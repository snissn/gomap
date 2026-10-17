package caching

import (
	"errors"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"testing"
)

func TestCOWTxnCallerOwnedBackendAndForgedOwnerReject(t *testing.T) {
	db, c := cowCutFixture(t)
	before := c.budget.Stats()
	if tx, err := db.NewConditionalTxn(); !errors.Is(err, backenddb.ErrConditionalTxnUnsupported) || tx != nil {
		t.Fatalf("caller-owned backend txn=%v %v", tx, err)
	}
	if err := db.InitOwnedCOWTxn(new(ConditionalTxn), transactionowner.New(), nil); !errors.Is(err, backenddb.ErrConditionalTxnUnsupported) {
		t.Fatal(err)
	}
	after := c.budget.Stats()
	if before != after {
		t.Fatalf("rejected owner admitted resources: before=%+v after=%+v", before, after)
	}
}
