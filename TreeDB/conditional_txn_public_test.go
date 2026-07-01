package treedb

import (
	"errors"
	"testing"
)

func TestPublicConditionalTxnCachedHandleFailsClosed(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.NewConditionalTxn(); !errors.Is(err, ErrConditionalTxnUnsupported) {
		t.Fatalf("NewConditionalTxn error=%v, want ErrConditionalTxnUnsupported", err)
	}
}
