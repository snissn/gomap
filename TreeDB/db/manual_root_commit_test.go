package db

import (
	"errors"
	"testing"
)

func TestCommitAtStateRejectsAdvancedBasis(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = database.Close() }()

	basis, ok := database.StateToken()
	if !ok {
		t.Fatal("initial state token unavailable")
	}
	if err := database.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("advance visible root: %v", err)
	}

	err = database.CommitAtState(basis.RootPageID, basis)
	if !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("stale CommitAtState error=%v want %v", err, ErrConcurrentModification)
	}
	value, err := database.Get([]byte("key"))
	if err != nil || string(value) != "value" {
		t.Fatalf("value after stale rejection=%q err=%v want value", string(value), err)
	}

	current, ok := database.StateToken()
	if !ok {
		t.Fatal("current state token unavailable")
	}
	if err := database.CommitAtState(current.RootPageID, current); err != nil {
		t.Fatalf("CommitAtState at current basis: %v", err)
	}
}
