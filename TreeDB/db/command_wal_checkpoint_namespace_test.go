package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

func TestAutomaticCOWJournalRotationRequiresOriginalOwner(t *testing.T) {
	owner := new(transactionowner.Owner)
	database := &DB{privateCOWOwner: owner}
	for _, supplied := range []*transactionowner.Owner{nil, new(transactionowner.Owner)} {
		finish, err := database.RotateCommandWALAtAutomaticCOWCut(supplied)
		if finish != nil || !errors.Is(err, ErrNativeSemanticUnsupported) {
			t.Fatalf("unbound owner: finish=%t error=%v", finish != nil, err)
		}
	}
	finish, err := database.RotateCommandWALAtAutomaticCOWCut(owner)
	if finish != nil || err != nil {
		t.Fatalf("owned non-WAL fallback: finish=%t error=%v", finish != nil, err)
	}
}
