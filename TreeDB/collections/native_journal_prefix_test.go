package collections

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestNativeJournalPrefixManagerAdmissionPrecedence(t *testing.T) {
	failure := errors.New("original manager initialization refused")
	manager := &CollectionManager{initializationError: failure}
	if physical, err := manager.SyncForStandaloneWriteConcern(); physical || !errors.Is(err, failure) {
		t.Fatalf("initialization precedence=%t/%v", physical, err)
	}
	if manager.CanShareStandaloneJournalBoundary() || (&CollectionManager{}).CanShareStandaloneJournalBoundary() {
		t.Fatal("initialization-error/classic manager granted sharing")
	}
	var missing *CollectionManager
	if missing.CanShareStandaloneJournalBoundary() {
		t.Fatal("nil manager granted sharing")
	}
	if physical, err := missing.SyncForStandaloneWriteConcern(); physical || !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("nil manager=%t/%v", physical, err)
	}
}
