//go:build windows

package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCaptureStableIndexFileResourceFailsClosedWithoutNamespacePrimitive(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	snapshot := database.AcquireStableSnapshot()
	if snapshot == nil {
		t.Fatal("AcquireStableSnapshot returned nil")
	}
	defer snapshot.Close()
	token, err := snapshot.CaptureStableIndexFileResource()
	if token != nil {
		token.Release()
		t.Fatal("unsupported stable index capture returned authority")
	}
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("stable index capture error=%v want ErrNamespacePersistenceUnsupported", err)
	}
}
