//go:build windows

package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestLeafGenerationPackPromotionPreflightFailsClosedOnWindows(t *testing.T) {
	if err := leafGenerationPackPromotionPreflight(); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("preflight error=%v want typed namespace unsupported", err)
	}
}
