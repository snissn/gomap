//go:build darwin || freebsd || netbsd || openbsd

package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestLeafGenerationPackPromotionPreflightFailsClosedWithoutAtomicMove(t *testing.T) {
	if err := leafGenerationPackPromotionPreflight(); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("preflight error=%v want typed namespace unsupported", err)
	}
}
