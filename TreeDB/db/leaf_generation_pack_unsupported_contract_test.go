//go:build windows || darwin || freebsd || netbsd || openbsd

package db

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestLeafGenerationPackLockedFailsClosedForActualPromotionWithoutNamespaceSupport(t *testing.T) {
	db := &DB{leafGenerationManifest: &leafGenerationManifest{Generations: []leafGenerationRecord{{
		GenerationID: 1,
		State:        leafGenerationStateSealed,
		FileIDs:      []uint32{1},
	}}}}
	_, err := db.leafGenerationPackLocked(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{1},
	}, LeafGenerationPlan{}, LeafGenerationPackStats{}, nil)
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("LeafGenerationPack error=%v want typed namespace unsupported", err)
	}
}

func TestLeafGenerationPackLockedNoOpNeedsNoNamespaceSupport(t *testing.T) {
	db := &DB{}
	stats, err := db.leafGenerationPackLocked(context.Background(), LeafGenerationPackOptions{}, LeafGenerationPlan{}, LeafGenerationPackStats{}, nil)
	if err != nil {
		t.Fatalf("no-op LeafGenerationPack error=%v", err)
	}
	if stats.SourceFilesRequested != 0 {
		t.Fatalf("SourceFilesRequested=%d want 0", stats.SourceFilesRequested)
	}
}
