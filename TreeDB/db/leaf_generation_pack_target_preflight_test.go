package db

import (
	"context"
	"errors"
	"os"
	"testing"
)

func TestLeafGenerationPackTargetProbeBypassedForZeroSourceNoOp(t *testing.T) {
	originalProbe := leafGenerationPackPromotionTargetProbe
	probeCalls := 0
	testErr := errors.New("target probe must not run")
	leafGenerationPackPromotionTargetProbe = func(*os.File) error {
		probeCalls++
		return testErr
	}
	defer func() { leafGenerationPackPromotionTargetProbe = originalProbe }()

	db := &DB{}
	stats, err := db.leafGenerationPackLocked(context.Background(), LeafGenerationPackOptions{}, LeafGenerationPlan{}, LeafGenerationPackStats{}, nil)
	if err != nil {
		t.Fatalf("zero-source LeafGenerationPack error=%v", err)
	}
	if probeCalls != 0 || stats.SourceFilesRequested != 0 {
		t.Fatalf("probe calls=%d source files=%d want 0,0", probeCalls, stats.SourceFilesRequested)
	}
}
