package db

import (
	"context"
	"io"
	"testing"
)

func TestPhysicalCutGenerationLeaseSurvivesSourceClose5105(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true, DisableSideStores: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	allocator := database.idx.Load().allocator
	cut, err := database.CapturePhysicalSnapshotCutV1(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer cut.Close()
	if got := allocator.ResidentGenerationProfileV1(); got.PhysicalCutLeases != 1 {
		t.Fatalf("capture missing=%+v", got)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if got := allocator.ResidentGenerationProfileV1(); got.PhysicalCutLeases != 1 {
		t.Fatalf("source Close erased lease=%+v", got)
	}
	if err := cut.WriteToContext(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	if got := allocator.ResidentGenerationProfileV1(); got.PhysicalCutLeases != 0 {
		t.Fatalf("cut Close retained lease=%+v", got)
	}
}
