package db

import (
	"strings"
	"testing"
)

func TestOpen_IndexOuterLeafModeV2_Enabled(t *testing.T) {
	modes := []string{
		IndexOuterLeafModeV2BlockPtr,
		IndexOuterLeafModeV2FencePtr,
	}
	for _, mode := range modes {
		db, err := Open(Options{
			Dir:                t.TempDir(),
			IndexOuterLeafMode: mode,
		})
		if err != nil {
			t.Fatalf("open %q: %v", mode, err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}
}

func TestOpen_IndexOuterLeafMode_DefaultEmptyUsesV2FencePtr(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open default: %v", err)
	}
	defer func() { _ = db.Close() }()
	if got := db.indexOuterLeafMode; got != IndexOuterLeafModeV2FencePtr {
		t.Fatalf("default index outer leaf mode = %q, want %q", got, IndexOuterLeafModeV2FencePtr)
	}
}

func TestOpen_ValueLogOuterLeafBlockTargetBytes_NegativeRejected(t *testing.T) {
	_, err := Open(Options{
		Dir: t.TempDir(),
		ValueLog: ValueLogOptions{
			OuterLeafBlockTargetBytes: -1,
		},
	})
	if err == nil {
		t.Fatalf("expected validation error")
	}
	if !strings.Contains(err.Error(), "outer-leaf block target bytes") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpen_ValueLogWALFenceMode_DefaultAndSimpleInline(t *testing.T) {
	dbDefault, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open default: %v", err)
	}
	if err := dbDefault.Close(); err != nil {
		t.Fatalf("close default: %v", err)
	}

	dbSimpleInline, err := Open(Options{
		Dir: t.TempDir(),
		ValueLog: ValueLogOptions{
			WALFenceMode: ValueLogWALFenceModeSimpleInline,
		},
	})
	if err != nil {
		t.Fatalf("open simple_inline: %v", err)
	}
	if err := dbSimpleInline.Close(); err != nil {
		t.Fatalf("close simple_inline: %v", err)
	}
}

func TestOpen_ValueLogWALFenceMode_InvalidRejected(t *testing.T) {
	_, err := Open(Options{
		Dir: t.TempDir(),
		ValueLog: ValueLogOptions{
			WALFenceMode: ValueLogWALFenceMode("bogus"),
		},
	})
	if err == nil {
		t.Fatalf("expected validation error")
	}
	if !strings.Contains(err.Error(), "WAL fence mode") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpen_ValueLogWALFenceMode_V2FencePtrWALOn_ExplicitRIDJoinAllowed(t *testing.T) {
	dbRIDJoin, err := Open(Options{
		Dir:                t.TempDir(),
		IndexOuterLeafMode: IndexOuterLeafModeV2FencePtr,
		Durability:         DurabilityWALOnRelaxed,
		ValueLog: ValueLogOptions{
			WALFenceMode: ValueLogWALFenceModeRIDJoin,
		},
	})
	if err != nil {
		t.Fatalf("open WAL-on rid_join: %v", err)
	}
	if err := dbRIDJoin.Close(); err != nil {
		t.Fatalf("close WAL-on rid_join: %v", err)
	}
}

func TestOpen_ValueLogWALFenceMode_V2FencePtrWALOn_DefaultAutoSimpleInline(t *testing.T) {
	dbWALOn, err := Open(Options{
		Dir:                t.TempDir(),
		IndexOuterLeafMode: IndexOuterLeafModeV2FencePtr,
		Durability:         DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("open WAL-on default fence mode: %v", err)
	}
	if err := dbWALOn.Close(); err != nil {
		t.Fatalf("close WAL-on default fence mode: %v", err)
	}
}

func TestOpen_ValueLogWALFenceMode_V2FencePtrWALOff_ExplicitRIDJoinAllowed(t *testing.T) {
	dbWALOff, err := Open(Options{
		Dir:                t.TempDir(),
		IndexOuterLeafMode: IndexOuterLeafModeV2FencePtr,
		Durability:         DurabilityWALOffRelaxed,
		ValueLog: ValueLogOptions{
			WALFenceMode: ValueLogWALFenceModeRIDJoin,
		},
	})
	if err != nil {
		t.Fatalf("open WAL-off rid_join: %v", err)
	}
	if err := dbWALOff.Close(); err != nil {
		t.Fatalf("close WAL-off rid_join: %v", err)
	}
}

func TestOpen_ValueLogOuterLeafBlobThresholdBytes_NegativeRejected(t *testing.T) {
	_, err := Open(Options{
		Dir: t.TempDir(),
		ValueLog: ValueLogOptions{
			OuterLeafBlobThresholdBytes: -1,
		},
	})
	if err == nil {
		t.Fatalf("expected validation error")
	}
	if !strings.Contains(err.Error(), "outer-leaf blob threshold bytes") {
		t.Fatalf("unexpected error: %v", err)
	}
}
