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

func TestOpen_ValueLogWALFenceMode_V2FencePtrWALOn_ExplicitRIDJoinRejected(t *testing.T) {
	_, err := Open(Options{
		Dir:                t.TempDir(),
		IndexOuterLeafMode: IndexOuterLeafModeV2FencePtr,
		Durability:         DurabilityWALOnRelaxed,
		ValueLog: ValueLogOptions{
			WALFenceMode: ValueLogWALFenceModeRIDJoin,
		},
	})
	if err == nil {
		t.Fatalf("expected WAL-enabled v2_fenceptr rid_join to be rejected")
	}
	if !strings.Contains(err.Error(), "WAL-enabled index outer leaf mode") {
		t.Fatalf("unexpected error: %v", err)
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
