package db

import (
	"strings"
	"testing"
)

func TestOpen_IndexOuterLeafModeV2_Enabled(t *testing.T) {
	db, err := Open(Options{
		Dir:                t.TempDir(),
		IndexOuterLeafMode: IndexOuterLeafModeV2BlockPtr,
	})
	if err != nil {
		t.Fatalf("open %q: %v", IndexOuterLeafModeV2BlockPtr, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
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
