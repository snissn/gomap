package db

import (
	"strings"
	"testing"
)

func TestOpen_IndexOuterLeafModeV2_NotImplemented(t *testing.T) {
	_, err := Open(Options{
		Dir:                t.TempDir(),
		IndexOuterLeafMode: IndexOuterLeafModeV2BlockPtr,
	})
	if err == nil {
		t.Fatalf("expected error for %q mode", IndexOuterLeafModeV2BlockPtr)
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Fatalf("unexpected error: %v", err)
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
