package db

import (
	"strings"
	"testing"
)

func TestNormalizeIndexOuterLeafMode_V1Only(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "", want: IndexOuterLeafModeV1},
		{in: " v1 ", want: IndexOuterLeafModeV1},
		{in: "V1", want: IndexOuterLeafModeV1},
		// Unsupported legacy mode strings remain normalized text and are rejected
		// by validateOptions/Open.
		{in: " V1_LEAFLOG ", want: "v1_leaflog"},
	}
	for _, tc := range tests {
		if got := normalizeIndexOuterLeafMode(tc.in); got != tc.want {
			t.Fatalf("normalizeIndexOuterLeafMode(%q)=%q want %q", tc.in, got, tc.want)
		}
	}
}

func TestOpen_IndexOuterLeafMode_DefaultAndV1(t *testing.T) {
	dbDefault, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open default: %v", err)
	}
	if got := dbDefault.indexOuterLeafMode; got != IndexOuterLeafModeV1 {
		t.Fatalf("default index outer leaf mode = %q, want %q", got, IndexOuterLeafModeV1)
	}
	_ = dbDefault.Close()

	dbExplicit, err := Open(Options{
		Dir:                t.TempDir(),
		IndexOuterLeafMode: IndexOuterLeafModeV1,
	})
	if err != nil {
		t.Fatalf("open explicit v1: %v", err)
	}
	if got := dbExplicit.indexOuterLeafMode; got != IndexOuterLeafModeV1 {
		t.Fatalf("explicit index outer leaf mode = %q, want %q", got, IndexOuterLeafModeV1)
	}
	_ = dbExplicit.Close()
}

func TestOpen_IndexOuterLeafMode_UnsupportedRejected(t *testing.T) {
	_, err := Open(Options{
		Dir:                t.TempDir(),
		IndexOuterLeafMode: "v1_leaflog_route",
	})
	if err == nil {
		t.Fatalf("expected unsupported mode error")
	}
	if !strings.Contains(err.Error(), "invalid index outer leaf mode") {
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

func TestOpen_ValueLogWALFenceMode_SimpleInlineRejected(t *testing.T) {
	_, err := Open(Options{
		Dir:                t.TempDir(),
		IndexOuterLeafMode: IndexOuterLeafModeV1,
		ValueLog: ValueLogOptions{
			WALFenceMode: ValueLogWALFenceModeSimpleInline,
		},
	})
	if err == nil {
		t.Fatalf("expected validation error")
	}
	if !strings.Contains(err.Error(), "unsupported with index outer leaf mode") {
		t.Fatalf("unexpected error: %v", err)
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
