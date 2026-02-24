package db

import (
	"strings"
	"testing"
)

func TestNormalizeIndexOuterLeafMode_LeafLogModesRemainDistinct(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "leaflog",
			in:   " V1_LEAFLOG ",
			want: IndexOuterLeafModeV1LeafLog,
		},
		{
			name: "leaflog legacy",
			in:   " V1_LEAFLOG_LEGACY ",
			want: IndexOuterLeafModeV1LeafLogLegacy,
		},
		{
			name: "leaflog route",
			in:   " V1_LEAFLOG_ROUTE ",
			want: IndexOuterLeafModeV1LeafLogRoute,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeIndexOuterLeafMode(tc.in); got != tc.want {
				t.Fatalf("normalizeIndexOuterLeafMode(%q)=%q want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestOpen_IndexOuterLeafModeV2_Enabled(t *testing.T) {
	modes := []string{
		IndexOuterLeafModeV1LeafLog,
		IndexOuterLeafModeV1LeafLogLegacy,
		IndexOuterLeafModeV1LeafLogRoute,
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

func TestOpen_IndexOuterLeafMode_MixedCaseLeafLogModesCanonicalized(t *testing.T) {
	tests := []struct {
		name string
		mode string
		want string
	}{
		{
			name: "v1_leaflog",
			mode: " V1_LEAFLOG ",
			want: IndexOuterLeafModeV1LeafLog,
		},
		{
			name: "v1_leaflog_legacy",
			mode: " V1_LEAFLOG_LEGACY ",
			want: IndexOuterLeafModeV1LeafLogLegacy,
		},
		{
			name: "v1_leaflog_route",
			mode: " V1_LEAFLOG_ROUTE ",
			want: IndexOuterLeafModeV1LeafLogRoute,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(Options{
				Dir:                t.TempDir(),
				IndexOuterLeafMode: tc.mode,
			})
			if err != nil {
				t.Fatalf("open %q: %v", tc.mode, err)
			}
			defer func() { _ = db.Close() }()
			if got := db.indexOuterLeafMode; got != tc.want {
				t.Fatalf("index outer leaf mode = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestOpen_IndexOuterLeafMode_V1LeafLogLegacyPreserved(t *testing.T) {
	db, err := Open(Options{
		Dir:                t.TempDir(),
		IndexOuterLeafMode: IndexOuterLeafModeV1LeafLogLegacy,
	})
	if err != nil {
		t.Fatalf("open %q: %v", IndexOuterLeafModeV1LeafLogLegacy, err)
	}
	defer func() { _ = db.Close() }()
	if got := db.indexOuterLeafMode; got != IndexOuterLeafModeV1LeafLogLegacy {
		t.Fatalf("index outer leaf mode = %q, want %q", got, IndexOuterLeafModeV1LeafLogLegacy)
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

func TestOpen_ValueLogWALFenceMode_V1LeafLog_RejectsSimpleInline(t *testing.T) {
	_, err := Open(Options{
		Dir:                t.TempDir(),
		IndexOuterLeafMode: IndexOuterLeafModeV1LeafLog,
		ValueLog: ValueLogOptions{
			WALFenceMode: ValueLogWALFenceModeSimpleInline,
		},
	})
	if err == nil {
		t.Fatalf("expected validation error")
	}
	if !strings.Contains(err.Error(), "requires index outer leaf mode") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpen_ValueLogWALFenceMode_V1LeafLogLegacy_RejectsSimpleInline(t *testing.T) {
	_, err := Open(Options{
		Dir:                t.TempDir(),
		IndexOuterLeafMode: IndexOuterLeafModeV1LeafLogLegacy,
		ValueLog: ValueLogOptions{
			WALFenceMode: ValueLogWALFenceModeSimpleInline,
		},
	})
	if err == nil {
		t.Fatalf("expected validation error")
	}
	if !strings.Contains(err.Error(), "requires index outer leaf mode") {
		t.Fatalf("unexpected error: %v", err)
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
