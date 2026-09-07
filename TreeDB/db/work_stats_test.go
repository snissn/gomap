package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

func TestWorkStatsReplayError(t *testing.T) {
	before := workstats.Read().Replay
	err := applyCommandWALFrame(nil, commitlog.CommandEnvelope{Kind: 65535}, nil, nil, nil, nil)
	if !errors.Is(err, commitlog.ErrCommandWALUnsupportedKind) {
		t.Fatal(err)
	}
	after := workstats.Read().Replay
	if after.FramesAttempted-before.FramesAttempted != 1 || after.FrameErrors-before.FrameErrors != 1 || after.FramesApplied != before.FramesApplied {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
}

func TestWorkStatsReplayPanic(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	kind := commitlog.CommandKind(60000 + testRegisteredReplayHandlerKind.Add(1))
	RegisterCommandWALReplayHandler(kind, func(*DB, commitlog.CommandEnvelope) error { panic("observed replay panic") })
	before := workstats.Read().Replay
	defer func() {
		if p := recover(); p != "observed replay panic" {
			t.Errorf("panic changed: %v", p)
		}
		after := workstats.Read().Replay
		if after.FramesAttempted-before.FramesAttempted != 1 || after.FrameErrors-before.FrameErrors != 1 || after.FramesApplied != before.FramesApplied {
			t.Errorf("panic recorded as apply: before=%+v after=%+v", before, after)
		}
		if db.commandWALReplayLSN.Load() != 0 || db.commandWALReplayToken.Load() != 0 {
			t.Error("panic left replay authority active")
		}
	}()
	_ = applyCommandWALFrame(db, commitlog.CommandEnvelope{LSN: 77, Kind: kind}, nil, nil, nil, nil)
	t.Fatal("panic not propagated")
}
