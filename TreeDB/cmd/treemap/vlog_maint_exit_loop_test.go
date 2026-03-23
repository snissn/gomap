package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValueLogMaintModeOptionsStageConfirmExitSkipsCheckpoint(t *testing.T) {
	opts, err := valueLogMaintModeOptions("stage-confirm-exit")
	if err != nil {
		t.Fatalf("valueLogMaintModeOptions: %v", err)
	}
	if !opts.SkipCheckpoint {
		t.Fatalf("SkipCheckpoint=false want true")
	}
	if !opts.RewriteDebtDrain {
		t.Fatalf("RewriteDebtDrain=false want true")
	}
	if !opts.SuppressFollowOn {
		t.Fatalf("SuppressFollowOn=false want true")
	}
}

func TestShouldStopVlogMaintExitLoop(t *testing.T) {
	t.Run("continues while queue shrinks and reclaim stays positive", func(t *testing.T) {
		stop, reason := shouldStopVlogMaintExitLoop(valueLogMaintExitLoopPassReport{
			ReclaimedBytes: 307 << 20,
			QueueBefore:    5,
			QueueAfter:     4,
		}, 1, 3, 1, true)
		if stop {
			t.Fatalf("stop=%t reason=%q want continue", stop, reason)
		}
	})

	t.Run("stops when existing queue does not shrink", func(t *testing.T) {
		stop, reason := shouldStopVlogMaintExitLoop(valueLogMaintExitLoopPassReport{
			ReclaimedBytes: 513 << 20,
			QueueBefore:    0,
			QueueAfter:     2,
		}, 1, 3, 1, true)
		if stop {
			t.Fatalf("stop=%t reason=%q want continue", stop, reason)
		}
	})

	t.Run("stops when queued work does not shrink", func(t *testing.T) {
		stop, reason := shouldStopVlogMaintExitLoop(valueLogMaintExitLoopPassReport{
			ReclaimedBytes: 1,
			QueueBefore:    2,
			QueueAfter:     2,
		}, 2, 3, 1, true)
		if !stop || reason != "queue_nondecreasing" {
			t.Fatalf("stop=%t reason=%q want queue_nondecreasing", stop, reason)
		}
	})

	t.Run("stops on low reclaim", func(t *testing.T) {
		stop, reason := shouldStopVlogMaintExitLoop(valueLogMaintExitLoopPassReport{
			ReclaimedBytes: 0,
			QueueBefore:    2,
			QueueAfter:     1,
		}, 1, 3, 1, true)
		if !stop || reason != "low_reclaim" {
			t.Fatalf("stop=%t reason=%q want low_reclaim", stop, reason)
		}
	})

	t.Run("stops on max passes", func(t *testing.T) {
		stop, reason := shouldStopVlogMaintExitLoop(valueLogMaintExitLoopPassReport{
			ReclaimedBytes: 128,
			QueueBefore:    2,
			QueueAfter:     1,
		}, 3, 3, 1, true)
		if !stop || reason != "max_passes" {
			t.Fatalf("stop=%t reason=%q want max_passes", stop, reason)
		}
	})
}

func TestDirLogicalSizeBytes(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("abcd"), 0o600); err != nil {
		t.Fatalf("write a: %v", err)
	}
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "sub", "b"), []byte("hello"), 0o600); err != nil {
		t.Fatalf("write b: %v", err)
	}
	got, err := dirLogicalSizeBytes(root)
	if err != nil {
		t.Fatalf("dirLogicalSizeBytes: %v", err)
	}
	if want := int64(9); got != want {
		t.Fatalf("size=%d want=%d", got, want)
	}
}
