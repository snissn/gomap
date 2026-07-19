package treedb

import (
	"bytes"
	"fmt"
	"testing"
)

type profileLifecycleFrontiers struct {
	commitSeq      uint64
	durableRoot    uint64
	appliedCommand uint64
	nextWAL        uint64
	durableWAL     uint64
}

func profileLifecycleOptions(profile Profile, dir string) Options {
	opts := OptionsFor(profile, dir)
	opts.CommandWALStatsScan = true
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true
	opts.FlushThreshold = 1 << 30
	opts.MaxQueuedMemtables = -1
	opts.WriterFlushMaxMemtables = 0
	opts.WriterFlushMaxDuration = 0
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	return opts
}

func readProfileLifecycleFrontiers(t *testing.T, db *DB, commandWAL bool) profileLifecycleFrontiers {
	t.Helper()
	stats := db.Stats()
	frontiers := profileLifecycleFrontiers{
		commitSeq:   statMapUint64(t, stats, "treedb.commit_seq"),
		durableRoot: statMapUint64(t, stats, "treedb.durable_root.commit_seq"),
	}
	if commandWAL {
		frontiers.appliedCommand = statMapUint64(t, stats, "treedb.applied_command_lsn")
		frontiers.nextWAL = statMapUint64(t, stats, "treedb.command_wal.next_lsn")
		frontiers.durableWAL = statMapUint64(t, stats, "treedb.command_wal.durable_wal_lsn")
	}
	return frontiers
}

func requireProfileLifecycleRootUnchanged(t *testing.T, before, after profileLifecycleFrontiers) {
	t.Helper()
	if after.commitSeq != before.commitSeq || after.durableRoot != before.durableRoot {
		t.Fatalf("root frontier moved: before=%+v after=%+v", before, after)
	}
}

func requireProfileLifecycleDurableRoot(t *testing.T, before, after profileLifecycleFrontiers) {
	t.Helper()
	if after.commitSeq <= before.commitSeq {
		t.Fatalf("commit frontier did not advance: before=%+v after=%+v", before, after)
	}
	if after.durableRoot != after.commitSeq {
		t.Fatalf("durable root=%d, want visible commit=%d (before=%+v after=%+v)", after.durableRoot, after.commitSeq, before, after)
	}
}

func writeEmptyProfileLifecycleSync(t *testing.T, db *DB) {
	t.Helper()
	batch := db.NewBatch()
	if err := batch.WriteSync(); err != nil {
		_ = batch.Close()
		t.Fatalf("empty WriteSync: %v", err)
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("empty batch Close: %v", err)
	}
}

func TestProductionProfileLifecycleFrontiersMatchFrozenContract(t *testing.T) {
	tests := []struct {
		profile    Profile
		commandWAL bool
	}{
		{profile: ProfileCommandWALDurable, commandWAL: true},
		{profile: ProfileCommandWALRelaxed, commandWAL: true},
		{profile: ProfileNoWALFast},
	}

	for _, tc := range tests {
		t.Run(string(tc.profile), func(t *testing.T) {
			dir := t.TempDir()
			opts := profileLifecycleOptions(tc.profile, dir)
			db, err := Open(opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			closed := false
			defer func() {
				if !closed {
					_ = db.Close()
				}
			}()

			if got := db.ResolvedProfile(); got != tc.profile {
				t.Fatalf("resolved profile=%q, want %q", got, tc.profile)
			}
			before := readProfileLifecycleFrontiers(t, db, tc.commandWAL)

			if err := db.Set([]byte("ordinary"), []byte("one")); err != nil {
				t.Fatalf("ordinary Set: %v", err)
			}
			afterOrdinary := readProfileLifecycleFrontiers(t, db, tc.commandWAL)
			requireProfileLifecycleRootUnchanged(t, before, afterOrdinary)
			if tc.commandWAL {
				if afterOrdinary.appliedCommand != before.appliedCommand {
					t.Fatalf("ordinary Set applied command frontier: before=%+v after=%+v", before, afterOrdinary)
				}
				// A serial durable writer syncs its mutation frame directly. A
				// relaxed writer appends the same single mutation without a sync.
				wantNext := before.nextWAL + 1
				if afterOrdinary.nextWAL != wantNext {
					t.Fatalf("ordinary Set next WAL=%d, want single-mutation frontier %d", afterOrdinary.nextWAL, wantNext)
				}
				wantDurable := before.durableWAL
				if tc.profile == ProfileCommandWALDurable {
					wantDurable = afterOrdinary.nextWAL - 1
				}
				if afterOrdinary.durableWAL != wantDurable {
					t.Fatalf("ordinary Set durable WAL=%d, want %d (frontiers=%+v)", afterOrdinary.durableWAL, wantDurable, afterOrdinary)
				}
			}

			writeEmptyProfileLifecycleSync(t, db)
			afterEmptySync := readProfileLifecycleFrontiers(t, db, tc.commandWAL)
			if tc.commandWAL {
				requireProfileLifecycleRootUnchanged(t, afterOrdinary, afterEmptySync)
				if afterEmptySync.appliedCommand != afterOrdinary.appliedCommand {
					t.Fatalf("empty WriteSync applied command frontier: before=%+v after=%+v", afterOrdinary, afterEmptySync)
				}
				if tc.profile == ProfileCommandWALDurable {
					if afterEmptySync.nextWAL != afterOrdinary.nextWAL || afterEmptySync.durableWAL != afterOrdinary.durableWAL {
						t.Fatalf("empty durable WriteSync emitted a redundant barrier: before=%+v after=%+v", afterOrdinary, afterEmptySync)
					}
				} else {
					if afterEmptySync.nextWAL != afterOrdinary.nextWAL+1 || afterEmptySync.durableWAL != afterEmptySync.nextWAL-1 {
						t.Fatalf("empty relaxed WriteSync did not close the WAL prefix: before=%+v after=%+v", afterOrdinary, afterEmptySync)
					}
				}
			} else {
				requireProfileLifecycleDurableRoot(t, afterOrdinary, afterEmptySync)
			}

			if err := db.SetSync([]byte("explicit"), []byte("two")); err != nil {
				t.Fatalf("SetSync: %v", err)
			}
			afterSetSync := readProfileLifecycleFrontiers(t, db, tc.commandWAL)
			if tc.commandWAL {
				requireProfileLifecycleRootUnchanged(t, afterEmptySync, afterSetSync)
				if afterSetSync.nextWAL != afterEmptySync.nextWAL+1 || afterSetSync.durableWAL != afterSetSync.nextWAL-1 {
					t.Fatalf("SetSync did not append and directly sync one mutation: before=%+v after=%+v", afterEmptySync, afterSetSync)
				}
			} else {
				requireProfileLifecycleDurableRoot(t, afterEmptySync, afterSetSync)
			}

			if err := db.Set([]byte("checkpoint"), []byte("three")); err != nil {
				t.Fatalf("pre-checkpoint Set: %v", err)
			}
			beforeCheckpoint := readProfileLifecycleFrontiers(t, db, tc.commandWAL)
			requireProfileLifecycleRootUnchanged(t, afterSetSync, beforeCheckpoint)
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			afterCheckpoint := readProfileLifecycleFrontiers(t, db, tc.commandWAL)
			requireProfileLifecycleDurableRoot(t, beforeCheckpoint, afterCheckpoint)
			if tc.commandWAL {
				wantApplied := beforeCheckpoint.nextWAL - 1
				wantNext := beforeCheckpoint.nextWAL
				if afterCheckpoint.nextWAL != wantNext || afterCheckpoint.appliedCommand != wantApplied || afterCheckpoint.durableWAL != wantApplied {
					t.Fatalf("checkpoint did not publish the dependency-closed WAL frontier: before=%+v after=%+v", beforeCheckpoint, afterCheckpoint)
				}
			}

			if err := db.Set([]byte("close"), []byte("four")); err != nil {
				t.Fatalf("pre-close Set: %v", err)
			}
			beforeClose := readProfileLifecycleFrontiers(t, db, tc.commandWAL)
			requireProfileLifecycleRootUnchanged(t, afterCheckpoint, beforeClose)
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			closed = true

			reopen, err := Open(opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer func() { _ = reopen.Close() }()
			afterReopen := readProfileLifecycleFrontiers(t, reopen, tc.commandWAL)
			requireProfileLifecycleDurableRoot(t, beforeClose, afterReopen)
			if tc.commandWAL {
				wantApplied := beforeClose.nextWAL - 1
				if afterReopen.appliedCommand != wantApplied || afterReopen.durableWAL != wantApplied {
					t.Fatalf("clean Close did not publish the final dependency-closed WAL frontier %d: before=%+v after=%+v", wantApplied, beforeClose, afterReopen)
				}
			}
			for key, want := range map[string]string{
				"ordinary":   "one",
				"explicit":   "two",
				"checkpoint": "three",
				"close":      "four",
			} {
				got, err := reopen.Get([]byte(key))
				if err != nil {
					t.Fatalf("reopen Get(%q): %v", key, err)
				}
				if string(got) != want {
					t.Fatalf("reopen Get(%q)=%q, want %q", key, got, want)
				}
			}
		})
	}
}

func TestProductionProfilesForcedPointersDeleteReuseRotationReopen(t *testing.T) {
	for _, profile := range []Profile{
		ProfileCommandWALDurable,
		ProfileCommandWALRelaxed,
		ProfileNoWALFast,
	} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			opts := profileLifecycleOptions(profile, dir)
			opts.ValueLog.ForcePointers = true
			opts.ValueLog.PointerThreshold = 1
			opts.CommandWALSegmentTargetBytes = 1
			opts.WALMaxSegmentBytes = 1 << 20

			database, err := Open(opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			closed := false
			defer func() {
				if !closed {
					_ = database.Close()
				}
			}()

			const keyCount = 64
			for i := 0; i < keyCount; i++ {
				key := []byte(fmt.Sprintf("asset-%03d", i))
				value := bytes.Repeat([]byte{byte(i + 1)}, 4096)
				if err := database.Set(key, value); err != nil {
					t.Fatalf("initial Set(%d): %v", i, err)
				}
			}
			writeEmptyProfileLifecycleSync(t, database)

			if profile != ProfileNoWALFast {
				stats := database.Stats()
				if got := statMapUint64(t, stats, "treedb.command_wal.segments.total"); got < 2 {
					t.Fatalf("command-WAL rotation segments=%d, want at least 2", got)
				}
			}

			for i := 0; i < keyCount; i += 2 {
				if err := database.Delete([]byte(fmt.Sprintf("asset-%03d", i))); err != nil {
					t.Fatalf("Delete(%d): %v", i, err)
				}
			}
			writeEmptyProfileLifecycleSync(t, database)

			for i := 0; i < keyCount; i += 2 {
				key := []byte(fmt.Sprintf("asset-%03d", i))
				value := bytes.Repeat([]byte{byte(255 - i)}, 6144)
				if err := database.Set(key, value); err != nil {
					t.Fatalf("reuse Set(%d): %v", i, err)
				}
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			if err := database.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			closed = true

			reopened, err := Open(opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer func() { _ = reopened.Close() }()
			for i := 0; i < keyCount; i++ {
				key := []byte(fmt.Sprintf("asset-%03d", i))
				wantByte := byte(i + 1)
				wantLen := 4096
				if i%2 == 0 {
					wantByte = byte(255 - i)
					wantLen = 6144
				}
				got, err := reopened.Get(key)
				if err != nil {
					t.Fatalf("reopen Get(%d): %v", i, err)
				}
				if len(got) != wantLen || !bytes.Equal(got, bytes.Repeat([]byte{wantByte}, wantLen)) {
					t.Fatalf("reopen Get(%d) mismatch: len=%d want=%d", i, len(got), wantLen)
				}
			}
		})
	}
}
