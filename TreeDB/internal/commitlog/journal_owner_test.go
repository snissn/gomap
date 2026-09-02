package commitlog

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

var errCommandJournalInjectedWrite = errors.New("injected command journal write failure")

type commandJournalFailWriter struct{}

func (commandJournalFailWriter) Write([]byte) (int, error) {
	return 0, errCommandJournalInjectedWrite
}

func (j *CommandJournal) installBufferedWriterForTest(w io.Writer, size int) func() {
	j.mu.Lock()
	defer j.mu.Unlock()
	old := j.writer.bw
	j.writer.bw = bufio.NewWriterSize(w, size)
	return func() {
		j.mu.Lock()
		defer j.mu.Unlock()
		j.writer.bw = old
	}
}

func TestCommandJournalAllocatesContiguousLSNs(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{InitialLSN: 40})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	for i, want := range []uint64{41, 42, 43} {
		got, err := j.AppendCommand(CommandEnvelope{
			Kind:          CommandKindRawKVBatch,
			Scope:         CommandScopeRawKV,
			PayloadFormat: PayloadFormatRawKVBatchV1,
		})
		if err != nil {
			t.Fatalf("AppendCommand(%d): %v", i, err)
		}
		if got != want {
			t.Fatalf("AppendCommand(%d) LSN=%d, want %d", i, got, want)
		}
	}
	if err := j.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	frames, err := ScanCommandFrames(j.Path(), Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrames: %v", err)
	}
	if len(frames) != 3 {
		t.Fatalf("len(frames)=%d, want 3", len(frames))
	}
	for i, frame := range frames {
		if want := uint64(41 + i); frame.LSN != want {
			t.Fatalf("frame[%d].LSN=%d, want %d", i, frame.LSN, want)
		}
	}
}

func TestCommandJournalObservedBoundariesHoldJournalLock(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer func() { _ = j.Close() }()

	var (
		events   []durabilitycut.Event
		unlocked []durabilitycut.Point
	)
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource != durabilitycut.ResourceCommandWAL || event.Point == "" {
			return nil
		}
		events = append(events, event)
		if j.mu.TryLock() {
			unlocked = append(unlocked, event.Point)
			j.mu.Unlock()
		}
		return nil
	})
	defer restore()

	lsn, err := j.AppendCommandObserved(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("AppendCommandObserved: %v", err)
	}
	if lsn != 1 {
		t.Fatalf("AppendCommandObserved LSN=%d, want 1", lsn)
	}
	if err := j.FlushObserved(true); err != nil {
		t.Fatalf("FlushObserved: %v", err)
	}
	if len(unlocked) != 0 {
		t.Fatalf("durability boundaries emitted without journal lock: %v", unlocked)
	}
	wantPoints := []durabilitycut.Point{
		durabilitycut.BeforeDependencyAppend,
		durabilitycut.AfterDependencyAppend,
		durabilitycut.BeforeDependencyFileSync,
		durabilitycut.AfterDependencyFileSync,
	}
	if len(events) != len(wantPoints) {
		t.Fatalf("events=%#v, want points %v", events, wantPoints)
	}
	activePath, _ := j.ActiveSegmentSnapshot()
	for i, want := range wantPoints {
		if events[i].Point != want {
			t.Fatalf("event[%d].Point=%q, want %q", i, events[i].Point, want)
		}
		if want != durabilitycut.BeforeDependencyAppend && events[i].Path != activePath {
			t.Fatalf("event[%d].Path=%q, want active path %q", i, events[i].Path, activePath)
		}
	}
}

func TestCommandJournalAppendHooksHoldLockAndTransferCurrentRotation(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{
		SegmentTargetBytes:     1,
		CaptureStableResources: true,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer func() { _ = j.Close() }()

	envelope := CommandEnvelope{
		Version:         CommandFrameVersionV2,
		DurabilityClass: CommandDurabilityRelaxed,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
	}
	assertLocked := func(stage string) {
		t.Helper()
		if j.mu.TryLock() {
			j.mu.Unlock()
			t.Fatalf("journal mutex was not held during %s hook", stage)
		}
	}
	appendWithHooks := func(wantLSN uint64, wantRotations int) {
		t.Helper()
		lsn, err := j.AppendCommandObservedWithHooks(envelope,
			func() error {
				assertLocked("before-append")
				return nil
			},
			func(gotLSN uint64, rotations []*CommandJournalStableRotation) error {
				assertLocked("after-append")
				if gotLSN != wantLSN {
					t.Fatalf("post-hook LSN=%d, want %d", gotLSN, wantLSN)
				}
				if len(rotations) != wantRotations {
					t.Fatalf("post-hook rotations=%d, want %d", len(rotations), wantRotations)
				}
				for _, rotation := range rotations {
					if rotation == nil || rotation.Closed == nil || rotation.Active == nil {
						t.Fatalf("post-hook rotation=%+v, want exact closed and active tokens", rotation)
					}
					rotation.Release()
				}
				return nil
			},
		)
		if err != nil {
			t.Fatalf("AppendCommandObservedWithHooks: %v", err)
		}
		if lsn != wantLSN {
			t.Fatalf("append LSN=%d, want %d", lsn, wantLSN)
		}
	}

	appendWithHooks(1, 0)
	appendWithHooks(2, 1)
	if rotations, err := j.TakePendingStableRotations(); err != nil {
		t.Fatalf("TakePendingStableRotations: %v", err)
	} else if len(rotations) != 0 {
		for _, rotation := range rotations {
			rotation.Release()
		}
		t.Fatalf("post hook left %d rotations for a later append to steal", len(rotations))
	}
}

func TestCommandJournalRejectsOutOfRangeLane(t *testing.T) {
	for _, lane := range []int{-1, MaxCommandJournalLane + 1} {
		if _, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{Lane: lane}); err == nil {
			t.Fatalf("OpenCommandJournal lane=%d unexpectedly succeeded", lane)
		}
	}
}

func TestCommandJournalSeedsLSNFromExistingFrames(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal first: %v", err)
	}
	lsn, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("AppendCommand first: %v", err)
	}
	if lsn != 1 {
		t.Fatalf("first LSN=%d, want 1", lsn)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	reopen, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal reopen: %v", err)
	}
	defer reopen.Close()
	lsn, err = reopen.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("AppendCommand after reopen: %v", err)
	}
	if lsn != 2 {
		t.Fatalf("reopened LSN=%d, want 2", lsn)
	}
}

func TestCommandJournalSeedsLSNFromExistingSegmentFamily(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal first: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := j.AppendCommand(CommandEnvelope{
			Kind:          CommandKindRawKVBatch,
			Scope:         CommandScopeRawKV,
			PayloadFormat: PayloadFormatRawKVBatchV1,
		}); err != nil {
			t.Fatalf("AppendCommand first segment %d: %v", i, err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	reopen, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 2})
	if err != nil {
		t.Fatalf("OpenCommandJournal second segment: %v", err)
	}
	defer reopen.Close()
	lsn, err := reopen.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("AppendCommand second segment: %v", err)
	}
	if lsn != 3 {
		t.Fatalf("second segment LSN=%d, want 3", lsn)
	}
}

func TestCommandJournalDefaultSegmentSeqUsesLatestSegment(t *testing.T) {
	dir := t.TempDir()
	first, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal first: %v", err)
	}
	if _, err := first.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand first segment: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	second, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 2})
	if err != nil {
		t.Fatalf("OpenCommandJournal second: %v", err)
	}
	if _, err := second.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand second segment: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("Close second: %v", err)
	}

	reopen, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal default latest: %v", err)
	}
	lsn, err := reopen.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("AppendCommand default latest: %v", err)
	}
	if lsn != 3 {
		t.Fatalf("default latest LSN=%d, want 3", lsn)
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("Close reopen: %v", err)
	}
	seg1, err := ScanCommandFrames(filepath.Join(dir, CommandSegmentName(0, 1)), Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrames segment 1: %v", err)
	}
	if len(seg1) != 1 || seg1[0].LSN != 1 {
		t.Fatalf("segment 1 frames=%+v, want only LSN 1", seg1)
	}
	seg2, err := ScanCommandFrames(filepath.Join(dir, CommandSegmentName(0, 2)), Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrames segment 2: %v", err)
	}
	if len(seg2) != 2 || seg2[0].LSN != 2 || seg2[1].LSN != 3 {
		t.Fatalf("segment 2 frames=%+v, want LSNs 2,3", seg2)
	}
}

func TestCommandJournalSegmentTargetRotatesBeforeLSNReservation(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}

	lsn1, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		_ = j.Close()
		t.Fatalf("AppendCommand first: %v", err)
	}
	lsn2, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		_ = j.Close()
		t.Fatalf("AppendCommand second: %v", err)
	}
	if lsn1 != 1 || lsn2 != 2 {
		_ = j.Close()
		t.Fatalf("LSNs=(%d,%d), want (1,2)", lsn1, lsn2)
	}
	if got := filepath.Base(j.Path()); got != CommandSegmentName(0, 2) {
		_ = j.Close()
		t.Fatalf("active segment=%s, want %s", got, CommandSegmentName(0, 2))
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	seg1, err := ScanCommandFrames(filepath.Join(dir, CommandSegmentName(0, 1)), Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrames segment 1: %v", err)
	}
	if len(seg1) != 1 || seg1[0].LSN != 1 {
		t.Fatalf("segment 1 frames=%+v, want only LSN 1", seg1)
	}
	seg2, err := ScanCommandFrames(filepath.Join(dir, CommandSegmentName(0, 2)), Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrames segment 2: %v", err)
	}
	if len(seg2) != 1 || seg2[0].LSN != 2 {
		t.Fatalf("segment 2 frames=%+v, want only LSN 2", seg2)
	}
}

func TestCommandJournalPointAppendAndFlushSyncsRotatedSegment(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	if _, err := j.AppendRawKVPointCommandTrustedAndFlush(0, RawKVOpSet, []byte("k1"), []byte("v1"), false); err != nil {
		t.Fatalf("AppendRawKVPointCommandTrustedAndFlush first: %v", err)
	}

	syncedSegments := []string{}
	j.mu.Lock()
	j.writer.syncFn = func(f *os.File) error {
		syncedSegments = append(syncedSegments, filepath.Base(f.Name()))
		return nil
	}
	j.mu.Unlock()

	if _, err := j.AppendRawKVPointCommandTrustedAndFlush(0, RawKVOpSet, []byte("k2"), []byte("v2"), true); err != nil {
		t.Fatalf("AppendRawKVPointCommandTrustedAndFlush second: %v", err)
	}
	wantSyncedSegments := []string{CommandSegmentName(0, 1), CommandSegmentName(0, 2)}
	if runtime.GOOS == "windows" {
		wantSyncedSegments = []string{CommandSegmentName(0, 1), CommandSegmentName(0, 2), CommandSegmentName(0, 2)}
	}
	if !slices.Equal(syncedSegments, wantSyncedSegments) {
		t.Fatalf("synced segments=%v, want %v", syncedSegments, wantSyncedSegments)
	}
}

func TestCommandJournalDirectAppendAndFlushCutPoints(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSet, Key: []byte("batch"), Value: []byte("value")}})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	tests := []struct {
		name   string
		sync   bool
		append func(*CommandJournal) (uint64, error)
	}{
		{
			name: "single-relaxed",
			append: func(j *CommandJournal) (uint64, error) {
				return j.AppendRawKVSingleCommandAndFlush(0, RawKVOperation{Op: RawKVOpSet, Key: []byte("single"), Value: []byte("value")}, false)
			},
		},
		{
			name: "point-relaxed",
			append: func(j *CommandJournal) (uint64, error) {
				return j.AppendRawKVPointCommandTrustedAndFlush(0, RawKVOpSet, []byte("point"), []byte("value"), false)
			},
		},
		{
			name: "point-sync",
			sync: true,
			append: func(j *CommandJournal) (uint64, error) {
				return j.AppendRawKVPointCommandTrustedAndFlush(0, RawKVOpSet, []byte("point"), []byte("value"), true)
			},
		},
		{
			name: "payload-relaxed",
			append: func(j *CommandJournal) (uint64, error) {
				return j.AppendRawKVBatchPayloadCommandTrustedAndFlush(0, payload, false)
			},
		},
		{
			name: "payload-sync",
			sync: true,
			append: func(j *CommandJournal) (uint64, error) {
				return j.AppendRawKVBatchPayloadCommandTrustedAndFlush(0, payload, true)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			j, err := OpenCommandJournal(dir, CommandJournalOptions{})
			if err != nil {
				t.Fatalf("OpenCommandJournal: %v", err)
			}
			defer j.Close()

			var events []durabilitycut.Event
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource == durabilitycut.ResourceCommandWAL && event.Point != "" {
					events = append(events, event)
				}
				return nil
			})
			lsn, err := tt.append(j)
			restore()
			if err != nil {
				t.Fatalf("append: %v", err)
			}
			if lsn != 1 {
				t.Fatalf("lsn=%d, want 1", lsn)
			}

			beforeFlush, afterFlush := durabilitycut.BeforeUserspaceFlush, durabilitycut.AfterUserspaceFlush
			if tt.sync {
				beforeFlush, afterFlush = durabilitycut.BeforeDependencyFileSync, durabilitycut.AfterDependencyFileSync
			}
			want := []durabilitycut.Point{
				durabilitycut.BeforeDependencyAppend,
				durabilitycut.AfterDependencyAppend,
				beforeFlush,
				afterFlush,
			}
			if len(events) != len(want) {
				t.Fatalf("events=%+v, want points %v", events, want)
			}
			for i, point := range want {
				if events[i].Point != point {
					t.Fatalf("event[%d].Point=%q, want %q (events=%+v)", i, events[i].Point, point, events)
				}
			}
			if events[1].LSN != lsn {
				t.Fatalf("after-append LSN=%d, want %d", events[1].LSN, lsn)
			}
			if events[1].Path == "" {
				t.Fatal("after-append event omitted exact segment path")
			}
			if events[2].Path != j.Path() || events[3].Path != j.Path() {
				t.Fatalf("flush paths=(%q,%q), want active path %q", events[2].Path, events[3].Path, j.Path())
			}
		})
	}
}

func TestCommandJournalDirectSyncRotationCutOrderAndPaths(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()
	if _, err := j.AppendRawKVPointCommandTrustedAndFlush(0, RawKVOpSet, []byte("first"), []byte("value"), false); err != nil {
		t.Fatalf("first append: %v", err)
	}
	oldPath := j.Path()

	var events []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point != "" {
			events = append(events, event)
		}
		return nil
	})
	lsn, err := j.AppendRawKVPointCommandTrustedAndFlush(0, RawKVOpSet, []byte("second"), []byte("value"), true)
	restore()
	if err != nil {
		t.Fatalf("second append: %v", err)
	}
	if lsn != 2 {
		t.Fatalf("second lsn=%d, want 2", lsn)
	}
	newPath := j.Path()
	want := []durabilitycut.Point{
		durabilitycut.BeforeDependencyAppend,
		durabilitycut.BeforeDependencyFileSync,
		durabilitycut.AfterDependencyFileSync,
		durabilitycut.BeforeNewFileDirectorySync,
		durabilitycut.AfterNewFileDirectorySync,
		durabilitycut.AfterDependencyAppend,
		durabilitycut.BeforeDependencyFileSync,
		durabilitycut.AfterDependencyFileSync,
	}
	if len(events) != len(want) {
		t.Fatalf("events=%+v, want points %v", events, want)
	}
	for i, point := range want {
		if events[i].Point != point {
			t.Fatalf("event[%d].Point=%q, want %q (events=%+v)", i, events[i].Point, point, events)
		}
	}
	if events[1].Path != oldPath || events[2].Path != oldPath {
		t.Fatalf("old-segment sync paths=(%q,%q), want %q", events[1].Path, events[2].Path, oldPath)
	}
	if events[5].LSN != lsn {
		t.Fatalf("after-append LSN=%d, want %d", events[5].LSN, lsn)
	}
	if events[6].Path != newPath || events[7].Path != newPath {
		t.Fatalf("new-segment sync paths=(%q,%q), want %q", events[6].Path, events[7].Path, newPath)
	}
}

func TestCommandJournalPointAppendAndFlushMeasuredReportsPhases(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()
	syncedSegments := []string{}
	j.mu.Lock()
	j.writer.syncFn = func(f *os.File) error {
		syncedSegments = append(syncedSegments, filepath.Base(f.Name()))
		return nil
	}
	j.mu.Unlock()

	lsn, timing, err := j.AppendRawKVPointCommandTrustedAndFlushMeasured(0, RawKVOpSet, []byte("k"), []byte("v"), true)
	if err != nil {
		t.Fatalf("AppendRawKVPointCommandTrustedAndFlushMeasured: %v", err)
	}
	if lsn != 1 {
		t.Fatalf("lsn=%d, want 1", lsn)
	}
	if timing.Append < 0 {
		t.Fatalf("append timing=%s, want non-negative", timing.Append)
	}
	if timing.Flush < 0 {
		t.Fatalf("flush timing=%s, want non-negative", timing.Flush)
	}
	if len(syncedSegments) != 1 || syncedSegments[0] != CommandSegmentName(0, 1) {
		t.Fatalf("synced segments=%v, want [%s]", syncedSegments, CommandSegmentName(0, 1))
	}
}

func TestCommandJournalPointAppendWithRevisionWritesCanonicalPayload(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	lsn, err := j.AppendRawKVPointCommandTrustedWithRevisionAndFlush(44, RawKVOpSet, []byte("k"), []byte("v"), 77, false)
	if err != nil {
		t.Fatalf("AppendRawKVPointCommandTrustedWithRevisionAndFlush: %v", err)
	}
	if lsn != 1 {
		t.Fatalf("lsn=%d, want 1", lsn)
	}
	if err := j.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	frames, err := ScanCommandFrames(j.Path(), Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrames: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("frames=%d, want 1", len(frames))
	}
	env := frames[0]
	if env.LSN != 1 || env.BaseAppliedLSN != 44 {
		t.Fatalf("frame identity lsn=%d base=%d, want 1/44", env.LSN, env.BaseAppliedLSN)
	}
	seen := 0
	err = ScanRawKVBatchPayloadWithRevision(env.Payload, func(op RawKVOp, key, value []byte, revision uint64) error {
		seen++
		if op != RawKVOpSet || !bytes.Equal(key, []byte("k")) || !bytes.Equal(value, []byte("v")) || revision != 77 {
			t.Fatalf("op=(%d,%q,%q,%d), want set k/v rev 77", op, key, value, revision)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ScanRawKVBatchPayloadWithRevision: %v", err)
	}
	if seen != 1 {
		t.Fatalf("seen=%d, want 1", seen)
	}
}

func TestCommandJournalSingleAppendAndFlushSyncsRotatedSegment(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	if _, err := j.AppendRawKVSingleCommandAndFlush(0, RawKVOperation{Op: RawKVOpSet, Key: []byte("k1"), Value: []byte("v1")}, false); err != nil {
		t.Fatalf("AppendRawKVSingleCommandAndFlush first: %v", err)
	}

	syncedSegments := []string{}
	j.mu.Lock()
	j.writer.syncFn = func(f *os.File) error {
		syncedSegments = append(syncedSegments, filepath.Base(f.Name()))
		return nil
	}
	j.mu.Unlock()

	if _, err := j.AppendRawKVSingleCommandAndFlush(0, RawKVOperation{Op: RawKVOpSet, Key: []byte("k2"), Value: []byte("v2")}, true); err != nil {
		t.Fatalf("AppendRawKVSingleCommandAndFlush second: %v", err)
	}
	wantSyncedSegments := []string{CommandSegmentName(0, 1), CommandSegmentName(0, 2)}
	if runtime.GOOS == "windows" {
		wantSyncedSegments = []string{CommandSegmentName(0, 1), CommandSegmentName(0, 2), CommandSegmentName(0, 2)}
	}
	if !slices.Equal(syncedSegments, wantSyncedSegments) {
		t.Fatalf("synced segments=%v, want %v", syncedSegments, wantSyncedSegments)
	}
}

func TestCommandJournalAppendsToPreexistingZeroByteSegment(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, CommandSegmentName(0, 1))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("WriteFile zero segment: %v", err)
	}

	j, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	lsn, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		_ = j.Close()
		t.Fatalf("AppendCommand: %v", err)
	}
	if lsn != 1 {
		_ = j.Close()
		t.Fatalf("LSN=%d, want 1", lsn)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	frames, err := ScanCommandFrames(path, Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrames: %v", err)
	}
	if len(frames) != 1 || frames[0].LSN != 1 {
		t.Fatalf("frames=%+v, want one LSN 1 frame", frames)
	}
}

func TestCommandJournalRejectsExplicitSegmentBehindLaneTail(t *testing.T) {
	dir := t.TempDir()
	first, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal first: %v", err)
	}
	if _, err := first.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand first: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	second, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 2})
	if err != nil {
		t.Fatalf("OpenCommandJournal second: %v", err)
	}
	if _, err := second.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand second: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("Close second: %v", err)
	}

	_, err = OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 1})
	if !errors.Is(err, ErrCommandWALStaleSegment) {
		t.Fatalf("OpenCommandJournal stale segment error=%v, want ErrCommandWALStaleSegment", err)
	}
}

func TestCommandJournalSeedsLSNFromExistingLanes(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 0, SegmentSeq: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal first lane: %v", err)
	}
	if _, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand first lane: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close first lane: %v", err)
	}

	reopen, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 1, SegmentSeq: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal second lane: %v", err)
	}
	defer reopen.Close()
	lsn, err := reopen.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("AppendCommand second lane: %v", err)
	}
	if lsn != 2 {
		t.Fatalf("second lane LSN=%d, want 2", lsn)
	}
}

func TestCommandJournalRejectsDuplicateLSNAcrossLanes(t *testing.T) {
	dir := t.TempDir()
	for _, lane := range []int{0, 1} {
		w, err := NewWriter(filepath.Join(dir, CommandSegmentName(lane, 1)))
		if err != nil {
			t.Fatalf("NewWriter lane %d: %v", lane, err)
		}
		if err := w.AppendCommand(CommandEnvelope{
			LSN:           1,
			Kind:          CommandKindRawKVBatch,
			Scope:         CommandScopeRawKV,
			PayloadFormat: PayloadFormatRawKVBatchV1,
		}); err != nil {
			_ = w.Close()
			t.Fatalf("AppendCommand lane %d: %v", lane, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close lane %d: %v", lane, err)
		}
	}
	_, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if !errors.Is(err, ErrCommandWALDuplicateLSN) {
		t.Fatalf("OpenCommandJournal duplicate LSN error=%v, want ErrCommandWALDuplicateLSN", err)
	}
}

func TestCommandJournalInitialLSNIgnoresLegacyRawSegments(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, CommandSegmentName(0, 1))
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter legacy raw: %v", err)
	}
	if err := w.AppendBatch([]Record{{Op: OpSetInline, Key: []byte("legacy"), Value: []byte("raw"), Seq: 99}}); err != nil {
		_ = w.Close()
		t.Fatalf("AppendBatch legacy raw: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close legacy raw: %v", err)
	}

	j, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 2})
	if err != nil {
		t.Fatalf("OpenCommandJournal with legacy raw segment: %v", err)
	}
	defer j.Close()
	lsn, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("AppendCommand after legacy raw segment: %v", err)
	}
	if lsn != 1 {
		t.Fatalf("LSN=%d, want 1 when only legacy raw segments exist", lsn)
	}
}

func TestCommandJournalRejectsActiveLegacyRawSegment(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, CommandSegmentName(0, 1))
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter legacy raw: %v", err)
	}
	if err := w.AppendBatch([]Record{{Op: OpSetInline, Key: []byte("legacy"), Value: []byte("raw"), Seq: 99}}); err != nil {
		_ = w.Close()
		t.Fatalf("AppendBatch legacy raw: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close legacy raw: %v", err)
	}

	_, err = OpenCommandJournal(dir, CommandJournalOptions{})
	if !errors.Is(err, ErrCommandWALLegacyPayload) {
		t.Fatalf("OpenCommandJournal active legacy error=%v, want ErrCommandWALLegacyPayload", err)
	}
}

func TestCommandJournalRejectsNonActiveTerminalTail(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal first: %v", err)
	}
	if _, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand first: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}
	path := filepath.Join(dir, CommandSegmentName(0, 1))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile append tail: %v", err)
	}
	if _, err := f.Write([]byte{0x01, 0x02, 0x03}); err != nil {
		_ = f.Close()
		t.Fatalf("Write torn tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close torn tail: %v", err)
	}

	_, err = OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 2})
	if err == nil {
		t.Fatalf("OpenCommandJournal with non-active tail unexpectedly succeeded")
	}
}

func TestCommandJournalTruncatesActiveTerminalTailPerLane(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 0, SegmentSeq: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal lane 0: %v", err)
	}
	if _, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand lane 0: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close lane 0: %v", err)
	}
	lane0Path := filepath.Join(dir, CommandSegmentName(0, 1))
	f, err := os.OpenFile(lane0Path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile append lane 0 tail: %v", err)
	}
	if _, err := f.Write([]byte{0x01, 0x02, 0x03}); err != nil {
		_ = f.Close()
		t.Fatalf("Write lane 0 torn tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close lane 0 torn tail: %v", err)
	}

	reopen, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 1, SegmentSeq: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal lane 1 with lane 0 active tail: %v", err)
	}
	defer reopen.Close()
	lsn, err := reopen.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("AppendCommand lane 1: %v", err)
	}
	if lsn != 2 {
		t.Fatalf("lane 1 LSN=%d, want 2 after lane 0 scan", lsn)
	}
	frames, err := ScanCommandFrames(lane0Path, Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrames lane 0: %v", err)
	}
	if len(frames) != 1 || frames[0].LSN != 1 {
		t.Fatalf("lane 0 frames=%+v, want only LSN 1 after active-tail truncation", frames)
	}
}

func TestCommandJournalTruncatesTerminalTailBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal first: %v", err)
	}
	if _, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand first: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}
	path := filepath.Join(dir, CommandSegmentName(0, 1))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile append tail: %v", err)
	}
	if _, err := f.Write([]byte{0x01, 0x02, 0x03}); err != nil {
		_ = f.Close()
		t.Fatalf("Write torn tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close torn tail: %v", err)
	}

	reopen, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal reopen: %v", err)
	}
	defer reopen.Close()
	lsn, err := reopen.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("AppendCommand after terminal tail: %v", err)
	}
	if lsn != 2 {
		t.Fatalf("LSN after terminal tail=%d, want 2", lsn)
	}
	if err := reopen.Flush(); err != nil {
		t.Fatalf("Flush reopen: %v", err)
	}
	frames, err := ScanCommandFrames(path, Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrames: %v", err)
	}
	if len(frames) != 2 {
		t.Fatalf("len(frames)=%d, want 2", len(frames))
	}
}

func TestCommandJournalConcurrentAppendsSerializeFrameOrder(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	const count = 64
	start := make(chan struct{})
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		go func() {
			<-start
			_, err := j.AppendCommand(CommandEnvelope{
				Kind:          CommandKindRawKVBatch,
				Scope:         CommandScopeRawKV,
				PayloadFormat: PayloadFormatRawKVBatchV1,
			})
			errs <- err
		}()
	}
	close(start)
	for i := 0; i < count; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("AppendCommand concurrent error: %v", err)
		}
	}
	if err := j.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	frames, err := ScanCommandFrames(j.Path(), Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrames: %v", err)
	}
	if len(frames) != count {
		t.Fatalf("len(frames)=%d, want %d", len(frames), count)
	}
	for i, frame := range frames {
		if want := uint64(i + 1); frame.LSN != want {
			t.Fatalf("frame[%d].LSN=%d, want %d", i, frame.LSN, want)
		}
	}
}

func TestCommandJournalRejectsIndependentMutableOwner(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal first: %v", err)
	}
	defer j.Close()

	_, err = OpenCommandJournal(dir, CommandJournalOptions{})
	if !errors.Is(err, ErrJournalOwnerExists) {
		t.Fatalf("OpenCommandJournal second error=%v, want ErrJournalOwnerExists", err)
	}
}

func TestJournalOwnerRollbackMaxLSNClearsExhausted(t *testing.T) {
	owner, err := AcquireJournalOwnerWithOptions(t.TempDir(), JournalOwnerOptions{InitialLSN: ^uint64(0) - 1})
	if err != nil {
		t.Fatalf("AcquireJournalOwnerWithOptions: %v", err)
	}
	defer owner.Close()

	lsn, err := owner.reserveLSN()
	if err != nil {
		t.Fatalf("ReserveLSN max: %v", err)
	}
	if lsn != ^uint64(0) {
		t.Fatalf("LSN=%d, want max uint64", lsn)
	}
	if _, err := owner.reserveLSN(); err == nil {
		t.Fatalf("ReserveLSN after exhaustion unexpectedly succeeded")
	}
	if err := owner.rollbackReservedLSN(lsn); err != nil {
		t.Fatalf("rollbackReservedLSN max: %v", err)
	}
	if _, _, err := owner.reserveLSNRange(2); err == nil {
		t.Fatalf("reserveLSNRange(2) after max rollback unexpectedly succeeded")
	}
	lsn, err = owner.reserveLSN()
	if err != nil {
		t.Fatalf("ReserveLSN after rollback: %v", err)
	}
	if lsn != ^uint64(0) {
		t.Fatalf("LSN after rollback=%d, want max uint64", lsn)
	}
}

func TestJournalOwnerReserveAfterCloseFails(t *testing.T) {
	owner, err := AcquireJournalOwner(t.TempDir())
	if err != nil {
		t.Fatalf("AcquireJournalOwner: %v", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := owner.reserveLSN(); err == nil {
		t.Fatalf("ReserveLSN after Close unexpectedly succeeded")
	}
	if _, _, err := owner.reserveLSNRange(2); err == nil {
		t.Fatalf("reserveLSNRange after Close unexpectedly succeeded")
	}
	if err := owner.rollbackReservedLSN(1); err == nil {
		t.Fatalf("rollbackReservedLSN after Close unexpectedly succeeded")
	}
}

func TestCommandJournalUsesCommitSegmentFamily(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 3, SegmentSeq: 9})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	if got, want := j.Path(), filepath.Join(dir, "commit-l3-000009.log"); got != want {
		t.Fatalf("Path=%q, want %q", got, want)
	}
}

func TestCommandJournalRejectsCallerAssignedLSN(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	_, err = j.AppendCommand(CommandEnvelope{
		LSN:           99,
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err == nil {
		t.Fatalf("AppendCommand with caller-assigned LSN unexpectedly succeeded")
	}
}

func TestCommandJournalValidationFailureDoesNotConsumeLSN(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	if _, err := j.AppendCommand(CommandEnvelope{}); err == nil {
		t.Fatalf("invalid AppendCommand unexpectedly succeeded")
	}
	got, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("valid AppendCommand after validation failure: %v", err)
	}
	if got != 1 {
		t.Fatalf("LSN after validation failure=%d, want 1", got)
	}
}

func TestCommandJournalDormantExternalRefRejectionDoesNotMutateJournal(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer func() { _ = j.Close() }()

	if lsn, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil || lsn != 1 {
		t.Fatalf("seed append LSN=%d error=%v, want LSN 1", lsn, err)
	}
	pathBefore, bytesBefore := j.ActiveSegmentSnapshot()
	nextBefore := j.NextLSN()
	segmentsBefore, err := filepath.Glob(filepath.Join(dir, "commit-l*-*.log"))
	if err != nil {
		t.Fatal(err)
	}

	lsn, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
		ExternalRefs:  []ExternalRef{{Class: ExternalRefValueLog, FileID: 1, Length: 1}},
	})
	if !errors.Is(err, ErrCommandWALUnsupportedExternalRef) || lsn != 0 {
		t.Fatalf("rejected append LSN=%d error=%v, want LSN 0 and ErrCommandWALUnsupportedExternalRef", lsn, err)
	}
	pathAfter, bytesAfter := j.ActiveSegmentSnapshot()
	if pathAfter != pathBefore || bytesAfter != bytesBefore {
		t.Fatalf("active segment mutated: before=(%q,%d) after=(%q,%d)", pathBefore, bytesBefore, pathAfter, bytesAfter)
	}
	if got := j.NextLSN(); got != nextBefore {
		t.Fatalf("NextLSN=%d after rejection, want unchanged %d", got, nextBefore)
	}
	segmentsAfter, err := filepath.Glob(filepath.Join(dir, "commit-l*-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(segmentsAfter, segmentsBefore) {
		t.Fatalf("segment family changed: before=%v after=%v", segmentsBefore, segmentsAfter)
	}
}

func TestCommandJournalUnsupportedVersionDoesNotConsumeLSN(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	_, err = j.AppendCommand(CommandEnvelope{
		Version:       CommandFrameVersionV2 + 1,
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if !errors.Is(err, ErrCommandWALUnsupportedVersion) {
		t.Fatalf("unsupported version AppendCommand error=%v, want ErrCommandWALUnsupportedVersion", err)
	}
	got, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("valid AppendCommand after unsupported version: %v", err)
	}
	if got != 1 {
		t.Fatalf("LSN after unsupported version=%d, want 1", got)
	}
}

func TestCommandJournalAppendFailureRollsBackLSN(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	restoreWriter := j.installBufferedWriterForTest(commandJournalFailWriter{}, 1)
	_, err = j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if !errors.Is(err, errCommandJournalInjectedWrite) {
		t.Fatalf("failed AppendCommand error=%v, want injected write failure", err)
	}

	restoreWriter()
	got, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("valid AppendCommand after write failure: %v", err)
	}
	if got != 1 {
		t.Fatalf("LSN after write failure=%d, want 1", got)
	}
}

func TestCommandJournalOversizedFrameDoesNotConsumeLSN(t *testing.T) {
	emptyPayload, err := EncodeRawKVBatchPayload(nil)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload empty: %v", err)
	}
	emptyFrameSize, err := commandFrameEncodedSize(CommandEnvelope{
		LSN:           1,
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
		Payload:       emptyPayload,
	})
	if err != nil {
		t.Fatalf("commandFrameEncodedSize empty: %v", err)
	}
	oversizedPayload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("k"), Value: bytes.Repeat([]byte("v"), 32)},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload oversized: %v", err)
	}

	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{MaxSegmentSize: int64(emptyFrameSize)})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	_, err = j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
		Payload:       oversizedPayload,
	})
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("oversized AppendCommand error=%v, want ErrRecordTooLarge", err)
	}
	got, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("valid AppendCommand after oversized frame: %v", err)
	}
	if got != 1 {
		t.Fatalf("LSN after oversized frame=%d, want 1", got)
	}
}

func TestCommandJournalV2OversizedFrameDoesNotConsumeLSN(t *testing.T) {
	emptyFrameSize, err := commandFrameV2EncodedSize(CommandEnvelope{
		Version:         CommandFrameVersionV2,
		DurabilityClass: CommandDurabilityDurable,
		LSN:             1,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("commandFrameV2EncodedSize empty: %v", err)
	}
	oversizedPayload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("k"), Value: bytes.Repeat([]byte("v"), 32)},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload oversized: %v", err)
	}

	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{MaxSegmentSize: int64(emptyFrameSize)})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	_, err = j.AppendCommand(CommandEnvelope{
		Version:         CommandFrameVersionV2,
		DurabilityClass: CommandDurabilityDurable,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
		Payload:         oversizedPayload,
	})
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("oversized AppendCommand V2 error=%v, want ErrRecordTooLarge", err)
	}
	got, err := j.AppendCommand(CommandEnvelope{
		Version:         CommandFrameVersionV2,
		DurabilityClass: CommandDurabilityDurable,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("valid AppendCommand V2 after oversized frame: %v", err)
	}
	if got != 1 {
		t.Fatalf("V2 LSN after oversized frame=%d, want 1", got)
	}
}

func TestCommandJournalDeterministicStressReopenAcrossLanesAndTails(t *testing.T) {
	dir := t.TempDir()
	var wantNext uint64 = 1

	for step := 0; step < 9; step++ {
		lane := step % 3
		seg := uint64(step/3 + 1)
		appendCount := 1 + (step*7)%5
		j, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: lane, SegmentSeq: seg})
		if err != nil {
			t.Fatalf("step %d OpenCommandJournal lane=%d seg=%d: %v", step, lane, seg, err)
		}
		for i := 0; i < appendCount; i++ {
			got, err := j.AppendCommand(CommandEnvelope{
				Kind:          CommandKindRawKVBatch,
				Scope:         CommandScopeRawKV,
				PayloadFormat: PayloadFormatRawKVBatchV1,
			})
			if err != nil {
				_ = j.Close()
				t.Fatalf("step %d AppendCommand %d: %v", step, i, err)
			}
			if got != wantNext {
				_ = j.Close()
				t.Fatalf("step %d AppendCommand %d LSN=%d, want %d", step, i, got, wantNext)
			}
			wantNext++
		}
		if err := j.Close(); err != nil {
			t.Fatalf("step %d Close: %v", step, err)
		}

		if step%2 == 0 {
			path := filepath.Join(dir, CommandSegmentName(lane, seg))
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatalf("step %d OpenFile tail: %v", step, err)
			}
			if _, err := f.Write([]byte{0xde, 0xad, 0xbe}); err != nil {
				_ = f.Close()
				t.Fatalf("step %d Write tail: %v", step, err)
			}
			if err := f.Close(); err != nil {
				t.Fatalf("step %d Close tail: %v", step, err)
			}

			reopen, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: lane, SegmentSeq: seg})
			if err != nil {
				t.Fatalf("step %d reopen after active tail: %v", step, err)
			}
			got, err := reopen.AppendCommand(CommandEnvelope{
				Kind:          CommandKindRawKVBatch,
				Scope:         CommandScopeRawKV,
				PayloadFormat: PayloadFormatRawKVBatchV1,
			})
			if err != nil {
				_ = reopen.Close()
				t.Fatalf("step %d AppendCommand after active tail: %v", step, err)
			}
			if got != wantNext {
				_ = reopen.Close()
				t.Fatalf("step %d LSN after active tail=%d, want %d", step, got, wantNext)
			}
			wantNext++
			if err := reopen.Close(); err != nil {
				t.Fatalf("step %d Close reopen: %v", step, err)
			}
		}
	}

	segments, err := commandJournalSegments(dir, "")
	if err != nil {
		t.Fatalf("commandJournalSegments: %v", err)
	}
	var gotLSNs []uint64
	for _, seg := range segments {
		frames, err := ScanCommandFrames(seg.path, Options{})
		if err != nil {
			t.Fatalf("ScanCommandFrames %s: %v", filepath.Base(seg.path), err)
		}
		for _, frame := range frames {
			gotLSNs = append(gotLSNs, frame.LSN)
		}
	}
	sort.Slice(gotLSNs, func(i, j int) bool { return gotLSNs[i] < gotLSNs[j] })
	if len(gotLSNs) != int(wantNext-1) {
		t.Fatalf("frame count=%d, want %d; LSNs=%v", len(gotLSNs), wantNext-1, gotLSNs)
	}
	for i, got := range gotLSNs {
		if want := uint64(i + 1); got != want {
			t.Fatalf("global LSN[%d]=%d, want %d; LSNs=%v", i, got, want, gotLSNs)
		}
	}
}

func TestCommandJournalCleanupSnapshotAcceptsMonotonicAppendAndRotation(t *testing.T) {
	journal, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{Lane: 4, SegmentSeq: 2})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer journal.Close()

	captured, err := journal.CaptureCleanupSnapshot()
	if err != nil {
		t.Fatalf("CaptureCleanupSnapshot: %v", err)
	}
	if _, err := journal.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand: %v", err)
	}
	if err := journal.WithCleanupSnapshot(captured, nil); err != nil {
		t.Fatalf("WithCleanupSnapshot after append: %v", err)
	}
	if err := journal.RotateActiveSegment(false); err != nil {
		t.Fatalf("RotateActiveSegment: %v", err)
	}
	if err := journal.WithCleanupSnapshot(captured, nil); err != nil {
		t.Fatalf("WithCleanupSnapshot after rotation: %v", err)
	}
}

func TestCommandJournalCleanupSnapshotRejectsRegressionAndPendingOwnership(t *testing.T) {
	journal, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{Lane: 4, SegmentSeq: 2})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer journal.Close()
	if _, err := journal.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand: %v", err)
	}
	captured, err := journal.CaptureCleanupSnapshot()
	if err != nil {
		t.Fatalf("CaptureCleanupSnapshot: %v", err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*CommandJournalCleanupSnapshot)
	}{
		{name: "cleanup-epoch", mutate: func(current *CommandJournalCleanupSnapshot) { current.CleanupEpoch-- }},
		{name: "namespace-generation", mutate: func(current *CommandJournalCleanupSnapshot) { current.NamespaceGeneration-- }},
		{name: "lane", mutate: func(current *CommandJournalCleanupSnapshot) { current.Lane++ }},
		{name: "segment-sequence", mutate: func(current *CommandJournalCleanupSnapshot) { current.SegmentSeq-- }},
		{name: "same-segment-path", mutate: func(current *CommandJournalCleanupSnapshot) { current.ActivePath += ".rebound" }},
		{name: "same-segment-bytes", mutate: func(current *CommandJournalCleanupSnapshot) { current.ActiveBytes-- }},
		{name: "same-segment-max-lsn", mutate: func(current *CommandJournalCleanupSnapshot) { current.ActiveSegmentMaxLSN-- }},
		{name: "pending-stable-rotation", mutate: func(current *CommandJournalCleanupSnapshot) { current.PendingStableRotation = 1 }},
		{name: "pending-successor", mutate: func(current *CommandJournalCleanupSnapshot) { current.PendingSuccessor = true }},
		{
			name: "rotation-without-counter-progress",
			mutate: func(current *CommandJournalCleanupSnapshot) {
				current.SegmentSeq++
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := captured
			tc.mutate(&current)
			if err := validateMonotonicCleanupSnapshot(captured, current); !errors.Is(err, ErrCommandWALCleanupSnapshotStale) {
				t.Fatalf("validateMonotonicCleanupSnapshot error=%v, want stale", err)
			}
		})
	}
}
