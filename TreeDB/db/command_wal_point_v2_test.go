package db

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestAppendRawKVPointCommandWALV2ModesRotationAndReopenReplay(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                          dir,
		Durability:                   DurabilityWALOnRelaxed,
		CommandWAL:                   true,
		DisableBackgroundPrune:       true,
		CommandWALSegmentTargetBytes: 512,
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = d.Close()
		}
	}()

	var nextRev uint64
	allocRev := func() page.EntryRevision {
		nextRev++
		return page.EntryRevision(nextRev)
	}

	// 1. Relaxed point writes across segment rotation (generates rotation debt).
	for i := 0; i < 6; i++ {
		key := []byte("k-relaxed-" + string(rune('a'+i)))
		val := bytes.Repeat([]byte{byte('A' + i)}, 128)
		lsn, err := d.AppendRawKVPointCommandWALTrustedWithPreparedRevisionAndMode(
			commitlog.RawKVOpSet, key, val, allocRev, RawKVCommandWALAppendRelaxed,
		)
		if err != nil {
			t.Fatalf("relaxed set %d: %v", i, err)
		}
		if lsn != uint64(i+1) {
			t.Fatalf("relaxed set %d lsn=%d, want %d", i, lsn, i+1)
		}
	}
	if d.commandWALDurableLSN.Load() != 0 {
		t.Fatalf("durable LSN=%d before sync, want 0", d.commandWALDurableLSN.Load())
	}

	// 2. Delete one relaxed key via RawKVCommandWALAppendDurable (sync=true, closes durable prefix + debt).
	syncLSN, err := d.AppendRawKVPointCommandWALTrustedWithPreparedRevisionAndMode(
		commitlog.RawKVOpDelete, []byte("k-relaxed-b"), nil, allocRev, RawKVCommandWALAppendDurable,
	)
	if err != nil {
		t.Fatalf("durable delete: %v", err)
	}
	if syncLSN != 7 {
		t.Fatalf("durable delete lsn=%d, want 7", syncLSN)
	}
	if got := d.commandWALDurableLSN.Load(); got != syncLSN {
		t.Fatalf("commandWALDurableLSN=%d after sync point write, want %d", got, syncLSN)
	}

	// 3. CapturedDurable point write updates commandWALDurableMarkerLSN without syncing.
	capLSN, err := d.AppendRawKVPointCommandWALTrustedWithPreparedRevisionAndMode(
		commitlog.RawKVOpSet, []byte("k-captured"), []byte("val-captured"), allocRev, RawKVCommandWALAppendCapturedDurable,
	)
	if err != nil {
		t.Fatalf("captured set: %v", err)
	}
	if capLSN != 8 {
		t.Fatalf("captured set lsn=%d, want 8", capLSN)
	}
	if got := d.commandWALDurableMarkerLSN.Load(); got != capLSN {
		t.Fatalf("commandWALDurableMarkerLSN=%d, want %d", got, capLSN)
	}
	if err := d.FlushCommandWALBarrier(true); err != nil {
		t.Fatalf("FlushCommandWALBarrier: %v", err)
	}

	// Verify V2 frames on disk before close.
	walDir := filepath.Join(dir, "wal")
	matches, err := filepath.Glob(filepath.Join(walDir, "commit-l0-*.log"))
	if err != nil || len(matches) < 2 {
		t.Fatalf("expected rotated WAL segments in %s, got %v err=%v", walDir, matches, err)
	}
	var frames []commitlog.CommandEnvelope
	for _, seg := range matches {
		segFrames, err := commitlog.ScanCommandFramesV2(seg, commitlog.Options{})
		if err != nil {
			t.Fatalf("ScanCommandFramesV2(%s): %v", seg, err)
		}
		frames = append(frames, segFrames...)
	}
	if len(frames) < 8 {
		t.Fatalf("decoded %d V2 frames, want at least 8", len(frames))
	}
	for i := 0; i < 6; i++ {
		if frames[i].Version != commitlog.CommandFrameVersionV2 || frames[i].DurabilityClass != commitlog.CommandDurabilityRelaxed {
			t.Fatalf("frame[%d] version=%d class=%d, want V2/Relaxed", i, frames[i].Version, frames[i].DurabilityClass)
		}
	}
	if frames[6].DurabilityClass != commitlog.CommandDurabilityDurable || frames[7].DurabilityClass != commitlog.CommandDurabilityDurable {
		t.Fatalf("durable/captured frames classes=%d/%d, want Durable/Durable", frames[6].DurabilityClass, frames[7].DurabilityClass)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed = true

	// Reopen and verify WAL replay recovered all sets and the delete.
	reopen, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if err := reopen.Close(); err != nil {
			t.Fatalf("reopen Close: %v", err)
		}
	}()
	for i := 0; i < 6; i++ {
		key := []byte("k-relaxed-" + string(rune('a'+i)))
		got, err := reopen.Get(key)
		if i == 1 {
			if err != nil || got != nil {
				t.Fatalf("reopen deleted key %q: got=%x err=%v", key, got, err)
			}
			continue
		}
		want := bytes.Repeat([]byte{byte('A' + i)}, 128)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("reopen key %q: got len=%d err=%v", key, len(got), err)
		}
	}
	if got, err := reopen.Get([]byte("k-captured")); err != nil || !bytes.Equal(got, []byte("val-captured")) {
		t.Fatalf("reopen k-captured: got=%q err=%v", got, err)
	}
}

func TestAppendRawKVPointCommandWALV2PoisonOnFlushCut(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:                    dir,
		Durability:             DurabilityWALOnRelaxed,
		CommandWAL:             true,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	wantErr := errors.New("injected flush cut")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.BeforeUserspaceFlush {
			return wantErr
		}
		return nil
	})
	lsn, err := d.AppendRawKVPointCommandWALTrustedWithPreparedRevisionAndMode(
		commitlog.RawKVOpSet, []byte("k"), []byte("v"),
		func() page.EntryRevision { return 1 },
		RawKVCommandWALAppendRelaxed,
	)
	restore()
	if !errors.Is(err, wantErr) {
		t.Fatalf("err=%v, want %v", err, wantErr)
	}
	if lsn != 1 {
		t.Fatalf("lsn=%d, want 1", lsn)
	}
	if !d.commandWALFlushPoisoned.Load() {
		t.Fatal("expected commandWALFlushPoisoned after post-append flush error")
	}
	if _, err := d.AppendRawKVPointCommandWALTrustedWithPreparedRevisionAndMode(
		commitlog.RawKVOpSet, []byte("k2"), []byte("v2"),
		func() page.EntryRevision { return 2 },
		RawKVCommandWALAppendRelaxed,
	); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("post-poison append err=%v, want ErrRecoveryRequired", err)
	}
}
