package commitlog

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestEncodeTrustedRawKVPointCommandFrameV2Parity(t *testing.T) {
	cases := []struct {
		name           string
		class          CommandDurabilityClass
		lsn            uint64
		baseAppliedLSN uint64
		op             RawKVOp
		key            []byte
		value          []byte
		revision       uint64
	}{
		{
			name:           "set_relaxed_no_rev",
			class:          CommandDurabilityRelaxed,
			lsn:            1,
			baseAppliedLSN: 0,
			op:             RawKVOpSet,
			key:            []byte("alpha"),
			value:          []byte("beta"),
		},
		{
			name:           "set_durable_with_rev",
			class:          CommandDurabilityDurable,
			lsn:            42,
			baseAppliedLSN: 40,
			op:             RawKVOpSet,
			key:            []byte("key/00001"),
			value:          bytes.Repeat([]byte("v"), 128),
			revision:       999,
		},
		{
			name:           "set_empty_key_and_value",
			class:          CommandDurabilityRelaxed,
			lsn:            7,
			baseAppliedLSN: 3,
			op:             RawKVOpSet,
			key:            []byte{},
			value:          []byte{},
			revision:       1,
		},
		{
			name:           "delete_relaxed_no_rev",
			class:          CommandDurabilityRelaxed,
			lsn:            9,
			baseAppliedLSN: 8,
			op:             RawKVOpDelete,
			key:            []byte("gone"),
		},
		{
			name:           "delete_durable_with_rev",
			class:          CommandDurabilityDurable,
			lsn:            100,
			baseAppliedLSN: 99,
			op:             RawKVOpDelete,
			key:            []byte("gone-rev"),
			revision:       123456,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := EncodeRawKVBatchPayload([]RawKVOperation{{
				Op:       tc.op,
				Key:      tc.key,
				Value:    tc.value,
				Revision: tc.revision,
			}})
			if err != nil {
				t.Fatalf("EncodeRawKVBatchPayload: %v", err)
			}
			want, err := EncodeCommandFrameV2(CommandEnvelope{
				Version:         CommandFrameVersionV2,
				DurabilityClass: tc.class,
				LSN:             tc.lsn,
				Kind:            CommandKindRawKVBatch,
				Scope:           CommandScopeRawKV,
				BaseAppliedLSN:  tc.baseAppliedLSN,
				PayloadFormat:   PayloadFormatRawKVBatchV1,
				Payload:         payload,
			})
			if err != nil {
				t.Fatalf("EncodeCommandFrameV2: %v", err)
			}

			valueLen, payloadLen, total, err := trustedRawKVPointCommandFrameV2Lens(tc.class, tc.op, tc.key, tc.value, tc.revision)
			if err != nil {
				t.Fatalf("trustedRawKVPointCommandFrameV2Lens: %v", err)
			}
			if total != len(want) {
				t.Fatalf("total=%d, want %d", total, len(want))
			}
			got := encodeTrustedRawKVPointCommandFrameV2SizedTo(nil, tc.class, tc.lsn, tc.baseAppliedLSN, tc.op, tc.key, tc.value, tc.revision, valueLen, payloadLen, total)
			if !bytes.Equal(got, want) {
				t.Fatalf("frame bytes differ:\n got=%x\nwant=%x", got, want)
			}
			env, err := DecodeCommandFrameV2(got)
			if err != nil {
				t.Fatalf("DecodeCommandFrameV2: %v", err)
			}
			if env.Version != CommandFrameVersionV2 || env.DurabilityClass != tc.class || env.LSN != tc.lsn || env.BaseAppliedLSN != tc.baseAppliedLSN {
				t.Fatalf("decoded envelope header mismatch: %+v", env)
			}
		})
	}

	// Validation parity checks.
	if _, _, _, err := trustedRawKVPointCommandFrameV2Lens(0, RawKVOpSet, []byte("k"), []byte("v"), 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("zero durability class err=%v, want ErrCorrupt", err)
	}
	if _, _, _, err := trustedRawKVPointCommandFrameV2Lens(CommandDurabilityRelaxed, RawKVOpSet, nil, []byte("v"), 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("nil key err=%v, want ErrCorrupt", err)
	}
	if _, _, _, err := trustedRawKVPointCommandFrameV2Lens(CommandDurabilityRelaxed, RawKVOpDelete, []byte("k"), []byte("nonempty"), 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("delete with value err=%v, want ErrCorrupt", err)
	}
}

func TestCommandJournalAppendRawKVPointV2ObservedAndFlush(t *testing.T) {
	modes := []struct {
		name           string
		seedStreamFile bool
	}{
		{name: "stream", seedStreamFile: true},
	}
	if FixedWALCreationSupported() {
		modes = append(modes, struct {
			name           string
			seedStreamFile bool
		}{name: "fixed", seedStreamFile: false})
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			walDir := t.TempDir()
			initialFrames := 0
			if mode.seedStreamFile {
				// Pre-seed segment 1 as a stream WAL file so OpenCommandJournal keeps
				// stream mode even on fixed-WAL-capable platforms.
				seedPath := filepath.Join(walDir, CommandSegmentName(0, 1))
				w, err := NewWriter(seedPath)
				if err != nil {
					t.Fatalf("NewWriter(stream seed): %v", err)
				}
				payload, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSet, Key: []byte("seed"), Value: []byte("v")}})
				if err != nil {
					t.Fatal(err)
				}
				if err := w.AppendCommandV2(CommandEnvelope{
					Version:         CommandFrameVersionV2,
					DurabilityClass: CommandDurabilityDurable,
					LSN:             1,
					Kind:            CommandKindRawKVBatch,
					Scope:           CommandScopeRawKV,
					PayloadFormat:   PayloadFormatRawKVBatchV1,
					Payload:         payload,
				}); err != nil {
					t.Fatalf("seed AppendCommandV2: %v", err)
				}
				if err := w.Close(); err != nil {
					t.Fatalf("seed Close: %v", err)
				}
				initialFrames = 1
			}
			opts := CommandJournalOptions{
				SegmentTargetBytes:     256,
				CaptureStableResources: true,
			}
			journal, err := OpenCommandJournal(walDir, opts)
			if err != nil {
				t.Fatalf("OpenCommandJournal: %v", err)
			}
			defer func() { _ = journal.Close() }()
			hasFixed, err := journal.HasFixedWriter()
			if err != nil {
				t.Fatalf("HasFixedWriter: %v", err)
			}
			if hasFixed == mode.seedStreamFile {
				t.Fatalf("HasFixedWriter=%t, want %t", hasFixed, !mode.seedStreamFile)
			}

			var rotationTokens int
			var beforeCalls int
			afterAppend := func(lsn uint64, rotations []*CommandJournalStableRotation) error {
				rotationTokens += len(rotations)
				for _, rot := range rotations {
					rot.Release()
				}
				return nil
			}
			beforeAppend := func() error {
				beforeCalls++
				return nil
			}

			// Append relaxed, durable+sync, and capturedDurable (flush=false) frames,
			// crossing segment target bytes to verify rotation + debt handoff.
			for i := 0; i < 8; i++ {
				op := RawKVOpSet
				val := bytes.Repeat([]byte{byte('a' + i)}, 96)
				if i%3 == 2 {
					op = RawKVOpDelete
					val = nil
				}
				class := CommandDurabilityRelaxed
				flush, sync := true, false
				var before func() error
				if i == 3 {
					class = CommandDurabilityDurable
					flush, sync = true, true
					before = beforeAppend
				} else if i == 6 {
					class = CommandDurabilityDurable
					flush, sync = false, false
					before = beforeAppend
				}
				lsn, flushAttempted, err := journal.AppendRawKVPointCommandV2ObservedAndFlush(
					class, uint64(i), op, []byte("key-"+string(rune('0'+i))), val, uint64(i+10), flush, sync, before, afterAppend,
				)
				if err != nil {
					t.Fatalf("op %d AppendRawKVPointCommandV2ObservedAndFlush: %v", i, err)
				}
				if lsn != uint64(initialFrames+i+1) {
					t.Fatalf("op %d lsn=%d, want %d", i, lsn, initialFrames+i+1)
				}
				if flushAttempted != flush {
					t.Fatalf("op %d flushAttempted=%t, want %t", i, flushAttempted, flush)
				}
			}
			if beforeCalls != 2 {
				t.Fatalf("beforeCalls=%d, want 2", beforeCalls)
			}
			if rotationTokens == 0 {
				t.Fatal("expected at least one segment rotation with stable rotation tokens")
			}
			if err := journal.Flush(); err != nil {
				t.Fatalf("final Flush: %v", err)
			}
			if err := journal.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			// Verify every written segment decodes cleanly as strict V2 frames.
			matches, err := filepath.Glob(filepath.Join(walDir, "commit-l0-*.log"))
			if err != nil || len(matches) < 2 {
				t.Fatalf("expected rotated WAL segments, got %v err=%v", matches, err)
			}
			var decoded []CommandEnvelope
			for _, seg := range matches {
				frames, err := ScanCommandFramesV2(seg, Options{})
				if err != nil {
					t.Fatalf("ScanCommandFramesV2(%s): %v", seg, err)
				}
				decoded = append(decoded, frames...)
			}
			if len(decoded) != initialFrames+8 {
				t.Fatalf("decoded %d V2 frames, want %d", len(decoded), initialFrames+8)
			}
			for i, env := range decoded[initialFrames:] {
				wantClass := CommandDurabilityRelaxed
				if i == 3 || i == 6 {
					wantClass = CommandDurabilityDurable
				}
				if env.Version != CommandFrameVersionV2 || env.DurabilityClass != wantClass || env.LSN != uint64(initialFrames+i+1) {
					t.Fatalf("frame %d mismatch: version=%d class=%d lsn=%d", i, env.Version, env.DurabilityClass, env.LSN)
				}
			}
		})
	}
}

func TestCommandJournalAppendRawKVPointV2DurabilityCuts(t *testing.T) {
	walDir := t.TempDir()
	journal, err := OpenCommandJournal(walDir, CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer func() { _ = journal.Close() }()

	var seen []durabilitycut.Point
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL {
			seen = append(seen, event.Point)
		}
		return nil
	})
	defer restore()

	// Relaxed flush=true, sync=false
	if lsn, flushed, err := journal.AppendRawKVPointCommandV2ObservedAndFlush(
		CommandDurabilityRelaxed, 0, RawKVOpSet, []byte("k1"), []byte("v1"), 1, true, false, nil, nil,
	); err != nil || lsn != 1 || !flushed {
		t.Fatalf("relaxed append: lsn=%d flushed=%t err=%v", lsn, flushed, err)
	}
	wantRelaxed := []durabilitycut.Point{
		durabilitycut.BeforeDependencyAppend,
		durabilitycut.AfterDependencyAppend,
		durabilitycut.BeforeUserspaceFlush,
		durabilitycut.AfterUserspaceFlush,
	}
	if len(seen) != len(wantRelaxed) {
		t.Fatalf("relaxed cuts=%v, want %v", seen, wantRelaxed)
	}
	for i := range wantRelaxed {
		if seen[i] != wantRelaxed[i] {
			t.Fatalf("relaxed cut[%d]=%v, want %v", i, seen[i], wantRelaxed[i])
		}
	}

	// Durable sync=true
	seen = seen[:0]
	if lsn, flushed, err := journal.AppendRawKVPointCommandV2ObservedAndFlush(
		CommandDurabilityDurable, 1, RawKVOpSet, []byte("k2"), []byte("v2"), 2, true, true, nil, nil,
	); err != nil || lsn != 2 || !flushed {
		t.Fatalf("durable sync append: lsn=%d flushed=%t err=%v", lsn, flushed, err)
	}
	wantSync := []durabilitycut.Point{
		durabilitycut.BeforeDependencyAppend,
		durabilitycut.AfterDependencyAppend,
		durabilitycut.BeforeDependencyFileSync,
		durabilitycut.AfterDependencyFileSync,
	}
	if len(seen) != len(wantSync) {
		t.Fatalf("sync cuts=%v, want %v", seen, wantSync)
	}
	for i := range wantSync {
		if seen[i] != wantSync[i] {
			t.Fatalf("sync cut[%d]=%v, want %v", i, seen[i], wantSync[i])
		}
	}

	// CapturedDurable flush=false
	seen = seen[:0]
	if lsn, flushed, err := journal.AppendRawKVPointCommandV2ObservedAndFlush(
		CommandDurabilityDurable, 2, RawKVOpDelete, []byte("k2"), nil, 3, false, false, nil, nil,
	); err != nil || lsn != 3 || flushed {
		t.Fatalf("captured append: lsn=%d flushed=%t err=%v", lsn, flushed, err)
	}
	wantCaptured := []durabilitycut.Point{
		durabilitycut.BeforeDependencyAppend,
		durabilitycut.AfterDependencyAppend,
	}
	if len(seen) != len(wantCaptured) {
		t.Fatalf("captured cuts=%v, want %v", seen, wantCaptured)
	}
	for i := range wantCaptured {
		if seen[i] != wantCaptured[i] {
			t.Fatalf("captured cut[%d]=%v, want %v", i, seen[i], wantCaptured[i])
		}
	}
}

func TestCommandJournalAppendRawKVPointV2ZeroAllocs(t *testing.T) {
	walDir := t.TempDir()
	journal, err := OpenCommandJournal(walDir, CommandJournalOptions{
		SegmentTargetBytes: 64 << 20,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer func() { _ = journal.Close() }()

	key := []byte("key/00001")
	val := bytes.Repeat([]byte("z"), 128)
	// Warm up writer scratch / fixed.pending buffers.
	for i := 0; i < 4; i++ {
		if _, _, err := journal.AppendRawKVPointCommandV2ObservedAndFlush(
			CommandDurabilityRelaxed, 0, RawKVOpSet, key, val, uint64(i+1), true, false, nil, nil,
		); err != nil {
			t.Fatalf("warmup: %v", err)
		}
	}

	var rev uint64 = 100
	allocs := testing.AllocsPerRun(50, func() {
		rev++
		if _, _, err := journal.AppendRawKVPointCommandV2ObservedAndFlush(
			CommandDurabilityRelaxed, 0, RawKVOpSet, key, val, rev, true, false, nil, nil,
		); err != nil {
			t.Fatalf("append: %v", err)
		}
	})
	if allocs > 0 {
		t.Fatalf("AppendRawKVPointCommandV2ObservedAndFlush allocated %.2f allocs/op, want 0", allocs)
	}
}

func BenchmarkAppendRawKVPointV2DirectVsGeneric(b *testing.B) {
	key := []byte("key/00001")
	val := bytes.Repeat([]byte("z"), 128)

	b.Run("generic_encode_append_flush", func(b *testing.B) {
		journal, err := OpenCommandJournal(b.TempDir(), CommandJournalOptions{
			SegmentTargetBytes: 64 << 20,
		})
		if err != nil {
			b.Fatal(err)
		}
		defer func() { _ = journal.Close() }()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			payload, err := EncodeRawKVBatchPayload([]RawKVOperation{{
				Op: RawKVOpSet, Key: key, Value: val, Revision: uint64(i + 1),
			}})
			if err != nil {
				b.Fatal(err)
			}
			_, err = journal.AppendCommandObservedWithHooks(CommandEnvelope{
				Version:         CommandFrameVersionV2,
				DurabilityClass: CommandDurabilityRelaxed,
				Kind:            CommandKindRawKVBatch,
				Scope:           CommandScopeRawKV,
				BaseAppliedLSN:  0,
				PayloadFormat:   PayloadFormatRawKVBatchV1,
				Payload:         payload,
			}, func() error { return nil }, func(uint64, []*CommandJournalStableRotation) error { return nil })
			if err != nil {
				b.Fatal(err)
			}
			if err := journal.FlushObserved(false); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("direct_v2_append_and_flush", func(b *testing.B) {
		journal, err := OpenCommandJournal(b.TempDir(), CommandJournalOptions{
			SegmentTargetBytes: 64 << 20,
		})
		if err != nil {
			b.Fatal(err)
		}
		defer func() { _ = journal.Close() }()
		after := func(uint64, []*CommandJournalStableRotation) error { return nil }
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := journal.AppendRawKVPointCommandV2ObservedAndFlush(
				CommandDurabilityRelaxed, 0, RawKVOpSet, key, val, uint64(i+1), true, false, nil, after,
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
