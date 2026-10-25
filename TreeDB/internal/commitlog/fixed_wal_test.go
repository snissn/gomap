package commitlog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFixedWALImmutableCaptureAndCorruption(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed-container publication is unsupported on this platform")
	}
	dir := t.TempDir()
	journal, err := openFixedTestCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 4096, CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err = journal.AppendCommand(fixedWALTestBarrier()); err != nil {
			t.Fatal(err)
		}
		capture, e := journal.CaptureSync(nil)
		if e != nil {
			t.Fatal(e)
		}
		if e = capture.Sync(); e != nil {
			t.Fatal(e)
		}
		capture.Release()
	}
	path := journal.path
	state, _, err := loadFixedWAL(journal.writer.f)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.extents) != 2 {
		t.Fatalf("extents=%d", len(state.extents))
	}
	image, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		mutate  func([]byte)
		corrupt bool
	}{
		{"intact", func([]byte) {}, false},
		{"torn_redundant_footer", func(p []byte) { clear(p[state.extents[1].end-fixedWALWitnessSize : state.extents[1].end-16]) }, false},
		{"zero_ack_start", func(p []byte) { clear(p[state.extents[1].physical : state.extents[1].physical+fixedWALWitnessSize]) }, true},
		{"zero_ack_inner_header", func(p []byte) { off := state.extents[1].physical + fixedWALWitnessSize; clear(p[off : off+8]) }, true},
		{"ack_payload_crc", func(p []byte) { p[state.extents[1].physical+fixedWALWitnessSize+20] ^= 1 }, true},
		{"stale_suffix", func(p []byte) { copy(p[state.physical+64:], []byte("unaccounted suffix")) }, true},
		{"foreign_intact_footer", func(p []byte) {
			copy(p[state.extents[1].end-fixedWALWitnessSize:state.extents[1].end], p[state.extents[0].end-fixedWALWitnessSize:state.extents[0].end])
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			variant := bytes.Clone(image)
			tc.mutate(variant)
			target := filepath.Join(t.TempDir(), CommandSegmentName(0, 1))
			if err := os.WriteFile(target, variant, 0600); err != nil {
				t.Fatal(err)
			}
			reader, err := NewReader(target)
			if tc.corrupt {
				if !errors.Is(err, ErrCorrupt) {
					t.Fatalf("err=%v want corruption", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			for lsn := uint64(1); lsn <= 2; lsn++ {
				env, e := reader.ReadCommandFrameV2()
				if e != nil || env.LSN != lsn {
					t.Fatalf("frame=%+v err=%v", env, e)
				}
			}
			recovered, e := openFixedTestCommandJournal(filepath.Dir(target), CommandJournalOptions{SegmentTargetBytes: 4096})
			if e != nil {
				t.Fatal(e)
			}
			defer recovered.Close()
			if recovered.NextLSN() != 3 {
				t.Fatalf("next=%d", recovered.NextLSN())
			}
			if info, e := os.Stat(target); e != nil || info.Size() != int64(len(image)) {
				t.Fatalf("fixed size changed: %v %v", info, e)
			}
		})
	}
}

func TestFixedWALRepairRetainsImmutablePrefix(t *testing.T) {
	_, path, offset, original := fixedWALRepairFixture(t)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	state, _, err := loadFixedWAL(file)
	if err != nil {
		t.Fatal(err)
	}
	prefix := original[:state.extents[0].end]
	if err = TruncateLogicalFile(&file, offset); err != nil {
		t.Fatal(err)
	}
	after := make([]byte, len(prefix))
	if _, err = file.ReadAt(after, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(prefix, after) {
		t.Fatal("repair changed retained prefix bytes")
	}
	loaded, _, err := loadFixedWAL(file)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.extents) != 1 {
		t.Fatalf("extents=%d", len(loaded.extents))
	}
}

func fixedWALTestBarrier() CommandEnvelope {
	env := NewDurablePrefixBarrierV1(1, 0)
	env.LSN = 0
	return env
}

func TestFixedWALEncodeReusePreservesFramesAndZeroPadding(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed-container publication is unsupported on this platform")
	}
	journal, err := openFixedTestCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: 16 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	// Exercise growth, reuse for a smaller image, and multiple pending frames.
	// Poisoning encode scratch after its buffered copy makes stale padding and
	// accidental aliasing observable through the strict recovery reader.
	for _, sizes := range [][]int{{1024}, {7, 128}, {3}} {
		for _, size := range sizes {
			if _, err := journal.AppendCommand(fixedWALLifecycleFrame(t, size)); err != nil {
				t.Fatal(err)
			}
		}
		if err := journal.writer.bw.Flush(); err != nil {
			t.Fatal(err)
		}
		for i := range journal.writer.scratch[:cap(journal.writer.scratch)] {
			journal.writer.scratch[:cap(journal.writer.scratch)][i] = 0xa5
		}
		if err := journal.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	reader, err := NewReader(journal.path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for i, size := range []int{1024, 7, 128, 3} {
		env, err := reader.ReadCommandFrameV2()
		if err != nil {
			t.Fatal(err)
		}
		if env.LSN != uint64(i+1) || !bytes.Equal(env.Payload, fixedWALLifecycleFrame(t, size).Payload) {
			t.Fatalf("recovered frame %d changed after encode reuse", i+1)
		}
	}
}
