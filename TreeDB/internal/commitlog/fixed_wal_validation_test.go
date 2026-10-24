package commitlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
)

func fixedWALValidationFrame(t *testing.T, version uint16, lsn uint64) []byte {
	t.Helper()
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSet, Key: []byte("key"), Value: []byte("value")}})
	if err != nil {
		t.Fatal(err)
	}
	env := CommandEnvelope{LSN: lsn, Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1, Payload: payload}
	var frame []byte
	if version == CommandFrameVersionV2 {
		env.DurabilityClass = CommandDurabilityDurable
		frame, err = EncodeCommandFrameV2(env)
	} else {
		frame, err = EncodeCommandFrame(env)
	}
	if err != nil {
		t.Fatal(err)
	}
	segment := make([]byte, segmentHeaderSize+len(frame))
	binary.LittleEndian.PutUint32(segment[:4], uint32(len(frame)))
	copy(segment[segmentHeaderSize:], frame)
	fixedWALValidationChecksum(segment)
	return segment
}

func fixedWALValidationChecksum(segment []byte) {
	binary.LittleEndian.PutUint32(segment[4:8], crc.Checksum(segment[segmentHeaderSize:]))
}

func fixedWALValidationWriter(t *testing.T) *Writer {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	state, err := writeInitializedFixedWAL(file, 4096)
	if err != nil {
		t.Fatal(err)
	}
	return &Writer{f: file, fixed: state}
}

// This guards the whole-buffer preflight, not merely rejection of each frame
// immediately before its write: a valid first frame must never escape first.
func TestFixedWALMalformedLaterFrameWritesNothing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func([]byte) []byte
		want   error
	}{
		{"outer_checksum", func(p []byte) []byte { p[len(p)-1] ^= 1; return p }, ErrCorrupt},
		{"envelope", func(p []byte) []byte { p[segmentHeaderSize] ^= 1; fixedWALValidationChecksum(p); return p }, ErrCorrupt},
		{"payload", func(p []byte) []byte {
			p[segmentHeaderSize+commandFrameHeaderSize] = 0xff
			fixedWALValidationChecksum(p)
			return p
		}, ErrCommandWALUnsupportedVersion},
		{"duplicate", func(p []byte) []byte {
			binary.LittleEndian.PutUint64(p[segmentHeaderSize+20:segmentHeaderSize+28], 1)
			fixedWALValidationChecksum(p)
			return p
		}, ErrCommandWALDuplicateLSN},
		{"terminal_tail", func(p []byte) []byte { return p[:len(p)-1] }, ErrCommandWALTerminalTail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := fixedWALValidationWriter(t)
			state := w.fixed
			first := fixedWALValidationFrame(t, CommandFrameVersion, 1)
			later := tc.mutate(fixedWALValidationFrame(t, CommandFrameVersionV2, 2))
			state.pending = append(first, later...)
			pending := bytes.Clone(state.pending)
			before, err := os.ReadFile(w.f.Name())
			if err != nil {
				t.Fatal(err)
			}
			if err = state.flush(w); !errors.Is(err, tc.want) {
				t.Fatalf("flush=%v want %v", err, tc.want)
			}
			after, err := os.ReadFile(w.f.Name())
			if err != nil {
				t.Fatal(err)
			}
			if w.writeSyscalls.Load() != 0 || !bytes.Equal(before, after) || state.physical != fixedWALHeaderSize || state.logical != 0 || state.lastLSN != 0 || !bytes.Equal(state.pending, pending) {
				t.Fatal("malformed later frame wrote an extent or changed pending/frontiers")
			}
		})
	}
}

func TestFixedWALValidatedMixedFramesRetainRetryAndScratchOwnership(t *testing.T) {
	for _, failure := range []string{"write_error", "later_capacity"} {
		t.Run(failure, func(t *testing.T) {
			w := fixedWALValidationWriter(t)
			state := w.fixed
			first := fixedWALValidationFrame(t, CommandFrameVersion, 1)
			second := fixedWALValidationFrame(t, CommandFrameVersionV2, 2)
			want := append(bytes.Clone(first), second...)
			state.pending = bytes.Clone(want)
			capacity, file := state.capacity, w.f
			if failure == "write_error" {
				readOnly, err := os.Open(file.Name())
				if err != nil {
					t.Fatal(err)
				}
				defer readOnly.Close()
				w.f = readOnly
			} else {
				state.capacity = fixedWALAlign(state.physical+fixedWALWitnessSize+int64(len(first))) + fixedWALWitnessSize
			}
			if err := state.flush(w); err == nil {
				t.Fatal("injected emission failure succeeded")
			}
			if failure == "write_error" {
				if state.logical != 0 || !bytes.Equal(state.pending, want) {
					t.Fatal("write failure lost retry bytes")
				}
			} else if state.logical != int64(len(first)) || state.lastLSN != 1 || !bytes.Equal(state.pending, second) {
				t.Fatal("later failure lost exact unflushed suffix")
			}
			// Only this internal control retries a physical emission. Public writers
			// still poison on flush failure and never retry an assigned command.
			w.f, state.capacity = file, capacity
			for i := range w.scratch {
				w.scratch[i] = 0xa5
			}
			if err := state.flush(w); err != nil {
				t.Fatal(err)
			}
			clear(w.scratch)
			loaded, _, err := loadFixedWAL(file)
			if err != nil {
				t.Fatal(err)
			}
			if len(loaded.extents) != 2 || loaded.lastLSN != 2 || state.lastLSN != 2 || len(state.pending) != 0 {
				t.Fatal("mixed frame frontier or extent count changed")
			}
			logical, err := io.ReadAll(&fixedWALStream{file: file, state: loaded})
			if err != nil || !bytes.Equal(logical, want) {
				t.Fatalf("mixed logical bytes changed: %v", err)
			}
			for i, extent := range loaded.extents {
				var witness [fixedWALWitnessSize]byte
				if _, err := file.ReadAt(witness[:], extent.physical); err != nil {
					t.Fatal(err)
				}
				if binary.LittleEndian.Uint64(witness[56:64]) != uint64(i+1) || binary.LittleEndian.Uint64(witness[64:72]) != uint64(i+1) {
					t.Fatal("per-frame LSN witness changed")
				}
			}
		})
	}
}
