package commitlog

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
)

func fixedPendingWriter(t *testing.T) *Writer {
	t.Helper()
	if !FixedWALCreationSupported() {
		t.Skip("fixed-container creation unsupported")
	}
	w, err := NewWriterWithOptions(filepath.Join(t.TempDir(), "command.log"), Options{
		fixedCapacity: 16 << 10, DeferredCommandBufferSize: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func fixedPendingSegment(t *testing.T, env CommandEnvelope) []byte {
	t.Helper()
	frame, err := EncodeCommandFrameV2(env)
	if err != nil {
		t.Fatal(err)
	}
	segment := make([]byte, segmentHeaderSize+len(frame))
	binary.LittleEndian.PutUint32(segment[:4], uint32(len(frame)))
	binary.LittleEndian.PutUint32(segment[4:8], crc.Checksum(frame))
	copy(segment[segmentHeaderSize:], frame)
	return segment
}

// This is representation activation/ownership evidence, not a behavioral RED:
// all fixed V2 kinds use the canonical pending destination before Flush.
func TestFixedPendingV2CanonicalOwnership(t *testing.T) {
	collection, err := EncodeCollectionInsertBatchByIDPayload("users", []CollectionDocument{{ID: []byte("one"), Document: []byte(`{"name":"Ada"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		env  CommandEnvelope
	}{
		{"raw", encodingPlanEnvelope(t, 0)},
		{"rid", encodingPlanEnvelope(t, 42)},
		{"barrier", NewDurablePrefixBarrierV1(1, 0)},
		{"collection", CommandEnvelope{Version: CommandFrameVersionV2, DurabilityClass: CommandDurabilityDurable, Kind: CommandKindCollectionInsertBatchByID, Scope: CommandScopeCollection, PayloadFormat: PayloadFormatCollectionInsertBatchByIDV1, Payload: collection}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := fixedPendingWriter(t)
			env := tc.env
			env.LSN = 1
			want := fixedPendingSegment(t, env)
			w.scratch = bytes.Repeat([]byte{0xa5}, 1024)
			beforeScratch := bytes.Clone(w.scratch)
			if err := w.AppendCommandV2(env); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(w.fixed.pending, want) || w.bw.Buffered() != 0 || !bytes.Equal(w.scratch, beforeScratch) {
				t.Fatal("fixed V2 did not emit directly into canonical pending storage")
			}
			if w.Size() != int64(len(want)) || w.fixed.logical != 0 || w.fixed.physical != fixedWALHeaderSize || w.writeSyscalls.Load() != 0 {
				t.Fatal("memory append changed physical frontier or size accounting")
			}
			clear(env.Payload)
			clear(w.scratch)
			if !bytes.Equal(w.fixed.pending, want) {
				t.Fatal("pending frame borrowed caller or scratch storage")
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
			state, _, err := loadFixedWAL(w.f)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := io.ReadAll(&fixedWALStream{file: w.f, state: state})
			if err != nil || !bytes.Equal(actual, want) || w.writeSyscalls.Load() != 1 || len(state.extents) != 1 {
				t.Fatal("fixed encoding, physical write count or reopen changed", err)
			}
		})
	}
}

func TestFixedPendingV2DrainsOlderV1BuffersInOrder(t *testing.T) {
	w := fixedPendingWriter(t)
	first := encodingPlanEnvelope(t, 0)
	first.Version, first.DurabilityClass, first.LSN = CommandFrameVersion, 0, 1
	if err := w.AppendCommand(first); err != nil {
		t.Fatal(err)
	}
	if w.bw.Buffered() == 0 {
		t.Fatal("fixture did not leave an older V1 bufio frame")
	}
	if err := w.AppendRawKVPointCommandDirectTrusted(2, 0, RawKVOpSet, []byte("second"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if len(w.commandBuf) == 0 {
		t.Fatal("fixture did not leave an older deferred command")
	}
	third := encodingPlanEnvelope(t, 0)
	third.LSN = 3
	if err := w.AppendCommandV2(third); err != nil {
		t.Fatal(err)
	}
	if len(w.commandBuf) != 0 || w.bw.Buffered() != 0 || w.Size() != int64(len(w.fixed.pending)) || w.writeSyscalls.Load() != 0 {
		t.Fatal("older buffers were not drained logically before the V2 append")
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(w.f.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for lsn := uint64(1); lsn <= 3; lsn++ {
		env, err := r.ReadCommandFrame()
		if err != nil || env.LSN != lsn {
			t.Fatal("mixed entrypoints reordered frames", lsn, env.LSN, err)
		}
	}
	if _, err := r.ReadCommandFrame(); !errors.Is(err, io.EOF) {
		t.Fatal("unexpected extra frame", err)
	}
}

func TestFixedPendingV2RetainsConfiguredScratchCapAfterV1(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed-container creation unsupported")
	}
	w, err := NewWriterWithOptions(filepath.Join(t.TempDir(), "command.log"), Options{
		fixedCapacity: 16 << 10, DeferredCommandBufferSize: 4096, DeferredCommandBufferRetainSize: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSet, Key: []byte("older"), Value: bytes.Repeat([]byte("x"), 2048)}})
	if err != nil {
		t.Fatal(err)
	}
	first := CommandEnvelope{Version: CommandFrameVersion, LSN: 1, Kind: CommandKindRawKVBatch,
		Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1, Payload: payload}
	if err := w.AppendCommand(first); err != nil {
		t.Fatal(err)
	}
	if w.BufferStats().ScratchCapacity <= 128 {
		t.Fatal("fixture did not leave a large V1 encode scratch")
	}
	second := encodingPlanEnvelope(t, 0)
	second.LSN = 2
	if err := w.AppendCommandV2(second); err != nil {
		t.Fatal(err)
	}
	if got := w.BufferStats().ScratchCapacity; got != 128 {
		t.Fatalf("V2 append retained prior V1 scratch capacity %d, want 128", got)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(w.f.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := r.ReadCommandFrame()
	if err != nil || got.LSN != 1 || !bytes.Equal(got.Payload, payload) {
		t.Fatal("scratch release changed the older V1 frame", err)
	}
	if got, err = r.ReadCommandFrame(); err != nil || got.LSN != 2 || !bytes.Equal(got.Payload, second.Payload) {
		t.Fatal("scratch release changed the V2 frame", err)
	}
}

func TestFixedPendingV2RejectsWithoutAppending(t *testing.T) {
	w := fixedPendingWriter(t)
	env := encodingPlanEnvelope(t, 0)
	env.LSN = 1
	if err := w.AppendCommandV2(env); err != nil {
		t.Fatal(err)
	}
	before, beforeSize := bytes.Clone(w.fixed.pending), w.Size()
	invalid := env
	invalid.LSN, invalid.Payload = 2, []byte{}
	if err := w.AppendCommandV2(invalid); err == nil {
		t.Fatal("invalid payload accepted")
	}
	w.maxSegmentSize = 1
	if err := w.AppendCommandV2(env); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatal("capacity refusal changed", err)
	}
	w.maxSegmentSize = defaultMaxSegmentSize
	oldBuffer := w.bw
	w.bw = bufio.NewWriterSize(commandJournalFailWriter{}, 1)
	if _, err := w.bw.Write([]byte{0xa5}); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendCommandV2(env); !errors.Is(err, errCommandJournalInjectedWrite) {
		t.Fatal("older buffer drain failure changed", err)
	}
	w.bw = oldBuffer // Only the injected unaccepted byte is discarded.
	if !bytes.Equal(w.fixed.pending, before) || w.Size() != beforeSize || w.fixed.logical != 0 || w.writeSyscalls.Load() != 0 {
		t.Fatal("refusal appended bytes, changed size or wrote physical data")
	}
	env.LSN = 2
	if err := w.AppendCommandV2(env); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	state, _, err := loadFixedWAL(w.f)
	if err != nil || state.lastLSN != 2 || len(state.extents) != 2 {
		t.Fatal("refused frame affected following append/reopen", err)
	}
}

func TestFixedPendingV2MalformedLaterAppendStillWritesNothing(t *testing.T) {
	w := fixedPendingWriter(t)
	for lsn := uint64(1); lsn <= 2; lsn++ {
		env := encodingPlanEnvelope(t, 0)
		env.LSN = lsn
		if err := w.AppendCommandV2(env); err != nil {
			t.Fatal(err)
		}
	}
	w.fixed.pending[len(w.fixed.pending)-1] ^= 1
	pending := bytes.Clone(w.fixed.pending)
	before, err := os.ReadFile(w.f.Name())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); !errors.Is(err, ErrCorrupt) {
		t.Fatal("malformed later frame was not rejected", err)
	}
	after, err := os.ReadFile(w.f.Name())
	if err != nil || !bytes.Equal(before, after) || !bytes.Equal(w.fixed.pending, pending) || w.writeSyscalls.Load() != 0 || w.fixed.logical != 0 {
		t.Fatal("later corruption emitted a valid physical prefix", err)
	}
}
