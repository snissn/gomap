package valuelog

import (
	"bytes"
	"errors"
	"testing"

	"github.com/golang/snappy"
	"github.com/snissn/compress/zstd"
)

func TestNoDictFrameDecodeRejectsOversizedDeclaredOutput(t *testing.T) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()

	raw := bytes.Repeat([]byte("bounded-output"), 1<<16)
	payload := enc.EncodeAll(raw, nil)
	const admittedRawLen = 32
	_, err = decodeFramePayloadTo(FrameHeader{Flags: FrameFlagCompressed}, payload, nil, admittedRawLen, nil)
	if !errors.Is(err, zstd.ErrDecoderSizeExceeded) {
		t.Fatalf("oversized frame error=%v, want decoder size limit before output growth", err)
	}
}

func TestNoDictFrameDecodeIgnoresExcessDestinationCapacity(t *testing.T) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	raw := bytes.Repeat([]byte("bounded-output"), 1<<12)
	_, err = decodeFramePayloadTo(FrameHeader{Flags: FrameFlagCompressed}, enc.EncodeAll(raw, nil), nil, 32, make([]byte, 0, len(raw)))
	if !errors.Is(err, zstd.ErrDecoderSizeExceeded) {
		t.Fatalf("pooled destination error=%v, want decoder size limit", err)
	}
}

func TestNoDictFrameDecodeRejectsConcatenatedOutput(t *testing.T) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()

	raw := bytes.Repeat([]byte("frame"), 64)
	payload := append(enc.EncodeAll(raw, nil), enc.EncodeAll(raw, nil)...)
	_, err = decodeFramePayloadTo(FrameHeader{Flags: FrameFlagCompressed}, payload, nil, uint32(len(raw)), nil)
	if !errors.Is(err, zstd.ErrDecoderSizeExceeded) {
		t.Fatalf("concatenated frame error=%v, want decoder size limit", err)
	}
}

func TestNoDictFrameDecodeRejectsUnknownSizeOutput(t *testing.T) {
	var payload bytes.Buffer
	enc, err := zstd.NewWriter(&payload, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	raw := bytes.Repeat([]byte("streaming-frame"), 1<<17)
	if _, err := enc.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	var header zstd.Header
	if err := header.Decode(payload.Bytes()); err != nil {
		t.Fatal(err)
	}
	if header.HasFCS {
		t.Fatal("streaming test frame unexpectedly declares its decoded size")
	}

	decoded, err := decodeFramePayloadTo(FrameHeader{Flags: FrameFlagCompressed}, payload.Bytes(), nil, 32, make([]byte, 0, 32))
	if !errors.Is(err, zstd.ErrDecoderSizeExceeded) {
		t.Fatalf("unknown-size frame error=%v, want bounded-size rejection", err)
	}
	if cap(decoded) > 32 {
		t.Fatalf("unknown-size frame grew output capacity to %d beyond admitted 32 bytes", cap(decoded))
	}
}

func TestNoDictFrameDecodeBoundsUnknownSizeRawBlock(t *testing.T) {
	var payload bytes.Buffer
	enc, err := zstd.NewWriter(&payload, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, 1<<20)
	x := uint32(1)
	for i := range raw {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		raw[i] = byte(x)
	}
	if _, err := enc.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	var header zstd.Header
	if err := header.Decode(payload.Bytes()); err != nil {
		t.Fatal(err)
	}
	if header.HasFCS {
		t.Fatal("streaming raw-block test frame unexpectedly declares its decoded size")
	}
	decoded, err := decodeBlockPayload(uint8(BlockCodecZSTD), payload.Bytes(), 32, make([]byte, 0, 32))
	if !errors.Is(err, zstd.ErrDecoderSizeExceeded) {
		t.Fatalf("unknown-size raw block error=%v, want bounded-size rejection", err)
	}
	if cap(decoded) > 32 {
		t.Fatalf("unknown-size raw block grew output capacity to %d", cap(decoded))
	}
}

func TestNoDictFrameDecodeAcceptsMatchingOutput(t *testing.T) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	raw := bytes.Repeat([]byte("valid-frame"), 1<<10)
	decoded, err := decodeFramePayloadTo(FrameHeader{Flags: FrameFlagCompressed}, enc.EncodeAll(raw, nil), nil, uint32(len(raw)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, raw) {
		t.Fatal("decoded frame differs from raw input")
	}
}

func TestNoDictFrameDecodeAcceptsEncodeAllPartsOutput(t *testing.T) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()

	parts := [][]byte{
		bytes.Repeat([]byte("first-part"), 1<<10),
		bytes.Repeat([]byte("second-part"), 1<<9),
		bytes.Repeat([]byte("third-part"), 1<<8),
	}
	want := bytes.Join(parts, nil)
	decoded, err := decodeFramePayloadTo(
		FrameHeader{Flags: FrameFlagCompressed},
		enc.EncodeAllParts(parts, nil),
		nil,
		uint32(len(want)),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, want) {
		t.Fatal("decoded multipart frame differs from raw input")
	}
}

func TestSnappyBlockRejectsDeclaredLengthBeforeDecode(t *testing.T) {
	payload := snappy.Encode(nil, bytes.Repeat([]byte("oversized"), 1<<17))
	_, err := decodeBlockPayload(uint8(BlockCodecSnappy), payload, 32, nil)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("snappy decoded-length mismatch error=%v, want early corrupt-record rejection", err)
	}
}

func TestNoDictBlockDecodeUsesExpectedRawCapacity(t *testing.T) {
	raw := bytes.Repeat([]byte("bounded-block"), 1<<10)
	payload, err := encodeBlockPayload(BlockCodecZSTD, raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeBlockPayload(uint8(BlockCodecZSTD), payload, uint32(len(raw)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, raw) {
		t.Fatal("decoded block differs from raw input")
	}
}

func TestNoDictBlockDecodeIgnoresExcessDestinationCapacity(t *testing.T) {
	raw := bytes.Repeat([]byte("bounded-block"), 1<<10)
	payload, err := encodeBlockPayload(BlockCodecZSTD, raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = decodeBlockPayload(uint8(BlockCodecZSTD), payload, 32, make([]byte, 0, len(raw)))
	if !errors.Is(err, zstd.ErrDecoderSizeExceeded) {
		t.Fatalf("pooled block destination error=%v, want decoder size limit", err)
	}
}
