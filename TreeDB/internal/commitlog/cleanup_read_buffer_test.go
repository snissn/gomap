package commitlog

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCleanupReadBufferPreservesStreamValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{{
		Op: RawKVOpSet, Key: []byte("key"), Value: bytes.Repeat([]byte("value"), 500),
	}})
	if err != nil {
		t.Fatal(err)
	}
	largePayload, err := EncodeRawKVBatchPayload([]RawKVOperation{{
		Op: RawKVOpSet, Key: []byte("large"), Value: bytes.Repeat([]byte("value"), cleanupReadWindowBytes),
	}})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	const frameCount = 80
	for lsn := uint64(1); lsn <= frameCount; lsn++ {
		frame := payload
		if lsn == frameCount {
			frame = largePayload
		}
		if err := writer.AppendCommand(CommandEnvelope{
			LSN: lsn, Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV,
			PayloadFormat: PayloadFormatRawKVBatchV1, Payload: frame,
		}); err != nil {
			_ = writer.Close()
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(original) <= 2*(64<<10) {
		t.Fatalf("fixture must cross multiple read windows, got %d bytes", len(original))
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	readFrames := func(reader *Reader) ([]int64, error) {
		t.Helper()
		defer reader.Close()
		var offsets []int64
		for {
			lsn, readErr := reader.ReadValidatedCommandFrameLSN()
			offset, err := reader.Offset()
			if err != nil {
				t.Fatal(err)
			}
			if readErr != nil {
				return append(offsets, offset), readErr
			}
			if lsn != uint64(len(offsets)+1) {
				t.Fatalf("read command LSN %d at position %d", lsn, len(offsets))
			}
			offsets = append(offsets, offset)
		}
	}
	read := func(buffer *CleanupReadBuffer) ([]int64, error) {
		t.Helper()
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		reader, err := buffer.Open(file, Options{})
		if err != nil {
			t.Fatal(err)
		}
		return readFrames(reader)
	}
	var buffer CleanupReadBuffer
	var fileIO sync.Mutex
	var released atomic.Bool
	detached := NewDetachedStreamCleanupReadBuffer(&fileIO, &released)
	plain, err := read(nil)
	if len(plain) != frameCount+1 || !errors.Is(err, io.EOF) || plain[frameCount-1] != int64(len(original)) {
		t.Fatalf("original valid fixture offsets=%v err=%v", plain, err)
	}
	firstByteOfLastFrame := plain[frameCount-2]
	for _, tc := range []struct {
		name    string
		end     int64
		corrupt bool
		want    error
	}{
		{"valid", int64(len(original)), false, io.EOF},
		{"same_inode_corruption", int64(len(original)), true, ErrCorrupt},
		{"short_payload", int64(len(original) - 1), false, ErrCommandWALTerminalTail},
		{"short_header", firstByteOfLastFrame + 3, false, ErrCommandWALTerminalTail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := file.WriteAt(original[:tc.end], 0); err != nil {
				t.Fatal(err)
			}
			if err := file.Truncate(tc.end); err != nil {
				t.Fatal(err)
			}
			if tc.corrupt {
				if _, err := file.WriteAt([]byte{original[tc.end-1] ^ 1}, tc.end-1); err != nil {
					t.Fatal(err)
				}
			}
			wantOffsets, wantErr := read(nil)
			gotOffsets, gotErr := read(&buffer)
			if !errors.Is(wantErr, tc.want) || !errors.Is(gotErr, tc.want) || !slices.Equal(gotOffsets, wantOffsets) {
				t.Fatalf("offsets and result: buffered %v %v, ordinary %v %v, want %v", gotOffsets, gotErr, wantOffsets, wantErr, tc.want)
			}
			detachedOffsets, detachedErr := read(detached)
			if !errors.Is(detachedErr, tc.want) || !slices.Equal(detachedOffsets, wantOffsets) {
				t.Fatalf("offsets and result: detachable %v %v, ordinary %v %v", detachedOffsets, detachedErr, wantOffsets, wantErr)
			}
			copyBytes, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			copiedOffsets, copiedErr := readFrames(NewReaderFromCapturedStream(copyBytes, Options{}))
			if !errors.Is(copiedErr, tc.want) || !slices.Equal(copiedOffsets, wantOffsets) {
				t.Fatalf("offsets and result: captured %v %v, ordinary %v %v", copiedOffsets, copiedErr, wantOffsets, wantErr)
			}

		})
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := detached.Open(file, Options{}); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 3*cleanupReadWindowBytes)
	if n, err := detached.Read(buf); n != cleanupReadWindowBytes || err != nil {
		t.Fatalf("direct read beyond the window: n=%d err=%v", n, err)
	}
	released.Store(true)
	if n, err := detached.Read(buf); n != 0 || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("released reader started another read: n=%d err=%v", n, err)
	}
}

func TestCleanupReadBufferLeavesFixedValidationIntact(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed WAL creation unavailable")
	}
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	writer, err := NewWriterWithOptions(path, Options{fixedCapacity: 8192})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var buffer CleanupReadBuffer
	reader, err := buffer.Open(file, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if !reader.IsEmptyFixedContainer() || buffer.input != nil {
		t.Fatal("fixed reserve was not validated by the original reader")
	}
}
