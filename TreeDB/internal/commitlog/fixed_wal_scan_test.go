package commitlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// Mix records smaller and larger than read-ahead, with many tiny records after
// the largest one. Returned frames must not borrow the scanner's reused bytes.
func fixedWALScanFixture(t *testing.T) (string, []byte, [][]byte) {
	t.Helper()
	if !FixedWALCreationSupported() {
		t.Skip("fixed-container publication unsupported")
	}
	path := filepath.Join(t.TempDir(), CommandSegmentName(0, 1))
	w, err := NewFixedWriterWithCapacity(path, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	var logical []byte
	var payloads [][]byte
	for i := 0; i < 513; i++ {
		size := 129 + i%17
		if i%127 == 0 {
			size = 96<<10 + i
		}
		env := fixedWALLifecycleFrame(t, size)
		env.LSN = uint64(i + 1)
		frame, e := EncodeCommandFrameV2(env)
		if e != nil {
			t.Fatal(e)
		}
		record := make([]byte, segmentHeaderSize+len(frame))
		binary.LittleEndian.PutUint32(record[:4], uint32(len(frame)))
		copy(record[segmentHeaderSize:], frame)
		fixedWALValidationChecksum(record)
		logical = append(logical, record...)
		payloads = append(payloads, bytes.Clone(env.Payload))
		if e = w.AppendCommandV2(env); e != nil {
			t.Fatal(e)
		}
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	return path, logical, payloads
}

func TestFixedWALScanAcrossBuffersAndSeeks(t *testing.T) {
	path, logical, payloads := fixedWALScanFixture(t)
	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	first, err := r.ReadCommandFrameV2()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(payloads); i++ {
		env, e := r.ReadCommandFrameV2()
		if e != nil || env.LSN != uint64(i+1) || !bytes.Equal(env.Payload, payloads[i]) {
			t.Fatalf("frame %d: lsn=%d err=%v", i, env.LSN, e)
		}
	}
	if !bytes.Equal(first.Payload, payloads[0]) {
		t.Fatal("later scanner/reader reuse changed a retained frame")
	}
	if _, err = r.ReadCommandFrameV2(); !errors.Is(err, io.EOF) {
		t.Fatalf("terminal read: %v", err)
	}
	stream := &fixedWALStream{file: r.f, state: r.fixed}
	want := bytes.NewReader(logical)
	for _, step := range []struct {
		offset int64
		whence int
		size   int
	}{
		{0, io.SeekStart, 7}, {0, io.SeekCurrent, 127},
		{64<<10 - 3, io.SeekStart, 170003}, {10, io.SeekStart, 90001},
		{-71, io.SeekEnd, 80}, {5, io.SeekEnd, 10},
		{0, io.SeekStart, len(logical) + 1},
	} {
		gotOffset, e := stream.Seek(step.offset, step.whence)
		wantOffset, we := want.Seek(step.offset, step.whence)
		if e != we || gotOffset != wantOffset {
			t.Fatalf("seek: %d/%v want %d/%v", gotOffset, e, wantOffset, we)
		}
		got, expected := make([]byte, step.size), make([]byte, step.size)
		n, e := io.ReadFull(stream, got)
		wn, we := io.ReadFull(want, expected)
		if n != wn || e != we || !bytes.Equal(got[:n], expected[:wn]) {
			t.Fatalf("read after seek %+v: %d/%v want %d/%v", step, n, e, wn, we)
		}
	}
}

func TestFixedWALScanRejectsCorruptionBeyondReadAhead(t *testing.T) {
	path, _, _ := fixedWALScanFixture(t)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := loadFixedWAL(f)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	image, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	later := state.extents[130]
	paddingOffset := later.physical + fixedWALWitnessSize + later.length
	if fixedWALAlign(paddingOffset) == paddingOffset {
		t.Fatal("fixture must include padding at the selected later extent")
	}
	for _, tc := range []struct {
		name string
		off  int64
	}{
		{"start", later.physical},
		{"payload", later.physical + fixedWALWitnessSize + 73},
		{"padding", paddingOffset},
		{"far_reserve", state.physical + 2*fixedWALReadBufferSize + 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			variant := bytes.Clone(image)
			variant[tc.off] ^= 0x71
			target := filepath.Join(t.TempDir(), "corrupt.log")
			if e := os.WriteFile(target, variant, 0600); e != nil {
				t.Fatal(e)
			}
			r, e := NewReader(target)
			if r != nil {
				_ = r.Close()
			}
			if !errors.Is(e, ErrCorrupt) {
				t.Fatalf("corruption at %d accepted: %v", tc.off, e)
			}
		})
	}
}

func TestFixedWALStreamPartialPhysicalReadKeepsEOF(t *testing.T) {
	path, logical, _ := fixedWALScanFixture(t)
	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// Retain the validated state, then lose part of its first physical extent.
	// The low-level stream historically returns a partial count with io.EOF.
	const remaining = 73
	if err = os.Truncate(path, r.fixed.extents[0].physical+fixedWALWitnessSize+remaining); err != nil {
		t.Fatal(err)
	}
	stream := &fixedWALStream{file: r.f, state: r.fixed}
	got := make([]byte, fixedWALReadBufferSize)
	n, err := stream.Read(got)
	if n != remaining || err != io.EOF || !bytes.Equal(got[:n], logical[:remaining]) {
		t.Fatalf("partial physical read: n=%d err=%v", n, err)
	}
}
