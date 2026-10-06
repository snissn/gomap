package valuelog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/page"
)

var cowTestLimits = COWReadLimits{MaxRecordBytes: 1 << 20, MaxRawBytes: 1 << 20, MaxValueBytes: 1 << 20}

func cowTestRaceEnabled() bool {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return false
	}
	for _, setting := range info.Settings {
		if setting.Key == "-race" && setting.Value == "true" {
			return true
		}
	}
	return false
}

func cowTestFrame(dictID uint64, codec BlockCodec, values [][]byte, compressed []byte) []byte {
	k := len(values)
	p := FrameHeaderSize + k*8 + (k+1)*4
	n := 0
	for _, v := range values {
		n += len(v)
	}
	var payload []byte
	if compressed != nil {
		payload = compressed
	} else {
		payload = make([]byte, n)
		at := 0
		for _, v := range values {
			at += copy(payload[at:], v)
		}
	}
	body := make([]byte, p+len(payload))
	body[0], body[2] = FrameVersion, byte(k)
	if compressed != nil {
		body[1], body[3] = FrameFlagCompressed, byte(codec)
	}
	binary.LittleEndian.PutUint64(body[4:12], dictID)
	pos := 0
	for i, v := range values {
		binary.LittleEndian.PutUint64(body[FrameHeaderSize+i*8:], uint64(i+1))
		binary.LittleEndian.PutUint32(body[FrameHeaderSize+k*8+i*4:], uint32(pos))
		pos += len(v)
	}
	binary.LittleEndian.PutUint32(body[p-4:], uint32(pos))
	copy(body[p:], payload)
	return body
}

func cowTestRecord(t *testing.T, body []byte, grouped bool, sub uint8) (*os.File, page.ValuePtr, []byte) {
	t.Helper()
	record := make([]byte, HeaderSize+len(body))
	record[4] = Version
	if grouped {
		record[5] = recordFlagGrouped
	} else {
		binary.LittleEndian.PutUint64(record[8:16], 1)
	}
	binary.LittleEndian.PutUint32(record[16:20], uint32(len(body)))
	copy(record[HeaderSize:], body)
	binary.LittleEndian.PutUint32(record[:4], crc.ChecksumParts(record[4:HeaderSize], body))
	f, err := os.OpenFile(filepath.Join(t.TempDir(), "record.log"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if _, err := f.WriteAt(record, 0); err != nil {
		t.Fatal(err)
	}
	length := uint32(headerWithoutCRC + len(body))
	if grouped {
		length = page.ValuePtrMarkGrouped(length, sub)
	}
	return f, page.ValuePtr{Offset: 4, Length: length, FileID: page.ValueLogFileID(1)}, record
}

func cowTestRead(t *testing.T, f *os.File, ptr page.ValuePtr, shape COWRecordShape, decode COWDecodeFunc) []byte {
	t.Helper()
	out, err := ReadCOWRecord(f, ptr, shape, true, make([]byte, shape.PayloadBytes()), make([]byte, shape.RawBytes), make([]byte, shape.ValueBytes), decode)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestCOWRecordRawAndGroupedNoAllocation(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		t.Run(map[bool]string{false: "raw", true: "grouped"}[grouped], func(t *testing.T) {
			want := []byte("second")
			body := want
			var sub uint8
			if grouped {
				body = cowTestFrame(73, BlockCodecNone, [][]byte{[]byte("first"), want, {}}, nil)
				sub = 1
			}
			f, ptr, _ := cowTestRecord(t, body, grouped, sub)
			shape, err := InspectCOWRecord(f, ptr, cowTestLimits)
			if err != nil {
				t.Fatal(err)
			}
			if shape.ValueBytes != uint64(len(want)) || shape.Compressed {
				t.Fatalf("shape=%+v", shape)
			}
			if !bytes.Equal(cowTestRead(t, f, ptr, shape, nil), want) {
				t.Fatal("value mismatch")
			}
			payload, raw, out := make([]byte, shape.PayloadBytes()), make([]byte, shape.RawBytes), make([]byte, shape.ValueBytes)
			// Race instrumentation makes os.ReadAt's stack input buffers escape;
			// normal builds verify the preflight's zero-allocation contract.
			raceEnabled := cowTestRaceEnabled()
			if n := testing.AllocsPerRun(100, func() {
				if _, err := InspectCOWRecord(f, ptr, cowTestLimits); err != nil {
					panic(err)
				}
			}); n != 0 && !raceEnabled {
				t.Fatalf("Inspect allocs=%g", n)
			}
			if n := testing.AllocsPerRun(100, func() {
				if _, err := ReadCOWRecord(f, ptr, shape, true, payload, raw, out, nil); err != nil {
					panic(err)
				}
			}); n != 0 {
				t.Fatalf("Read allocs=%g", n)
			}
			// Grouped length hints may be omitted; metadata still provides bounds.
			if grouped {
				ptr.Length = page.ValuePtrMarkGrouped(0, sub)
				s, err := InspectCOWRecord(f, ptr, cowTestLimits)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(cowTestRead(t, f, ptr, s, nil), want) {
					t.Fatal("omitted hint")
				}
				ptr.Length = page.ValuePtrMarkGrouped(0, 2)
				empty, err := InspectCOWRecord(f, ptr, cowTestLimits)
				if err != nil || empty.ValueBytes != 0 {
					t.Fatalf("empty shape=%+v err=%v", empty, err)
				}
				if got := cowTestRead(t, f, ptr, empty, nil); len(got) != 0 {
					t.Fatal("empty grouped value")
				}
			}
		})
	}
}

func TestCOWRecordRefusesLimitsMalformedAndEncodings(t *testing.T) {
	f, ptr, _ := cowTestRecord(t, []byte("value"), false, 0)
	for _, lim := range []COWReadLimits{{}, {HeaderSize + 4, 1 << 20, 1 << 20}, {1 << 20, 4, 1 << 20}, {1 << 20, 1 << 20, 4}} {
		if _, err := InspectCOWRecord(f, ptr, lim); !errors.Is(err, ErrCOWReadCapacity) {
			t.Fatalf("limits=%+v err=%v", lim, err)
		}
	}
	for _, value := range [][]byte{{'T', 'M', 1, 1, 1, 1}, append(compactLeafPagePayloadMagic[:], 0, 0, 0, 0)} {
		f, p, _ := cowTestRecord(t, value, false, 0)
		if _, err := InspectCOWRecord(f, p, cowTestLimits); !errors.Is(err, ErrCOWReadUnsupported) {
			t.Fatalf("encoded=%x err=%v", value, err)
		}
	}
	leafID, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatal(err)
	}
	ptr.FileID = leafID
	if _, err := InspectCOWRecord(f, ptr, cowTestLimits); !errors.Is(err, ErrCOWReadUnsupported) {
		t.Fatal(err)
	}
	for _, mutate := range []func([]byte){
		func(b []byte) { b[0] = 99 }, func(b []byte) { b[1] = 2 }, func(b []byte) { b[2] = 0 },
		func(b []byte) { b[FrameHeaderSize] = 0 },
		func(b []byte) { binary.LittleEndian.PutUint32(b[FrameHeaderSize+16:], 1) },
		func(b []byte) { binary.LittleEndian.PutUint32(b[FrameHeaderSize+20:], 20) },
		func(b []byte) { b[3] = 99 },
	} {
		body := cowTestFrame(0, 0, [][]byte{[]byte("first"), []byte("second")}, nil)
		mutate(body)
		f, p, _ := cowTestRecord(t, body, true, 1)
		if _, err := InspectCOWRecord(f, p, cowTestLimits); err == nil {
			t.Fatalf("accepted malformed=%x", body)
		}
	}
}

func TestCOWRecordRevalidatesBeforeDecode(t *testing.T) {
	body := cowTestFrame(55, 0, [][]byte{[]byte("value")}, []byte("encoded"))
	f, ptr, record := cowTestRecord(t, body, true, 0)
	shape, err := InspectCOWRecord(f, ptr, cowTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	payload, raw, out := make([]byte, shape.PayloadBytes()), make([]byte, shape.RawBytes), make([]byte, shape.ValueBytes)
	calls := 0
	decode := func(frame FrameHeader, p, dst []byte) ([]byte, error) { calls++; copy(dst, "value"); return dst, nil }
	if shape.PayloadBytes() != shape.RecordBytes {
		t.Fatal("input admission omitted record header")
	}
	if _, err := ReadCOWRecord(f, ptr, shape, true, payload[:0:len(payload)-1], raw, out, decode); !errors.Is(err, ErrCOWReadCapacity) {
		t.Fatal(err)
	}
	other := ptr
	other.Offset++
	if _, err := ReadCOWRecord(f, other, shape, true, payload, raw, out, decode); !errors.Is(err, ErrCOWReadCapacity) {
		t.Fatal(err)
	}
	if _, err := ReadCOWRecord(f, ptr, shape, true, payload, raw[:0:0], out, decode); !errors.Is(err, ErrCOWReadCapacity) {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("decoded before capacity admission")
	}
	changed := append([]byte(nil), record...)
	changed[HeaderSize+4]++ // encoded dictionary identity
	if _, err := f.WriteAt(changed, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCOWRecord(f, ptr, shape, false, payload, raw, out, decode); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("decoded changed metadata")
	}
	if _, err := f.WriteAt(record, 0); err != nil {
		t.Fatal(err)
	}
	changed = append([]byte(nil), record...)
	changed[len(changed)-1]++
	if _, err := f.WriteAt(changed, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCOWRecord(f, ptr, shape, true, payload, raw, out, decode); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("decoded corrupt CRC")
	}
	if _, err := f.WriteAt(record, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCOWRecord(f, ptr, shape, true, payload, raw, out, nil); !errors.Is(err, ErrMissingDict) {
		t.Fatal(err)
	}
	bad := func(frame FrameHeader, p, dst []byte) ([]byte, error) {
		return bytes.Repeat([]byte("x"), len(dst)), nil
	}
	if _, err := ReadCOWRecord(f, ptr, shape, true, payload, raw, out, bad); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if got, err := ReadCOWRecord(f, ptr, shape, true, payload, raw, out, decode); err != nil || string(got) != "value" {
		t.Fatalf("got=%q err=%v", got, err)
	}
}

func TestCOWDecoderBlockCodecsAndCompressedTemplateRefusal(t *testing.T) {
	d, err := NewCOWDecoder(0, nil, 1<<20, 1<<20, 1<<20, func(COWDecoderAllocationSizes) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for _, codec := range []BlockCodec{BlockCodecSnappy, BlockCodecLZ4, BlockCodecZSTD} {
		t.Run(map[BlockCodec]string{BlockCodecSnappy: "snappy", BlockCodecLZ4: "lz4", BlockCodecZSTD: "zstd"}[codec], func(t *testing.T) {
			want := bytes.Repeat([]byte("bounded-codec-value"), 1024)
			encoded, err := encodeBlockPayload(codec, want, nil)
			if err != nil {
				t.Fatal(err)
			}
			f, ptr, _ := cowTestRecord(t, cowTestFrame(0, codec, [][]byte{want}, encoded), true, 0)
			shape, err := InspectCOWRecord(f, ptr, cowTestLimits)
			if err != nil {
				t.Fatal(err)
			}
			if !shape.Compressed || shape.Codec != codec {
				t.Fatalf("shape=%+v", shape)
			}
			if !bytes.Equal(cowTestRead(t, f, ptr, shape, d.Decode), want) {
				t.Fatal("decoded mismatch")
			}
			payload, raw, out := make([]byte, shape.PayloadBytes()), make([]byte, shape.RawBytes), make([]byte, shape.ValueBytes)
			callback := d.Decode
			if n := testing.AllocsPerRun(100, func() {
				if _, err := ReadCOWRecord(f, ptr, shape, true, payload, raw, out, callback); err != nil {
					panic(err)
				}
			}); n != 0 {
				t.Fatalf("warm read allocs=%g", n)
			}
		})
	}
	value := []byte{'T', 'M', 1, 1, 1, 1}
	encoded, err := encodeBlockPayload(BlockCodecSnappy, value, nil)
	if err != nil {
		t.Fatal(err)
	}
	f, p, _ := cowTestRecord(t, cowTestFrame(0, BlockCodecSnappy, [][]byte{value}, encoded), true, 0)
	s, err := InspectCOWRecord(f, p, cowTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCOWRecord(f, p, s, true, make([]byte, s.PayloadBytes()), make([]byte, s.RawBytes), make([]byte, s.ValueBytes), d.Decode); !errors.Is(err, ErrCOWReadUnsupported) {
		t.Fatal(err)
	}
}

func TestCOWDecoderProvenanceAdmissionAndClose(t *testing.T) {
	denied := errors.New("denied")
	invalid := []byte("not a dictionary")
	digest := sha256.Sum256(invalid)
	id := binary.BigEndian.Uint64(digest[:8])
	called := false
	if _, err := NewCOWDecoder(id, invalid, 1<<20, 1<<20, 1<<20, func(s COWDecoderAllocationSizes) error {
		called = true
		if s.DefinitionCopy != uint64(len(invalid)) || s.FixedAllocationCount != 64 || s.RawBacking != (2<<20)+(512<<10) || s.InputBacking != 1<<20 {
			t.Fatalf("sizes=%+v", s)
		}
		return denied
	}); !errors.Is(err, denied) || !called {
		t.Fatalf("called=%v err=%v", called, err)
	}
	if _, err := NewCOWDecoder(id, nil, 1<<20, 1<<20, 1<<20, func(COWDecoderAllocationSizes) error { t.Fatal("admitted missing definition"); return nil }); !errors.Is(err, ErrMissingDict) {
		t.Fatal(err)
	}
	if _, err := NewCOWDecoder(id+1, invalid, 1<<20, 1<<20, 1<<20, func(COWDecoderAllocationSizes) error { t.Fatal("admitted corrupt definition"); return nil }); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, err := NewCOWDecoder(0, nil, 1<<20, 1<<20, 1<<20, nil); !errors.Is(err, ErrCOWReadCapacity) {
		t.Fatal(err)
	}
	for _, caps := range [][2]uint64{{0, 1024}, {1, 0}, {1, 1023}, {1 << 32, 1024}} {
		if _, err := COWDecoderRetentionSizes(0, caps[0], 1024, caps[1]); !errors.Is(err, ErrCOWReadCapacity) {
			t.Fatal(err)
		}
	}
	if _, err := COWDecoderRetentionSizes(0, 1024, 0, 1024); !errors.Is(err, ErrCOWReadCapacity) {
		t.Fatal(err)
	}
	d, err := NewCOWDecoder(0, nil, 1024, 1024, 1024, func(COWDecoderAllocationSizes) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Decode(FrameHeader{DictID: 1}, nil, nil); !errors.Is(err, ErrMissingDict) {
		t.Fatal(err)
	}
	if _, err := d.Decode(FrameHeader{}, make([]byte, 1025), nil); !errors.Is(err, ErrCOWReadCapacity) {
		t.Fatal(err)
	}
	d.Close()
	d.Close()
	if _, err := d.Decode(FrameHeader{}, nil, nil); !errors.Is(err, ErrCOWDecoderClosed) {
		t.Fatal(err)
	}
	if d.decoder != nil {
		t.Fatal("Close retained decoder")
	}
}

func TestCOWDecoderHotGlobalCacheCannotSupplyDefinition(t *testing.T) {
	samples := make([][]byte, 16)
	for i := range samples {
		samples[i] = bytes.Repeat([]byte("dictionary-owned-value-with-varied-content"), 256)
		samples[i][0] = byte(i)
	}
	def, err := buildBenchDict(317, samples)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(def)
	id := binary.BigEndian.Uint64(digest[:8])
	warm := getDictCodecs(id, def)
	if warm == nil {
		t.Fatal("cache setup")
	}
	if getDictCodecs(id, nil) == nil {
		t.Fatal("cache is not hot")
	}
	if _, err := NewCOWDecoder(id, nil, 1<<20, 1<<20, 1<<20, func(COWDecoderAllocationSizes) error { t.Fatal("missing definition admitted"); return nil }); !errors.Is(err, ErrMissingDict) {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		d, err := NewCOWDecoder(id, def, 1<<20, 1<<20, 1<<20, func(COWDecoderAllocationSizes) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		want := samples[i]
		body, header, err := EncodeFrame(id, def, []Record{{RID: 1, Value: want}})
		if err != nil {
			t.Fatal(err)
		}
		if header.Flags&FrameFlagCompressed == 0 {
			t.Fatal("dictionary fixture did not compress")
		}
		f, p, _ := cowTestRecord(t, body, true, 0)
		s, err := InspectCOWRecord(f, p, cowTestLimits)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(cowTestRead(t, f, p, s, d.Decode), want) {
			t.Fatal("dictionary decode")
		}
		frame := s.frame
		frame.DictID++
		if _, err := d.Decode(frame, nil, make([]byte, s.RawBytes)); !errors.Is(err, ErrMissingDict) {
			t.Fatal(err)
		}
		d.Close()
	}
}

func TestCOWDecoderConcurrentReadsDrainBeforeClose(t *testing.T) {
	want := bytes.Repeat([]byte("concurrent"), 100)
	encoded, err := encodeBlockPayload(BlockCodecZSTD, want, nil)
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewCOWDecoder(0, nil, 1024, 1024, 1024, func(COWDecoderAllocationSizes) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	frame := FrameHeader{Version: FrameVersion, Flags: FrameFlagCompressed, K: 1, Reserved: byte(BlockCodecZSTD)}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dst := make([]byte, len(want))
			for j := 0; j < 20; j++ {
				out, err := d.Decode(frame, encoded, dst)
				if err != nil || !bytes.Equal(out, want) {
					t.Errorf("decode=%v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	d.Close()
}

func cowTestLeafRecord(t *testing.T, value []byte, codec BlockCodec) (*os.File, page.ValuePtr) {
	t.Helper()
	var encoded []byte
	if codec != BlockCodecNone {
		var err error
		encoded, err = encodeBlockPayload(codec, value, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	_, ptr, record := cowTestRecord(t, cowTestFrame(0, codec, [][]byte{value}, encoded), true, 0)
	path := compactLeafPayloadTestPath(t, t.TempDir(), 1)
	if err := os.WriteFile(path, record, 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	ptr.FileID, err = EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatal(err)
	}
	return f, ptr
}

func TestCOWLeafRecordBoundedRawCompactAndCompressed(t *testing.T) {
	pageBytes := buildSparseLeafPageForPayloadTest(t)
	compact, ok, err := MaybeCompactLeafLogPayload(pageBytes)
	if err != nil || !ok {
		t.Fatalf("compact=%v err=%v", ok, err)
	}
	d, err := NewCOWDecoder(0, nil, MaxFrameK*page.PageSize, 1<<20, 1<<20, func(COWDecoderAllocationSizes) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for _, tc := range []struct {
		name  string
		value []byte
		codec BlockCodec
	}{
		{"raw", pageBytes, BlockCodecNone}, {"compact", compact, BlockCodecNone}, {"compressed-compact", compact, BlockCodecSnappy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, ptr := cowTestLeafRecord(t, tc.value, tc.codec)
			if _, err := InspectCOWRecord(f, ptr, cowTestLimits); !errors.Is(err, ErrCOWReadUnsupported) {
				t.Fatal(err)
			}
			s, err := InspectCOWLeafRecord(f, ptr, cowTestLimits)
			if err != nil {
				t.Fatal(err)
			}
			payload, raw, out := make([]byte, s.PayloadBytes()), make([]byte, s.RawBytes), make([]byte, page.PageSize)
			callback := d.Decode
			if _, err := ReadCOWLeafPage(f, ptr, s, true, payload, raw, out[:0:0], callback); !errors.Is(err, ErrCOWReadCapacity) {
				t.Fatal(err)
			}
			got, err := ReadCOWLeafPage(f, ptr, s, true, payload, raw, out, callback)
			if err != nil {
				t.Fatal(err)
			}
			requireLeafPagesLogicallyEqual(t, pageBytes, got)
			if n := testing.AllocsPerRun(100, func() {
				if _, err := ReadCOWLeafPage(f, ptr, s, true, payload, raw, out, callback); err != nil {
					panic(err)
				}
			}); n != 0 {
				t.Fatalf("leaf read allocs=%g", n)
			}
		})
	}
	for _, value := range [][]byte{bytes.Repeat([]byte("x"), page.PageSize+1), []byte("short leaf")} {
		f, p := cowTestLeafRecord(t, value, BlockCodecNone)
		_, err := InspectCOWLeafRecord(f, p, cowTestLimits)
		if len(value) > page.PageSize {
			if !errors.Is(err, ErrCOWReadCapacity) {
				t.Fatal(err)
			}
		} else {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
		}
	}
	bad := append([]byte(nil), compact...)
	binary.LittleEndian.PutUint16(bad[len(compactLeafPagePayloadMagic):], page.PageSize)
	f, p := cowTestLeafRecord(t, bad, BlockCodecNone)
	if _, err := InspectCOWLeafRecord(f, p, cowTestLimits); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}
