package valuelog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func continuationWork() *iterator.OrdinalScanWork {
	return &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 8192}
}
func TestCOWBlockContinuationOriginalCodecDifferential(t *testing.T) {
	for _, codec := range []BlockCodec{BlockCodecSnappy, BlockCodecLZ4} {
		for _, n := range []int{1, 31, 4096, 131071, MaxFrameK * page.PageSize} {
			t.Run(fmt.Sprintf("%d/%d", codec, n), func(t *testing.T) {
				want := make([]byte, n)
				for i := range want {
					want[i] = byte(i % 113)
				}
				encoded, err := encodeBlockPayload(codec, want, nil)
				if err != nil {
					t.Fatal(err)
				}
				got := make([]byte, n)
				var d COWBlockContinuation
				ok, err := d.Begin(codec, encoded, got, continuationWork())
				if !ok || err != nil {
					t.Fatal(ok, err)
				}
				done := false
				for turns := 0; turns < 100000 && !done; turns++ {
					in, out, phase := d.in, d.out, d.phase
					if yes, err := d.Step(&iterator.OrdinalScanWork{}); yes || err != nil || d.in != in || d.out != out || d.phase != phase {
						t.Fatal("zero budget effect")
					}
					w := continuationWork()
					done, err = d.Step(w)
					if err != nil {
						t.Fatal(err)
					}
					if w.Records > 1 || w.Bytes > 8192 {
						t.Fatal("quantum exceeded")
					}
				}
				if !done || !bytes.Equal(got, want) {
					t.Fatal("stalled/differential")
				}
				if !d.Cancel(continuationWork()) || d.input != nil || d.output != nil {
					t.Fatal("cancel aliases")
				}
			})
		}
	}
	// Literal-only LZ4 tail with a nonzero match nibble is malformed upstream.
	var d COWBlockContinuation
	if ok, err := d.Begin(BlockCodecLZ4, []byte{0x11, 'a'}, make([]byte, 1), continuationWork()); !ok || err != nil {
		t.Fatal(err)
	}
	var err error
	for i := 0; i < 10 && err == nil; i++ {
		_, err = d.Step(continuationWork())
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	before := d.out
	if _, err = d.Step(continuationWork()); !errors.Is(err, ErrCorrupt) || d.out != before {
		t.Fatal("failed decoder resumed")
	}
}
func continuationProducerFrame(t *testing.T, codec BlockCodec, compact bool, k int) (*os.File, []page.ValuePtr, []byte) {
	t.Helper()
	image := buildSparseLeafPageForPayloadTest(t)
	value := image
	if compact {
		var ok bool
		var err error
		value, ok, err = MaybeCompactLeafLogPayload(image)
		if !ok || err != nil {
			t.Fatal(ok, err)
		}
	}
	records := make([]Record, k)
	for i := range records {
		records[i] = Record{RID: uint64(i + 1), Value: value}
	}
	path := compactLeafPayloadTestPath(t, t.TempDir(), 1)
	id, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewWriter(path, id)
	if err != nil {
		t.Fatal(err)
	}
	writer.SetBlockCompression(codec, codec != BlockCodecNone)
	ptrs, stats, err := writer.AppendFrameWithStats(0, nil, records)
	if err != nil {
		t.Fatal(err)
	}
	if codec != BlockCodecNone && !stats.Kept {
		t.Fatal("producer did not compress")
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f, ptrs, image
}
func TestCOWLeafContinuationRealProducerFrames(t *testing.T) {
	limits := COWReadLimits{MaxRecordBytes: 2 << 20, MaxRawBytes: MaxFrameK * page.PageSize, MaxValueBytes: page.PageSize}
	for _, codec := range []BlockCodec{BlockCodecNone, BlockCodecSnappy, BlockCodecLZ4} {
		for _, compact := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/%v", codec, compact), func(t *testing.T) {
				f, ptrs, image := continuationProducerFrame(t, codec, compact, MaxFrameK)
				for _, index := range []int{0, 127, 254} {
					ptr := ptrs[index]
					shape, err := InspectCOWLeafRecord(f, ptr, limits)
					if err != nil {
						t.Fatal(err)
					}
					input, raw, dst := make([]byte, limits.MaxRecordBytes), make([]byte, limits.MaxRawBytes), make([]byte, page.PageSize)
					var c COWLeafContinuation
					ok, err := c.Begin(f, ptr, limits, input, raw, dst, true, continuationWork())
					if !ok || err != nil {
						t.Fatal(ok, err)
					}
					done := false
					var got []byte
					for turns := 0; turns < 200000 && !done; turns++ {
						readAt, crcAt, copyAt, rid, offset, phase := c.readAt, c.crcAt, c.copyAt, c.record.Directory.NextRID, c.record.Directory.NextOffset, c.phase
						if _, yes, err := c.Step(&iterator.OrdinalScanWork{}); yes || err != nil || c.readAt != readAt || c.crcAt != crcAt || c.copyAt != copyAt || c.record.Directory.NextRID != rid || c.record.Directory.NextOffset != offset || c.phase != phase {
							t.Fatal("zero budget effect")
						}
						w := continuationWork()
						got, done, err = c.Step(w)
						if err != nil {
							t.Fatal(err)
						}
						if w.Records > 1 || w.Bytes > 8192 {
							t.Fatal("quantum exceeded")
						}
					}
					if !done {
						t.Fatal("stalled")
					}
					if c.shape != shape {
						t.Fatalf("original shape differs: %+v / %+v", c.shape, shape)
					}
					requireLeafPagesLogicallyEqual(t, image, got)
					cancelled := false
					for i := 0; i < 3 && !cancelled; i++ {
						cancelled, err = c.Cancel(continuationWork())
						if err != nil {
							t.Fatal(err)
						}
					}
					if !cancelled || c.f != nil || c.input != nil || c.raw != nil || c.dst != nil {
						t.Fatal("cancel aliases")
					}
					if _, done, err = c.Step(continuationWork()); done || !errors.Is(err, ErrCOWDecoderClosed) {
						t.Fatal("cancelled consumer revived")
					}
					if _, err = f.Stat(); err != nil {
						t.Fatal("borrowed file was closed")
					}
				}
			})
		}
	}
}
func TestCOWLeafContinuationCancelFailureAndUnsupported(t *testing.T) {
	limits := COWReadLimits{MaxRecordBytes: 2 << 20, MaxRawBytes: MaxFrameK * page.PageSize, MaxValueBytes: page.PageSize}
	for _, codec := range []BlockCodec{BlockCodecSnappy, BlockCodecZSTD} {
		f, ptrs, _ := continuationProducerFrame(t, codec, false, 2)
		var c COWLeafContinuation
		input, raw, dst := make([]byte, limits.MaxRecordBytes), make([]byte, limits.MaxRawBytes), make([]byte, page.PageSize)
		ok, err := c.Begin(f, ptrs[0], limits, input, raw, dst, true, continuationWork())
		if !ok || err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 500; i++ {
			_, done, err := c.Step(continuationWork())
			if codec == BlockCodecZSTD && err != nil {
				if !errors.Is(err, ErrCOWReadUnsupported) || c.readAt != 0 || c.decoder.active {
					t.Fatal("unsupported payload effects", err)
				}
				_, _, again := c.Step(continuationWork())
				if again != err {
					t.Fatal("failure not sticky")
				}
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if codec == BlockCodecSnappy && c.decoder.out > 0 {
				break
			}
			if done {
				t.Fatal("fixture did not pause")
			}
		}
		cancelled := false
		for i := 0; i < 3 && !cancelled; i++ {
			cancelled, err = c.Cancel(continuationWork())
			if err != nil {
				t.Fatal(err)
			}
		}
		if !cancelled || c.f != nil || c.decoder.input != nil {
			t.Fatal("paused cancel stalled/retained")
		}
	}
}

func TestCOWLeafContinuationCorruptionCapacityAndCustody(t *testing.T) {
	limits := COWReadLimits{MaxRecordBytes: 2 << 20, MaxRawBytes: MaxFrameK * page.PageSize, MaxValueBytes: page.PageSize}
	for _, truncate := range []bool{false, true} {
		f, ptrs, _ := continuationProducerFrame(t, BlockCodecSnappy, false, 4)
		rw, err := os.OpenFile(f.Name(), os.O_RDWR, 0600)
		if err != nil {
			t.Fatal(err)
		}
		stat, err := rw.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if truncate {
			err = rw.Truncate(stat.Size() - 1)
		} else {
			var b [1]byte
			_, err = rw.ReadAt(b[:], stat.Size()-1)
			b[0] ^= 0xff
			if err == nil {
				_, err = rw.WriteAt(b[:], stat.Size()-1)
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = rw.Close(); err != nil {
			t.Fatal(err)
		}
		var c COWLeafContinuation
		input, raw, dst := make([]byte, limits.MaxRecordBytes), make([]byte, limits.MaxRawBytes), make([]byte, page.PageSize)
		ok, err := c.Begin(f, ptrs[0], limits, input, raw, dst, true, continuationWork())
		if !ok || err != nil {
			t.Fatal(ok, err)
		}
		var got []byte
		var done bool
		for turns := 0; turns < 10000 && err == nil; turns++ {
			got, done, err = c.Step(continuationWork())
			if done {
				t.Fatal("corrupt record exported")
			}
		}
		if err == nil || got != nil || done {
			t.Fatal("failure absent/exported")
		}
		readAt, crcAt := c.readAt, c.crcAt
		_, _, again := c.Step(continuationWork())
		if again != err || c.readAt != readAt || c.crcAt != crcAt {
			t.Fatal("failure replay")
		}
		for i := 0; i < 3; i++ {
			yes, e := c.Cancel(continuationWork())
			if e != nil {
				t.Fatal(e)
			}
			if yes {
				break
			}
		}
		if c.f != nil || c.input != nil {
			t.Fatal("failed custody retained")
		}
	}
	f, ptrs, _ := continuationProducerFrame(t, BlockCodecNone, false, 1)
	var c COWLeafContinuation
	shared := make([]byte, limits.MaxRecordBytes)
	if ok, err := c.Begin(f, ptrs[0], limits, shared, shared[:limits.MaxRawBytes], make([]byte, page.PageSize), true, continuationWork()); ok || !errors.Is(err, ErrCOWReadCapacity) || c.f != nil {
		t.Fatal("overlapping backing captured", err)
	}
	// Actual immutable file remains caller-owned through many independent cursors.
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var c COWLeafContinuation
			ok, err := c.Begin(f, ptrs[0], limits, make([]byte, limits.MaxRecordBytes), make([]byte, limits.MaxRawBytes), make([]byte, page.PageSize), true, continuationWork())
			if !ok || err != nil {
				t.Error(err)
				return
			}
			done := false
			for turns := 0; turns < 10000 && !done; turns++ {
				_, done, err = c.Step(continuationWork())
				if err != nil {
					t.Error(err)
					return
				}
			}
			if !done {
				t.Error("concurrent read stalled")
			}
			for turns := 0; turns < 3; turns++ {
				yes, err := c.Cancel(continuationWork())
				if err != nil {
					t.Error(err)
				}
				if yes {
					return
				}
			}
			t.Error("concurrent cancel stalled")
		}()
	}
	wg.Wait()
	if _, err := f.Stat(); err != nil {
		t.Fatal("original owner lost")
	}
}

func TestCOWLeafContinuationNoHiddenAllocations(t *testing.T) {
	limits := COWReadLimits{MaxRecordBytes: 2 << 20, MaxRawBytes: MaxFrameK * page.PageSize, MaxValueBytes: page.PageSize}
	for _, codec := range []BlockCodec{BlockCodecNone, BlockCodecSnappy, BlockCodecLZ4} {
		f, ptrs, _ := continuationProducerFrame(t, codec, true, 4)
		input, raw, dst := make([]byte, limits.MaxRecordBytes), make([]byte, limits.MaxRawBytes), make([]byte, page.PageSize)
		var c COWLeafContinuation
		run := func() {
			w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
			ok, err := c.Begin(f, ptrs[1], limits, input, raw, dst, true, &w)
			if !ok || err != nil {
				panic("begin")
			}
			done := false
			for !done {
				w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
				_, done, err = c.Step(&w)
				if err != nil {
					panic(err)
				}
			}
			done = false
			for !done {
				w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
				done, err = c.Cancel(&w)
				if err != nil {
					panic(err)
				}
			}
		}
		if n := testing.AllocsPerRun(8, run); n != 0 {
			t.Fatalf("codec %d hidden scan allocation=%g", codec, n)
		}
	}
}
func TestCOWLeafContinuationRealDictionaryRefusal(t *testing.T) {
	image := buildSparseLeafPageForPayloadTest(t)
	samples := make([][]byte, 16)
	for i := range samples {
		samples[i] = bytes.Clone(image)
		samples[i][0] = byte(i)
	}
	definition, err := buildBenchDict(317, samples)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(definition)
	dictID := binary.BigEndian.Uint64(digest[:8])
	body, header, err := EncodeFrame(dictID, definition, []Record{{RID: 1, Value: image}, {RID: 2, Value: image}})
	if err != nil || header.Flags&FrameFlagCompressed == 0 {
		t.Fatal("original dictionary producer", err)
	}
	path := compactLeafPayloadTestPath(t, t.TempDir(), 1)
	id, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewWriter(path, id)
	if err != nil {
		t.Fatal(err)
	}
	ptrs, err := writer.AppendEncodedFrameInto(body, 2, make([]page.ValuePtr, 2))
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	limits := COWReadLimits{MaxRecordBytes: 2 << 20, MaxRawBytes: MaxFrameK * page.PageSize, MaxValueBytes: page.PageSize}
	var c COWLeafContinuation
	ok, err := c.Begin(f, ptrs[0], limits, make([]byte, limits.MaxRecordBytes), make([]byte, limits.MaxRawBytes), make([]byte, page.PageSize), true, continuationWork())
	if !ok || err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10 && err == nil; i++ {
		_, _, err = c.Step(continuationWork())
	}
	if !errors.Is(err, ErrCOWReadUnsupported) || c.readAt != 0 || c.decoder.active {
		t.Fatal("dictionary effects", err)
	}
	for i := 0; i < 3; i++ {
		done, e := c.Cancel(continuationWork())
		if e != nil {
			t.Fatal(e)
		}
		if done {
			return
		}
	}
	t.Fatal("dictionary close stalled")
}

func TestCOWLeafContinuationSameFrameRetarget(t *testing.T) {
	limits := COWReadLimits{MaxRecordBytes: 2 << 20, MaxRawBytes: MaxFrameK * page.PageSize, MaxValueBytes: page.PageSize}
	for _, codec := range []BlockCodec{BlockCodecNone, BlockCodecSnappy, BlockCodecLZ4} {
		t.Run(fmt.Sprint(codec), func(t *testing.T) {
			f, ptrs, second, images := continuationProducerVariants(t, codec)
			input, raw, dst := make([]byte, limits.MaxRecordBytes), make([]byte, limits.MaxRawBytes), make([]byte, page.PageSize)
			var c COWLeafContinuation
			fullReads, fullCRC, fullDecode := 0, 0, 0
			readOne := func(ptr page.ValuePtr, file *os.File, policy bool, selectedLimits COWReadLimits, image []byte) {
				ok, err := c.Begin(file, ptr, selectedLimits, input, raw, dst, policy, continuationWork())
				if !ok || err != nil {
					t.Fatal(err)
				}
				if c.phase == 0 {
					fullReads++
				}
				done := false
				var got []byte
				for turns := 0; turns < 200000 && !done; turns++ {
					phase := c.phase
					got, done, err = c.Step(continuationWork())
					if err != nil {
						t.Fatal(err)
					}
					if phase == 6 && c.phase == 7 && policy {
						fullCRC++
					}
					if phase == 7 && c.phase == 8 {
						fullDecode++
					}
				}
				if !done {
					t.Fatal("retarget stalled")
				}
				requireLeafPagesLogicallyEqual(t, image, got)
				if c.shape.ptr != ptr || c.shape.valueStart != c.record.Directory.Start && c.record.Retarget {
					t.Fatal("selected identity")
				}
			}
			for _, reverse := range []bool{false, true} {
				for i := 0; i < MaxFrameK; i++ {
					index := i
					if reverse {
						index = MaxFrameK - 1 - i
					}
					readOne(ptrs[index], f, true, limits, images[index])
					if c.record.SelectedOffsets != 2 && !(i == 0 && !reverse) {
						t.Fatal("selected offsets not phased")
					}
				}
			}
			if fullReads != 1 || fullCRC != 1 || (codec != BlockCodecNone && fullDecode != 1) {
				t.Fatalf("repeated physical work reads=%d CRC=%d decode=%d", fullReads, fullCRC, fullDecode)
			}
			// Same backing and pointer do not excuse a different open-file identity,
			// checksum policy or caller limit. Each restarts original physical proof.
			other, err := os.Open(f.Name())
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			readOne(ptrs[0], other, true, limits, images[0])
			if fullReads != 2 {
				t.Fatal("different file reused")
			}
			readOne(ptrs[0], other, false, limits, images[0])
			if fullReads != 3 {
				t.Fatal("changed CRC policy reused")
			}
			readOne(ptrs[0], other, true, limits, images[0])
			if fullReads != 4 {
				t.Fatal("unproved CRC reused")
			}
			smaller := limits
			smaller.MaxValueBytes--
			readOne(ptrs[0], other, true, smaller, images[0])
			if fullReads != 5 {
				t.Fatal("changed limits reused")
			}
			readOne(second, other, true, smaller, images[0])
			if fullReads != 6 {
				t.Fatal("changed physical offset reused")
			}
			replacement := make([]byte, len(input))
			input = replacement
			readOne(second, other, true, smaller, images[0])
			if fullReads != 7 {
				t.Fatal("changed input backing reused")
			}
			raw = make([]byte, len(raw))
			readOne(second, other, true, smaller, images[0])
			if fullReads != 8 {
				t.Fatal("changed raw backing reused")
			}
			for i := 0; i < 3; i++ {
				done, err := c.Cancel(continuationWork())
				if err != nil {
					t.Fatal(err)
				}
				if done {
					return
				}
			}
			t.Fatal("retarget cancel")
		})
	}
}

func continuationProducerVariants(t *testing.T, codec BlockCodec) (*os.File, []page.ValuePtr, page.ValuePtr, [][]byte) {
	t.Helper()
	records := make([]Record, MaxFrameK)
	images := make([][]byte, MaxFrameK)
	for i := range records {
		image := buildSparseLeafPageForPayloadTest(t)
		n := node.NewNodeView(image)
		_, value, _, _, err := n.GetLeafEntryView(0)
		if err != nil {
			t.Fatal(err)
		}
		value[0] = byte(i)
		page.UpdateChecksum(image)
		images[i] = image
		compact, ok, err := MaybeCompactLeafLogPayload(image)
		if !ok || err != nil {
			t.Fatal(ok, err)
		}
		records[i] = Record{RID: uint64(i + 1), Value: compact}
	}
	path := compactLeafPayloadTestPath(t, t.TempDir(), 1)
	id, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewWriter(path, id)
	if err != nil {
		t.Fatal(err)
	}
	writer.SetBlockCompression(codec, codec != BlockCodecNone)
	ptrs, _, err := writer.AppendFrameWithStats(0, nil, records)
	if err != nil {
		t.Fatal(err)
	}
	next, _, err := writer.AppendFrameWithStats(0, nil, records[:1])
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f, ptrs, next[0], images
}
