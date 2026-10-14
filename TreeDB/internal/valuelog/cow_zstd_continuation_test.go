package valuelog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

type cowCodecTestOwner struct {
	live, peak, limit uint64
	refuse            bool
}

func (o *cowCodecTestOwner) RetainCodecBytes(n uint64) bool {
	if o.refuse || n > o.limit-o.live {
		return false
	}
	o.live += n
	o.peak = max(o.peak, o.live)
	return true
}
func (o *cowCodecTestOwner) ReleaseCodecBytes(n uint64) {
	if n > o.live {
		panic("unowned codec release")
	}
	o.live -= n
}
func codecQuantum() *iterator.OrdinalScanWork {
	return &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
}
func cancelCodec(t *testing.T, c *COWZSTDContinuation, o *cowCodecTestOwner) {
	t.Helper()
	for i := 0; i < 10000; i++ {
		done, err := c.Cancel(codecQuantum())
		if err != nil {
			t.Fatal(err)
		}
		if done {
			if o.live != 0 || c.Owner != nil || c.Definition != nil || c.file != nil {
				t.Fatal("custody after cancel", o.live)
			}
			return
		}
	}
	t.Fatal("cancel stalled")
}
func TestCOWZSTDLeafActualProducer(t *testing.T) {
	limits := COWReadLimits{MaxRecordBytes: 2 << 20, MaxRawBytes: MaxFrameK * page.PageSize, MaxValueBytes: page.PageSize}
	for _, compact := range []bool{false, true} {
		f, ptrs, want := continuationProducerFrame(t, BlockCodecZSTD, compact, MaxFrameK)
		o := &cowCodecTestOwner{limit: 8 << 20}
		z := &COWZSTDContinuation{Owner: f, Retention: o}
		c := COWLeafContinuation{ZSTD: z}
		input, raw, dst := make([]byte, limits.MaxRecordBytes), make([]byte, limits.MaxRawBytes), make([]byte, page.PageSize)
		// Retarget certified directories; disabling CRC on the final begin
		// forces a physical reread and old immutable codec alias detachment.
		for pass, index := range []int{0, 127, 254, 0} {
			ok, err := c.Begin(f, ptrs[index], limits, input, raw, dst, pass != 3, codecQuantum())
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			for i := 0; i < 100000; i++ {
				w := codecQuantum()
				got, done, err := c.StepWithCodec(w)
				if err != nil {
					t.Fatal(err)
				}
				if w.Records > 32 || w.Bytes > 1<<20 {
					t.Fatal("quantum exceeded")
				}
				if done {
					if !bytes.Equal(got, want) {
						t.Fatal("actual producer differential")
					}
					break
				}
				if i == 99999 {
					t.Fatal("stalled")
				}
			}
		}
		for i := 0; i < 10000; i++ {
			done, err := c.CancelWithCodec(codecQuantum())
			if err != nil {
				t.Fatal(err)
			}
			if done {
				break
			}
			if i == 9999 {
				t.Fatal("leaf cancel stalled")
			}
		}
		if o.live != 0 {
			t.Fatal("leaf kept codec control", o.live)
		}
	}
}
func TestCOWZSTDNativeDictionaryColdAndRefusal(t *testing.T) {
	samples := make([][]byte, 16)
	for i := range samples {
		samples[i] = bytes.Repeat([]byte("dictionary-owned-value-with-varied-content"), 256)
		samples[i][0] = byte(i)
	}
	definition, err := buildBenchDict(317, samples)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(definition)
	id := binary.BigEndian.Uint64(digest[:8])
	want := bytes.Repeat([]byte("dictionary-owned-value-with-varied-content"), 8192)
	body, _, err := EncodeFrame(id, definition, []Record{{RID: 1, Value: want}})
	if err != nil {
		t.Fatal(err)
	}
	prefix := FrameHeaderSize + 8 + 8
	input := body[prefix:]
	for _, route := range []string{"good", "wrong-id", "mutated-definition", "refused"} {
		t.Run(route, func(t *testing.T) {
			o := &cowCodecTestOwner{limit: 8 << 20}
			def := bytes.Clone(definition)
			z := COWZSTDContinuation{Owner: &body, Retention: o, ProducerID: id, Definition: def, DictionaryOwner: &def}
			if route == "wrong-id" {
				z.ProducerID++
			}
			if route == "mutated-definition" {
				def[len(def)-1] ^= 1
			}
			if route == "refused" {
				o.refuse = true
			}
			raw := make([]byte, len(want))
			if ok, err := z.Begin(input, raw, codecQuantum()); !ok || err != nil {
				t.Fatal(ok, err)
			}
			for i := 0; i < 100000; i++ {
				phase, at := z.phase, z.hashAt
				if n, done, err := z.Step(nil, &iterator.OrdinalScanWork{}); n != 0 || done || err != nil || z.phase != phase || z.hashAt != at {
					t.Fatal("zero-budget effect")
				}
				_, done, err := z.Step(nil, codecQuantum())
				if err != nil {
					if route == "good" {
						t.Fatal(err)
					}
					if route == "refused" && !errors.Is(err, zstd.ErrWorkRefused) {
						t.Fatal(err)
					}
					break
				}
				if done {
					if route != "good" {
						t.Fatal("accepted invalid binding")
					}
					got, err := z.RawResult()
					if err != nil || !bytes.Equal(got, want) {
						t.Fatal("dictionary differential", err)
					}
					break
				}
				if i == 99999 {
					t.Fatal("stalled")
				}
			}
			if done, err := z.Cancel(&iterator.OrdinalScanWork{}); done || err != nil || z.closing {
				t.Fatal("zero-budget cleanup effect", done, err)
			}
			cancelCodec(t, &z, o)
		})
	}
}
func TestCOWZSTDNativePointedStreamSpansCRC(t *testing.T) {
	want := make([]byte, (2<<20)+4096)
	x := uint32(1)
	for i := range want {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		want[i] = byte(x & 63)
		if i%2048 >= 1024 {
			want[i] = want[i-1024]
		}
	}
	path := filepath.Join(t.TempDir(), "value-000001.log")
	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatal(err)
	}
	writer.SetBlockCompression(BlockCodecZSTD, true)
	ptrs, stats, err := writer.AppendFrameWithStats(0, nil, []Record{{RID: 1, Value: want}})
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil || !stats.Kept {
		t.Fatal(err, stats)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	limits := COWReadLimits{MaxRecordBytes: 8 << 20, MaxRawBytes: 8 << 20, MaxValueBytes: 8 << 20}
	shape, err := InspectCOWRecord(f, ptrs[0], limits)
	if err != nil {
		t.Fatal(err)
	}
	prefix := HeaderSize + FrameHeaderSize + 8 + 8
	header := make([]byte, prefix)
	start := int64(ptrs[0].Offset - 4)
	if _, err = f.ReadAt(header, start); err != nil {
		t.Fatal(err)
	}
	for _, corrupt := range []bool{false, true} {
		o := &cowCodecTestOwner{limit: 16 << 20}
		z := COWZSTDContinuation{Owner: f, Retention: o}
		expected := binary.LittleEndian.Uint32(header[:4])
		if corrupt {
			expected ^= 1
		}
		if ok, err := z.BeginFile(f, start+int64(prefix), shape.RecordBytes-uint64(prefix), make([]byte, 4096), nil, true, crc.Checksum(header[4:]), expected, true, codecQuantum()); !ok || err != nil {
			t.Fatal(ok, err)
		}
		span := make([]byte, 4096)
		at, calls := 0, 0
		for ; calls < 100000; calls++ {
			w := codecQuantum()
			n, done, err := z.Step(span, w)
			if w.Records > 32 || w.Bytes > 1<<20 {
				t.Fatal("stream quantum exceeded")
			}
			if n > len(want)-at || !bytes.Equal(span[:n], want[at:at+n]) {
				t.Fatal("pointed differential", at, n)
			}
			at += n
			if err != nil {
				if !corrupt || !errors.Is(err, ErrCorrupt) {
					t.Fatal(err)
				}
				break
			}
			if done {
				if corrupt {
					t.Fatal("outer CRC admitted corrupt completion")
				}
				if at != len(want) {
					t.Fatal(at)
				}
				break
			}
		}
		if calls == 100000 {
			t.Fatal("stream stalled")
		}
		t.Logf("pointed=%d calls=%d peak-codec=%d corrupt=%v", len(want), calls, o.peak, corrupt)
		cancelCodec(t, &z, o)
	}
}

func TestCOWZSTDNativeColdCancellationAndWorkBinding(t *testing.T) {
	f, ptrs, _ := continuationProducerFrame(t, BlockCodecZSTD, false, 4)
	limits := COWReadLimits{MaxRecordBytes: 2 << 20, MaxRawBytes: MaxFrameK * page.PageSize, MaxValueBytes: page.PageSize}
	shape, err := InspectCOWLeafRecord(f, ptrs[0], limits)
	if err != nil {
		t.Fatal(err)
	}
	in := make([]byte, shape.RecordBytes)
	if _, err = f.ReadAt(in, int64(ptrs[0].Offset-4)); err != nil {
		t.Fatal(err)
	}
	payload := in[HeaderSize+int(shape.prefix):]
	for _, stop := range []int{0, 1, 2, 4, 16, 128} {
		o := &cowCodecTestOwner{limit: 8 << 20}
		z := COWZSTDContinuation{Owner: f, Retention: o}
		if ok, err := z.Begin(payload, make([]byte, shape.RawBytes), codecQuantum()); !ok || err != nil {
			t.Fatal(ok, err)
		}
		for i := 0; i < stop; i++ {
			_, done, err := z.Step(nil, codecQuantum())
			if err != nil {
				t.Fatal(err)
			}
			if done {
				break
			}
		}
		cancelCodec(t, &z, o)
	}
	o := &cowCodecTestOwner{limit: 8 << 20}
	z := &COWZSTDContinuation{Owner: f, Retention: o}
	c := COWLeafContinuation{ZSTD: z}
	if ok, err := c.Begin(f, ptrs[0], limits, make([]byte, limits.MaxRecordBytes), make([]byte, limits.MaxRawBytes), make([]byte, page.PageSize), true, codecQuantum()); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if _, _, err := c.Step(codecQuantum()); !errors.Is(err, ErrCOWCodecWorkUnbound) || c.phase != 0 || z.work != nil {
		t.Fatal("unbound work accepted", err)
	}
	if _, err := c.Cancel(codecQuantum()); !errors.Is(err, ErrCOWCodecWorkUnbound) || z.work != nil {
		t.Fatal("unbound cancel accepted", err)
	}
	for i := 0; i < 10000; i++ {
		done, err := c.CancelWithCodec(codecQuantum())
		if err != nil {
			t.Fatal(err)
		}
		if done {
			if o.live != 0 || z.work != nil {
				t.Fatal("scoped work survived", o.live)
			}
			return
		}
	}
	t.Fatal("cancel stalled")
}

func TestCOWZSTDLeafDictionaryActualProducer(t *testing.T) {
	want := buildSparseLeafPageForPayloadTest(t)
	samples := make([][]byte, 16)
	for i := range samples {
		samples[i] = bytes.Clone(want)
		samples[i][len(samples[i])-1] = byte(i)
	}
	definition, err := buildBenchDict(419, samples)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(definition)
	id := binary.BigEndian.Uint64(digest[:8])
	path := compactLeafPayloadTestPath(t, t.TempDir(), 1)
	fileID, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	ptrs, stats, err := writer.AppendFrameWithStats(id, definition, []Record{{RID: 1, Value: want}, {RID: 2, Value: want}})
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil || !stats.Kept {
		t.Fatal(err, stats)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	owner := &cowCodecTestOwner{limit: 8 << 20}
	z := &COWZSTDContinuation{Owner: f, Retention: owner, ProducerID: id, Definition: definition, DictionaryOwner: &definition}
	c := COWLeafContinuation{ZSTD: z}
	limits := COWReadLimits{MaxRecordBytes: 2 << 20, MaxRawBytes: MaxFrameK * page.PageSize, MaxValueBytes: page.PageSize}
	if ok, err := c.Begin(f, ptrs[1], limits, make([]byte, limits.MaxRecordBytes), make([]byte, limits.MaxRawBytes), make([]byte, page.PageSize), true, codecQuantum()); !ok || err != nil {
		t.Fatal(ok, err)
	}
	for calls := 0; ; calls++ {
		got, done, err := c.StepWithCodec(codecQuantum())
		if err != nil {
			t.Fatal(err)
		}
		if done {
			if !bytes.Equal(got, want) {
				t.Fatal("native dictionary leaf differential")
			}
			break
		}
		if calls > 100000 {
			t.Fatal("dictionary leaf stalled")
		}
	}
	for calls := 0; ; calls++ {
		done, err := c.CancelWithCodec(codecQuantum())
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
		if calls > 10000 {
			t.Fatal("dictionary cancel stalled")
		}
	}
	if owner.live != 0 {
		t.Fatal("dictionary retained codec storage", owner.live)
	}
}
