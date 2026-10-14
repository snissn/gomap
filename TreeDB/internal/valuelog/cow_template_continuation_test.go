package valuelog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func TestCOWTemplateActualGrammarColdCRCAndMasks(t *testing.T) {
	want := append(bytes.Repeat([]byte("complete-definition-owned-template-value"), 32), byte(7))
	mask := make([]byte, (len(want)+7)/8)
	for i := 0; i < len(want); i += 23 {
		mask[i/8] |= 1 << uint(i%8)
	}
	base := bytes.Clone(want)
	vars := []byte{}
	for i := range base {
		if mask[i/8]&(1<<uint(i%8)) != 0 {
			vars = append(vars, want[i])
			base[i] = 0
		}
	}
	def, err := templ.EncodeTemplateDef(templ.TemplateDef{Kind: templ.TemplateMask, Mask: mask, Base: base}, templ.Config{})
	if err != nil {
		t.Fatal(err)
	}
	id := templ.TemplateID(def, 0)
	payload, err := templ.EncodeMaskPayload(id, mask, vars)
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range []string{"good", "def-crc", "mask-tail", "payload-count", "wrong-generation"} {
		t.Run(route, func(t *testing.T) {
			d := bytes.Clone(def)
			p := bytes.Clone(payload)
			c := COWTemplateContinuation{Owner: &d, ID: id, Definition: d}
			out := bytes.Repeat([]byte{0xa5}, len(want))
			if route == "def-crc" {
				d[len(d)-1] ^= 1
			}
			if route == "payload-count" {
				p = append(p, 0)
			}
			if route == "wrong-generation" {
				c.ID++
			}
			if route == "mask-tail" {
				// Whole definition CRC is correct; an invalid derived mask must
				// still fail before any output copy.
				off := 2
				_, n := binary.Uvarint(d[off:])
				off += n
				ml, n := binary.Uvarint(d[off:])
				off += n
				_, n = binary.Uvarint(d[off:])
				off += n
				d[off+int(ml)-1] |= 0x80
				binary.LittleEndian.PutUint32(d[len(d)-4:], crc.Checksum(d[:len(d)-4]))
			}
			ok, err := c.Begin(p, out, continuationWork())
			if route == "wrong-generation" {
				if ok || !errors.Is(err, templ.ErrMissingTemplate) {
					t.Fatal(ok, err)
				}
				return
			}
			if !ok || err != nil {
				t.Fatal(ok, err)
			}
			for i := 0; i < 100000; i++ {
				phase, at := c.phase, c.crcAt
				if _, done, err := c.Step(&iterator.OrdinalScanWork{}); done || err != nil || c.phase != phase || c.crcAt != at {
					t.Fatal("zero-budget template effect")
				}
				w := continuationWork()
				got, done, err := c.Step(w)
				if w.Records > 1 || w.Bytes > 8192 {
					t.Fatal("template quantum exceeded")
				}
				if err != nil {
					if route == "good" {
						t.Fatal(err)
					}
					if !bytes.Equal(out, bytes.Repeat([]byte{0xa5}, len(want))) {
						t.Fatal("invalid definition/mask touched output")
					}
					break
				}
				if done {
					if route != "good" || !bytes.Equal(got, want) {
						t.Fatal("template differential")
					}
					break
				}
				if i == 99999 {
					t.Fatal("stalled")
				}
			}
			if !c.Cancel(continuationWork()) || c.Owner != nil || c.Definition != nil {
				t.Fatal("template custody")
			}
		})
	}
	// Legacy anchor grammar shares the original semantic oracle and validates
	// its complete definition before any suspended gap/anchor copies.
	anchors := [][]byte{bytes.Repeat([]byte("constant-anchor"), 4)}
	ad, err := templ.EncodeTemplateDef(templ.TemplateDef{Kind: templ.TemplateAnchors, Anchors: anchors}, templ.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ai := templ.TemplateID(ad, 0)
	ap, err := templ.EncodePayload(ai, [][]byte{[]byte("before"), []byte("after")})
	if err != nil {
		t.Fatal(err)
	}
	c := COWTemplateContinuation{Owner: &ad, ID: ai, Definition: ad}
	expected := append(append([]byte("before"), anchors[0]...), []byte("after")...)
	if ok, err := c.Begin(ap, make([]byte, len(expected)), continuationWork()); err != nil || !ok {
		t.Fatal(ok, err)
	}
	for i := 0; i < 100; i++ {
		got, done, err := c.Step(continuationWork())
		if err != nil {
			t.Fatal(err)
		}
		if done {
			if !bytes.Equal(got, expected) {
				t.Fatal("anchor differential")
			}
			return
		}
	}
	t.Fatal("anchor stalled")
}
func TestCOWTemplateActualLeafProducer(t *testing.T) {
	for _, compact := range []bool{false, true} {
		want := buildSparseLeafPageForPayloadTest(t)
		value := want
		if compact {
			var ok bool
			var err error
			value, ok, err = MaybeCompactLeafLogPayload(want)
			if !ok || err != nil {
				t.Fatal(ok, err)
			}
		}
		mask := make([]byte, (len(value)+7)/8)
		mask[0] = 1
		base := bytes.Clone(value)
		base[0] = 0
		definition, err := templ.EncodeTemplateDef(templ.TemplateDef{Kind: templ.TemplateMask, Mask: mask, Base: base}, templ.Config{})
		if err != nil {
			t.Fatal(err)
		}
		id := templ.TemplateID(definition, 0)
		encoded, err := templ.EncodeMaskPayload(id, mask, []byte{value[0]})
		if err != nil {
			t.Fatal(err)
		}
		path := compactLeafPayloadTestPath(t, t.TempDir(), 1)
		fileID, err := EncodeFileID(ReservedLeafLogLaneID, 1)
		if err != nil {
			t.Fatal(err)
		}
		writer, err := NewWriter(path, fileID)
		if err != nil {
			t.Fatal(err)
		}
		writer.SetBlockCompression(BlockCodecZSTD, true)
		ptrs, stats, err := writer.AppendFrameWithStats(0, nil, []Record{{RID: 1, Value: encoded}, {RID: 2, Value: encoded}})
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
		o := &cowCodecTestOwner{limit: 8 << 20}
		z := &COWZSTDContinuation{Owner: f, Retention: o}
		tc := &COWTemplateContinuation{Owner: &definition, ID: id, Definition: definition}
		c := COWLeafContinuation{ZSTD: z, Template: tc, TemplateScratch: make([]byte, page.PageSize)}
		limits := COWReadLimits{MaxRecordBytes: 2 << 20, MaxRawBytes: MaxFrameK * page.PageSize, MaxValueBytes: page.PageSize}
		in, raw, out := make([]byte, limits.MaxRecordBytes), make([]byte, limits.MaxRawBytes), make([]byte, page.PageSize)
		for _, ptr := range ptrs {
			if ok, err := c.Begin(f, ptr, limits, in, raw, out, true, codecQuantum()); !ok || err != nil {
				t.Fatal(ok, err)
			}
			for i := 0; i < 10000; i++ {
				got, done, err := c.StepWithCodec(codecQuantum())
				if err != nil {
					t.Fatal(err)
				}
				if done {
					if !bytes.Equal(got, want) {
						t.Fatal("native template leaf differential")
					}
					break
				}
				if i == 9999 {
					t.Fatal("leaf template stalled")
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
				t.Fatal("cancel stalled")
			}
		}
		if o.live != 0 {
			t.Fatal("template codec leaked", o.live)
		}
	}
}

func TestCOWTemplateDefinitionMaskVersionsAndPositionOverrides(t *testing.T) {
	base := bytes.Repeat([]byte("original-template-mask-semantics"), 8)
	mask := make([]byte, (len(base)+7)/8)
	positions := []uint16{0, 7, 125}
	vars := []byte{91, 92, 93}
	for _, p := range positions {
		mask[p/8] |= 1 << uint(p%8)
	}
	v3, err := templ.EncodeTemplateDef(templ.TemplateDef{Kind: templ.TemplateMask, Mask: mask, Base: base, VarPositions: positions}, templ.Config{})
	if err != nil {
		t.Fatal(err)
	}
	// The original v2 producer grammar omits the v3 position directory.
	v2 := []byte{2, byte(templ.TemplateMask)}
	v2 = binary.AppendUvarint(v2, uint64(len(base)))
	v2 = binary.AppendUvarint(v2, uint64(len(mask)))
	v2 = append(v2, mask...)
	v2 = append(v2, base...)
	v2 = binary.LittleEndian.AppendUint32(v2, crc.Checksum(v2))
	for _, definition := range [][]byte{v2, v3} {
		id := templ.TemplateID(definition, 0)
		full, err := templ.EncodeMaskPayload(id, mask, vars)
		if err != nil {
			t.Fatal(err)
		}
		_, n := binary.Uvarint(full[4:])
		partial := append(bytes.Clone(full[:4+n]), vars...)
		partial[3] &^= 4
		for _, payload := range [][]byte{full, partial} {
			oracle, err := templ.DecodePayload(payload, func(uint64) ([]byte, error) { return definition, nil }, templ.DecodeOptions{})
			if err != nil {
				t.Fatal(err)
			}
			c := COWTemplateContinuation{Owner: &definition, ID: id, Definition: definition}
			if ok, err := c.Begin(payload, make([]byte, len(base)), continuationWork()); !ok || err != nil {
				t.Fatal(ok, err)
			}
			for calls := 0; ; calls++ {
				got, done, err := c.Step(continuationWork())
				if err != nil {
					t.Fatal(err)
				}
				if done {
					if !bytes.Equal(got, oracle) {
						t.Fatal("original v2/v3 full/position differential")
					}
					break
				}
				if calls > 10000 {
					t.Fatal("mask stalled")
				}
			}
			if !c.Cancel(continuationWork()) {
				t.Fatal("cancel")
			}
		}
	}
}
