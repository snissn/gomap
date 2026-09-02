package nativewire

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"
)

var (
	benchHeaderSink    Header
	benchCommandSink   CommandHeader
	benchSectionsSink  []Section
	benchVectorSink    ByteVector
	benchValidatedSink ValidatedCommand
	benchEntrySink     DeterministicEntry
	benchDigestSink    [32]byte
	benchBytesSink     []byte
)

type nativewireBenchmarkCase struct {
	name          string
	commandID     CommandID
	sections      []Section
	body          []byte
	items         int
	deterministic bool
}

func TestDecodeSectionsIntoAndDecodeByteVectorIntoReuseBuffers(t *testing.T) {
	cases := nativewireBenchmarkCases()
	body := cases[0].body

	sectionsScratch := make([]Section, 0, len(cases[0].sections))
	sections, err := DecodeSectionsInto(sectionsScratch, body, Limits{})
	if err != nil {
		t.Fatalf("DecodeSectionsInto: %v", err)
	}
	if len(sections) != len(cases[0].sections) {
		t.Fatalf("sections len=%d want %d", len(sections), len(cases[0].sections))
	}
	if sectionBacking(sections) != sectionBacking(sectionsScratch) {
		t.Fatalf("DecodeSectionsInto did not reuse caller backing array")
	}

	vecBytes := AppendByteVector(nil, makeBenchmarkItems("doc", 8, 32)...)
	itemsScratch := make([][]byte, 0, 8)
	items, err := DecodeByteVectorItemsInto(itemsScratch, vecBytes, Limits{})
	if err != nil {
		t.Fatalf("DecodeByteVectorItemsInto: %v", err)
	}
	if len(items) != 8 {
		t.Fatalf("items len=%d want 8", len(items))
	}
	if cap(items) != cap(itemsScratch) {
		t.Fatalf("DecodeByteVectorItemsInto did not reuse caller capacity")
	}

	var scratch ByteVectorScratch
	vec, err := DecodeByteVectorInto(vecBytes, Limits{}, &scratch)
	if err != nil {
		t.Fatalf("DecodeByteVectorInto: %v", err)
	}
	if vec.Len() != 8 {
		t.Fatalf("vector len=%d want 8", vec.Len())
	}
	firstOffsets := intBacking(scratch.offsets)
	firstLengths := intBacking(scratch.lengths)

	vec, err = DecodeByteVectorInto(vecBytes, Limits{}, &scratch)
	if err != nil {
		t.Fatalf("DecodeByteVectorInto second: %v", err)
	}
	if vec.Len() != 8 {
		t.Fatalf("vector len second=%d want 8", vec.Len())
	}
	if intBacking(scratch.offsets) != firstOffsets || intBacking(scratch.lengths) != firstLengths {
		t.Fatalf("DecodeByteVectorInto did not reuse scratch backing arrays")
	}
}

func TestDecodeSectionsIntoClearsStaleReferences(t *testing.T) {
	cases := nativewireBenchmarkCases()
	largeBody := cases[0].body
	smallBody := benchmarkRequestBody([]Section{benchmarkCommandSection(CommandStats)})
	sections, err := DecodeSectionsInto(nil, largeBody, Limits{})
	if err != nil {
		t.Fatalf("DecodeSectionsInto large: %v", err)
	}
	largeLen := len(sections)
	sections, err = DecodeSectionsInto(sections, smallBody, Limits{})
	if err != nil {
		t.Fatalf("DecodeSectionsInto small: %v", err)
	}
	backing := sections[:largeLen]
	for i := len(sections); i < largeLen; i++ {
		if backing[i].ID != 0 || backing[i].Flags != 0 || backing[i].Bytes != nil {
			t.Fatalf("stale section at index %d: %#v", i, backing[i])
		}
	}
}

func sectionBacking(sections []Section) *Section {
	if cap(sections) == 0 {
		return nil
	}
	return &sections[:cap(sections)][0]
}

func intBacking(values []int) *int {
	if cap(values) == 0 {
		return nil
	}
	return &values[:cap(values)][0]
}

func TestNativewireBenchmarkRequiredCommandsCovered(t *testing.T) {
	covered := make(map[CommandID]struct{})
	for _, tc := range nativewireBenchmarkCases() {
		covered[tc.commandID] = struct{}{}
	}

	for _, schema := range v1CommandSchemas() {
		if !schema.BenchmarkRequired {
			continue
		}
		if _, ok := covered[schema.ID]; !ok {
			t.Fatalf("command %s (%d) is BenchmarkRequired but has no nativewire benchmark case", schema.Name, schema.ID)
		}
	}
}

func TestNativewireBenchmarkCasesValidate(t *testing.T) {
	registry := MustV1Registry()
	for _, tc := range nativewireBenchmarkCases() {
		t.Run(tc.name, func(t *testing.T) {
			sections, err := DecodeSections(tc.body, Limits{})
			if err != nil {
				t.Fatalf("DecodeSections: %v", err)
			}
			cmd, err := registry.ValidateRequestSections(sections)
			if err != nil {
				t.Fatalf("ValidateRequestSections: %v", err)
			}
			if cmd.Header.ID != tc.commandID {
				t.Fatalf("command ID=%d want %d", cmd.Header.ID, tc.commandID)
			}
			if tc.deterministic {
				if _, err := AppendDeterministicEntry(nil, cmd); err != nil {
					t.Fatalf("AppendDeterministicEntry: %v", err)
				}
			}
		})
	}
}

func TestNativewireCodecAllocationGuards(t *testing.T) {
	header, err := AppendHeader(make([]byte, 0, FrameHeaderLenV1), Header{
		Type:      FrameRequest,
		StreamID:  7,
		RequestID: 42,
		BodyLen:   4096,
	})
	if err != nil {
		t.Fatalf("AppendHeader: %v", err)
	}
	assertMaxAllocs(t, "AppendHeader/preallocated", 0, func() {
		dst := header[:0]
		dst, err = AppendHeader(dst, Header{Type: FrameRequest, StreamID: 7, RequestID: 42, BodyLen: 4096})
		if err != nil {
			t.Fatalf("AppendHeader: %v", err)
		}
		benchBytesSink = dst
	})
	assertMaxAllocs(t, "DecodeHeader", 0, func() {
		benchHeaderSink, err = DecodeHeader(header, Limits{})
		if err != nil {
			t.Fatalf("DecodeHeader: %v", err)
		}
	})

	commandHeader := AppendCommandHeader(make([]byte, 0, 16), CommandHeader{
		ID:      CommandInsertBatch,
		Version: 1,
	})
	assertMaxAllocs(t, "AppendCommandHeader/preallocated", 0, func() {
		dst := commandHeader[:0]
		benchBytesSink = AppendCommandHeader(dst, CommandHeader{ID: CommandInsertBatch, Version: 1})
	})
	assertMaxAllocs(t, "DecodeCommandHeader", 0, func() {
		benchCommandSink, err = DecodeCommandHeader(commandHeader)
		if err != nil {
			t.Fatalf("DecodeCommandHeader: %v", err)
		}
	})

	cases := nativewireBenchmarkCases()
	body := cases[0].body
	sectionsScratch := make([]Section, 0, len(cases[0].sections))
	var commandScratch CommandScratch
	assertMaxAllocs(t, "DecodeSectionsInto/preallocated", 0, func() {
		benchSectionsSink, err = DecodeSectionsInto(sectionsScratch, body, Limits{})
		if err != nil {
			t.Fatalf("DecodeSectionsInto: %v", err)
		}
	})
	sections, err := DecodeSectionsInto(sectionsScratch, body, Limits{})
	if err != nil {
		t.Fatalf("DecodeSectionsInto warm: %v", err)
	}
	registry := MustV1Registry()
	if _, err := registry.ValidateRequestSectionsInto(sections, &commandScratch); err != nil {
		t.Fatalf("ValidateRequestSectionsInto warm: %v", err)
	}
	assertMaxAllocs(t, "ValidateRequestSectionsInto/warm-scratch", 0, func() {
		benchValidatedSink, err = registry.ValidateRequestSectionsInto(sections, &commandScratch)
		if err != nil {
			t.Fatalf("ValidateRequestSectionsInto: %v", err)
		}
	})
	assertMaxAllocs(t, "DecodeSectionsInto+ValidateRequestSectionsInto/warm-scratch", 0, func() {
		sections, err = DecodeSectionsInto(sectionsScratch, body, Limits{})
		if err != nil {
			t.Fatalf("DecodeSectionsInto: %v", err)
		}
		benchValidatedSink, err = registry.ValidateRequestSectionsInto(sections, &commandScratch)
		if err != nil {
			t.Fatalf("ValidateRequestSectionsInto: %v", err)
		}
	})
	entry, err := AppendDeterministicEntry(nil, benchValidatedSink)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry warm: %v", err)
	}
	assertMaxAllocs(t, "AppendDeterministicEntry/preallocated", 0, func() {
		dst := entry[:0]
		benchBytesSink, err = AppendDeterministicEntry(dst, benchValidatedSink)
		if err != nil {
			t.Fatalf("AppendDeterministicEntry: %v", err)
		}
	})
	var entryScratch DeterministicEntryScratch
	if _, err := DecodeDeterministicEntryInto(entry, Limits{}, &entryScratch); err != nil {
		t.Fatalf("DecodeDeterministicEntryInto warm: %v", err)
	}
	assertMaxAllocs(t, "DecodeDeterministicEntryInto/warm-scratch", 0, func() {
		benchEntrySink, err = DecodeDeterministicEntryInto(entry, Limits{}, &entryScratch)
		if err != nil {
			t.Fatalf("DecodeDeterministicEntryInto: %v", err)
		}
	})
	assertMaxAllocs(t, "DeterministicEntryDigest", 0, func() {
		benchDigestSink = DeterministicEntryDigest(entry)
	})

	vecBytes := AppendByteVector(nil, makeBenchmarkItems("doc", 128, 64)...)
	var vecScratch ByteVectorScratch
	if _, err := DecodeByteVectorInto(vecBytes, Limits{}, &vecScratch); err != nil {
		t.Fatalf("warm DecodeByteVectorInto: %v", err)
	}
	assertMaxAllocs(t, "DecodeByteVectorInto/warm-scratch", 0, func() {
		benchVectorSink, err = DecodeByteVectorInto(vecBytes, Limits{}, &vecScratch)
		if err != nil {
			t.Fatalf("DecodeByteVectorInto: %v", err)
		}
	})
}

func TestCommandScratchReusesWideSeenOverflow(t *testing.T) {
	const (
		commandID    = CommandID(9001)
		sectionStart = SectionID(1000)
		sectionCount = sectionSeenInlineCapacity + 16
	)
	rules := make([]SectionRule, 0, sectionCount)
	sections := make([]Section, 0, sectionCount+1)
	sections = append(sections, Section{
		ID:    SectionCommandHeader,
		Bytes: AppendCommandHeader(nil, CommandHeader{ID: commandID, Version: 1}),
	})
	for i := 0; i < sectionCount; i++ {
		id := sectionStart + SectionID(i)
		rules = append(rules, SectionRule{ID: id, Name: fmt.Sprintf("section_%02d", i), Required: true})
		sections = append(sections, Section{ID: id, Bytes: []byte{byte(i)}})
	}
	registry, err := NewRegistry(CommandSchema{
		ID:       commandID,
		Version:  1,
		Name:     "wide_seen_test",
		Kind:     CommandKindMutation,
		Sections: rules,
	})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	var scratch CommandScratch
	if _, err := registry.ValidateRequestSectionsInto(sections, &scratch); err != nil {
		t.Fatalf("ValidateRequestSectionsInto warm: %v", err)
	}
	assertMaxAllocs(t, "ValidateRequestSectionsInto/wide-warm-scratch", 0, func() {
		benchValidatedSink, err = registry.ValidateRequestSectionsInto(sections, &scratch)
		if err != nil {
			t.Fatalf("ValidateRequestSectionsInto: %v", err)
		}
	})
}

func TestCommandScratchKeepsInlinePathAfterWideRequest(t *testing.T) {
	const (
		wideCommandID  = CommandID(9002)
		smallCommandID = CommandID(9003)
		sectionStart   = SectionID(1100)
		wideCount      = sectionSeenInlineCapacity + 4
	)
	wideRules := make([]SectionRule, 0, wideCount)
	wideSections := []Section{{
		ID:    SectionCommandHeader,
		Bytes: AppendCommandHeader(nil, CommandHeader{ID: wideCommandID, Version: 1}),
	}}
	for i := 0; i < wideCount; i++ {
		id := sectionStart + SectionID(i)
		wideRules = append(wideRules, SectionRule{ID: id, Name: fmt.Sprintf("wide_%02d", i), Required: true})
		wideSections = append(wideSections, Section{ID: id, Bytes: []byte{byte(i)}})
	}
	smallSections := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: smallCommandID, Version: 1})},
		{ID: sectionStart, Bytes: []byte{1}},
	}
	registry, err := NewRegistry(
		CommandSchema{ID: wideCommandID, Version: 1, Name: "wide_seen_test", Kind: CommandKindMutation, Sections: wideRules},
		CommandSchema{ID: smallCommandID, Version: 1, Name: "small_seen_test", Kind: CommandKindMutation, Sections: []SectionRule{{ID: sectionStart, Name: "small", Required: true}}},
	)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	var scratch CommandScratch
	if _, err := registry.ValidateRequestSectionsInto(wideSections, &scratch); err != nil {
		t.Fatalf("ValidateRequestSectionsInto wide: %v", err)
	}
	if scratch.seenOverflow == nil {
		t.Fatal("wide validation did not retain overflow scratch")
	}
	scratch.seenOverflow[sectionStart] = sectionSeenEntry{generation: scratch.seenGeneration, count: 99}
	if _, err := registry.ValidateRequestSectionsInto(smallSections, &scratch); err != nil {
		t.Fatalf("ValidateRequestSectionsInto small after wide: %v", err)
	}
	if scratch.seenOverflow == nil {
		t.Fatal("small validation discarded reusable overflow map")
	}
}

func BenchmarkNativewireFrameHeader(b *testing.B) {
	header, err := AppendHeader(nil, Header{
		Type:      FrameRequest,
		StreamID:  7,
		RequestID: 42,
		BodyLen:   4096,
	})
	if err != nil {
		b.Fatalf("AppendHeader: %v", err)
	}

	b.Run("append/preallocated", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(FrameHeaderLenV1))
		dst := make([]byte, 0, FrameHeaderLenV1)
		for i := 0; i < b.N; i++ {
			dst = dst[:0]
			dst, err = AppendHeader(dst, Header{Type: FrameRequest, StreamID: 7, RequestID: uint64(i), BodyLen: 4096})
			if err != nil {
				b.Fatalf("AppendHeader: %v", err)
			}
		}
		benchBytesSink = dst
	})

	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(header)))
		for i := 0; i < b.N; i++ {
			benchHeaderSink, err = DecodeHeader(header, Limits{})
			if err != nil {
				b.Fatalf("DecodeHeader: %v", err)
			}
		}
	})
}

func BenchmarkNativewireCommandHeader(b *testing.B) {
	commandHeader := AppendCommandHeader(nil, CommandHeader{
		ID:      CommandInsertBatch,
		Version: 1,
	})

	b.Run("append/preallocated", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(commandHeader)))
		dst := make([]byte, 0, 16)
		for i := 0; i < b.N; i++ {
			dst = dst[:0]
			benchBytesSink = AppendCommandHeader(dst, CommandHeader{ID: CommandInsertBatch, Version: 1})
		}
	})

	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(commandHeader)))
		var err error
		for i := 0; i < b.N; i++ {
			benchCommandSink, err = DecodeCommandHeader(commandHeader)
			if err != nil {
				b.Fatalf("DecodeCommandHeader: %v", err)
			}
		}
	})
}

func BenchmarkNativewireByteVector(b *testing.B) {
	for _, tc := range []struct {
		name  string
		count int
		size  int
	}{
		{name: "ids_128x16", count: 128, size: 16},
		{name: "docs_128x256", count: 128, size: 256},
		{name: "docs_1024x128", count: 1024, size: 128},
	} {
		items := makeBenchmarkItems(tc.name, tc.count, tc.size)
		encoded := AppendByteVector(nil, items...)
		limits := Limits{MaxByteVectorItems: tc.count + 1, MaxByteVectorBytes: uint64(len(encoded))}

		b.Run(tc.name+"/append/preallocated", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(encoded)))
			b.ReportMetric(float64(len(encoded))/float64(tc.count), "wire_B/item")
			dst := make([]byte, 0, len(encoded))
			for i := 0; i < b.N; i++ {
				dst = dst[:0]
				benchBytesSink = AppendByteVector(dst, items...)
			}
		})

		b.Run(tc.name+"/decode/allocating", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(encoded)))
			b.ReportMetric(float64(len(encoded))/float64(tc.count), "wire_B/item")
			var err error
			for i := 0; i < b.N; i++ {
				benchVectorSink, err = DecodeByteVector(encoded, limits)
				if err != nil {
					b.Fatalf("DecodeByteVector: %v", err)
				}
			}
		})

		b.Run(tc.name+"/decode/reuse", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(encoded)))
			var err error
			var scratch ByteVectorScratch
			if _, err = DecodeByteVectorInto(encoded, limits, &scratch); err != nil {
				b.Fatalf("warm DecodeByteVectorInto: %v", err)
			}
			b.ResetTimer()
			b.ReportMetric(float64(len(encoded))/float64(tc.count), "wire_B/item")
			for i := 0; i < b.N; i++ {
				benchVectorSink, err = DecodeByteVectorInto(encoded, limits, &scratch)
				if err != nil {
					b.Fatalf("DecodeByteVectorInto: %v", err)
				}
			}
		})
	}
}

func BenchmarkNativewireRequestBody(b *testing.B) {
	for _, tc := range nativewireBenchmarkCases() {
		tc := tc
		limits := Limits{MaxSections: len(tc.sections) + 1, MaxSectionLen: uint64(len(tc.body))}

		b.Run(tc.name+"/encode_sections/preallocated", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.body)))
			reportCommandMetrics(b, tc)
			dst := make([]byte, 0, len(tc.body))
			var err error
			for i := 0; i < b.N; i++ {
				dst = dst[:0]
				for _, section := range tc.sections {
					dst, err = AppendSection(dst, section)
					if err != nil {
						b.Fatalf("AppendSection: %v", err)
					}
				}
			}
			benchBytesSink = dst
		})

		b.Run(tc.name+"/decode_sections/allocating", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.body)))
			reportCommandMetrics(b, tc)
			var err error
			for i := 0; i < b.N; i++ {
				benchSectionsSink, err = DecodeSections(tc.body, limits)
				if err != nil {
					b.Fatalf("DecodeSections: %v", err)
				}
			}
		})

		b.Run(tc.name+"/decode_sections/reuse", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.body)))
			reportCommandMetrics(b, tc)
			var err error
			scratch := make([]Section, 0, len(tc.sections))
			for i := 0; i < b.N; i++ {
				benchSectionsSink, err = DecodeSectionsInto(scratch, tc.body, limits)
				if err != nil {
					b.Fatalf("DecodeSectionsInto: %v", err)
				}
			}
		})
	}
}

func BenchmarkNativewireDecodeValidateRequest(b *testing.B) {
	registry := MustV1Registry()
	for _, tc := range nativewireBenchmarkCases() {
		tc := tc
		limits := Limits{MaxSections: len(tc.sections) + 1, MaxSectionLen: uint64(len(tc.body))}

		b.Run(tc.name+"/allocating", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.body)))
			reportCommandMetrics(b, tc)
			var err error
			for i := 0; i < b.N; i++ {
				var sections []Section
				sections, err = DecodeSections(tc.body, limits)
				if err != nil {
					b.Fatalf("DecodeSections: %v", err)
				}
				benchValidatedSink, err = registry.ValidateRequestSections(sections)
				if err != nil {
					b.Fatalf("ValidateRequestSections: %v", err)
				}
			}
		})

		b.Run(tc.name+"/section_reuse", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.body)))
			reportCommandMetrics(b, tc)
			var err error
			scratch := make([]Section, 0, len(tc.sections))
			var commandScratch CommandScratch
			for i := 0; i < b.N; i++ {
				var sections []Section
				sections, err = DecodeSectionsInto(scratch, tc.body, limits)
				if err != nil {
					b.Fatalf("DecodeSectionsInto: %v", err)
				}
				benchValidatedSink, err = registry.ValidateRequestSectionsInto(sections, &commandScratch)
				if err != nil {
					b.Fatalf("ValidateRequestSectionsInto: %v", err)
				}
			}
		})
	}
}

func BenchmarkNativewireDeterministicEntry(b *testing.B) {
	registry := MustV1Registry()
	for _, tc := range nativewireBenchmarkCases() {
		if !tc.deterministic {
			continue
		}
		sections, err := DecodeSections(tc.body, Limits{})
		if err != nil {
			b.Fatalf("DecodeSections: %v", err)
		}
		cmd, err := registry.ValidateRequestSections(sections)
		if err != nil {
			b.Fatalf("ValidateRequestSections: %v", err)
		}
		entry, err := AppendDeterministicEntry(nil, cmd)
		if err != nil {
			b.Fatalf("AppendDeterministicEntry: %v", err)
		}

		b.Run(tc.name+"/append/preallocated", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(entry)))
			reportCommandMetrics(b, tc)
			dst := make([]byte, 0, len(entry))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst = dst[:0]
				benchBytesSink, err = AppendDeterministicEntry(dst, cmd)
				if err != nil {
					b.Fatalf("AppendDeterministicEntry: %v", err)
				}
			}
		})
	}
}

func BenchmarkNativewireDeterministicEntryReplicatedCommands(b *testing.B) {
	registry := MustV1Registry()
	for _, tc := range deterministicEntryFixtureCases() {
		cmd, err := registry.ValidateRequestSections(tc.sections)
		if err != nil {
			b.Fatalf("%s ValidateRequestSections: %v", tc.name, err)
		}
		entry, err := AppendDeterministicEntry(nil, cmd)
		if err != nil {
			b.Fatalf("%s AppendDeterministicEntry: %v", tc.name, err)
		}
		b.Run(tc.name+"/append/preallocated", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(entry)))
			dst := make([]byte, 0, len(entry))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst = dst[:0]
				benchBytesSink, err = AppendDeterministicEntry(dst, cmd)
				if err != nil {
					b.Fatalf("AppendDeterministicEntry: %v", err)
				}
			}
		})
		b.Run(tc.name+"/decode/warm_scratch", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(entry)))
			var scratch DeterministicEntryScratch
			if _, err := DecodeDeterministicEntryInto(entry, Limits{}, &scratch); err != nil {
				b.Fatalf("DecodeDeterministicEntryInto warm: %v", err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchEntrySink, err = DecodeDeterministicEntryInto(entry, Limits{}, &scratch)
				if err != nil {
					b.Fatalf("DecodeDeterministicEntryInto: %v", err)
				}
			}
		})
		b.Run(tc.name+"/digest", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(entry)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchDigestSink = DeterministicEntryDigest(entry)
			}
		})
	}
}

func nativewireBenchmarkCases() []nativewireBenchmarkCase {
	insertIDs := AppendByteVector(nil, makeBenchmarkItems("ins_id", 64, 16)...)
	templateRecord := deterministicTemplateRecord("active", "age", "city", "email")
	insertDocs := AppendByteVector(nil, makeBenchmarkTemplateDocuments("ins_doc", 64, 256, templateRecord, 4)...)
	replaceIDs := AppendByteVector(nil, makeBenchmarkItems("rep_id", 64, 16)...)
	replaceDocs := AppendByteVector(nil, makeBenchmarkTemplateDocuments("rep_doc", 64, 256, templateRecord, 4)...)
	updateID := AppendByteVector(nil, []byte("upd_id_000000001"))
	updateFieldNames := AppendByteVector(nil, []byte("field0"))
	updateFieldValues := AppendByteVector(nil, benchmarkBSONStringRawValue("updated-value"))
	deleteIDs := AppendByteVector(nil, makeBenchmarkItems("del_id", 128, 16)...)
	getIDs := AppendByteVector(nil, makeBenchmarkItems("get_id", 128, 16)...)
	templateRecords := AppendByteVector(nil, templateRecord)
	indexName := benchmarkString("city")
	indexValue := benchmarkScalarString("hnl")
	indexLower := benchmarkIndexBound("h", true, false)
	indexUpper := benchmarkIndexBound("z", true, false)
	cursorLimits := benchmarkCursorLimits(128, 0)

	cases := []nativewireBenchmarkCase{
		{
			name:          "insert_batch_64x256_template",
			commandID:     CommandInsertBatch,
			items:         64,
			deterministic: true,
			sections: []Section{
				benchmarkCommandSection(CommandInsertBatch),
				{ID: SectionIdempotencyKey, Bytes: []byte("client-a:insert:1")},
				{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("orders")},
				{ID: SectionDocumentFormat, Bytes: benchmarkUvarint(uint64(DocumentFormatTemplateV1))},
				{ID: SectionDocumentIDs, Bytes: insertIDs},
				{ID: SectionDocuments, Bytes: insertDocs},
				{ID: SectionTemplateRecords, Bytes: templateRecords},
				{ID: SectionExpectedCatalogVersion, Bytes: benchmarkUvarint(7)},
			},
		},
		{
			name:          "replace_batch_64x256_template",
			commandID:     CommandReplaceBatch,
			items:         64,
			deterministic: true,
			sections: []Section{
				benchmarkCommandSection(CommandReplaceBatch),
				{ID: SectionIdempotencyKey, Bytes: []byte("client-a:replace:1")},
				{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("orders")},
				{ID: SectionDocumentFormat, Bytes: benchmarkUvarint(uint64(DocumentFormatTemplateV1))},
				{ID: SectionDocumentIDs, Bytes: replaceIDs},
				{ID: SectionDocuments, Bytes: replaceDocs},
				{ID: SectionTemplateRecords, Bytes: templateRecords},
				{ID: SectionExpectedCatalogVersion, Bytes: benchmarkUvarint(7)},
				{ID: SectionReplacementMode, Bytes: benchmarkUvarint(1)},
			},
		},
		{
			name:          "update_bson_set_1_field",
			commandID:     CommandUpdateBSONSet,
			items:         1,
			deterministic: true,
			sections: []Section{
				benchmarkCommandSection(CommandUpdateBSONSet),
				{ID: SectionIdempotencyKey, Bytes: []byte("client-a:update-bson-set:1")},
				{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("orders")},
				{ID: SectionDocumentIDs, Bytes: updateID},
				{ID: SectionUpdateFieldNames, Bytes: updateFieldNames},
				{ID: SectionUpdateFieldValues, Bytes: updateFieldValues},
				{ID: SectionExpectedCatalogVersion, Bytes: benchmarkUvarint(7)},
			},
		},
		{
			name:          "delete_batch_128_ids",
			commandID:     CommandDeleteBatch,
			items:         128,
			deterministic: true,
			sections: []Section{
				benchmarkCommandSection(CommandDeleteBatch),
				{ID: SectionIdempotencyKey, Bytes: []byte("client-a:delete:1")},
				{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("orders")},
				{ID: SectionDocumentIDs, Bytes: deleteIDs},
				{ID: SectionExpectedCatalogVersion, Bytes: benchmarkUvarint(7)},
			},
		},
		{
			name:      "get_many_128_ids",
			commandID: CommandGetMany,
			items:     128,
			sections: []Section{
				benchmarkCommandSection(CommandGetMany),
				{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("orders")},
				{ID: SectionDocumentIDs, Bytes: getIDs},
			},
		},
		{
			name:      "index_lookup_128_limit",
			commandID: CommandIndexLookup,
			items:     128,
			sections: []Section{
				benchmarkCommandSection(CommandIndexLookup),
				{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("orders")},
				{ID: SectionIndexName, Bytes: indexName},
				{ID: SectionIndexValue, Bytes: indexValue},
				{ID: SectionCursorLimits, Bytes: cursorLimits},
			},
		},
		{
			name:      "index_range_bounded_128_limit",
			commandID: CommandIndexRange,
			items:     128,
			sections: []Section{
				benchmarkCommandSection(CommandIndexRange),
				{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("orders")},
				{ID: SectionIndexName, Bytes: indexName},
				{ID: SectionIndexLowerBound, Bytes: indexLower},
				{ID: SectionIndexUpperBound, Bytes: indexUpper},
				{ID: SectionCursorLimits, Bytes: cursorLimits},
			},
		},
		{
			name:      "open_scan_primary",
			commandID: CommandOpenScan,
			items:     1,
			sections: []Section{
				benchmarkCommandSection(CommandOpenScan),
				{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("orders")},
			},
		},
	}
	for i := range cases {
		cases[i].body = benchmarkRequestBody(cases[i].sections)
	}
	return cases
}

func benchmarkCommandSection(id CommandID) Section {
	return Section{
		ID:    SectionCommandHeader,
		Bytes: AppendCommandHeader(nil, CommandHeader{ID: id, Version: 1}),
	}
}

func benchmarkString(s string) []byte {
	dst := appendUvarint(nil, uint64(len(s)))
	return append(dst, s...)
}

func benchmarkScalarString(s string) []byte {
	dst := appendUvarint(nil, 1)
	return append(dst, benchmarkString(s)...)
}

func benchmarkBSONStringRawValue(s string) []byte {
	dst := make([]byte, 1+4+len(s)+1)
	dst[0] = 0x02
	binary.LittleEndian.PutUint32(dst[1:5], uint32(len(s)+1))
	copy(dst[5:], s)
	return dst
}

func benchmarkIndexBound(s string, inclusive, unbounded bool) []byte {
	var dst []byte
	if unbounded {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	if inclusive {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	if unbounded {
		return dst
	}
	return append(dst, benchmarkScalarString(s)...)
}

func benchmarkCursorLimits(maxItems, maxBytes uint64) []byte {
	dst := appendUvarint(nil, maxItems)
	return appendUvarint(dst, maxBytes)
}

func benchmarkRequestBody(sections []Section) []byte {
	var body []byte
	var err error
	for _, section := range sections {
		body, err = AppendSection(body, section)
		if err != nil {
			panic(fmt.Sprintf("benchmark section %d: %v", section.ID, err))
		}
	}
	return body
}

func benchmarkUvarint(v uint64) []byte {
	return appendUvarint(nil, v)
}

func makeBenchmarkItems(prefix string, count, size int) [][]byte {
	items := make([][]byte, count)
	for i := range items {
		item := make([]byte, size)
		label := fmt.Sprintf("%s_%06d", prefix, i)
		copy(item, label)
		for j := len(label); j < size; j++ {
			item[j] = byte('a' + (i+j)%26)
		}
		items[i] = item
	}
	return items
}

func makeBenchmarkTemplateDocuments(prefix string, count, size int, record []byte, fields int) [][]byte {
	headerLen := len(deterministicTemplateV1StoredMagic) + sha256.Size
	if size < headerLen {
		size = headerLen
	}
	payloadLen := size - headerLen
	if fields <= 0 {
		fields = 1
	}
	items := make([][]byte, count)
	for i := range items {
		payload := benchmarkTemplateValuePayload(prefix, i, payloadLen, fields)
		items[i] = deterministicTemplateStoredDocumentForRecord(record, payload)
	}
	return items
}

func benchmarkTemplateValuePayload(prefix string, i, payloadLen, fields int) []byte {
	if payloadLen < fields {
		payloadLen = fields
	}
	payload := make([]byte, 0, payloadLen)
	for field := 0; field < fields-1; field++ {
		payload = append(payload, deterministicTemplateV1KindNull)
	}
	label := []byte(fmt.Sprintf("%s_%06d", prefix, i))
	maxStringLen := payloadLen - len(payload) - 1
	if maxStringLen < 0 {
		maxStringLen = 0
	}
	for maxStringLen > 0 && 1+uvarintLen(uint64(maxStringLen))+maxStringLen > payloadLen-len(payload) {
		maxStringLen--
	}
	value := make([]byte, maxStringLen)
	copy(value, label)
	for j := len(label); j < len(value); j++ {
		value[j] = byte('a' + (i+j)%26)
	}
	payload = append(payload, deterministicTemplateV1KindString)
	payload = appendUvarint(payload, uint64(len(value)))
	payload = append(payload, value...)
	return payload
}

func reportCommandMetrics(b *testing.B, tc nativewireBenchmarkCase) {
	b.Helper()
	items := tc.items
	if items == 0 {
		items = 1
	}
	b.ReportMetric(float64(len(tc.body))/float64(items), "wire_B/item")
}

func assertMaxAllocs(t *testing.T, name string, max float64, fn func()) {
	t.Helper()
	if raceEnabled {
		t.Skipf("%s allocation guard is noisy under -race", name)
	}
	got := testing.AllocsPerRun(50, fn)
	if got > max {
		t.Fatalf("%s allocations=%0.2f want <= %0.2f", name, got, max)
	}
}
