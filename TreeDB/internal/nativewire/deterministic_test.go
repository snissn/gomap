package nativewire

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"
)

func TestDeterministicEntryGoldenAndTransportIndependence(t *testing.T) {
	registry := MustV1Registry()
	cmd0, err := registry.ValidateRequestSections(insertBatchDeterministicSections())
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry0, err := AppendDeterministicEntry(nil, cmd0)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	assertHexFixture(t, "insert_batch_entry.hex", entry0)

	sections := []Section{
		{ID: 9000, Bytes: []byte("ignored")},
		{ID: SectionTraceContext, Bytes: []byte("trace")},
		{ID: SectionDocuments, Bytes: AppendByteVector(nil, deterministicBSONEmptyDocument())},
		{ID: SectionDocumentFormat, Bytes: []byte{byte(DocumentFormatBSON)}},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionExpectedCatalogVersion, Bytes: deterministicUvarintPayload(deterministicFixtureCatalogVersion)},
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 1})},
		{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
		{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("c")},
	}
	cmd1, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections shuffled: %v", err)
	}
	entry1, err := AppendDeterministicEntry(nil, cmd1)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry shuffled: %v", err)
	}
	entry0Hex := hex.EncodeToString(entry0)
	entry1Hex := hex.EncodeToString(entry1)
	if entry1Hex != entry0Hex {
		t.Fatalf("deterministic entries differ:\n%s\n%s", entry0Hex, entry1Hex)
	}

	withResponseFlag := insertBatchDeterministicSections()
	withResponseFlag[0].Bytes = AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 1, Flags: CommandFlagOmitResultIDs | CommandFlagOmitResponseMeta})
	cmd2, err := registry.ValidateRequestSections(withResponseFlag)
	if err != nil {
		t.Fatalf("ValidateRequestSections response flag: %v", err)
	}
	entry2, err := AppendDeterministicEntry(nil, cmd2)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry response flag: %v", err)
	}
	if hex.EncodeToString(entry2) != hex.EncodeToString(entry0) {
		t.Fatalf("response-shaping flag changed deterministic entry:\n%s\n%s", hex.EncodeToString(entry0), hex.EncodeToString(entry2))
	}
}

func TestDecodeDeterministicEntryGolden(t *testing.T) {
	registry := MustV1Registry()
	cmd, err := registry.ValidateRequestSections(insertBatchDeterministicSections())
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entryBytes, err := AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	var scratch DeterministicEntryScratch
	entry, err := DecodeDeterministicEntryInto(entryBytes, Limits{}, &scratch)
	if err != nil {
		t.Fatalf("DecodeDeterministicEntryInto: %v", err)
	}
	if entry.Version != DeterministicEntryVersion || entry.CommandID != CommandInsertBatch || entry.CommandVersion != 1 || entry.CommandFlags != 0 {
		t.Fatalf("decoded entry header=%+v", entry)
	}
	if len(entry.Sections) != 6 || len(scratch.Sections) != 6 {
		t.Fatalf("decoded sections=%d scratch=%d want 6", len(entry.Sections), len(scratch.Sections))
	}
	wantIDs := []SectionID{
		SectionIdempotencyKey,
		SectionCollectionRef,
		SectionDocumentFormat,
		SectionDocumentIDs,
		SectionDocuments,
		SectionExpectedCatalogVersion,
	}
	for i, want := range wantIDs {
		if entry.Sections[i].ID != want {
			t.Fatalf("section %d id=%d want %d", i, entry.Sections[i].ID, want)
		}
	}
}

func TestDecodeDeterministicEntryWithRegistryUsesCallerRegistry(t *testing.T) {
	const (
		commandID = CommandID(9100)
		sectionID = SectionID(1200)
	)
	registry, err := NewRegistry(CommandSchema{
		ID:                  commandID,
		Version:             1,
		Name:                "custom_replicated",
		Kind:                CommandKindMutation,
		Replicated:          true,
		RequiresIdempotency: true,
		Sections: []SectionRule{
			{ID: sectionID, Name: "custom", Required: true, Deterministic: true},
		},
	})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	raw := deterministicEntryTestRaw(commandID,
		Section{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		Section{ID: sectionID, Bytes: []byte("payload")},
	)
	if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != ErrUnsupportedVersion {
		t.Fatalf("DecodeDeterministicEntry default err=%v code=%d want unsupported version", err, codeOf(err))
	}
	entry, err := DecodeDeterministicEntryWithRegistry(raw, Limits{}, registry)
	if err != nil {
		t.Fatalf("DecodeDeterministicEntryWithRegistry: %v", err)
	}
	if entry.CommandID != commandID || len(entry.Sections) != 2 {
		t.Fatalf("entry=%+v", entry)
	}
}

func TestDecodeDeterministicEntryHonorsMaxFrameSize(t *testing.T) {
	registry := MustV1Registry()
	cmd, err := registry.ValidateRequestSections(insertBatchDeterministicSections())
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	if _, err := DecodeDeterministicEntry(entry, Limits{MaxFrameSize: uint64(len(entry) - 1)}); codeOf(err) != ErrResourceExhausted {
		t.Fatalf("DecodeDeterministicEntry err=%v code=%d want resource exhausted", err, codeOf(err))
	}
}

func TestDecodeDeterministicEntryRejectsMalformedEnvelope(t *testing.T) {
	registry := MustV1Registry()
	cmd, err := registry.ValidateRequestSections(insertBatchDeterministicSections())
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	entryWithFlags := []byte("TDC1")
	entryWithFlags = appendUvarint(entryWithFlags, DeterministicEntryVersion)
	entryWithFlags = appendUvarint(entryWithFlags, uint64(CommandInsertBatch))
	entryWithFlags = appendUvarint(entryWithFlags, 1)
	entryWithFlags = appendUvarint(entryWithFlags, 1)
	entryWithFlags = appendUvarint(entryWithFlags, 0)
	unsupportedVersion := []byte("TDC1")
	unsupportedVersion = appendUvarint(unsupportedVersion, 2)
	sectionCountLimit := []byte("TDC1")
	sectionCountLimit = appendUvarint(sectionCountLimit, DeterministicEntryVersion)
	sectionCountLimit = appendUvarint(sectionCountLimit, uint64(CommandInsertBatch))
	sectionCountLimit = appendUvarint(sectionCountLimit, 1)
	sectionCountLimit = appendUvarint(sectionCountLimit, 0)
	sectionCountLimit = appendUvarint(sectionCountLimit, 2)
	sectionCountImpossible := []byte("TDC1")
	sectionCountImpossible = appendUvarint(sectionCountImpossible, DeterministicEntryVersion)
	sectionCountImpossible = appendUvarint(sectionCountImpossible, uint64(CommandInsertBatch))
	sectionCountImpossible = appendUvarint(sectionCountImpossible, 1)
	sectionCountImpossible = appendUvarint(sectionCountImpossible, 0)
	sectionCountImpossible = appendUvarint(sectionCountImpossible, 128)
	sectionCountHardLimit := []byte("TDC1")
	sectionCountHardLimit = appendUvarint(sectionCountHardLimit, DeterministicEntryVersion)
	sectionCountHardLimit = appendUvarint(sectionCountHardLimit, uint64(CommandInsertBatch))
	sectionCountHardLimit = appendUvarint(sectionCountHardLimit, 1)
	sectionCountHardLimit = appendUvarint(sectionCountHardLimit, 0)
	sectionCountHardLimit = appendUvarint(sectionCountHardLimit, maxDeterministicEntrySections+1)
	sectionLenIntLimit := []byte("TDC1")
	sectionLenIntLimit = appendUvarint(sectionLenIntLimit, DeterministicEntryVersion)
	sectionLenIntLimit = appendUvarint(sectionLenIntLimit, uint64(CommandInsertBatch))
	sectionLenIntLimit = appendUvarint(sectionLenIntLimit, 1)
	sectionLenIntLimit = appendUvarint(sectionLenIntLimit, 0)
	sectionLenIntLimit = appendUvarint(sectionLenIntLimit, 1)
	sectionLenIntLimit = appendUvarint(sectionLenIntLimit, uint64(SectionDocumentIDs))
	sectionLenIntLimit = appendUvarint(sectionLenIntLimit, uint64(maxInt)+1)
	for _, tc := range []struct {
		name   string
		raw    []byte
		limits Limits
		code   ErrorCode
	}{
		{name: "bad_magic", raw: []byte("bad"), code: ErrMalformedFrame},
		{name: "unsupported_version", raw: unsupportedVersion, code: ErrUnsupportedVersion},
		{name: "unsupported_flags", raw: entryWithFlags, code: ErrUnsupportedFeature},
		{name: "trailing", raw: append(append([]byte(nil), entry...), 0), code: ErrMalformedFrame},
		{name: "truncated_section", raw: append([]byte(nil), entry[:len(entry)-1]...), code: ErrMalformedFrame},
		{name: "section_count_limit", raw: sectionCountLimit, limits: Limits{MaxSections: 1}, code: ErrResourceExhausted},
		{name: "section_count_impossible", raw: sectionCountImpossible, code: ErrMalformedFrame},
		{name: "section_count_hard_limit", raw: sectionCountHardLimit, limits: Limits{MaxSections: maxDeterministicEntrySections + 2}, code: ErrResourceExhausted},
		{name: "section_len_int_limit", raw: sectionLenIntLimit, limits: Limits{MaxSectionLen: uint64(maxInt) + 1}, code: ErrResourceExhausted},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeDeterministicEntry(tc.raw, tc.limits); codeOf(err) != tc.code {
				t.Fatalf("DecodeDeterministicEntry err=%v code=%d want %d", err, codeOf(err), tc.code)
			}
		})
	}
}

func TestDecodeDeterministicEntryRejectsUnsortedSections(t *testing.T) {
	raw := []byte("TDC1")
	raw = appendUvarint(raw, DeterministicEntryVersion)
	raw = appendUvarint(raw, uint64(CommandInsertBatch))
	raw = appendUvarint(raw, 1)
	raw = appendUvarint(raw, 0)
	raw = appendUvarint(raw, 2)
	for _, id := range []SectionID{SectionDocuments, SectionDocumentIDs} {
		raw = appendUvarint(raw, uint64(id))
		raw = appendUvarint(raw, 0)
	}
	if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("DecodeDeterministicEntry err=%v code=%d want malformed", err, codeOf(err))
	}
}

func TestDecodeDeterministicEntryRejectsDuplicateSingletonSections(t *testing.T) {
	raw := deterministicEntryTestRaw(CommandInsertBatch,
		Section{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
		Section{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("b"))},
	)
	if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("DecodeDeterministicEntry err=%v code=%d want invalid command", err, codeOf(err))
	}
}

func TestDeterministicEntryReplicatedGoldenFixtures(t *testing.T) {
	registry := MustV1Registry()
	for _, tc := range deterministicEntryFixtureCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := registry.ValidateRequestSections(tc.sections)
			if err != nil {
				t.Fatalf("ValidateRequestSections: %v", err)
			}
			if cmd.Header.ID != tc.commandID {
				t.Fatalf("fixture command header=%d want %d", cmd.Header.ID, tc.commandID)
			}
			entry, err := AppendDeterministicEntry(nil, cmd)
			if err != nil {
				t.Fatalf("AppendDeterministicEntry: %v", err)
			}
			assertHexFixture(t, tc.fixture, entry)
			if decoded, err := DecodeDeterministicEntry(entry, Limits{}); err != nil {
				t.Fatalf("DecodeDeterministicEntry: %v", err)
			} else if decoded.CommandID != tc.commandID || decoded.CommandVersion != 1 {
				t.Fatalf("decoded header=%+v want command %d v1", decoded, tc.commandID)
			}
		})
	}
}

func TestDeterministicEntryFixturesCoverReplicatedCommands(t *testing.T) {
	covered := make(map[CommandID]string)
	for _, tc := range deterministicEntryFixtureCases() {
		if previous, ok := covered[tc.commandID]; ok {
			t.Fatalf("duplicate deterministic-entry fixture command %d in %s and %s", tc.commandID, previous, tc.name)
		}
		covered[tc.commandID] = tc.name
	}
	for _, schema := range v1CommandSchemas() {
		_, ok := covered[schema.ID]
		if schema.Replicated && !schema.LocalOnly && !ok {
			t.Fatalf("%s is replicated without deterministic-entry fixture", schema.Name)
		}
		if ok && (!schema.Replicated || schema.LocalOnly) {
			t.Fatalf("%s has deterministic-entry fixture but is not replicated", schema.Name)
		}
	}
}

func TestDecodeDeterministicEntryRejectsInvalidSectionPayload(t *testing.T) {
	raw := deterministicEntryTestRaw(CommandInsertBatch, Section{ID: SectionDocumentFormat, Bytes: []byte{99}})
	if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("DecodeDeterministicEntry err=%v code=%d want invalid command", err, codeOf(err))
	}
}

func TestDecodeDeterministicEntryRejectsInvalidCommandSet(t *testing.T) {
	for _, tc := range []struct {
		name      string
		commandID CommandID
		sections  []Section
	}{
		{
			name:      "read_command",
			commandID: CommandGetMany,
			sections: []Section{
				{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("c")},
				{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
			},
		},
		{
			name:      "missing_idempotency",
			commandID: CommandInsertBatch,
			sections:  deterministicEntrySectionsOnly(removeSection(insertBatchDeterministicSections(), SectionIdempotencyKey)),
		},
		{
			name:      "missing_catalog_guard",
			commandID: CommandInsertBatch,
			sections:  deterministicEntrySectionsOnly(removeSection(insertBatchDeterministicSections(), SectionExpectedCatalogVersion)),
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			raw := deterministicEntryTestRaw(tc.commandID, tc.sections...)
			if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != ErrInvalidCommand {
				t.Fatalf("DecodeDeterministicEntry err=%v code=%d want invalid command", err, codeOf(err))
			}
		})
	}
}

func TestDeterministicEntryRejectsLocalAndReadCommands(t *testing.T) {
	registry := MustV1Registry()
	for _, tc := range []struct {
		name     string
		sections []Section
	}{
		{
			name: "stats",
			sections: []Section{
				{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandStats, Version: 1})},
			},
		},
		{
			name: "flush_collection",
			sections: []Section{
				{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandFlushCollection, Version: 1})},
				{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("users")},
			},
		},
		{
			name: "get_many",
			sections: []Section{
				{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandGetMany, Version: 1})},
				{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("users")},
				{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := registry.ValidateRequestSections(tc.sections)
			if err != nil {
				t.Fatalf("ValidateRequestSections: %v", err)
			}
			_, err = AppendDeterministicEntry(nil, cmd)
			if err == nil {
				t.Fatal("AppendDeterministicEntry succeeded, want invalid command")
			}
			if codeOf(err) != ErrInvalidCommand {
				t.Fatalf("AppendDeterministicEntry err=%v code=%d want invalid command", err, codeOf(err))
			}
		})
	}
}

func TestDecodeDeterministicEntryPreservesPayloadLimits(t *testing.T) {
	for _, tc := range []struct {
		name    string
		section Section
	}{
		{
			name:    "document_ids",
			section: Section{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"), []byte("b"))},
		},
		{
			name:    "documents",
			section: Section{ID: SectionDocuments, Bytes: AppendByteVector(nil, []byte("{}"), []byte("{}"))},
		},
		{
			name:    "template_records",
			section: Section{ID: SectionTemplateRecords, Bytes: AppendByteVector(nil, []byte("bad1"), []byte("bad2"))},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sections := deterministicEntrySectionsOnly(insertBatchDeterministicSections())
			replaced := false
			for i := range sections {
				if sections[i].ID == tc.section.ID {
					sections[i] = tc.section
					replaced = true
				}
				if tc.section.ID == SectionDocumentIDs && sections[i].ID == SectionDocuments {
					sections[i].Bytes = AppendByteVector(nil, []byte("{}"), []byte("{}"))
				}
				if tc.section.ID == SectionDocuments && sections[i].ID == SectionDocumentIDs {
					sections[i].Bytes = AppendByteVector(nil, []byte("a"), []byte("b"))
				}
			}
			if !replaced {
				sections = append(sections, tc.section)
				sortSectionsByID(sections)
			}
			raw := deterministicEntryTestRaw(CommandInsertBatch, sections...)
			if _, err := DecodeDeterministicEntry(raw, Limits{MaxByteVectorItems: 1}); codeOf(err) != ErrResourceExhausted {
				t.Fatalf("DecodeDeterministicEntry err=%v code=%d want resource exhausted", err, codeOf(err))
			}
		})
	}
}

func TestDeterministicEntryStableAcrossTransportOnlySections(t *testing.T) {
	registry := MustV1Registry()
	cmd0, err := registry.ValidateRequestSections(insertBatchDeterministicSections())
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry0, err := AppendDeterministicEntry(nil, cmd0)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	variant := []Section{
		{ID: SectionAckPolicy, Bytes: deterministicUvarintPayload(uint64(AckSynced))},
		{ID: SectionConsistencyPolicy, Bytes: deterministicUvarintPayload(3)},
		{ID: SectionCompression, Bytes: []byte{1}},
		{ID: SectionTraceContext, Bytes: []byte("trace")},
		{ID: SectionDeadline, Bytes: []byte("deadline")},
		{ID: 9000, Bytes: []byte("ignored")},
		{ID: SectionDocuments, Bytes: AppendByteVector(nil, deterministicBSONEmptyDocument())},
		{ID: SectionDocumentFormat, Bytes: []byte{byte(DocumentFormatBSON)}},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionExpectedCatalogVersion, Bytes: deterministicUvarintPayload(deterministicFixtureCatalogVersion)},
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: deterministicFixtureCommandVersion, Flags: CommandFlagOmitResultIDs | CommandFlagOmitResponseMeta})},
		{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
		{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("c")},
	}
	cmd1, err := registry.ValidateRequestSections(variant)
	if err != nil {
		t.Fatalf("ValidateRequestSections variant: %v", err)
	}
	entry1, err := AppendDeterministicEntry(nil, cmd1)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry variant: %v", err)
	}
	if !bytes.Equal(entry0, entry1) {
		t.Fatalf("transport-only sections changed entry\ngot  %s\nwant %s", hex.EncodeToString(entry1), hex.EncodeToString(entry0))
	}
	if DeterministicEntryDigest(entry0) != DeterministicEntryDigest(entry1) {
		t.Fatal("same deterministic entry produced different digest")
	}
}

func TestDeterministicEntryDigestChangesWithLogicalMutation(t *testing.T) {
	registry := MustV1Registry()
	base := insertBatchDeterministicSections()
	cmd0, err := registry.ValidateRequestSections(base)
	if err != nil {
		t.Fatalf("ValidateRequestSections base: %v", err)
	}
	entry0, err := AppendDeterministicEntry(nil, cmd0)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry base: %v", err)
	}
	changed := replaceSection(insertBatchDeterministicSections(), SectionDocuments, AppendByteVector(nil, []byte(`{"changed":true}`)))
	cmd1, err := registry.ValidateRequestSections(changed)
	if err != nil {
		t.Fatalf("ValidateRequestSections changed: %v", err)
	}
	entry1, err := AppendDeterministicEntry(nil, cmd1)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry changed: %v", err)
	}
	if bytes.Equal(entry0, entry1) {
		t.Fatal("logical mutation change did not change deterministic entry")
	}
	if DeterministicEntryDigest(entry0) == DeterministicEntryDigest(entry1) {
		t.Fatal("logical mutation change did not change deterministic entry digest")
	}
}

func TestDeterministicEntryRejectsAmbiguousCommandPayloads(t *testing.T) {
	registry := MustV1Registry()
	for _, tc := range []struct {
		name     string
		sections []Section
		code     ErrorCode
	}{
		{
			name:     "empty_idempotency",
			sections: replaceSection(insertBatchDeterministicSections(), SectionIdempotencyKey, nil),
			code:     ErrInvalidCommand,
		},
		{
			name: "empty_collection_meta",
			sections: []Section{
				{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCreateCollection, Version: deterministicFixtureCommandVersion})},
				{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
				{ID: SectionCollectionMeta, Bytes: nil},
				{ID: SectionExpectedCatalogVersion, Bytes: deterministicUvarintPayload(deterministicFixtureCatalogVersion)},
			},
			code: ErrInvalidCommand,
		},
		{
			name:     "invalid_index_name",
			sections: replaceSection(deterministicEntryFixtureCases()[2].sections, SectionIndexName, appendDeterministicString(nil, "bad/name")),
			code:     ErrInvalidCommand,
		},
		{
			name:     "insert_ids_docs_mismatch",
			sections: replaceSection(insertBatchDeterministicSections(), SectionDocuments, AppendByteVector(nil, []byte("{}"), []byte("{}"))),
			code:     ErrInvalidCommand,
		},
		{
			name:     "replace_ids_docs_mismatch",
			sections: replaceSection(deterministicEntryFixtureCases()[4].sections, SectionDocuments, AppendByteVector(nil, []byte("{}"), []byte("{}"))),
			code:     ErrInvalidCommand,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := registry.ValidateRequestSections(tc.sections)
			if err != nil {
				t.Fatalf("ValidateRequestSections: %v", err)
			}
			_, err = AppendDeterministicEntry(nil, cmd)
			if codeOf(err) != tc.code {
				t.Fatalf("AppendDeterministicEntry err=%v code=%d want %d", err, codeOf(err), tc.code)
			}
		})
	}
}

func TestDecodeDeterministicEntryClearsScratchTail(t *testing.T) {
	scratch := &DeterministicEntryScratch{
		Sections: []Section{
			{ID: SectionDocumentFormat, Bytes: []byte{byte(DocumentFormatBSON)}},
			{ID: SectionDocuments, Bytes: []byte("stale")},
			{ID: SectionDocumentIDs, Bytes: []byte("stale")},
			{ID: SectionExpectedCatalogVersion, Bytes: []byte("stale")},
			{ID: SectionCollectionRef, Bytes: []byte("stale")},
			{ID: SectionIdempotencyKey, Bytes: []byte("stale")},
		},
	}
	sections := []Section{
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("c")},
		{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
		{ID: SectionExpectedCatalogVersion, Bytes: deterministicUvarintPayload(deterministicFixtureCatalogVersion)},
	}
	sortSectionsByID(sections)
	raw := deterministicEntryTestRaw(CommandDeleteBatch, sections...)
	if _, err := DecodeDeterministicEntryInto(raw, Limits{}, scratch); err != nil {
		t.Fatalf("DecodeDeterministicEntryInto: %v", err)
	}
	backing := scratch.Sections[:cap(scratch.Sections)]
	for i := len(scratch.Sections); i < len(backing); i++ {
		if backing[i].ID != 0 || backing[i].Bytes != nil {
			t.Fatalf("scratch tail[%d]=%+v want zero", i, backing[i])
		}
	}
}

func TestDeterministicEntryAcceptsMaxLengthEncodedIndexName(t *testing.T) {
	registry := MustV1Registry()
	limit := int(DefaultLimits().MaxDeterministicNameBytes)
	sections := deterministicEntryFixtureCases()[2].sections
	sections = replaceSection(sections, SectionIndexName, appendDeterministicString(nil, strings.Repeat("x", limit)))
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
}

func TestAppendDeterministicEntryWithLimitsHonorsNameLimit(t *testing.T) {
	registry := MustV1Registry()
	sections := deterministicEntryFixtureCases()[2].sections
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	_, err = AppendDeterministicEntryWithLimits(nil, cmd, Limits{MaxDeterministicNameBytes: 3})
	if codeOf(err) != ErrResourceExhausted {
		t.Fatalf("AppendDeterministicEntryWithLimits err=%v code=%d want resource exhausted", err, codeOf(err))
	}
	if _, err := AppendDeterministicEntryWithLimits(nil, cmd, DefaultLimits()); err != nil {
		t.Fatalf("AppendDeterministicEntryWithLimits default limits: %v", err)
	}
}

func TestAppendDeterministicEntryWithLimitsHonorsEnvelopeLimits(t *testing.T) {
	registry := MustV1Registry()
	cmd, err := registry.ValidateRequestSections(insertBatchDeterministicSections())
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := AppendDeterministicEntryWithLimits(nil, cmd, DefaultLimits())
	if err != nil {
		t.Fatalf("AppendDeterministicEntryWithLimits default limits: %v", err)
	}
	_, err = AppendDeterministicEntryWithLimits(nil, cmd, Limits{MaxSectionLen: 1})
	if codeOf(err) != ErrResourceExhausted {
		t.Fatalf("AppendDeterministicEntryWithLimits section err=%v code=%d want resource exhausted", err, codeOf(err))
	}
	_, err = AppendDeterministicEntryWithLimits(nil, cmd, Limits{MaxSections: 5})
	if codeOf(err) != ErrResourceExhausted {
		t.Fatalf("AppendDeterministicEntryWithLimits sections err=%v code=%d want resource exhausted", err, codeOf(err))
	}
	prefix := []byte("prefix")
	got, err := AppendDeterministicEntryWithLimits(append([]byte(nil), prefix...), cmd, Limits{MaxSections: 5})
	if codeOf(err) != ErrResourceExhausted {
		t.Fatalf("AppendDeterministicEntryWithLimits prefixed sections err=%v code=%d want resource exhausted", err, codeOf(err))
	}
	if !bytes.Equal(got, prefix) {
		t.Fatalf("AppendDeterministicEntryWithLimits returned %q after error, want %q", got, prefix)
	}
	_, err = AppendDeterministicEntryWithLimits(nil, cmd, Limits{MaxFrameSize: uint64(len(entry) - 1)})
	if codeOf(err) != ErrResourceExhausted {
		t.Fatalf("AppendDeterministicEntryWithLimits frame err=%v code=%d want resource exhausted", err, codeOf(err))
	}
	backing := make([]byte, len(prefix), len(prefix)+len(entry)+8)
	copy(backing, prefix)
	sentinel := bytes.Repeat([]byte{0xa5}, cap(backing)-len(prefix))
	backing = backing[:cap(backing)]
	copy(backing[len(prefix):], sentinel)
	got, err = AppendDeterministicEntryWithLimits(backing[:len(prefix)], cmd, Limits{MaxFrameSize: uint64(len(entry) - 1)})
	if codeOf(err) != ErrResourceExhausted {
		t.Fatalf("AppendDeterministicEntryWithLimits prefixed frame err=%v code=%d want resource exhausted", err, codeOf(err))
	}
	if !bytes.Equal(got, prefix) {
		t.Fatalf("AppendDeterministicEntryWithLimits returned %q after frame error, want %q", got, prefix)
	}
	if !bytes.Equal(backing[len(prefix):], sentinel) {
		t.Fatal("AppendDeterministicEntryWithLimits modified spare backing on frame-size failure")
	}
}

func TestAppendDeterministicEntryEnforcesHardSectionCap(t *testing.T) {
	const (
		commandID    = CommandID(9200)
		sectionStart = SectionID(1300)
	)
	rules := make([]SectionRule, 0, maxDeterministicEntrySections+1)
	sections := []Section{{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: commandID, Version: 1})}}
	for i := 0; i < maxDeterministicEntrySections+1; i++ {
		id := sectionStart + SectionID(i)
		rules = append(rules, SectionRule{ID: id, Name: "deterministic", Required: true, Deterministic: true})
		sections = append(sections, Section{ID: id, Bytes: []byte{byte(i)}})
	}
	registry, err := NewRegistry(CommandSchema{
		ID:         commandID,
		Version:    1,
		Name:       "too_many_deterministic_sections",
		Kind:       CommandKindMutation,
		Replicated: true,
		Sections:   rules,
	})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	_, err = AppendDeterministicEntryWithLimits(nil, cmd, Limits{MaxSections: maxDeterministicEntrySections + 1})
	if codeOf(err) != ErrResourceExhausted {
		t.Fatalf("AppendDeterministicEntryWithLimits err=%v code=%d want resource exhausted", err, codeOf(err))
	}
}

func TestDeterministicEntryRejectsInvalidEncodedIndexName(t *testing.T) {
	registry := MustV1Registry()
	limit := int(DefaultLimits().MaxDeterministicNameBytes)
	for _, tc := range []struct {
		name string
		raw  []byte
		code ErrorCode
	}{
		{name: "too_long", raw: appendDeterministicString(nil, strings.Repeat("x", limit+1)), code: ErrResourceExhausted},
		{name: "trailing", raw: append(appendDeterministicString(nil, "email_1"), 0), code: ErrMalformedFrame},
		{name: "empty_payload", raw: nil, code: ErrInvalidCommand},
		{name: "empty_name", raw: appendDeterministicString(nil, ""), code: ErrInvalidCommand},
		{name: "raw_string", raw: []byte("email_1"), code: ErrMalformedFrame},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sections := replaceSection(deterministicEntryFixtureCases()[2].sections, SectionIndexName, tc.raw)
			cmd, err := registry.ValidateRequestSections(sections)
			if err != nil {
				t.Fatalf("ValidateRequestSections: %v", err)
			}
			if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != tc.code {
				t.Fatalf("AppendDeterministicEntry err=%v code=%d want %d", err, codeOf(err), tc.code)
			}
		})
	}
}

func TestDecodeDeterministicEntryClearsScratchOnSchemaError(t *testing.T) {
	scratch := &DeterministicEntryScratch{
		Sections: make([]Section, 0, 4),
	}
	raw := deterministicEntryTestRaw(CommandInsertBatch,
		Section{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("c")},
		Section{ID: SectionDocumentFormat, Bytes: []byte{byte(DocumentFormatBSON)}},
		Section{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
		Section{ID: SectionDocuments, Bytes: AppendByteVector(nil, []byte("{}"))},
	)
	if _, err := DecodeDeterministicEntryInto(raw, Limits{}, scratch); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("DecodeDeterministicEntryInto err=%v code=%d want invalid command", err, codeOf(err))
	}
	if len(scratch.Sections) != 0 {
		t.Fatalf("scratch len=%d want 0", len(scratch.Sections))
	}
	backing := scratch.Sections[:cap(scratch.Sections)]
	for i, section := range backing {
		if section.ID != 0 || section.Bytes != nil {
			t.Fatalf("scratch backing[%d]=%+v want zero", i, section)
		}
	}
}

func TestDecodeDeterministicEntryClearsScratchOnMidDecodeError(t *testing.T) {
	scratch := &DeterministicEntryScratch{
		Sections: make([]Section, 0, 1),
	}
	sections := deterministicEntrySectionsOnly(insertBatchDeterministicSections())
	for i := range sections {
		switch sections[i].ID {
		case SectionDocumentIDs:
			sections[i].Bytes = AppendByteVector(nil, bytes.Repeat([]byte("x"), 1024))
		case SectionDocuments:
			sections[i].Bytes = []byte{1, 0x81, 0x00}
		}
	}
	raw := deterministicEntryTestRaw(CommandInsertBatch, sections...)
	if _, err := DecodeDeterministicEntryInto(raw, Limits{}, scratch); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("DecodeDeterministicEntryInto err=%v code=%d want malformed", err, codeOf(err))
	}
	if len(scratch.Sections) != 0 {
		t.Fatalf("scratch len=%d want 0", len(scratch.Sections))
	}
	backing := scratch.Sections[:cap(scratch.Sections)]
	for i, section := range backing {
		if section.ID != 0 || section.Bytes != nil {
			t.Fatalf("scratch backing[%d]=%+v want zero", i, section)
		}
	}
}

func TestDecodeDeterministicEntryDoesNotRetainAllocatedScratchOnDecodeError(t *testing.T) {
	scratch := &DeterministicEntryScratch{
		Sections: []Section{{ID: SectionDocuments, Bytes: bytes.Repeat([]byte("x"), 1024)}},
	}
	raw := []byte("TDC1")
	raw = appendUvarint(raw, DeterministicEntryVersion)
	raw = appendUvarint(raw, uint64(CommandInsertBatch))
	raw = appendUvarint(raw, 1)
	raw = appendUvarint(raw, 0)
	raw = appendUvarint(raw, 2)
	raw = appendUvarint(raw, uint64(SectionDocumentIDs))
	raw = appendUvarint(raw, 1)
	raw = append(raw, 0, 0)
	if _, err := DecodeDeterministicEntryInto(raw, Limits{}, scratch); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("DecodeDeterministicEntryInto err=%v code=%d want malformed", err, codeOf(err))
	}
	if len(scratch.Sections) != 0 {
		t.Fatalf("scratch len=%d want 0", len(scratch.Sections))
	}
	if cap(scratch.Sections) != 1 {
		t.Fatalf("scratch cap=%d want original cap 1", cap(scratch.Sections))
	}
	if section := scratch.Sections[:cap(scratch.Sections)][0]; section.ID != 0 || section.Bytes != nil {
		t.Fatalf("scratch backing retained stale section: %+v", section)
	}
}

func TestDecodeDeterministicEntryClearsScratchOnHeaderError(t *testing.T) {
	scratch := &DeterministicEntryScratch{
		Sections: []Section{
			{ID: SectionDocuments, Bytes: bytes.Repeat([]byte("x"), 1024)},
			{ID: SectionDocumentIDs, Bytes: []byte("stale")},
		},
	}
	if _, err := DecodeDeterministicEntryInto([]byte("bad"), Limits{}, scratch); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("DecodeDeterministicEntryInto err=%v code=%d want malformed", err, codeOf(err))
	}
	if len(scratch.Sections) != 0 {
		t.Fatalf("scratch len=%d want 0", len(scratch.Sections))
	}
	if cap(scratch.Sections) != 0 {
		t.Fatalf("scratch cap=%d want 0 after header failure", cap(scratch.Sections))
	}
}

func TestDeterministicEntryRejectsMissingDistributedGuards(t *testing.T) {
	registry := MustV1Registry()

	sections := removeSection(insertBatchDeterministicSections(), SectionIdempotencyKey)
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing idempotency err=%v code=%d", err, codeOf(err))
	}

	sections = removeSection(insertBatchDeterministicSections(), SectionExpectedCatalogVersion)
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing catalog guard err=%v code=%d", err, codeOf(err))
	}
}

func TestDeterministicEntryRejectsEmptyIdempotencyKey(t *testing.T) {
	registry := MustV1Registry()
	sections := insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionIdempotencyKey {
			sections[i].Bytes = nil
		}
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("AppendDeterministicEntry err=%v code=%d want invalid command", err, codeOf(err))
	}
	raw := deterministicEntryTestRaw(CommandInsertBatch, deterministicEntrySectionsOnly(sections)...)
	if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("DecodeDeterministicEntry err=%v code=%d want invalid command", err, codeOf(err))
	}
}

func TestDeterministicEntryRequiresGuardsInCanonicalSections(t *testing.T) {
	registry, err := NewRegistry(CommandSchema{
		ID:                   CommandInsertBatch,
		Version:              1,
		Name:                 "insert_batch",
		Kind:                 CommandKindMutation,
		Replicated:           true,
		RequiresIdempotency:  true,
		RequiresCatalogGuard: true,
		Sections: []SectionRule{
			{ID: SectionCollectionRef, Name: "collection_ref", Required: true, Deterministic: true},
			{ID: SectionDocumentFormat, Name: "document_format", Required: true, Deterministic: true},
			{ID: SectionDocumentIDs, Name: "document_ids", Required: true, Deterministic: true},
			{ID: SectionDocuments, Name: "documents", Required: true, Deterministic: true},
			{ID: SectionExpectedCatalogVersion, Name: "expected_catalog_version", Required: true},
			{ID: SectionIdempotencyKey, Name: "idempotency_key", Required: true},
		},
	})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	cmd, err := registry.ValidateRequestSections(insertBatchDeterministicSections())
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("non-deterministic guards err=%v code=%d", err, codeOf(err))
	}
}

func TestDeterministicEntryPreservesRepeatableSectionOrder(t *testing.T) {
	const (
		commandID = CommandID(9000)
		sectionID = SectionID(1000)
	)
	registry, err := NewRegistry(CommandSchema{
		ID:         commandID,
		Version:    1,
		Name:       "repeatable_test",
		Kind:       CommandKindMutation,
		Replicated: true,
		Sections: []SectionRule{
			{ID: sectionID, Name: "repeatable", Required: true, Repeatable: true, Deterministic: true},
		},
	})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	sections := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: commandID, Version: 1})},
		{ID: sectionID, Bytes: []byte("first")},
		{ID: sectionID, Bytes: []byte("second")},
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	firstAt := bytes.Index(entry, []byte("first"))
	secondAt := bytes.Index(entry, []byte("second"))
	if firstAt < 0 || secondAt < 0 || firstAt > secondAt {
		t.Fatalf("repeatable section order not preserved: first=%d second=%d entry=%x", firstAt, secondAt, entry)
	}
}

func deterministicEntryTestRaw(commandID CommandID, sections ...Section) []byte {
	raw := []byte("TDC1")
	raw = appendUvarint(raw, DeterministicEntryVersion)
	raw = appendUvarint(raw, uint64(commandID))
	raw = appendUvarint(raw, 1)
	raw = appendUvarint(raw, 0)
	raw = appendUvarint(raw, uint64(len(sections)))
	for _, section := range sections {
		raw = appendUvarint(raw, uint64(section.ID))
		raw = appendUvarint(raw, uint64(len(section.Bytes)))
		raw = append(raw, section.Bytes...)
	}
	return raw
}

func deterministicEntrySectionsOnly(sections []Section) []Section {
	out := make([]Section, 0, len(sections))
	for _, section := range sections {
		if section.ID != SectionCommandHeader {
			out = append(out, section)
		}
	}
	sortSectionsByID(out)
	return out
}

func TestDeterministicEntryRejectsUnsupportedCommandFlags(t *testing.T) {
	registry := registryWithInsertBatchAllowedFlags(1 << 32)
	sections := insertBatchDeterministicSections()
	sections[0].Bytes = AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 1, Flags: 1 << 32})
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrUnsupportedFeature {
		t.Fatalf("command flags err=%v code=%d", err, codeOf(err))
	}
}

func TestDeterministicEntryRequiresMetadataCatalogGuard(t *testing.T) {
	registry := MustV1Registry()
	sections := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCreateCollection, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionCollectionMeta, Bytes: deterministicCollectionMetaPayload("users")},
	}
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing metadata catalog guard err=%v code=%d", err, codeOf(err))
	}

	sections = append(sections, Section{ID: SectionExpectedCatalogVersion, Bytes: deterministicUvarintPayload(deterministicFixtureCatalogVersion)})
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections guarded: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); err != nil {
		t.Fatalf("AppendDeterministicEntry guarded: %v", err)
	}
}

func TestDeterministicEntryRejectsMalformedMetadataPayloads(t *testing.T) {
	registry := MustV1Registry()
	sections := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCreateCollection, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionExpectedCatalogVersion, Bytes: deterministicUvarintPayload(deterministicFixtureCatalogVersion)},
		{ID: SectionCollectionMeta, Bytes: []byte{1}},
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("malformed collection_meta err=%v code=%d", err, codeOf(err))
	}
}

func TestDecodeDeterministicEntryRejectsCollectionMetaApplyDecoderConstraints(t *testing.T) {
	for _, tc := range []struct {
		name string
		meta []byte
		code ErrorCode
	}{
		{
			name: "unsupported_document_format",
			meta: deterministicCollectionMetaPayloadWithOptions("users", deterministicCollectionMetaOptions{
				documentFormat: 99,
			}),
			code: ErrInvalidCommand,
		},
		{
			name: "unsupported_root_storage",
			meta: deterministicCollectionMetaPayloadWithOptions("users", deterministicCollectionMetaOptions{
				dataRootStorage: 99,
			}),
			code: ErrInvalidCommand,
		},
		{
			name: "unsupported_index_state_storage",
			meta: deterministicCollectionMetaPayloadWithOptions("users", deterministicCollectionMetaOptions{
				indexRootStorage: 99,
			}),
			code: ErrInvalidCommand,
		},
		{
			name: "negative_max_documents",
			meta: deterministicCollectionMetaPayloadWithOptions("users", deterministicCollectionMetaOptions{
				maxDocuments: -1,
			}),
			code: ErrInvalidCommand,
		},
		{
			name: "negative_max_bytes",
			meta: deterministicCollectionMetaPayloadWithOptions("users", deterministicCollectionMetaOptions{
				maxBytes: -1,
			}),
			code: ErrInvalidCommand,
		},
		{
			name: "too_many_indexes",
			meta: deterministicCollectionMetaPayloadWithOptions("users", deterministicCollectionMetaOptions{
				indexCount: maxDeterministicCollectionIndexes + 1,
			}),
			code: ErrResourceExhausted,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := deterministicMetadataEntryRaw(CommandCreateCollection, SectionCollectionMeta, tc.meta)
			if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != tc.code {
				t.Fatalf("DecodeDeterministicEntry err=%v code=%d want %d", err, codeOf(err), tc.code)
			}
		})
	}
}

func TestDeterministicCollectionMetaRejectsUnknownScalarEnums(t *testing.T) {
	registry := MustV1Registry()
	for _, tc := range []struct {
		name        string
		docFormat   uint64
		dataStorage uint64
		indexState  uint64
	}{
		{name: "document_format", docFormat: 99},
		{name: "data_root_storage", dataStorage: 99},
		{name: "index_state_storage", indexState: 99},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sections := []Section{
				{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCreateCollection, Version: 1})},
				{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
				{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
				{ID: SectionCollectionMeta, Bytes: deterministicCollectionMetaPayloadWithScalars("users", tc.docFormat, tc.dataStorage, tc.indexState)},
			}
			cmd, err := registry.ValidateRequestSections(sections)
			if err != nil {
				t.Fatalf("ValidateRequestSections: %v", err)
			}
			if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
				t.Fatalf("unknown scalar enum err=%v code=%d want invalid command", err, codeOf(err))
			}
		})
	}
}

func TestDecodeDeterministicEntryRejectsIndexDefinitionApplyDecoderConstraints(t *testing.T) {
	for _, tc := range []struct {
		name  string
		index []byte
	}{
		{
			name:  "unsupported_value_type",
			index: deterministicIndexDefinitionPayload("email", "email", 99, false, false, 0),
		},
		{
			name:  "unsupported_storage_policy",
			index: deterministicIndexDefinitionPayload("email", "email", deterministicIndexValueString, false, false, 99),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := deterministicMetadataEntryRaw(CommandCreateIndex, SectionIndexDefinition, tc.index)
			if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != ErrInvalidCommand {
				t.Fatalf("DecodeDeterministicEntry err=%v code=%d want invalid command", err, codeOf(err))
			}
		})
	}
}

func deterministicMetadataEntryRaw(command CommandID, sectionID SectionID, payload []byte) []byte {
	sections := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: command, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionExpectedCatalogVersion, Bytes: deterministicUvarintPayload(deterministicFixtureCatalogVersion)},
		{ID: sectionID, Bytes: payload},
	}
	if command == CommandCreateIndex {
		sections = append(sections, Section{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("users")})
	}
	return deterministicEntryTestRaw(command, deterministicEntrySectionsOnly(sections)...)
}

func TestDeterministicMetadataRequiresTaggedCollectionName(t *testing.T) {
	registry := MustV1Registry()
	sections := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCreateIndex, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionExpectedCatalogVersion, Bytes: deterministicUvarintPayload(deterministicFixtureCatalogVersion)},
		{ID: SectionCollectionRef, Bytes: []byte("users")},
		{ID: SectionIndexDefinition, Bytes: deterministicIndexDefinitionPayload("email", "email", deterministicIndexValueString, false, false, 0)},
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("raw metadata collection_ref err=%v code=%d", err, codeOf(err))
	}
	for i := range sections {
		if sections[i].ID == SectionCollectionRef {
			sections[i].Bytes = append([]byte{deterministicCollectionRefTagName}, []byte("users")...)
		}
	}
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections tagged: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); err != nil {
		t.Fatalf("AppendDeterministicEntry tagged: %v", err)
	}
}

func TestDeterministicIndexDefinitionRejectsUnknownEnums(t *testing.T) {
	registry := MustV1Registry()
	for _, tc := range []struct {
		name        string
		valueType   uint64
		storageEnum uint64
	}{
		{name: "index_value_type", valueType: 99},
		{name: "storage_policy", valueType: 1, storageEnum: 99},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sections := []Section{
				{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCreateIndex, Version: 1})},
				{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
				{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
				{ID: SectionCollectionRef, Bytes: append([]byte{deterministicCollectionRefTagName}, []byte("users")...)},
				{ID: SectionIndexDefinition, Bytes: deterministicIndexDefinitionPayloadWithEnums("email", "email", tc.valueType, tc.storageEnum)},
			}
			cmd, err := registry.ValidateRequestSections(sections)
			if err != nil {
				t.Fatalf("ValidateRequestSections: %v", err)
			}
			if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
				t.Fatalf("unknown index enum err=%v code=%d want invalid command", err, codeOf(err))
			}
		})
	}
}

func TestDeterministicEntryRejectsDuplicateIdempotencyInValidatedView(t *testing.T) {
	registry := MustV1Registry()
	cmd, err := registry.ValidateRequestSections(insertBatchDeterministicSections())
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	cmd.Known = append(cmd.Known, Section{ID: SectionIdempotencyKey, Bytes: []byte("id2")})
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("duplicate idempotency err=%v code=%d", err, codeOf(err))
	}
}

func TestDeterministicByteVectorCountRejectsDuplicateSections(t *testing.T) {
	sections := []Section{
		{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
		{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("b"))},
	}
	if _, err := deterministicByteVectorCount(sections, SectionDocumentIDs); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("duplicate deterministic vector err=%v code=%d", err, codeOf(err))
	}
}

func TestDeterministicEntryRejectsCollectionHandleRefs(t *testing.T) {
	registry := MustV1Registry()
	sections := insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionCollectionRef {
			sections[i].Bytes = []byte{2, 1}
		}
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("collection handle ref err=%v code=%d", err, codeOf(err))
	}
}

func TestDeterministicEntryRejectsInvalidCollectionNames(t *testing.T) {
	registry := MustV1Registry()
	for _, tc := range []struct {
		raw  []byte
		code ErrorCode
	}{
		{raw: []byte("bad/name"), code: ErrMalformedFrame},
		{raw: []byte{3, 'c'}, code: ErrMalformedFrame},
		{raw: []byte{1}, code: ErrInvalidCommand},
		{raw: deterministicCollectionNameRef(" c"), code: ErrInvalidCommand},
	} {
		sections := insertBatchDeterministicSections()
		for i := range sections {
			if sections[i].ID == SectionCollectionRef {
				sections[i].Bytes = tc.raw
			}
		}
		cmd, err := registry.ValidateRequestSections(sections)
		if err != nil {
			t.Fatalf("ValidateRequestSections: %v", err)
		}
		if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != tc.code {
			t.Fatalf("invalid collection ref %x err=%v code=%d want %d", tc.raw, err, codeOf(err), tc.code)
		}
	}
}

func TestDeterministicEntryAcceptsUTF8CollectionNames(t *testing.T) {
	registry := MustV1Registry()
	sections := insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionCollectionRef {
			sections[i].Bytes = deterministicCollectionNameRef("用户")
		}
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
}

func TestDeterministicEntryRejectsNonCanonicalSectionPayloads(t *testing.T) {
	registry := MustV1Registry()
	sections := insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionDocumentIDs {
			sections[i].Bytes = []byte{1, 0x81, 0x00, 'a'}
		}
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("non-canonical byte-vector err=%v code=%d", err, codeOf(err))
	}

	sections = insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionExpectedCatalogVersion {
			sections[i].Bytes = []byte{0x87, 0x00}
		}
	}
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections expected version: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("non-canonical catalog guard err=%v code=%d", err, codeOf(err))
	}

	sections = insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionDocumentFormat {
			sections[i].Bytes = []byte{0x82, 0x00}
		}
	}
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections document format: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("non-canonical document_format err=%v code=%d", err, codeOf(err))
	}

	sections = insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionDocumentFormat {
			sections[i].Bytes = []byte{byte(DocumentFormatBSON), 0}
		}
	}
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections document format trailing: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("trailing document_format err=%v code=%d", err, codeOf(err))
	}

	sections = insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionDocumentFormat {
			sections[i].Bytes = []byte{99}
		}
	}
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections unsupported document format: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("unsupported document_format err=%v code=%d", err, codeOf(err))
	}
}

func TestDeterministicEntryStripsAckPolicy(t *testing.T) {
	registry := MustV1Registry()
	baseSections := insertBatchDeterministicSections()
	cmd, err := registry.ValidateRequestSections(baseSections)
	if err != nil {
		t.Fatalf("ValidateRequestSections base: %v", err)
	}
	base, err := AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry base: %v", err)
	}

	for _, policy := range []AckPolicy{AckVisible, AckFlushed, AckSynced} {
		sections := append(insertBatchDeterministicSections(), Section{ID: SectionAckPolicy, Bytes: deterministicUvarintPayload(uint64(policy))})
		cmd, err := registry.ValidateRequestSections(sections)
		if err != nil {
			t.Fatalf("ValidateRequestSections ack=%d: %v", policy, err)
		}
		entry, err := AppendDeterministicEntry(nil, cmd)
		if err != nil {
			t.Fatalf("AppendDeterministicEntry ack=%d: %v", policy, err)
		}
		if !bytes.Equal(entry, base) {
			t.Fatalf("ack_policy=%d changed deterministic entry\ngot  %s\nwant %s", policy, hex.EncodeToString(entry), hex.EncodeToString(base))
		}
		if DeterministicEntryDigest(entry) != DeterministicEntryDigest(base) {
			t.Fatalf("ack_policy=%d changed deterministic digest", policy)
		}
	}
}

func TestDecodeDeterministicEntryRejectsAckPolicySection(t *testing.T) {
	sections := append(deterministicEntrySectionsOnly(insertBatchDeterministicSections()),
		Section{ID: SectionAckPolicy, Bytes: deterministicUvarintPayload(uint64(AckVisible))},
	)
	sortSectionsByID(sections)
	raw := deterministicEntryTestRaw(CommandInsertBatch, sections...)
	if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("DecodeDeterministicEntry ack_policy err=%v code=%d want invalid command", err, codeOf(err))
	}
}

func TestDeterministicEntryRejectsBatchVectorArityMismatch(t *testing.T) {
	registry := MustV1Registry()
	sections := insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionDocuments {
			sections[i].Bytes = AppendByteVector(nil, []byte("{}"), []byte("{}"))
		}
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("arity mismatch err=%v code=%d", err, codeOf(err))
	}
}

func TestDecodeDeterministicEntryRejectsUnappliableTemplateRecords(t *testing.T) {
	record := deterministicTemplateRecord("email")
	for _, tc := range []struct {
		name   string
		format DocumentFormat
		record []byte
		doc    []byte
		code   ErrorCode
	}{
		{
			name:   "non_template_format",
			format: DocumentFormatBSON,
			record: record,
			code:   ErrInvalidCommand,
		},
		{
			name:   "malformed_record",
			format: DocumentFormatTemplateV1,
			record: []byte("not-a-template-record"),
			code:   ErrMalformedFrame,
		},
		{
			name:   "raw_document",
			format: DocumentFormatTemplateV1,
			record: record,
			doc:    []byte("{}"),
			code:   ErrMalformedFrame,
		},
		{
			name:   "unknown_value_kind",
			format: DocumentFormatTemplateV1,
			record: record,
			doc:    deterministicTemplateStoredDocumentForRecord(record, []byte{99}),
			code:   ErrMalformedFrame,
		},
		{
			name:   "missing_value",
			format: DocumentFormatTemplateV1,
			record: record,
			doc:    deterministicTemplateStoredDocumentForRecord(record, nil),
			code:   ErrMalformedFrame,
		},
		{
			name:   "extra_value",
			format: DocumentFormatTemplateV1,
			record: record,
			doc:    deterministicTemplateStoredDocumentForRecord(record, []byte{deterministicTemplateV1KindNull, deterministicTemplateV1KindNull}),
			code:   ErrMalformedFrame,
		},
		{
			name:   "template_count_exceeds_limit",
			format: DocumentFormatTemplateV1,
			record: record,
			doc:    deterministicTemplateInputWithCount(deterministicTemplateV1MaxTemplates + 1),
			code:   ErrResourceExhausted,
		},
		{
			name:   "template_count_exceeds_remaining",
			format: DocumentFormatTemplateV1,
			record: record,
			doc:    deterministicTemplateInputWithCount(1),
			code:   ErrMalformedFrame,
		},
		{
			name:   "nested_depth",
			format: DocumentFormatTemplateV1,
			record: record,
			doc:    deterministicTemplateStoredDocumentForRecord(record, deterministicTemplateNestedObjectPayload(record, deterministicTemplateV1MaxDepth+1)),
			code:   ErrResourceExhausted,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sections := insertBatchDeterministicSections()
			for i := range sections {
				if sections[i].ID == SectionDocumentFormat {
					sections[i].Bytes = []byte{byte(tc.format)}
				}
				if tc.doc != nil && sections[i].ID == SectionDocuments {
					sections[i].Bytes = AppendByteVector(nil, tc.doc)
				}
			}
			sections = append(sections, Section{ID: SectionTemplateRecords, Bytes: AppendByteVector(nil, tc.record)})
			raw := deterministicEntryTestRaw(CommandInsertBatch, deterministicEntrySectionsOnly(sections)...)
			if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != tc.code {
				t.Fatalf("DecodeDeterministicEntry err=%v code=%d want %d", err, codeOf(err), tc.code)
			}
		})
	}
}

func TestDecodeDeterministicEntryAllowsStoredTemplateReferencesOutsideSidecar(t *testing.T) {
	sidecarRecord := deterministicTemplateRecord("email")
	persistedRecord := deterministicTemplateRecord("name")
	for _, tc := range []struct {
		name string
		doc  []byte
	}{
		{
			name: "td1d_root",
			doc:  deterministicTemplateStoredDocumentForRecord(persistedRecord, []byte{deterministicTemplateV1KindNull}),
		},
		{
			name: "td1d_nested_object",
			doc: deterministicTemplateStoredDocumentForRecord(sidecarRecord,
				deterministicTemplateObjectPayload(persistedRecord, []byte{deterministicTemplateV1KindNull})),
		},
		{
			name: "td1i_root_without_embedded_record",
			doc: append(deterministicTemplateInputWithCount(0),
				deterministicTemplateStoredDocumentForRecord(persistedRecord, []byte{deterministicTemplateV1KindNull})...),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sections := insertBatchDeterministicSections()
			for i := range sections {
				if sections[i].ID == SectionDocumentFormat {
					sections[i].Bytes = []byte{byte(DocumentFormatTemplateV1)}
				}
				if sections[i].ID == SectionDocuments {
					sections[i].Bytes = AppendByteVector(nil, tc.doc)
				}
			}
			sections = append(sections, Section{ID: SectionTemplateRecords, Bytes: AppendByteVector(nil, sidecarRecord)})
			raw := deterministicEntryTestRaw(CommandInsertBatch, deterministicEntrySectionsOnly(sections)...)
			if _, err := DecodeDeterministicEntry(raw, Limits{}); err != nil {
				t.Fatalf("DecodeDeterministicEntry: %v", err)
			}
		})
	}
}

func TestDecodeDeterministicEntryRejectsMalformedTemplateDocuments(t *testing.T) {
	sections := insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionDocumentFormat {
			sections[i].Bytes = []byte{byte(DocumentFormatTemplateV1)}
		}
		if sections[i].ID == SectionDocuments {
			sections[i].Bytes = AppendByteVector(nil, []byte(deterministicTemplateV1StoredMagic))
		}
	}
	sections = append(sections, Section{ID: SectionTemplateRecords, Bytes: AppendByteVector(nil, deterministicTemplateRecord("email"))})
	raw := deterministicEntryTestRaw(CommandInsertBatch, deterministicEntrySectionsOnly(sections)...)
	if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("DecodeDeterministicEntry err=%v code=%d want malformed frame", err, codeOf(err))
	}
}

func TestDeterministicEntryRejectsUnsupportedReplacementMode(t *testing.T) {
	registry := MustV1Registry()
	sections := replaceBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionReplacementMode {
			sections[i].Bytes = []byte{2}
		}
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("AppendDeterministicEntry replacement_mode err=%v code=%d want invalid command", err, codeOf(err))
	}

	raw := deterministicEntryTestRaw(CommandReplaceBatch, deterministicEntrySectionsOnly(sections)...)
	if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("DecodeDeterministicEntry replacement_mode err=%v code=%d want invalid command", err, codeOf(err))
	}
}

func TestDeterministicEntryRejectsInvalidDocumentIDs(t *testing.T) {
	registry := MustV1Registry()
	for _, tc := range []struct {
		name string
		ids  [][]byte
		code ErrorCode
	}{
		{name: "empty", ids: [][]byte{[]byte("a"), nil}, code: ErrInvalidCommand},
		{name: "duplicate", ids: [][]byte{[]byte("a"), []byte("a")}, code: ErrDuplicateDocumentID},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sections := insertBatchDeterministicSections()
			for i := range sections {
				if sections[i].ID == SectionDocumentIDs {
					sections[i].Bytes = AppendByteVector(nil, tc.ids...)
				}
			}
			cmd, err := registry.ValidateRequestSections(sections)
			if err != nil {
				t.Fatalf("ValidateRequestSections: %v", err)
			}
			if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != tc.code {
				t.Fatalf("AppendDeterministicEntry err=%v code=%d want %d", err, codeOf(err), tc.code)
			}
		})
	}
}

func TestDeterministicEntryRejectsTooManyDocumentIDs(t *testing.T) {
	raw := appendUvarint(nil, maxDeterministicDocumentIDs+1)
	for i := 0; i < maxDeterministicDocumentIDs+1; i++ {
		raw = append(raw, 0)
	}
	if err := validateDeterministicDocumentIDs(raw, Limits{}); codeOf(err) != ErrResourceExhausted {
		t.Fatalf("validateDeterministicDocumentIDs err=%v code=%d want resource exhausted", err, codeOf(err))
	}
}

func TestDeterministicDropIndexValidatesEncodedIndexName(t *testing.T) {
	registry := MustV1Registry()
	sections := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandDropIndex, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionExpectedCatalogVersion, Bytes: deterministicUvarintPayload(deterministicFixtureCatalogVersion)},
		{ID: SectionCollectionRef, Bytes: append([]byte{deterministicCollectionRefTagName}, []byte("users")...)},
		{ID: SectionIndexName, Bytes: []byte{1, 'e', 'x'}},
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("malformed encoded index name err=%v code=%d", err, codeOf(err))
	}

	for i := range sections {
		if sections[i].ID == SectionIndexName {
			sections[i].Bytes = appendDeterministicString(nil, "email")
		}
	}
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections valid: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); err != nil {
		t.Fatalf("AppendDeterministicEntry valid: %v", err)
	}
}

func TestDeterministicIndexDefinitionRejectsInvalidIndexPaths(t *testing.T) {
	registry := MustV1Registry()
	sections := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCreateIndex, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionExpectedCatalogVersion, Bytes: deterministicUvarintPayload(deterministicFixtureCatalogVersion)},
		{ID: SectionCollectionRef, Bytes: append([]byte{deterministicCollectionRefTagName}, []byte("users")...)},
		{ID: SectionIndexDefinition, Bytes: deterministicIndexDefinitionPayload("email", ".email", deterministicIndexValueString, false, false, 0)},
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("invalid index path err=%v code=%d", err, codeOf(err))
	}
}

func registryWithInsertBatchAllowedFlags(flags uint64) *Registry {
	schemas := v1CommandSchemas()
	for i := range schemas {
		if schemas[i].ID == CommandInsertBatch {
			schemas[i].AllowedCommandFlags = flags
		}
	}
	r, err := NewRegistry(schemas...)
	if err != nil {
		panic(err)
	}
	return r
}

func insertBatchDeterministicSections() []Section {
	return deterministicFixtureSections(CommandInsertBatch, deterministicInsertBatchIdempotency,
		Section{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("c")},
		Section{ID: SectionDocumentFormat, Bytes: []byte{byte(DocumentFormatBSON)}},
		Section{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
		Section{ID: SectionDocuments, Bytes: AppendByteVector(nil, deterministicBSONEmptyDocument())},
	)
}

func replaceSection(sections []Section, id SectionID, raw []byte) []Section {
	out := append([]Section(nil), sections...)
	for i := range out {
		if out[i].ID == id {
			out[i].Bytes = raw
			return out
		}
	}
	return append(out, Section{ID: id, Bytes: raw})
}

func replaceBatchDeterministicSections() []Section {
	sections := insertBatchDeterministicSections()
	sections[0].Bytes = AppendCommandHeader(nil, CommandHeader{ID: CommandReplaceBatch, Version: 1})
	sections = append(sections, Section{ID: SectionReplacementMode, Bytes: []byte{deterministicReplacementExisting}})
	return sections
}

func updateBSONSetDeterministicSections() []Section {
	return deterministicFixtureSections(CommandUpdateBSONSet, "client-a:update-bson-set:1",
		Section{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("c")},
		Section{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
		Section{ID: SectionUpdateFieldNames, Bytes: AppendByteVector(nil, []byte("x"))},
		Section{ID: SectionUpdateFieldValues, Bytes: AppendByteVector(nil, deterministicBSONSetValueInt32(2))},
	)
}

func deterministicCollectionNameRef(name string) []byte {
	return append([]byte{1}, name...)
}

type deterministicEntryFixtureCase struct {
	name      string
	commandID CommandID
	fixture   string
	sections  []Section
}

func deterministicEntryFixtureCases() []deterministicEntryFixtureCase {
	return []deterministicEntryFixtureCase{
		{
			name:      "create_collection",
			commandID: CommandCreateCollection,
			fixture:   "create_collection_entry.hex",
			sections: deterministicFixtureSections(CommandCreateCollection, "client-a:create:users",
				Section{ID: SectionCollectionMeta, Bytes: deterministicCollectionMetaPayload("users")},
			),
		},
		{
			name:      "create_index",
			commandID: CommandCreateIndex,
			fixture:   "create_index_entry.hex",
			sections: deterministicFixtureSections(CommandCreateIndex, "client-a:create-index:email",
				Section{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("users")},
				Section{ID: SectionIndexDefinition, Bytes: deterministicIndexDefinitionPayload("email_1", "email", deterministicIndexValueString, true, false, 0)},
			),
		},
		{
			name:      "drop_index",
			commandID: CommandDropIndex,
			fixture:   "drop_index_entry.hex",
			sections: deterministicFixtureSections(CommandDropIndex, "client-a:drop-index:email",
				Section{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("users")},
				Section{ID: SectionIndexName, Bytes: appendDeterministicString(nil, "email_1")},
			),
		},
		{
			name:      "insert_batch",
			commandID: CommandInsertBatch,
			fixture:   "insert_batch_entry.hex",
			sections:  insertBatchDeterministicSections(),
		},
		{
			name:      "replace_batch",
			commandID: CommandReplaceBatch,
			fixture:   "replace_batch_entry.hex",
			sections: deterministicFixtureSections(CommandReplaceBatch, "client-a:replace:1",
				Section{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("c")},
				Section{ID: SectionDocumentFormat, Bytes: []byte{byte(DocumentFormatBSON)}},
				Section{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
				Section{ID: SectionDocuments, Bytes: AppendByteVector(nil, deterministicBSONDocumentXInt32(1))},
				Section{ID: SectionReplacementMode, Bytes: deterministicUvarintPayload(deterministicReplacementModeExistingOnly)},
			),
		},
		{
			name:      "delete_batch",
			commandID: CommandDeleteBatch,
			fixture:   "delete_batch_entry.hex",
			sections: deterministicFixtureSections(CommandDeleteBatch, "client-a:delete:1",
				Section{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("c")},
				Section{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"), []byte("b"))},
			),
		},
		{
			name:      "update_bson_set",
			commandID: CommandUpdateBSONSet,
			fixture:   "update_bson_set_entry.hex",
			sections:  updateBSONSetDeterministicSections(),
		},
	}
}

const (
	deterministicFixtureCommandVersion        = 1
	deterministicFixtureCatalogVersion        = 7
	deterministicInsertBatchIdempotency       = "id1"
	deterministicReplacementModeExistingOnly  = 1
	deterministicCollectionMetaVersion        = 1
	deterministicIndexDefinitionVersion       = 1
	deterministicCollectionDefaultRootPolicy  = 0
	deterministicCollectionDefaultIndexPolicy = 0
	deterministicCollectionDefaultMaxDocs     = int64(0)
	deterministicCollectionDefaultMaxBytes    = int64(0)
	deterministicCollectionDefaultMaxRootRuns = int64(0)
	deterministicCollectionDefaultMaxQueued   = int64(0)
	deterministicCollectionDefaultIndexCount  = 0
	deterministicIndexValueString             = 1
)

func deterministicFixtureSections(commandID CommandID, idempotency string, sections ...Section) []Section {
	out := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: commandID, Version: deterministicFixtureCommandVersion})},
		{ID: SectionIdempotencyKey, Bytes: []byte(idempotency)},
	}
	out = append(out, sections...)
	out = append(out, Section{ID: SectionExpectedCatalogVersion, Bytes: deterministicUvarintPayload(deterministicFixtureCatalogVersion)})
	sortSectionsByID(out)
	return out
}

type deterministicCollectionMetaOptions struct {
	documentFormat   uint64
	dataRootStorage  uint64
	indexRootStorage uint64
	maxDocuments     int64
	maxBytes         int64
	maxRootRuns      int64
	maxQueuedUnits   int64
	indexCount       uint64
}

func deterministicCollectionMetaPayload(name string) []byte {
	return deterministicCollectionMetaPayloadWithOptions(name, deterministicCollectionMetaOptions{
		documentFormat:   uint64(DocumentFormatDefault),
		dataRootStorage:  deterministicCollectionDefaultRootPolicy,
		indexRootStorage: deterministicCollectionDefaultIndexPolicy,
		maxDocuments:     deterministicCollectionDefaultMaxDocs,
		maxBytes:         deterministicCollectionDefaultMaxBytes,
		maxRootRuns:      deterministicCollectionDefaultMaxRootRuns,
		maxQueuedUnits:   deterministicCollectionDefaultMaxQueued,
		indexCount:       deterministicCollectionDefaultIndexCount,
	})
}

func deterministicCollectionMetaPayloadWithScalars(name string, docFormat, dataRootStorage, indexStateStorage uint64) []byte {
	return deterministicCollectionMetaPayloadWithOptions(name, deterministicCollectionMetaOptions{
		documentFormat:   docFormat,
		dataRootStorage:  dataRootStorage,
		indexRootStorage: indexStateStorage,
	})
}

func deterministicCollectionMetaPayloadWithOptions(name string, opts deterministicCollectionMetaOptions) []byte {
	// Field order mirrors encodeCollectionMeta: version, name, document_format,
	// data_root_storage_policy, index_state_storage_policy,
	// allow_array_values_in_index, disable_indexed_write_memtables,
	// buffered_indexed_writes, max_documents, max_bytes, max_root_runs,
	// async_flush, overlay_roots, max_queued_units, and index_count.
	dst := deterministicUvarintPayload(deterministicCollectionMetaVersion) // version
	dst = appendDeterministicString(dst, name)                             // name
	dst = appendUvarint(dst, opts.documentFormat)                          // document_format
	dst = appendUvarint(dst, opts.dataRootStorage)                         // data_root_storage_policy
	dst = appendUvarint(dst, opts.indexRootStorage)                        // index_state_storage_policy
	dst = appendDeterministicBool(dst, false)                              // allow_array_values_in_index
	dst = appendDeterministicBool(dst, false)                              // disable_indexed_write_memtables
	dst = appendDeterministicBool(dst, false)                              // buffered_indexed_writes
	dst = binary.AppendVarint(dst, opts.maxDocuments)                      // buffered_indexed_write_max_documents
	dst = binary.AppendVarint(dst, opts.maxBytes)                          // buffered_indexed_write_max_bytes
	dst = binary.AppendVarint(dst, opts.maxRootRuns)                       // buffered_indexed_write_max_root_runs
	dst = appendDeterministicBool(dst, false)                              // buffered_indexed_async_flush
	dst = appendDeterministicBool(dst, false)                              // buffered_indexed_overlay_roots
	dst = binary.AppendVarint(dst, opts.maxQueuedUnits)                    // buffered_indexed_async_flush_max_queued_units
	dst = appendUvarint(dst, opts.indexCount)                              // index_count
	return dst
}

func deterministicIndexDefinitionPayload(name, field string, valueType uint64, unique, multiKey bool, storagePolicy uint64) []byte {
	dst := deterministicUvarintPayload(deterministicIndexDefinitionVersion)
	dst = appendDeterministicString(dst, name)
	dst = appendDeterministicString(dst, field)
	dst = appendUvarint(dst, valueType)
	dst = appendDeterministicBool(dst, unique)
	dst = appendDeterministicBool(dst, multiKey)
	dst = appendUvarint(dst, storagePolicy)
	return dst
}

func deterministicIndexDefinitionPayloadWithEnums(name, field string, valueType, storagePolicy uint64) []byte {
	return deterministicIndexDefinitionPayload(name, field, valueType, false, false, storagePolicy)
}

func deterministicIndexDefinitionPayloadWithOptions(name, field string, valueType, storagePolicy uint64) []byte {
	return deterministicIndexDefinitionPayload(name, field, valueType, false, false, storagePolicy)
}

func deterministicUvarintPayload(value uint64) []byte {
	return appendUvarint(nil, value)
}

func deterministicBSONDocumentXInt32(value int32) []byte {
	const (
		valueTypeInt32 = 0x10
		valueKey       = 'x'
	)
	var valueBytes [4]byte
	binary.LittleEndian.PutUint32(valueBytes[:], uint32(value))
	out := []byte{
		0, 0, 0, 0,
		valueTypeInt32, valueKey, 0,
		valueBytes[0], valueBytes[1], valueBytes[2], valueBytes[3],
		0,
	}
	binary.LittleEndian.PutUint32(out[:4], uint32(len(out)))
	return out
}

func deterministicBSONSetValueInt32(value int32) []byte {
	var valueBytes [4]byte
	binary.LittleEndian.PutUint32(valueBytes[:], uint32(value))
	return []byte{0x10, valueBytes[0], valueBytes[1], valueBytes[2], valueBytes[3]}
}

func deterministicBSONEmptyDocument() []byte {
	return []byte{5, 0, 0, 0, 0}
}

func deterministicTemplateRecord(fields ...string) []byte {
	dst := []byte(deterministicTemplateV1RecordMagic)
	dst = appendUvarint(dst, uint64(len(fields)))
	for _, field := range fields {
		dst = appendUvarint(dst, uint64(len(field)))
		dst = append(dst, field...)
	}
	return dst
}

func deterministicTemplateStoredDocument(payload []byte) []byte {
	dst := []byte(deterministicTemplateV1StoredMagic)
	dst = append(dst, make([]byte, sha256.Size)...)
	return append(dst, payload...)
}

func deterministicTemplateStoredDocumentForRecord(record, payload []byte) []byte {
	dst := []byte(deterministicTemplateV1StoredMagic)
	id := sha256.Sum256(record)
	dst = append(dst, id[:]...)
	return append(dst, payload...)
}

func deterministicTemplateInputWithCount(count uint64) []byte {
	dst := []byte(deterministicTemplateV1InputMagic)
	return appendUvarint(dst, count)
}

func deterministicTemplateNestedObjectPayload(record []byte, depth int) []byte {
	id := sha256.Sum256(record)
	var dst []byte
	for i := 0; i < depth; i++ {
		dst = appendDeterministicTemplateObjectHeader(dst, id)
	}
	return append(dst, deterministicTemplateV1KindNull)
}

func deterministicTemplateObjectPayload(record []byte, payload []byte) []byte {
	id := sha256.Sum256(record)
	dst := appendDeterministicTemplateObjectHeader(nil, id)
	return append(dst, payload...)
}

func appendDeterministicTemplateObjectHeader(dst []byte, id [sha256.Size]byte) []byte {
	dst = append(dst, deterministicTemplateV1KindObject)
	return append(dst, id[:]...)
}

func appendDeterministicString(dst []byte, value string) []byte {
	dst = appendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func appendDeterministicBool(dst []byte, value bool) []byte {
	if value {
		return append(dst, 1)
	}
	return append(dst, 0)
}
