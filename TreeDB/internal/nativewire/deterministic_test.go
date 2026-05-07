package nativewire

import (
	"encoding/binary"
	"encoding/hex"
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
		{ID: SectionDocuments, Bytes: AppendByteVector(nil, []byte("{}"))},
		{ID: SectionDocumentFormat, Bytes: []byte{byte(DocumentFormatBSON)}},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 1})},
		{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
		{ID: SectionCollectionRef, Bytes: []byte("c")},
	}
	cmd1, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections shuffled: %v", err)
	}
	entry1, err := AppendDeterministicEntry(nil, cmd1)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry shuffled: %v", err)
	}
	if hex.EncodeToString(entry1) != hex.EncodeToString(entry0) {
		t.Fatalf("deterministic entries differ:\n%s\n%s", hex.EncodeToString(entry0), hex.EncodeToString(entry1))
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
	for _, tc := range []struct {
		name string
		raw  []byte
		code ErrorCode
	}{
		{name: "bad_magic", raw: []byte("bad"), code: ErrMalformedFrame},
		{name: "unsupported_version", raw: unsupportedVersion, code: ErrUnsupportedVersion},
		{name: "unsupported_flags", raw: entryWithFlags, code: ErrUnsupportedFeature},
		{name: "trailing", raw: append(append([]byte(nil), entry...), 0), code: ErrMalformedFrame},
		{name: "truncated_section", raw: append([]byte(nil), entry[:len(entry)-1]...), code: ErrMalformedFrame},
		{name: "section_count_limit", raw: sectionCountLimit, code: ErrResourceExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limits := Limits{}
			if tc.name == "section_count_limit" {
				limits.MaxSections = 1
			}
			if _, err := DecodeDeterministicEntry(tc.raw, limits); codeOf(err) != tc.code {
				t.Fatalf("DecodeDeterministicEntry err=%v code=%d want %d", err, codeOf(err), tc.code)
			}
		})
	}
}

func TestDecodeDeterministicEntryRejectsUnsortedSections(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sections []SectionID
	}{
		{name: "decreasing", sections: []SectionID{SectionDocuments, SectionDocumentIDs}},
		{name: "duplicate", sections: []SectionID{SectionDocumentIDs, SectionDocumentIDs}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte("TDC1")
			raw = appendUvarint(raw, DeterministicEntryVersion)
			raw = appendUvarint(raw, uint64(CommandInsertBatch))
			raw = appendUvarint(raw, 1)
			raw = appendUvarint(raw, 0)
			raw = appendUvarint(raw, uint64(len(tc.sections)))
			for _, id := range tc.sections {
				raw = appendUvarint(raw, uint64(id))
				raw = appendUvarint(raw, 0)
			}
			if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != ErrMalformedFrame {
				t.Fatalf("DecodeDeterministicEntry err=%v code=%d want malformed", err, codeOf(err))
			}
		})
	}
}

func TestDeterministicEntryReplicatedGoldenFixtures(t *testing.T) {
	registry := MustV1Registry()
	for _, tc := range deterministicEntryFixtureCases() {
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
				{ID: SectionCollectionRef, Bytes: []byte("c")},
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
		t.Run(tc.name, func(t *testing.T) {
			raw := deterministicEntryTestRaw(tc.commandID, tc.sections...)
			if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != ErrInvalidCommand {
				t.Fatalf("DecodeDeterministicEntry err=%v code=%d want invalid command", err, codeOf(err))
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := deterministicEntryTestRaw(CommandInsertBatch, tc.section)
			if _, err := DecodeDeterministicEntry(raw, Limits{MaxByteVectorItems: 1}); codeOf(err) != ErrResourceExhausted {
				t.Fatalf("DecodeDeterministicEntry err=%v code=%d want resource exhausted", err, codeOf(err))
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
		{ID: SectionCollectionRef, Bytes: []byte("c")},
		{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
		{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
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
				{ID: SectionCollectionRef, Bytes: []byte("users")},
			},
		},
		{
			name: "get_many",
			sections: []Section{
				{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandGetMany, Version: 1})},
				{ID: SectionCollectionRef, Bytes: []byte("users")},
				{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
			},
		},
	} {
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

func TestDeterministicEntryRejectsMissingDistributedGuards(t *testing.T) {
	registry := MustV1Registry()

	sections := removeSection(insertBatchDeterministicSections(), SectionIdempotencyKey)
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections without idempotency: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing idempotency err=%v code=%d", err, codeOf(err))
	}

	sections = removeSection(insertBatchDeterministicSections(), SectionExpectedCatalogVersion)
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections without catalog guard: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing catalog guard err=%v code=%d", err, codeOf(err))
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
	out := sections[:0]
	for _, section := range sections {
		if section.ID != SectionCommandHeader {
			out = append(out, section)
		}
	}
	sortSectionsByID(out)
	return out
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

func TestDeterministicEntryRejectsUnsupportedCommandFlags(t *testing.T) {
	registry := MustV1Registry()
	sections := insertBatchDeterministicSections()
	sections[0].Bytes = AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 1, Flags: 1 << 8})
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
		{ID: SectionCollectionMeta, Bytes: []byte("users")},
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing metadata catalog guard err=%v code=%d", err, codeOf(err))
	}

	sections = append(sections, Section{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}})
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections guarded: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); err != nil {
		t.Fatalf("AppendDeterministicEntry guarded: %v", err)
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
	sections := insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionCollectionRef {
			sections[i].Bytes = []byte("bad/name")
		}
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("invalid collection ref err=%v code=%d", err, codeOf(err))
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

func insertBatchDeterministicSections() []Section {
	return []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionCollectionRef, Bytes: []byte("c")},
		{ID: SectionDocumentFormat, Bytes: []byte{byte(DocumentFormatBSON)}},
		{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
		{ID: SectionDocuments, Bytes: AppendByteVector(nil, []byte("{}"))},
		{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
	}
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
				Section{ID: SectionCollectionRef, Bytes: []byte("users")},
				Section{ID: SectionIndexDefinition, Bytes: deterministicIndexDefinitionPayload("email_1", "email", 1, true, false, 0)},
			),
		},
		{
			name:      "drop_index",
			commandID: CommandDropIndex,
			fixture:   "drop_index_entry.hex",
			sections: deterministicFixtureSections(CommandDropIndex, "client-a:drop-index:email",
				Section{ID: SectionCollectionRef, Bytes: []byte("users")},
				Section{ID: SectionIndexName, Bytes: []byte("email_1")},
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
				Section{ID: SectionCollectionRef, Bytes: []byte("c")},
				Section{ID: SectionDocumentFormat, Bytes: []byte{byte(DocumentFormatBSON)}},
				Section{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
				Section{ID: SectionDocuments, Bytes: AppendByteVector(nil, []byte(`{"x":1}`))},
				Section{ID: SectionReplacementMode, Bytes: deterministicUvarintPayload(deterministicReplacementModeExistingOnly)},
			),
		},
		{
			name:      "delete_batch",
			commandID: CommandDeleteBatch,
			fixture:   "delete_batch_entry.hex",
			sections: deterministicFixtureSections(CommandDeleteBatch, "client-a:delete:1",
				Section{ID: SectionCollectionRef, Bytes: []byte("c")},
				Section{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"), []byte("b"))},
			),
		},
	}
}

const deterministicReplacementModeExistingOnly = 1

func deterministicFixtureSections(commandID CommandID, idempotency string, sections ...Section) []Section {
	out := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: commandID, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte(idempotency)},
	}
	out = append(out, sections...)
	out = append(out, Section{ID: SectionExpectedCatalogVersion, Bytes: deterministicUvarintPayload(7)})
	return out
}

func deterministicCollectionMetaPayload(name string) []byte {
	dst := deterministicUvarintPayload(1)
	dst = appendDeterministicString(dst, name)
	dst = appendUvarint(dst, uint64(DocumentFormatDefault))
	dst = appendUvarint(dst, 0)
	dst = appendUvarint(dst, 0)
	dst = appendDeterministicBool(dst, false)
	dst = appendDeterministicBool(dst, false)
	dst = appendDeterministicBool(dst, false)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = appendDeterministicBool(dst, false)
	dst = appendDeterministicBool(dst, false)
	dst = binary.AppendVarint(dst, 0)
	dst = appendUvarint(dst, 0)
	return dst
}

func deterministicIndexDefinitionPayload(name, field string, valueType uint64, unique, multiKey bool, storagePolicy uint64) []byte {
	dst := deterministicUvarintPayload(1)
	dst = appendDeterministicString(dst, name)
	dst = appendDeterministicString(dst, field)
	dst = appendUvarint(dst, valueType)
	dst = appendDeterministicBool(dst, unique)
	dst = appendDeterministicBool(dst, multiKey)
	dst = appendUvarint(dst, storagePolicy)
	return dst
}

func deterministicUvarintPayload(value uint64) []byte {
	return appendUvarint(nil, value)
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
