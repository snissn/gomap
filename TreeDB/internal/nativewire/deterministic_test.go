package nativewire

import (
	"bytes"
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
	entry0Hex := hex.EncodeToString(entry0)
	entry1Hex := hex.EncodeToString(entry1)
	if entry1Hex != entry0Hex {
		t.Fatalf("deterministic entries differ:\n%s\n%s", entry0Hex, entry1Hex)
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

func TestDeterministicEntryRejectsUnsupportedCommandFlags(t *testing.T) {
	registry := registryWithInsertBatchAllowedFlags(1)
	sections := insertBatchDeterministicSections()
	sections[0].Bytes = AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 1, Flags: 1})
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

	sections = append(sections, Section{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}})
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
		{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
		{ID: SectionCollectionMeta, Bytes: nil},
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("malformed collection_meta err=%v code=%d", err, codeOf(err))
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

func TestDeterministicMetadataRequiresTaggedCollectionName(t *testing.T) {
	registry := MustV1Registry()
	sections := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCreateIndex, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
		{ID: SectionCollectionRef, Bytes: []byte("users")},
		{ID: SectionIndexDefinition, Bytes: deterministicIndexDefinitionPayload("email", "email")},
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

func TestDeterministicEntryAcceptsUTF8CollectionNames(t *testing.T) {
	registry := MustV1Registry()
	sections := insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionCollectionRef {
			sections[i].Bytes = []byte("用户")
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

func TestDeterministicEntryIncludesAckPolicy(t *testing.T) {
	registry := MustV1Registry()
	sections := append(insertBatchDeterministicSections(), Section{ID: SectionAckPolicy, Bytes: []byte{byte(AckVisible)}})
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections visible ack: %v", err)
	}
	visible, err := AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry visible ack: %v", err)
	}

	sections[len(sections)-1].Bytes = []byte{byte(AckFlushed)}
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections flushed ack: %v", err)
	}
	flushed, err := AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry flushed ack: %v", err)
	}
	if bytes.Equal(visible, flushed) {
		t.Fatalf("deterministic entry did not include ack_policy")
	}
}

func TestDeterministicEntryRejectsUnsupportedAckPolicy(t *testing.T) {
	registry := MustV1Registry()
	sections := append(insertBatchDeterministicSections(), Section{ID: SectionAckPolicy, Bytes: []byte{99}})
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("unsupported ack_policy err=%v code=%d want invalid command", err, codeOf(err))
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

func TestDeterministicEntryRejectsEmptyDocumentIDs(t *testing.T) {
	registry := MustV1Registry()
	sections := insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionDocumentIDs {
			sections[i].Bytes = AppendByteVector(nil, []byte("a"), nil)
		}
		if sections[i].ID == SectionDocuments {
			sections[i].Bytes = AppendByteVector(nil, []byte("{}"), []byte("{}"))
		}
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("empty document id err=%v code=%d", err, codeOf(err))
	}
}

func TestDeterministicDropIndexValidatesEncodedIndexName(t *testing.T) {
	registry := MustV1Registry()
	sections := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandDropIndex, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
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
			sections[i].Bytes = appendDeterministicTestString(nil, "email")
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
		{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
		{ID: SectionCollectionRef, Bytes: append([]byte{deterministicCollectionRefTagName}, []byte("users")...)},
		{ID: SectionIndexDefinition, Bytes: deterministicIndexDefinitionPayload("email", ".email")},
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

func deterministicCollectionMetaPayload(name string) []byte {
	return deterministicCollectionMetaPayloadWithScalars(name, uint64(DocumentFormatDefault), 0, 0)
}

func deterministicCollectionMetaPayloadWithScalars(name string, docFormat, dataRootStorage, indexStateStorage uint64) []byte {
	dst := appendUvarint(nil, 1)
	dst = appendDeterministicTestString(dst, name)
	dst = appendUvarint(dst, docFormat)
	dst = appendUvarint(dst, dataRootStorage)
	dst = appendUvarint(dst, indexStateStorage)
	dst = append(dst, 0, 0, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = append(dst, 0, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = appendUvarint(dst, 0)
	return dst
}

func deterministicIndexDefinitionPayload(name, field string) []byte {
	return deterministicIndexDefinitionPayloadWithEnums(name, field, 1, 0)
}

func deterministicIndexDefinitionPayloadWithEnums(name, field string, valueType, storagePolicy uint64) []byte {
	dst := appendUvarint(nil, 1)
	dst = appendDeterministicTestString(dst, name)
	dst = appendDeterministicTestString(dst, field)
	dst = appendUvarint(dst, valueType)
	dst = append(dst, 0, 0)
	dst = appendUvarint(dst, storagePolicy)
	return dst
}

func appendDeterministicTestString(dst []byte, value string) []byte {
	dst = appendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}
