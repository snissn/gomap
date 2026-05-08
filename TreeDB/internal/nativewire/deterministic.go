package nativewire

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"sync"
	"unicode/utf8"

	"github.com/snissn/gomap/TreeDB/collections"
)

const (
	DeterministicEntryMagic   = "TDC1"
	DeterministicEntryVersion = uint64(1)

	maxDeterministicDocumentIDs = defaultMaxByteVectorItems

	deterministicSectionScratchCapacity = sectionSeenInlineCapacity
	maxDeterministicEntrySections       = defaultMaxSections

	deterministicCollectionRefTagName   = 1
	deterministicCollectionRefTagHandle = 2
	deterministicReplacementExisting    = 1
	deterministicTemplateV1InputMagic   = "TD1I"
	deterministicTemplateV1StoredMagic  = "TD1D"
	deterministicTemplateV1RecordMagic  = "TD1T"
	deterministicTemplateV1KindNull     = byte(0)
	deterministicTemplateV1KindFalse    = byte(1)
	deterministicTemplateV1KindTrue     = byte(2)
	deterministicTemplateV1KindFloat64  = byte(3)
	deterministicTemplateV1KindString   = byte(4)
	deterministicTemplateV1KindObject   = byte(5)
	deterministicTemplateV1KindArray    = byte(6)
	deterministicTemplateV1MaxArray     = 1 << 20
	minDeterministicIndexDefinitionLen  = 6
	maxDeterministicCollectionIndexes   = 1 << 16
)

var (
	deterministicEntryRegistryOnce sync.Once
	deterministicEntryRegistry     *Registry
	deterministicEntryRegistryErr  error
)

func deterministicRegistry() *Registry {
	deterministicEntryRegistryOnce.Do(func() {
		deterministicEntryRegistry, deterministicEntryRegistryErr = NewRegistry(v1CommandSchemas()...)
	})
	if deterministicEntryRegistryErr != nil {
		panic(deterministicEntryRegistryErr)
	}
	return deterministicEntryRegistry
}

type DeterministicEntry struct {
	Version        uint64
	CommandID      CommandID
	CommandVersion uint64
	CommandFlags   uint64
	Sections       []Section
}

// DeterministicEntryScratch carries reusable buffers for
// DecodeDeterministicEntryInto.
type DeterministicEntryScratch struct {
	Sections []Section
}

func AppendDeterministicEntry(dst []byte, cmd ValidatedCommand) ([]byte, error) {
	if cmd.Schema == nil {
		return nil, protocolError(ErrInvalidCommand, "missing command schema")
	}
	if !cmd.Schema.Replicated {
		return nil, protocolError(ErrInvalidCommand, "command %s is not replicated", cmd.Schema.Name)
	}
	unsupportedFlags := UnsupportedDeterministicCommandFlags(cmd.Header.Flags)
	if unsupportedFlags != 0 {
		return nil, protocolError(ErrUnsupportedFeature, "unsupported deterministic command flags 0x%x", unsupportedFlags)
	}
	deterministicFlags := DeterministicCommandFlags(cmd.Header.Flags)

	var deterministicScratch [deterministicSectionScratchCapacity]Section
	deterministic, err := cmd.deterministicSectionsInto(deterministicScratch[:0])
	if err != nil {
		return nil, err
	}
	sortSectionsByID(deterministic)
	if cmd.Schema.RequiresIdempotency && deterministicSectionCount(deterministic, SectionIdempotencyKey) == 0 {
		return nil, protocolError(ErrInvalidCommand, "missing deterministic idempotency key")
	}
	if cmd.Schema.RequiresCatalogGuard && deterministicSectionCount(deterministic, SectionExpectedCatalogVersion) == 0 {
		return nil, protocolError(ErrInvalidCommand, "missing deterministic catalog guard")
	}
	if err := validateDeterministicCommand(cmd.Header.ID, deterministic); err != nil {
		return nil, err
	}

	dst = append(dst, DeterministicEntryMagic...)
	dst = appendUvarint(dst, DeterministicEntryVersion)
	dst = appendUvarint(dst, uint64(cmd.Header.ID))
	dst = appendUvarint(dst, cmd.Header.Version)
	dst = appendUvarint(dst, deterministicFlags)
	dst = appendUvarint(dst, uint64(len(deterministic)))
	for _, section := range deterministic {
		dst = appendUvarint(dst, uint64(section.ID))
		dst = appendUvarint(dst, uint64(len(section.Bytes)))
		dst = append(dst, section.Bytes...)
	}
	return dst, nil
}

func DecodeDeterministicEntry(src []byte, limits Limits) (DeterministicEntry, error) {
	return DecodeDeterministicEntryWithRegistry(src, limits, nil)
}

// DecodeDeterministicEntryWithRegistry decodes a deterministic command-entry
// envelope using registry for command validation. A nil registry uses the v1
// native-wire registry.
func DecodeDeterministicEntryWithRegistry(src []byte, limits Limits, registry *Registry) (DeterministicEntry, error) {
	return DecodeDeterministicEntryIntoWithRegistry(src, limits, nil, registry)
}

// DecodeDeterministicEntryInto decodes a deterministic command-entry envelope.
//
// The returned section payloads borrow from src and the section slice may borrow
// from scratch. Callers that need to retain the entry after reusing src or
// scratch must copy it.
func DecodeDeterministicEntryInto(src []byte, limits Limits, scratch *DeterministicEntryScratch) (DeterministicEntry, error) {
	return DecodeDeterministicEntryIntoWithRegistry(src, limits, scratch, nil)
}

// DecodeDeterministicEntryIntoWithRegistry is DecodeDeterministicEntryInto with
// caller-supplied command registry validation. A nil registry uses the v1
// native-wire registry.
func DecodeDeterministicEntryIntoWithRegistry(src []byte, limits Limits, scratch *DeterministicEntryScratch, registry *Registry) (DeterministicEntry, error) {
	limits = limits.withDefaults()
	if registry == nil {
		registry = deterministicRegistry()
	}
	failBeforeSections := func(err error) (DeterministicEntry, error) {
		clearDeterministicEntryScratch(nil, scratch, true)
		return DeterministicEntry{}, err
	}
	if uint64(len(src)) > limits.MaxFrameSize {
		return failBeforeSections(protocolError(ErrResourceExhausted, "deterministic entry length %d exceeds limit %d", len(src), limits.MaxFrameSize))
	}
	if len(src) < len(DeterministicEntryMagic) ||
		src[0] != DeterministicEntryMagic[0] ||
		src[1] != DeterministicEntryMagic[1] ||
		src[2] != DeterministicEntryMagic[2] ||
		src[3] != DeterministicEntryMagic[3] {
		return failBeforeSections(protocolError(ErrMalformedFrame, "bad deterministic entry magic"))
	}
	off := len(DeterministicEntryMagic)
	version, err := readEntryUvarint(src, &off, "entry_version")
	if err != nil {
		return failBeforeSections(err)
	}
	if version != DeterministicEntryVersion {
		return failBeforeSections(protocolError(ErrUnsupportedVersion, "deterministic entry version %d", version))
	}
	commandID, err := readEntryUvarint(src, &off, "command_id")
	if err != nil {
		return failBeforeSections(err)
	}
	commandVersion, err := readEntryUvarint(src, &off, "command_version")
	if err != nil {
		return failBeforeSections(err)
	}
	commandFlags, err := readEntryUvarint(src, &off, "command_flags")
	if err != nil {
		return failBeforeSections(err)
	}
	if commandFlags != 0 {
		return failBeforeSections(protocolError(ErrUnsupportedFeature, "unsupported deterministic command flags 0x%x", commandFlags))
	}
	sectionCount64, err := readEntryUvarint(src, &off, "section_count")
	if err != nil {
		return failBeforeSections(err)
	}
	if sectionCount64 > uint64(limits.MaxSections) {
		return failBeforeSections(protocolError(ErrResourceExhausted, "deterministic entry section count %d exceeds limit %d", sectionCount64, limits.MaxSections))
	}
	if sectionCount64 > maxDeterministicEntrySections {
		return failBeforeSections(protocolError(ErrResourceExhausted, "deterministic entry section count %d exceeds deterministic limit %d", sectionCount64, maxDeterministicEntrySections))
	}
	if sectionCount64 > uint64(maxInt) {
		return failBeforeSections(protocolError(ErrResourceExhausted, "deterministic entry section count exceeds int capacity"))
	}
	if sectionCount64 > uint64((len(src)-off)/2) {
		return failBeforeSections(protocolError(ErrMalformedFrame, "deterministic entry section count %d exceeds remaining header bytes %d", sectionCount64, len(src)-off))
	}
	sectionCount := int(sectionCount64)
	sections, borrowedScratch := deterministicEntrySectionsBuffer(sectionCount, scratch)
	fail := func(err error) (DeterministicEntry, error) {
		clearDeterministicEntryScratch(sections, scratch, borrowedScratch)
		return DeterministicEntry{}, err
	}
	var previous SectionID
	for i := 0; i < sectionCount; i++ {
		id, err := readEntryUvarint(src, &off, "section_id")
		if err != nil {
			return fail(err)
		}
		sectionID := SectionID(id)
		if i > 0 && sectionID < previous {
			return fail(protocolError(ErrMalformedFrame, "deterministic entry sections are not sorted"))
		}
		previous = sectionID
		sectionLen, err := readEntryUvarint(src, &off, "section_length")
		if err != nil {
			return fail(err)
		}
		if sectionLen > limits.MaxSectionLen {
			return fail(protocolError(ErrResourceExhausted, "deterministic entry section %d length %d exceeds limit %d", sectionID, sectionLen, limits.MaxSectionLen))
		}
		if sectionLen > uint64(maxInt) {
			return fail(protocolError(ErrResourceExhausted, "deterministic entry section %d length exceeds int capacity", sectionID))
		}
		if sectionLen > uint64(len(src)-off) {
			return fail(protocolError(ErrMalformedFrame, "deterministic entry section %d length %d exceeds remaining %d", sectionID, sectionLen, len(src)-off))
		}
		sections[i] = Section{ID: sectionID, Bytes: src[off : off+int(sectionLen)]}
		off += int(sectionLen)
	}
	if off != len(src) {
		return fail(protocolError(ErrMalformedFrame, "deterministic entry has %d trailing bytes", len(src)-off))
	}
	if err := validateDeterministicSectionPayloads(sections, limits); err != nil {
		return fail(err)
	}
	if _, err := validateDecodedDeterministicEntry(registry, CommandID(commandID), commandVersion, sections); err != nil {
		return fail(err)
	}
	if scratch != nil {
		scratch.Sections = sections
	}
	return DeterministicEntry{
		Version:        version,
		CommandID:      CommandID(commandID),
		CommandVersion: commandVersion,
		CommandFlags:   commandFlags,
		Sections:       sections,
	}, nil
}

func validateDeterministicSectionPayloads(sections []Section, limits Limits) error {
	for _, section := range sections {
		if err := validateDeterministicSectionPayload(section, limits); err != nil {
			return err
		}
	}
	return nil
}

func deterministicEntrySectionsBuffer(count int, scratch *DeterministicEntryScratch) ([]Section, bool) {
	if scratch == nil {
		return make([]Section, count), false
	}
	if cap(scratch.Sections) < count {
		return make([]Section, count), false
	}
	previousUsed := len(scratch.Sections)
	backing := scratch.Sections[:cap(scratch.Sections)]
	if previousUsed > count {
		clear(backing[count:previousUsed])
	}
	return backing[:count], true
}

func clearDeterministicEntryScratch(sections []Section, scratch *DeterministicEntryScratch, borrowedScratch bool) {
	if scratch == nil {
		return
	}
	if borrowedScratch {
		clear(sections)
		scratch.Sections = sections[:0]
		return
	}
	clear(scratch.Sections)
	scratch.Sections = scratch.Sections[:0]
}

func readEntryUvarint(src []byte, off *int, field string) (uint64, error) {
	if off == nil || *off >= len(src) {
		return 0, protocolError(ErrMalformedFrame, "invalid deterministic entry offset for %s", field)
	}
	value, n, err := readUvarint(src[*off:])
	if err != nil {
		return 0, err
	}
	*off += n
	return value, nil
}

func (cmd ValidatedCommand) hasSection(id SectionID) bool {
	for _, section := range cmd.Known {
		if section.ID == id {
			return true
		}
	}
	return false
}

func (cmd ValidatedCommand) deterministicSectionsInto(dst []Section) ([]Section, error) {
	if cmd.Schema == nil {
		return nil, protocolError(ErrInvalidCommand, "missing command schema")
	}
	rules := cmd.Schema.ruleMap()
	out := dst[:0]
	var seen sectionSeenSet
	for _, section := range cmd.Known {
		rule := rules[section.ID]
		if !rule.Deterministic {
			continue
		}
		seenCount := seen.add(section.ID)
		if !rule.Repeatable && seenCount > 1 {
			return nil, protocolError(ErrInvalidCommand, "duplicate deterministic singleton section %d", section.ID)
		}
		if err := validateDeterministicSectionPayload(section, Limits{}); err != nil {
			return nil, err
		}
		out = append(out, Section{ID: section.ID, Bytes: section.Bytes})
	}
	return out, nil
}

func validateDecodedDeterministicEntry(registry *Registry, commandID CommandID, commandVersion uint64, sections []Section) (*CommandSchema, error) {
	if registry == nil {
		registry = deterministicRegistry()
	}
	schema, ok := registry.LookupCommand(commandID, commandVersion)
	if !ok {
		return nil, protocolError(ErrUnsupportedVersion, "unsupported deterministic command %d version %d", commandID, commandVersion)
	}
	if !schema.Replicated || schema.LocalOnly {
		return nil, protocolError(ErrInvalidCommand, "command %s is not replicated", schema.Name)
	}
	rules := schema.ruleMap()
	var seen sectionSeenSet
	for _, section := range sections {
		rule, ok := rules[section.ID]
		if !ok || !rule.Deterministic {
			return nil, protocolError(ErrInvalidCommand, "section %d is not deterministic for command %s", section.ID, schema.Name)
		}
		seenCount := seen.add(section.ID)
		if !rule.Repeatable && seenCount > 1 {
			return nil, protocolError(ErrInvalidCommand, "duplicate deterministic singleton section %d", section.ID)
		}
	}
	if schema.RequiresIdempotency && seen.get(SectionIdempotencyKey) == 0 {
		return nil, protocolError(ErrInvalidCommand, "missing idempotency key")
	}
	if schema.RequiresCatalogGuard && seen.get(SectionExpectedCatalogVersion) == 0 {
		return nil, protocolError(ErrInvalidCommand, "missing catalog guard")
	}
	for _, rule := range schema.Sections {
		if rule.Required && rule.Deterministic && seen.get(rule.ID) == 0 {
			return nil, protocolError(ErrInvalidCommand, "missing deterministic section %d", rule.ID)
		}
	}
	if err := validateDeterministicCommand(commandID, sections); err != nil {
		return nil, err
	}
	return schema, nil
}

func validateDeterministicCommand(commandID CommandID, deterministic []Section) error {
	switch commandID {
	case CommandInsertBatch, CommandReplaceBatch:
		idCount, err := deterministicByteVectorCount(deterministic, SectionDocumentIDs)
		if err != nil {
			return err
		}
		docCount, err := deterministicByteVectorCount(deterministic, SectionDocuments)
		if err != nil {
			return err
		}
		if idCount != docCount {
			return protocolError(ErrInvalidCommand, "document_ids length %d does not match documents length %d", idCount, docCount)
		}
		if err := validateDeterministicTemplateRecords(deterministic); err != nil {
			return err
		}
	case CommandDeleteBatch:
		if _, err := deterministicByteVectorCount(deterministic, SectionDocumentIDs); err != nil {
			return err
		}
	case CommandCreateIndex, CommandDropIndex:
		raw, ok := deterministicSectionPayload(deterministic, SectionCollectionRef)
		if !ok {
			return protocolError(ErrInvalidCommand, "missing deterministic section %d", SectionCollectionRef)
		}
		local, err := validateDeterministicCollectionRef(raw, true)
		if err != nil {
			return err
		}
		if local {
			return protocolError(ErrInvalidCommand, "collection handle ref is not deterministic")
		}
	}
	return nil
}

func validateDeterministicTemplateRecords(sections []Section) error {
	raw, ok := deterministicSectionPayload(sections, SectionTemplateRecords)
	if !ok {
		return nil
	}
	formatRaw, ok := deterministicSectionPayload(sections, SectionDocumentFormat)
	if !ok {
		return protocolError(ErrInvalidCommand, "missing deterministic section %d", SectionDocumentFormat)
	}
	format, n, err := readUvarint(formatRaw)
	if err != nil {
		return err
	}
	if n != len(formatRaw) {
		return protocolError(ErrMalformedFrame, "document_format has %d trailing bytes", len(formatRaw)-n)
	}
	if DocumentFormat(format) != DocumentFormatTemplateV1 {
		return protocolError(ErrInvalidCommand, "template_records require template-v1 document format")
	}
	var templateScratch [8]deterministicTemplate
	templates, err := validateDeterministicTemplateRecordVectorInto(templateScratch[:0], raw)
	if err != nil {
		return err
	}
	docsRaw, ok := deterministicSectionPayload(sections, SectionDocuments)
	if !ok {
		return protocolError(ErrInvalidCommand, "missing deterministic section %d", SectionDocuments)
	}
	return validateDeterministicTemplateDocuments(docsRaw, templates)
}

type deterministicTemplate struct {
	id         [sha256.Size]byte
	fieldCount int
}

type deterministicTemplateSet []deterministicTemplate

func validateDeterministicTemplateRecordVectorInto(dst deterministicTemplateSet, raw []byte) (deterministicTemplateSet, error) {
	templates := dst[:0]
	err := walkDeterministicByteVector(raw, "template_records", func(i int, record []byte) error {
		fieldCount, err := deterministicTemplateRecordFieldCount(record)
		if err != nil {
			return deterministicContextError(err, ErrInvalidCommand, "template_records[%d]", i)
		}
		templates = append(templates, deterministicTemplate{id: sha256.Sum256(record), fieldCount: fieldCount})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return templates, nil
}

func (templates deterministicTemplateSet) lookup(id [sha256.Size]byte) (int, bool) {
	for i := len(templates) - 1; i >= 0; i-- {
		if templates[i].id == id {
			return templates[i].fieldCount, true
		}
	}
	return 0, false
}

func validateDeterministicTemplateDocuments(raw []byte, templates deterministicTemplateSet) error {
	return walkDeterministicByteVector(raw, "documents", func(i int, doc []byte) error {
		if err := validateDeterministicTemplateDocument(doc, templates); err != nil {
			return deterministicContextError(err, ErrInvalidCommand, "template-v1 document %d", i)
		}
		return nil
	})
}

func deterministicContextError(err error, fallback ErrorCode, format string, args ...any) error {
	if err == nil {
		return nil
	}
	code := fallback
	if wrappedCode, ok := ErrorCodeOf(err); ok {
		code = wrappedCode
	}
	args = append(args, err)
	return protocolError(code, format+": %v", args...)
}

func walkDeterministicByteVector(raw []byte, name string, fn func(int, []byte) error) error {
	count64, lengthsOff, err := readUvarint(raw)
	if err != nil {
		return err
	}
	if count64 > uint64(maxInt) {
		return protocolError(ErrResourceExhausted, "%s count exceeds int capacity", name)
	}
	count := int(count64)
	lengthsPos := lengthsOff
	payloadLen := 0
	for i := 0; i < count; i++ {
		length64, n, err := readUvarint(raw[lengthsPos:])
		if err != nil {
			return err
		}
		if length64 > uint64(maxInt) || int(length64) > maxInt-payloadLen {
			return protocolError(ErrResourceExhausted, "%s payload length exceeds int capacity", name)
		}
		payloadLen += int(length64)
		lengthsPos += n
	}
	if payloadLen != len(raw)-lengthsPos {
		return protocolError(ErrMalformedFrame, "%s payload length %d does not match declared lengths %d", name, len(raw)-lengthsPos, payloadLen)
	}
	payloadPos := lengthsPos
	lengthsPos = lengthsOff
	for i := 0; i < count; i++ {
		length64, n, err := readUvarint(raw[lengthsPos:])
		if err != nil {
			return err
		}
		lengthsPos += n
		length := int(length64)
		item := raw[payloadPos : payloadPos+length]
		if err := fn(i, item); err != nil {
			return err
		}
		payloadPos += length
	}
	return nil
}

func validateDeterministicTemplateDocument(raw []byte, templates deterministicTemplateSet) error {
	pos := 0
	if consumeDeterministicTemplateMagic(raw, &pos, deterministicTemplateV1StoredMagic) {
		return validateDeterministicTemplateStoredDocument(raw, &pos, templates)
	}
	pos = 0
	if !consumeDeterministicTemplateMagic(raw, &pos, deterministicTemplateV1InputMagic) {
		return protocolError(ErrMalformedFrame, "document is not TD1I or TD1D")
	}
	templateCount, err := readDeterministicTemplateUvarint(raw, &pos, "template count")
	if err != nil {
		return err
	}
	if templateCount > uint64(len(raw)) {
		return protocolError(ErrMalformedFrame, "malformed template-v1 template count")
	}
	for i := uint64(0); i < templateCount; i++ {
		var id [sha256.Size]byte
		if len(raw)-pos < len(id) {
			return protocolError(ErrMalformedFrame, "malformed template-v1 template id")
		}
		copy(id[:], raw[pos:pos+len(id)])
		pos += len(id)
		recordLen, err := readDeterministicTemplateUvarint(raw, &pos, "template record length")
		if err != nil {
			return err
		}
		if recordLen > uint64(len(raw)-pos) {
			return protocolError(ErrMalformedFrame, "malformed template-v1 template record length")
		}
		record := raw[pos : pos+int(recordLen)]
		pos += int(recordLen)
		fieldCount, err := deterministicTemplateRecordFieldCount(record)
		if err != nil {
			return err
		}
		if recordID := sha256.Sum256(record); recordID != id {
			return protocolError(ErrMalformedFrame, "template-v1 template id does not match record")
		} else {
			templates = append(templates, deterministicTemplate{id: id, fieldCount: fieldCount})
		}
	}
	if !consumeDeterministicTemplateMagic(raw, &pos, deterministicTemplateV1StoredMagic) {
		return protocolError(ErrMalformedFrame, "malformed template-v1 stored document")
	}
	return validateDeterministicTemplateStoredDocument(raw, &pos, templates)
}

func validateDeterministicTemplateStoredDocument(raw []byte, pos *int, templates deterministicTemplateSet) error {
	var id [sha256.Size]byte
	if pos == nil || *pos < 0 || len(raw)-*pos < len(id) {
		return protocolError(ErrMalformedFrame, "malformed template-v1 root template id")
	}
	copy(id[:], raw[*pos:*pos+len(id)])
	*pos += len(id)
	fieldCount, ok := templates.lookup(id)
	if !ok {
		return protocolError(ErrInvalidCommand, "template-v1 template id is not included")
	}
	for i := 0; i < fieldCount; i++ {
		if err := skipDeterministicTemplateValue(raw, pos, templates); err != nil {
			return err
		}
	}
	if *pos != len(raw) {
		return protocolError(ErrMalformedFrame, "trailing template-v1 object values")
	}
	return nil
}

func validateDeterministicTemplateRecord(raw []byte) error {
	_, err := deterministicTemplateRecordFieldCount(raw)
	return err
}

func deterministicTemplateRecordFieldCount(raw []byte) (int, error) {
	pos := 0
	if !consumeDeterministicTemplateMagic(raw, &pos, deterministicTemplateV1RecordMagic) {
		return 0, protocolError(ErrMalformedFrame, "malformed template-v1 template record")
	}
	fieldCount, err := readDeterministicTemplateUvarint(raw, &pos, "template field count")
	if err != nil {
		return 0, err
	}
	if fieldCount > uint64(len(raw)) {
		return 0, protocolError(ErrMalformedFrame, "malformed template-v1 field count")
	}
	if fieldCount > uint64(maxInt) {
		return 0, protocolError(ErrResourceExhausted, "template-v1 field count exceeds int capacity")
	}
	var previous []byte
	for i := uint64(0); i < fieldCount; i++ {
		fieldLen, err := readDeterministicTemplateUvarint(raw, &pos, "template field length")
		if err != nil {
			return 0, err
		}
		if fieldLen > uint64(len(raw)-pos) {
			return 0, protocolError(ErrMalformedFrame, "malformed template-v1 field length")
		}
		field := raw[pos : pos+int(fieldLen)]
		pos += int(fieldLen)
		if len(field) == 0 || bytes.IndexAny(field, "\x00.") >= 0 {
			return 0, protocolError(ErrInvalidCommand, "invalid template-v1 field %q", field)
		}
		if !utf8.Valid(field) {
			return 0, protocolError(ErrInvalidCommand, "invalid template-v1 field %q", field)
		}
		if i > 0 && bytes.Compare(previous, field) >= 0 {
			return 0, protocolError(ErrInvalidCommand, "template-v1 fields are not strictly sorted")
		}
		previous = field
	}
	if pos != len(raw) {
		return 0, protocolError(ErrMalformedFrame, "trailing template-v1 template bytes")
	}
	return int(fieldCount), nil
}

func skipDeterministicTemplateValue(raw []byte, pos *int, templates deterministicTemplateSet) error {
	if pos == nil || *pos < 0 || *pos >= len(raw) {
		return protocolError(ErrMalformedFrame, "malformed template-v1 value")
	}
	kind := raw[*pos]
	*pos = *pos + 1
	switch kind {
	case deterministicTemplateV1KindNull, deterministicTemplateV1KindFalse, deterministicTemplateV1KindTrue:
		return nil
	case deterministicTemplateV1KindFloat64:
		if len(raw)-*pos < 8 {
			return protocolError(ErrMalformedFrame, "malformed template-v1 number")
		}
		*pos += 8
		return nil
	case deterministicTemplateV1KindString:
		n, err := readDeterministicTemplateUvarint(raw, pos, "template string length")
		if err != nil {
			return err
		}
		if n > uint64(len(raw)-*pos) {
			return protocolError(ErrMalformedFrame, "malformed template-v1 string")
		}
		*pos += int(n)
		return nil
	case deterministicTemplateV1KindArray:
		count, err := readDeterministicTemplateUvarint(raw, pos, "template array count")
		if err != nil {
			return err
		}
		if count > deterministicTemplateV1MaxArray || count > uint64(len(raw)-*pos) {
			return protocolError(ErrMalformedFrame, "malformed template-v1 array")
		}
		for i := uint64(0); i < count; i++ {
			if err := skipDeterministicTemplateValue(raw, pos, templates); err != nil {
				return err
			}
		}
		return nil
	case deterministicTemplateV1KindObject:
		var id [sha256.Size]byte
		if len(raw)-*pos < len(id) {
			return protocolError(ErrMalformedFrame, "malformed template-v1 object")
		}
		copy(id[:], raw[*pos:*pos+len(id)])
		*pos += len(id)
		fieldCount, ok := templates.lookup(id)
		if !ok {
			return protocolError(ErrInvalidCommand, "template-v1 object template id is not included")
		}
		for i := 0; i < fieldCount; i++ {
			if err := skipDeterministicTemplateValue(raw, pos, templates); err != nil {
				return err
			}
		}
		return nil
	default:
		return protocolError(ErrMalformedFrame, "unknown template-v1 value kind %d", kind)
	}
}

func consumeDeterministicTemplateMagic(raw []byte, pos *int, magic string) bool {
	if pos == nil || *pos < 0 || len(raw)-*pos < len(magic) {
		return false
	}
	for i := 0; i < len(magic); i++ {
		if raw[*pos+i] != magic[i] {
			return false
		}
	}
	*pos += len(magic)
	return true
}

func readDeterministicTemplateUvarint(raw []byte, pos *int, field string) (uint64, error) {
	if pos == nil || *pos < 0 || *pos > len(raw) {
		return 0, protocolError(ErrMalformedFrame, "invalid %s offset", field)
	}
	value, n, err := readUvarint(raw[*pos:])
	if err != nil {
		return 0, err
	}
	*pos += n
	return value, nil
}

func deterministicSectionPayload(sections []Section, id SectionID) ([]byte, bool) {
	for _, section := range sections {
		if section.ID == id {
			return section.Bytes, true
		}
	}
	return nil, false
}

func deterministicByteVectorCount(sections []Section, id SectionID) (int, error) {
	found := false
	var count int
	for _, section := range sections {
		if section.ID != id {
			continue
		}
		if found {
			return 0, protocolError(ErrInvalidCommand, "duplicate deterministic section %d", id)
		}
		count64, _, err := readUvarint(section.Bytes)
		if err != nil {
			return 0, err
		}
		if count64 > uint64(maxInt) {
			return 0, protocolError(ErrResourceExhausted, "section %d byte-vector count exceeds int capacity", id)
		}
		found = true
		count = int(count64)
	}
	if found {
		return count, nil
	}
	return 0, protocolError(ErrInvalidCommand, "missing deterministic section %d", id)
}

func deterministicSectionCount(sections []Section, id SectionID) int {
	count := 0
	for _, section := range sections {
		if section.ID == id {
			count++
		}
	}
	return count
}

func validateDeterministicSectionPayload(section Section, limits Limits) error {
	switch section.ID {
	case SectionCollectionRef:
		local, err := validateDeterministicCollectionRef(section.Bytes, true)
		if err != nil {
			return err
		}
		if local {
			return protocolError(ErrInvalidCommand, "collection handle ref is not deterministic")
		}
	case SectionDocumentFormat:
		format, n, err := readUvarint(section.Bytes)
		if err != nil {
			return err
		}
		if n != len(section.Bytes) {
			return protocolError(ErrMalformedFrame, "document_format has %d trailing bytes", len(section.Bytes)-n)
		}
		return validateDeterministicDocumentFormatEnum(format)
	case SectionDocumentIDs, SectionDocuments, SectionTemplateRecords:
		if section.ID == SectionDocumentIDs {
			return validateDeterministicDocumentIDs(section.Bytes, limits)
		}
		return validateByteVector(section.Bytes, limits)
	case SectionAckPolicy:
		policy, n, err := readUvarint(section.Bytes)
		if err != nil {
			return err
		}
		if n != len(section.Bytes) {
			return protocolError(ErrMalformedFrame, "section %d has %d trailing bytes", section.ID, len(section.Bytes)-n)
		}
		if err := validateDeterministicAckPolicyEnum(policy); err != nil {
			return err
		}
	case SectionExpectedCatalogVersion:
		_, n, err := readUvarint(section.Bytes)
		if err != nil {
			return err
		}
		if n != len(section.Bytes) {
			return protocolError(ErrMalformedFrame, "section %d has %d trailing bytes", section.ID, len(section.Bytes)-n)
		}
	case SectionReplacementMode:
		mode, n, err := readUvarint(section.Bytes)
		if err != nil {
			return err
		}
		if n != len(section.Bytes) {
			return protocolError(ErrMalformedFrame, "section %d has %d trailing bytes", section.ID, len(section.Bytes)-n)
		}
		if mode != deterministicReplacementExisting {
			return protocolError(ErrInvalidCommand, "unsupported replacement_mode %d", mode)
		}
	case SectionCollectionMeta:
		return validateDeterministicCollectionMeta(section.Bytes)
	case SectionIndexDefinition:
		return validateDeterministicIndexDefinition(section.Bytes, true)
	case SectionIndexName:
		return validateDeterministicEncodedName(section.Bytes, "index_name")
	}
	return nil
}

func validateDeterministicDocumentIDs(raw []byte, limits Limits) error {
	if err := validateByteVector(raw, limits); err != nil {
		return err
	}
	count64, off, err := readUvarint(raw)
	if err != nil {
		return err
	}
	if count64 > uint64(maxInt) {
		return protocolError(ErrResourceExhausted, "document_ids count exceeds int capacity")
	}
	if count64 > maxDeterministicDocumentIDs {
		return protocolError(ErrResourceExhausted, "document_ids count %d exceeds deterministic limit %d", count64, maxDeterministicDocumentIDs)
	}
	count := int(count64)
	var stackItems [256]deterministicIDItem
	items := stackItems[:0]
	if count > cap(items) {
		items = make([]deterministicIDItem, count)
	} else {
		items = items[:count]
	}
	payloadLen := 0
	for i := 0; i < count; i++ {
		length, n, err := readUvarint(raw[off:])
		if err != nil {
			return err
		}
		off += n
		if length == 0 {
			return protocolError(ErrInvalidCommand, "empty document id at index %d", i)
		}
		if length > uint64(maxInt) || payloadLen > maxInt-int(length) {
			return protocolError(ErrResourceExhausted, "document_ids payload length exceeds int capacity")
		}
		items[i] = deterministicIDItem{offset: payloadLen, length: int(length)}
		payloadLen += int(length)
	}
	payload := raw[off:]
	if payloadLen != len(payload) {
		return protocolError(ErrMalformedFrame, "document_ids payload length %d does not match declared lengths %d", len(payload), payloadLen)
	}
	sortDeterministicIDItems(payload, items)
	for i := 1; i < count; i++ {
		left := payload[items[i-1].offset : items[i-1].offset+items[i-1].length]
		right := payload[items[i].offset : items[i].offset+items[i].length]
		if bytes.Equal(left, right) {
			return protocolError(ErrDuplicateDocumentID, "duplicate document id")
		}
	}
	return nil
}

type deterministicIDItem struct {
	offset int
	length int
}

func sortDeterministicIDItems(payload []byte, items []deterministicIDItem) {
	slices.SortFunc(items, func(leftItem, rightItem deterministicIDItem) int {
		left := payload[leftItem.offset : leftItem.offset+leftItem.length]
		right := payload[rightItem.offset : rightItem.offset+rightItem.length]
		return bytes.Compare(left, right)
	})
}

func validateDeterministicCollectionRef(raw []byte, requireTaggedName bool) (bool, error) {
	if len(raw) == 0 {
		return false, protocolError(ErrInvalidCommand, "empty collection_ref")
	}
	switch raw[0] {
	case deterministicCollectionRefTagName:
		return false, validateDeterministicName(raw[1:], "collection name")
	case deterministicCollectionRefTagHandle:
		_, n, err := readUvarint(raw[1:])
		if err != nil {
			return true, err
		}
		if n+1 != len(raw) {
			return true, protocolError(ErrMalformedFrame, "collection handle ref has trailing bytes")
		}
		return true, nil
	default:
		if requireTaggedName {
			return false, protocolError(ErrMalformedFrame, "collection_ref must use tagged collection name")
		}
		return false, validateDeterministicName(raw, "collection name")
	}
}

func validateDeterministicName(raw []byte, field string) error {
	name := string(raw)
	switch field {
	case "collection name":
		if err := collections.ValidateCollectionName(name); err != nil {
			return protocolError(ErrInvalidCommand, "%v", err)
		}
	default:
		if err := collections.ValidateIndexName(name); err != nil {
			return protocolError(ErrInvalidCommand, "%v", err)
		}
	}
	return nil
}

func validateDeterministicIndexPath(path string, field string) error {
	if err := collections.ValidateIndexPath(path); err != nil {
		return protocolError(ErrInvalidCommand, "%v", err)
	}
	return nil
}

func validateDeterministicDocumentFormatEnum(value uint64) error {
	switch DocumentFormat(value) {
	case DocumentFormatDefault, DocumentFormatJSON, DocumentFormatBSON, DocumentFormatTemplateV1:
		return nil
	default:
		return protocolError(ErrInvalidCommand, "unsupported document_format enum %d", value)
	}
}

func validateDeterministicRootStorageEnum(value uint64) error {
	switch value {
	case 0, 1, 2:
		return nil
	default:
		return protocolError(ErrInvalidCommand, "unsupported root_storage enum %d", value)
	}
}

func validateDeterministicIndexValueTypeEnum(value uint64) error {
	switch value {
	case 1, 2, 3, 4:
		return nil
	default:
		return protocolError(ErrInvalidCommand, "unsupported index_value_type enum %d", value)
	}
}

func validateDeterministicAckPolicyEnum(value uint64) error {
	switch AckPolicy(value) {
	case 0, AckVisible, AckFlushed, AckSynced, AckRaftCommitted:
		return nil
	default:
		return protocolError(ErrInvalidCommand, "unsupported ack_policy enum %d", value)
	}
}

func validateDeterministicCollectionMeta(raw []byte) error {
	off := 0
	version, err := readDeterministicUvarintField(raw, &off, "collection_meta.version")
	if err != nil {
		return err
	}
	if version != 1 {
		return protocolError(ErrUnsupportedVersion, "collection_meta version %d", version)
	}
	if err := readDeterministicNameField(raw, &off, "collection name"); err != nil {
		return err
	}
	documentFormat, err := readDeterministicUvarintField(raw, &off, "document_format")
	if err != nil {
		return err
	}
	if err := validateDeterministicDocumentFormatEnum(documentFormat); err != nil {
		return err
	}
	dataRootStorage, err := readDeterministicUvarintField(raw, &off, "data_root_storage")
	if err != nil {
		return err
	}
	if err := validateDeterministicRootStorageEnum(dataRootStorage); err != nil {
		return err
	}
	indexStateStorage, err := readDeterministicUvarintField(raw, &off, "index_state_storage")
	if err != nil {
		return err
	}
	if err := validateDeterministicRootStorageEnum(indexStateStorage); err != nil {
		return err
	}
	for _, field := range []string{"allow_array_values_in_index", "disable_indexed_write_memtables", "buffered_indexed_writes"} {
		if err := readDeterministicBoolField(raw, &off, field); err != nil {
			return err
		}
	}
	maxDocs, err := readDeterministicVarintField(raw, &off, "buffered_indexed_write_max_documents")
	if err != nil {
		return err
	}
	if err := validateDeterministicNonNegativeIntCapacity("buffered_indexed_write_max_documents", maxDocs); err != nil {
		return err
	}
	maxBytes, err := readDeterministicVarintField(raw, &off, "buffered_indexed_write_max_bytes")
	if err != nil {
		return err
	}
	if err := validateDeterministicNonNegativeInt64("buffered_indexed_write_max_bytes", maxBytes); err != nil {
		return err
	}
	maxRootRuns, err := readDeterministicVarintField(raw, &off, "buffered_indexed_write_max_root_runs")
	if err != nil {
		return err
	}
	if err := validateDeterministicNonNegativeIntCapacity("buffered_indexed_write_max_root_runs", maxRootRuns); err != nil {
		return err
	}
	for _, field := range []string{"buffered_indexed_async_flush", "buffered_indexed_overlay_roots"} {
		if err := readDeterministicBoolField(raw, &off, field); err != nil {
			return err
		}
	}
	maxQueued, err := readDeterministicVarintField(raw, &off, "buffered_indexed_async_flush_max_queued_units")
	if err != nil {
		return err
	}
	if err := validateDeterministicNonNegativeIntCapacity("buffered_indexed_async_flush_max_queued_units", maxQueued); err != nil {
		return err
	}
	indexCount, err := readDeterministicUvarintField(raw, &off, "index_count")
	if err != nil {
		return err
	}
	if indexCount > uint64(maxInt) {
		return protocolError(ErrResourceExhausted, "index count exceeds int capacity")
	}
	if indexCount > maxDeterministicCollectionIndexes {
		return protocolError(ErrResourceExhausted, "index count %d exceeds limit %d", indexCount, maxDeterministicCollectionIndexes)
	}
	if indexCount > uint64((len(raw)-off)/minDeterministicIndexDefinitionLen) {
		return protocolError(ErrMalformedFrame, "index count %d exceeds remaining collection_meta payload", indexCount)
	}
	for i := uint64(0); i < indexCount; i++ {
		next, err := validateDeterministicIndexDefinitionAt(raw, off, false)
		if err != nil {
			return err
		}
		off = next
	}
	if off != len(raw) {
		return protocolError(ErrMalformedFrame, "collection_meta has %d trailing bytes", len(raw)-off)
	}
	return nil
}

func validateDeterministicDocumentFormat(format uint64) error {
	switch DocumentFormat(format) {
	case DocumentFormatDefault, DocumentFormatJSON, DocumentFormatBSON, DocumentFormatTemplateV1:
		return nil
	default:
		return protocolError(ErrInvalidCommand, "unsupported document_format %d", format)
	}
}

func validateDeterministicRootStorage(field string, policy uint64) error {
	switch policy {
	case 0, 1, 2:
		return nil
	default:
		return protocolError(ErrInvalidCommand, "unsupported %s enum %d", field, policy)
	}
}

func validateDeterministicIndexValueType(valueType uint64) error {
	switch valueType {
	case 1, 2, 3, 4:
		return nil
	default:
		return protocolError(ErrInvalidCommand, "unsupported index_value_type enum %d", valueType)
	}
}

func validateDeterministicNonNegativeInt64(field string, value int64) error {
	if value < 0 {
		return protocolError(ErrInvalidCommand, "%s cannot be negative", field)
	}
	return nil
}

func validateDeterministicNonNegativeIntCapacity(field string, value int64) error {
	if err := validateDeterministicNonNegativeInt64(field, value); err != nil {
		return err
	}
	if value > int64(maxInt) {
		return protocolError(ErrResourceExhausted, "%s exceeds int capacity", field)
	}
	return nil
}

func validateDeterministicIndexDefinition(raw []byte, withVersion bool) error {
	off, err := validateDeterministicIndexDefinitionAt(raw, 0, withVersion)
	if err != nil {
		return err
	}
	if off != len(raw) {
		return protocolError(ErrMalformedFrame, "index_definition has %d trailing bytes", len(raw)-off)
	}
	return nil
}

func validateDeterministicIndexDefinitionAt(raw []byte, off int, withVersion bool) (int, error) {
	if withVersion {
		version, err := readDeterministicUvarintField(raw, &off, "index_definition.version")
		if err != nil {
			return 0, err
		}
		if version != 1 {
			return 0, protocolError(ErrUnsupportedVersion, "index_definition version %d", version)
		}
	}
	if err := readDeterministicNameField(raw, &off, "index name"); err != nil {
		return 0, err
	}
	field, err := readDeterministicStringField(raw, &off, "index field")
	if err != nil {
		return 0, err
	}
	if err := validateDeterministicIndexPath(field, "index field"); err != nil {
		return 0, err
	}
	valueType, err := readDeterministicUvarintField(raw, &off, "index value type")
	if err != nil {
		return 0, err
	}
	if err := validateDeterministicIndexValueTypeEnum(valueType); err != nil {
		return 0, err
	}
	if err := readDeterministicBoolField(raw, &off, "unique"); err != nil {
		return 0, err
	}
	if err := readDeterministicBoolField(raw, &off, "multi_key"); err != nil {
		return 0, err
	}
	storagePolicy, err := readDeterministicUvarintField(raw, &off, "index storage policy")
	if err != nil {
		return 0, err
	}
	if err := validateDeterministicRootStorageEnum(storagePolicy); err != nil {
		return 0, err
	}
	return off, nil
}

func validateDeterministicEncodedName(raw []byte, field string) error {
	off := 0
	if err := readDeterministicNameField(raw, &off, field); err != nil {
		return err
	}
	if off != len(raw) {
		return protocolError(ErrMalformedFrame, "%s has %d trailing bytes", field, len(raw)-off)
	}
	return nil
}

func readDeterministicNameField(raw []byte, off *int, field string) error {
	value, err := readDeterministicStringField(raw, off, field)
	if err != nil {
		return err
	}
	switch field {
	case "collection name":
		if err := collections.ValidateCollectionName(value); err != nil {
			return protocolError(ErrInvalidCommand, "%v", err)
		}
	default:
		if err := collections.ValidateIndexName(value); err != nil {
			return protocolError(ErrInvalidCommand, "%v", err)
		}
	}
	return nil
}

func readDeterministicStringField(raw []byte, off *int, field string) (string, error) {
	length, err := readDeterministicUvarintField(raw, off, field+".length")
	if err != nil {
		return "", err
	}
	if length > uint64(len(raw)-*off) {
		return "", protocolError(ErrMalformedFrame, "%s length exceeds remaining payload", field)
	}
	start := *off
	*off += int(length)
	return string(raw[start:*off]), nil
}

func readDeterministicUvarintField(raw []byte, off *int, field string) (uint64, error) {
	if off == nil || *off < 0 || *off > len(raw) {
		return 0, protocolError(ErrMalformedFrame, "invalid %s offset", field)
	}
	value, n, err := readUvarint(raw[*off:])
	if err != nil {
		return 0, err
	}
	*off += n
	return value, nil
}

func readDeterministicVarintField(raw []byte, off *int, field string) (int64, error) {
	if off == nil || *off < 0 || *off > len(raw) {
		return 0, protocolError(ErrMalformedFrame, "invalid %s offset", field)
	}
	value, n := binary.Varint(raw[*off:])
	if n <= 0 {
		return 0, protocolError(ErrMalformedFrame, "invalid %s", field)
	}
	if !isMinimalVarint(value, n) {
		return 0, protocolError(ErrMalformedFrame, "non-minimal %s", field)
	}
	*off += n
	return value, nil
}

func isMinimalVarint(value int64, n int) bool {
	var buf [binary.MaxVarintLen64]byte
	return n == binary.PutVarint(buf[:], value)
}

func readDeterministicBoolField(raw []byte, off *int, field string) error {
	if off == nil || *off < 0 || *off >= len(raw) {
		return protocolError(ErrMalformedFrame, "missing %s", field)
	}
	value := raw[*off]
	*off = *off + 1
	switch value {
	case 0, 1:
		return nil
	default:
		return protocolError(ErrMalformedFrame, "invalid %s bool %d", field, value)
	}
}

func sortSectionsByID(sections []Section) {
	slices.SortStableFunc(sections, func(a, b Section) int {
		return cmp.Compare(a.ID, b.ID)
	})
}
