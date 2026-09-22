package nativewire

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"errors"
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

	deterministicCollectionRefTagName     = 1
	deterministicCollectionRefTagHandle   = 2
	deterministicReplacementExisting      = 1
	deterministicTemplateV1InputMagic     = "TD1I"
	deterministicTemplateV1StoredMagic    = "TD1D"
	deterministicTemplateV1RecordMagic    = "TD1T"
	deterministicTemplateV1KindNull       = byte(0)
	deterministicTemplateV1KindFalse      = byte(1)
	deterministicTemplateV1KindTrue       = byte(2)
	deterministicTemplateV1KindFloat64    = byte(3)
	deterministicTemplateV1KindString     = byte(4)
	deterministicTemplateV1KindObject     = byte(5)
	deterministicTemplateV1KindArray      = byte(6)
	deterministicTemplateV1MaxArray       = 1 << 20
	deterministicTemplateV1MaxTemplates   = 1 << 16
	deterministicTemplateV1MaxDepth       = 64
	minDeterministicIndexDefinitionLen    = 6
	minDeterministicVectorDefinitionLen   = 7
	maxDeterministicCollectionMetaVersion = 5
	maxDeterministicCollectionIndexes     = 1 << 16
	maxDeterministicUint32                = uint64(^uint32(0))
)

var (
	deterministicEntryRegistryOnce sync.Once
	deterministicEntryRegistry     *Registry
	deterministicEntryRegistryErr  error

	errDeterministicTemplateUnresolved = errors.New("deterministic template requires storage resolver")
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

// AppendDeterministicEntry appends a canonical replicated command entry using
// default limits. On error it returns dst truncated to its original length.
func AppendDeterministicEntry(dst []byte, cmd ValidatedCommand) ([]byte, error) {
	return AppendDeterministicEntryWithLimits(dst, cmd, Limits{})
}

// AppendDeterministicEntryWithLimits appends a canonical replicated command
// entry while validating deterministic payloads against caller-provided limits.
// On error it returns dst truncated to its original length.
func AppendDeterministicEntryWithLimits(dst []byte, cmd ValidatedCommand, limits Limits) ([]byte, error) {
	limits = limits.withDefaults()
	start := len(dst)
	if cmd.Schema == nil {
		return dst[:start], protocolError(ErrInvalidCommand, "missing command schema")
	}
	if !cmd.Schema.Replicated {
		return dst[:start], protocolError(ErrInvalidCommand, "command %s is not replicated", cmd.Schema.Name)
	}
	unsupportedFlags := UnsupportedDeterministicCommandFlags(cmd.Header.Flags)
	if unsupportedFlags != 0 {
		return dst[:start], protocolError(ErrUnsupportedFeature, "unsupported deterministic command flags 0x%x", unsupportedFlags)
	}
	deterministicFlags := DeterministicCommandFlags(cmd.Header.Flags)

	var deterministicScratch [deterministicSectionScratchCapacity]Section
	deterministic, err := cmd.deterministicSectionsInto(deterministicScratch[:0], limits)
	if err != nil {
		return dst[:start], err
	}
	sortSectionsByID(deterministic)
	if cmd.Schema.RequiresIdempotency && deterministicSectionCount(deterministic, SectionIdempotencyKey) == 0 {
		return dst[:start], protocolError(ErrInvalidCommand, "missing deterministic idempotency key")
	}
	if cmd.Schema.RequiresCatalogGuard && deterministicSectionCount(deterministic, SectionExpectedCatalogVersion) == 0 {
		return dst[:start], protocolError(ErrInvalidCommand, "missing deterministic catalog guard")
	}
	if err := validateDeterministicCommand(cmd.Header.ID, deterministic, limits); err != nil {
		return dst[:start], err
	}
	if len(deterministic) > limits.MaxSections {
		return dst[:start], protocolError(ErrResourceExhausted, "deterministic entry section count %d exceeds limit %d", len(deterministic), limits.MaxSections)
	}
	if len(deterministic) > maxDeterministicEntrySections {
		return dst[:start], protocolError(ErrResourceExhausted, "deterministic entry section count %d exceeds deterministic limit %d", len(deterministic), maxDeterministicEntrySections)
	}
	for _, section := range deterministic {
		if uint64(len(section.Bytes)) > limits.MaxSectionLen {
			return dst[:start], protocolError(ErrResourceExhausted, "deterministic entry section %d length %d exceeds limit %d", section.ID, len(section.Bytes), limits.MaxSectionLen)
		}
	}
	encodedLen, err := deterministicEntryEncodedLen(cmd.Header.ID, cmd.Header.Version, deterministicFlags, deterministic)
	if err != nil {
		return dst[:start], err
	}
	if encodedLen > limits.MaxFrameSize {
		return dst[:start], protocolError(ErrResourceExhausted, "deterministic entry length %d exceeds limit %d", encodedLen, limits.MaxFrameSize)
	}
	if encodedLen > uint64(maxInt-start) {
		return dst[:start], protocolError(ErrResourceExhausted, "deterministic entry length exceeds int capacity")
	}
	dst = growBytes(dst, int(encodedLen))

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

func deterministicEntryEncodedLen(commandID CommandID, commandVersion, commandFlags uint64, sections []Section) (uint64, error) {
	total := uint64(len(DeterministicEntryMagic))
	add := func(n uint64) error {
		if total > ^uint64(0)-n {
			return protocolError(ErrResourceExhausted, "deterministic entry length overflows uint64")
		}
		total += n
		return nil
	}
	for _, value := range []uint64{DeterministicEntryVersion, uint64(commandID), commandVersion, commandFlags, uint64(len(sections))} {
		if err := add(uint64(uvarintLen(value))); err != nil {
			return 0, err
		}
	}
	for _, section := range sections {
		if err := add(uint64(uvarintLen(uint64(section.ID)))); err != nil {
			return 0, err
		}
		if err := add(uint64(uvarintLen(uint64(len(section.Bytes))))); err != nil {
			return 0, err
		}
		if err := add(uint64(len(section.Bytes))); err != nil {
			return 0, err
		}
	}
	return total, nil
}

// DeterministicEntryDigest returns the SHA-256 digest of canonical deterministic entry bytes.
func DeterministicEntryDigest(entry []byte) [32]byte {
	return sha256.Sum256(entry)
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
	if _, err := validateDecodedDeterministicEntry(registry, CommandID(commandID), commandVersion, sections, limits); err != nil {
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

func (cmd ValidatedCommand) deterministicSectionsInto(dst []Section, limits Limits) ([]Section, error) {
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
		if err := validateDeterministicSectionPayload(section, limits); err != nil {
			return nil, err
		}
		out = append(out, Section{ID: section.ID, Bytes: section.Bytes})
	}
	return out, nil
}

func validateDeterministicCommand(commandID CommandID, deterministic []Section, limits Limits) error {
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
	case CommandUpdateBSONSet:
		idCount, err := deterministicByteVectorCount(deterministic, SectionDocumentIDs)
		if err != nil {
			return err
		}
		if idCount != 1 {
			return protocolError(ErrInvalidCommand, "update_bson_set requires exactly one document id, got %d", idCount)
		}
		nameCount, err := deterministicByteVectorCount(deterministic, SectionUpdateFieldNames)
		if err != nil {
			return err
		}
		valueCount, err := deterministicByteVectorCount(deterministic, SectionUpdateFieldValues)
		if err != nil {
			return err
		}
		if nameCount == 0 {
			return protocolError(ErrInvalidCommand, "update_bson_set requires at least one field")
		}
		if nameCount != valueCount {
			return protocolError(ErrInvalidCommand, "update_field_names length %d does not match update_field_values length %d", nameCount, valueCount)
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
		local, err := validateDeterministicCollectionRef(raw, limits)
		if err != nil {
			return err
		}
		if local {
			return protocolError(ErrInvalidCommand, "collection handle ref is not deterministic")
		}
	}
	return nil
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
			if code, ok := ErrorCodeOf(err); ok {
				return 0, protocolError(code, "deterministic section %d byte-vector count: %v", id, err)
			}
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

func validateDecodedDeterministicEntry(registry *Registry, commandID CommandID, commandVersion uint64, sections []Section, limits Limits) (*CommandSchema, error) {
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
	if err := validateDeterministicCommand(commandID, sections, limits); err != nil {
		return nil, err
	}
	return schema, nil
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
		return validateDeterministicTemplateStoredDocument(raw, &pos, templates, true)
	}
	pos = 0
	if !consumeDeterministicTemplateMagic(raw, &pos, deterministicTemplateV1InputMagic) {
		return protocolError(ErrMalformedFrame, "document is not TD1I or TD1D")
	}
	templateCount, err := readDeterministicTemplateUvarint(raw, &pos, "template count")
	if err != nil {
		return err
	}
	if templateCount > deterministicTemplateV1MaxTemplates {
		return protocolError(ErrResourceExhausted, "template-v1 template count %d exceeds limit", templateCount)
	}
	if templateCount > uint64(len(raw)) {
		return protocolError(ErrMalformedFrame, "malformed template-v1 template count")
	}
	if templateCount > uint64((len(raw)-pos)/(sha256.Size+1)) {
		return protocolError(ErrMalformedFrame, "malformed template-v1 template count")
	}
	var embeddedScratch [8]deterministicTemplate
	embeddedTemplates := embeddedScratch[:0]
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
			embeddedTemplates = append(embeddedTemplates, deterministicTemplate{id: id, fieldCount: fieldCount})
		}
	}
	if !consumeDeterministicTemplateMagic(raw, &pos, deterministicTemplateV1StoredMagic) {
		return protocolError(ErrMalformedFrame, "malformed template-v1 stored document")
	}
	return validateDeterministicTemplateStoredDocument(raw, &pos, embeddedTemplates, true)
}

func validateDeterministicTemplateStoredDocument(raw []byte, pos *int, templates deterministicTemplateSet, allowUnresolved bool) error {
	var id [sha256.Size]byte
	if pos == nil || *pos < 0 || len(raw)-*pos < len(id) {
		return protocolError(ErrMalformedFrame, "malformed template-v1 root template id")
	}
	copy(id[:], raw[*pos:*pos+len(id)])
	*pos += len(id)
	fieldCount, ok := templates.lookup(id)
	if !ok {
		if allowUnresolved {
			*pos = len(raw)
			return nil
		}
		return protocolError(ErrInvalidCommand, "template-v1 template id is not included")
	}
	for i := 0; i < fieldCount; i++ {
		if err := skipDeterministicTemplateValue(raw, pos, templates, 0, allowUnresolved); err != nil {
			if allowUnresolved && err == errDeterministicTemplateUnresolved {
				*pos = len(raw)
				return nil
			}
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

func skipDeterministicTemplateValue(raw []byte, pos *int, templates deterministicTemplateSet, depth int, allowUnresolved bool) error {
	if pos == nil || *pos < 0 || *pos >= len(raw) {
		return protocolError(ErrMalformedFrame, "malformed template-v1 value")
	}
	if depth > deterministicTemplateV1MaxDepth {
		return protocolError(ErrResourceExhausted, "template-v1 nesting exceeds limit %d", deterministicTemplateV1MaxDepth)
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
			if err := skipDeterministicTemplateValue(raw, pos, templates, depth+1, allowUnresolved); err != nil {
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
			if allowUnresolved {
				return errDeterministicTemplateUnresolved
			}
			return protocolError(ErrInvalidCommand, "template-v1 object template id is not included")
		}
		for i := 0; i < fieldCount; i++ {
			if err := skipDeterministicTemplateValue(raw, pos, templates, depth+1, allowUnresolved); err != nil {
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
func validateDeterministicSectionPayload(section Section, limits Limits) error {
	switch section.ID {
	case SectionIdempotencyKey:
		return validateDeterministicOpaquePayload("idempotency_key", section.Bytes, limits)
	case SectionCollectionRef:
		local, err := validateDeterministicCollectionRef(section.Bytes, limits)
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
	case SectionDocumentIDs, SectionDocuments, SectionTemplateRecords, SectionUpdateFieldNames, SectionUpdateFieldValues:
		if section.ID == SectionDocumentIDs {
			return validateDeterministicDocumentIDs(section.Bytes, limits)
		}
		return validateByteVector(section.Bytes, limits)
	case SectionCollectionMeta:
		if err := validateDeterministicOpaquePayload("collection_meta", section.Bytes, limits); err != nil {
			return err
		}
		return validateDeterministicCollectionMeta(section.Bytes, limits)
	case SectionIndexDefinition:
		if err := validateDeterministicOpaquePayload("index_definition", section.Bytes, limits); err != nil {
			return err
		}
		return validateDeterministicIndexDefinition(section.Bytes, true, limits)
	case SectionIndexName:
		return validateDeterministicEncodedNameField("index_name", section.Bytes, limits)
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
	}
	return nil
}

func validateDeterministicOpaquePayload(name string, raw []byte, limits Limits) error {
	limits = limits.withDefaults()
	if len(raw) == 0 {
		return protocolError(ErrInvalidCommand, "%s cannot be empty", name)
	}
	if uint64(len(raw)) > limits.MaxDeterministicOpaquePayloadBytes {
		return protocolError(ErrResourceExhausted, "%s length %d exceeds limit %d", name, len(raw), limits.MaxDeterministicOpaquePayloadBytes)
	}
	return nil
}

func validateDeterministicEncodedNameField(name string, raw []byte, limits Limits) error {
	if len(raw) == 0 {
		return protocolError(ErrInvalidCommand, "%s cannot be empty", name)
	}
	off := 0
	if err := readDeterministicNameField(raw, &off, name, limits); err != nil {
		return err
	}
	if off != len(raw) {
		return protocolError(ErrMalformedFrame, "%s has %d trailing bytes", name, len(raw)-off)
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

func validateDeterministicCollectionRef(raw []byte, limits Limits) (bool, error) {
	if len(raw) == 0 {
		return false, protocolError(ErrInvalidCommand, "empty collection_ref")
	}
	switch raw[0] {
	case deterministicCollectionRefTagName:
		return false, validateDeterministicNameValue("collection name", raw[1:], limits)
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
		return false, protocolError(ErrMalformedFrame, "collection_ref must use tagged collection name")
	}
}

func validateDeterministicNameValue(field string, raw []byte, limits Limits) error {
	limits = limits.withDefaults()
	if len(raw) == 0 {
		return protocolError(ErrInvalidCommand, "%s cannot be empty", field)
	}
	if uint64(len(raw)) > limits.MaxDeterministicNameBytes {
		return protocolError(ErrResourceExhausted, "%s length %d exceeds limit %d", field, len(raw), limits.MaxDeterministicNameBytes)
	}
	name := string(raw)
	if !utf8.ValidString(name) {
		return protocolError(ErrInvalidCommand, "invalid %s", field)
	}
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

func validateDeterministicVectorMetricEnum(value uint64) error {
	switch value {
	case 1, 2, 3:
		return nil
	default:
		return protocolError(ErrInvalidCommand, "unsupported vector_metric enum %d", value)
	}
}

func validateDeterministicVectorIndexEncodingEnum(value uint64) error {
	switch value {
	case 1, 2:
		return nil
	default:
		return protocolError(ErrInvalidCommand, "unsupported vector_index_encoding enum %d", value)
	}
}

func validateDeterministicVectorIndexStrategyEnum(value uint64) error {
	switch value {
	case 1, 2:
		return nil
	default:
		return protocolError(ErrInvalidCommand, "unsupported vector_index_strategy enum %d", value)
	}
}

func validateDeterministicCollectionMeta(raw []byte, limits Limits) error {
	off := 0
	version, err := readDeterministicUvarintField(raw, &off, "collection_meta.version")
	if err != nil {
		return err
	}
	if version < 1 || version > maxDeterministicCollectionMetaVersion {
		return protocolError(ErrUnsupportedVersion, "collection_meta version %d", version)
	}
	if err := readDeterministicNameField(raw, &off, "collection name", limits); err != nil {
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
		next, err := validateDeterministicIndexDefinitionAt(raw, off, false, limits)
		if err != nil {
			return err
		}
		off = next
	}
	if version >= 2 {
		vectorIndexCount, err := readDeterministicUvarintField(raw, &off, "vector_index_count")
		if err != nil {
			return err
		}
		if vectorIndexCount > uint64(maxInt) {
			return protocolError(ErrResourceExhausted, "vector index count exceeds int capacity")
		}
		if vectorIndexCount > maxDeterministicCollectionIndexes {
			return protocolError(ErrResourceExhausted, "vector index count %d exceeds limit %d", vectorIndexCount, maxDeterministicCollectionIndexes)
		}
		if vectorIndexCount > uint64((len(raw)-off)/minDeterministicVectorDefinitionLen) {
			return protocolError(ErrMalformedFrame, "vector index count %d exceeds remaining collection_meta payload", vectorIndexCount)
		}
		for i := uint64(0); i < vectorIndexCount; i++ {
			next, err := validateDeterministicVectorIndexDefinitionAt(raw, off, version)
			if err != nil {
				return err
			}
			off = next
		}
	}
	if off != len(raw) {
		return protocolError(ErrMalformedFrame, "collection_meta has %d trailing bytes", len(raw)-off)
	}
	return nil
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

func validateDeterministicIndexDefinition(raw []byte, withVersion bool, limits Limits) error {
	off, err := validateDeterministicIndexDefinitionAt(raw, 0, withVersion, limits)
	if err != nil {
		return err
	}
	if off != len(raw) {
		return protocolError(ErrMalformedFrame, "index_definition has %d trailing bytes", len(raw)-off)
	}
	return nil
}

func validateDeterministicIndexDefinitionAt(raw []byte, off int, withVersion bool, limits Limits) (int, error) {
	if withVersion {
		version, err := readDeterministicUvarintField(raw, &off, "index_definition.version")
		if err != nil {
			return 0, err
		}
		if version != 1 {
			return 0, protocolError(ErrUnsupportedVersion, "index_definition version %d", version)
		}
	}
	if err := readDeterministicNameField(raw, &off, "index name", limits); err != nil {
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

func validateDeterministicVectorIndexDefinitionAt(raw []byte, off int, version uint64) (int, error) {
	if _, err := readDeterministicStringField(raw, &off, "vector index name"); err != nil {
		return 0, err
	}
	if _, err := readDeterministicStringField(raw, &off, "vector index field"); err != nil {
		return 0, err
	}
	metric, err := readDeterministicUvarintField(raw, &off, "vector index metric")
	if err != nil {
		return 0, err
	}
	if err := validateDeterministicVectorMetricEnum(metric); err != nil {
		return 0, err
	}
	for _, field := range []string{"vector index dimensions", "vector index m", "vector index ef_construction", "vector index ef_search"} {
		value, err := readDeterministicVarintField(raw, &off, field)
		if err != nil {
			return 0, err
		}
		if err := validateDeterministicNonNegativeIntCapacity(field, value); err != nil {
			return 0, err
		}
	}
	encoding, err := readDeterministicUvarintField(raw, &off, "vector index encoding")
	if err != nil {
		return 0, err
	}
	if err := validateDeterministicVectorIndexEncodingEnum(encoding); err != nil {
		return 0, err
	}
	if version < 3 {
		return off, nil
	}
	strategy, err := readDeterministicUvarintField(raw, &off, "vector index strategy")
	if err != nil {
		return 0, err
	}
	if err := validateDeterministicVectorIndexStrategyEnum(strategy); err != nil {
		return 0, err
	}
	quantizedCount, err := readDeterministicUvarintField(raw, &off, "quantized_index_count")
	if err != nil {
		return 0, err
	}
	if quantizedCount > uint64(maxInt) {
		return 0, protocolError(ErrResourceExhausted, "quantized vector index count exceeds int capacity")
	}
	if quantizedCount > maxDeterministicCollectionIndexes {
		return 0, protocolError(ErrResourceExhausted, "quantized vector index count %d exceeds limit %d", quantizedCount, maxDeterministicCollectionIndexes)
	}
	for i := uint64(0); i < quantizedCount; i++ {
		next, err := validateDeterministicQuantizedVectorIndexDefinitionAt(raw, off, version, i)
		if err != nil {
			return 0, err
		}
		off = next
	}
	return off, nil
}

func validateDeterministicQuantizedVectorIndexDefinitionAt(raw []byte, off int, version uint64, index uint64) (int, error) {
	name, err := readDeterministicStringField(raw, &off, "quantized index name")
	if err != nil {
		return 0, err
	}
	codec, err := readDeterministicStringField(raw, &off, "quantized index codec")
	if err != nil {
		return 0, err
	}
	codecVersion, err := readDeterministicUvarintField(raw, &off, "quantized index version")
	if err != nil {
		return 0, err
	}
	if codecVersion > maxDeterministicUint32 {
		return 0, protocolError(ErrResourceExhausted, "quantized vector index version %d exceeds uint32 capacity", codecVersion)
	}
	q := collections.QuantizedVectorIndexDefinition{
		Name:    name,
		Codec:   codec,
		Version: uint32(codecVersion),
	}
	if version >= 4 {
		hasScalarU8Calibration, err := readDeterministicBoolValueField(raw, &off, "scalar_u8_calibration")
		if err != nil {
			return 0, err
		}
		if hasScalarU8Calibration {
			mode, err := readDeterministicStringField(raw, &off, "scalar_u8_calibration.mode")
			if err != nil {
				return 0, err
			}
			grouping, err := readDeterministicStringField(raw, &off, "scalar_u8_calibration.grouping")
			if err != nil {
				return 0, err
			}
			policyName, err := readDeterministicStringField(raw, &off, "scalar_u8_calibration.alpha_policy.name")
			if err != nil {
				return 0, err
			}
			quantilePPM, err := readDeterministicUvarintField(raw, &off, "scalar_u8_calibration.alpha_policy.quantile_ppm")
			if err != nil {
				return 0, err
			}
			if quantilePPM > maxDeterministicUint32 {
				return 0, protocolError(ErrResourceExhausted, "scalar_u8 alpha policy quantile_ppm %d exceeds uint32 capacity", quantilePPM)
			}
			q.ScalarU8Calibration = &collections.ScalarU8CalibrationConfig{
				Mode:     collections.ScalarU8CalibrationMode(mode),
				Grouping: collections.ScalarU8CalibrationGrouping(grouping),
				AlphaPolicy: collections.ScalarU8AlphaPolicy{
					Name:        collections.ScalarU8AlphaPolicyName(policyName),
					QuantilePPM: uint32(quantilePPM),
				},
			}
		}
	}
	if q.ScalarU8Calibration != nil {
		if _, err := collections.NormalizeScalarU8CalibrationConfig("", int(index), q); err != nil {
			return 0, protocolError(ErrInvalidCommand, "%v", err)
		}
	}
	return off, nil
}

func readDeterministicNameField(raw []byte, off *int, field string, limits Limits) error {
	limits = limits.withDefaults()
	length, err := readDeterministicUvarintField(raw, off, field+".length")
	if err != nil {
		return err
	}
	if length > uint64(len(raw)-*off) {
		return protocolError(ErrMalformedFrame, "%s length exceeds remaining payload", field)
	}
	if length > limits.MaxDeterministicNameBytes {
		return protocolError(ErrResourceExhausted, "%s length %d exceeds limit %d", field, length, limits.MaxDeterministicNameBytes)
	}
	if length > uint64(maxInt) {
		return protocolError(ErrResourceExhausted, "%s length exceeds int capacity", field)
	}
	start := *off
	*off += int(length)
	return validateDeterministicNameValue(field, raw[start:*off], limits)
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
	_, err := readDeterministicBoolValueField(raw, off, field)
	return err
}

func readDeterministicBoolValueField(raw []byte, off *int, field string) (bool, error) {
	if off == nil || *off < 0 || *off >= len(raw) {
		return false, protocolError(ErrMalformedFrame, "missing %s", field)
	}
	value := raw[*off]
	*off = *off + 1
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, protocolError(ErrMalformedFrame, "invalid %s bool %d", field, value)
	}
}

func sortSectionsByID(sections []Section) {
	slices.SortStableFunc(sections, func(a, b Section) int {
		return cmp.Compare(a.ID, b.ID)
	})
}
