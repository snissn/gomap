package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	templateV1InputMagic  = "TD1I"
	templateV1StoredMagic = "TD1D"
	templateV1RecordMagic = "TD1T"

	templateV1KindNull byte = iota
	templateV1KindFalse
	templateV1KindTrue
	templateV1KindFloat64
	templateV1KindString
	templateV1KindObject
	templateV1KindArray
)

var (
	errTemplateV1MissingResolver     = errors.New("collections: missing template-v1 resolver")
	errTemplateV1MissingTemplateRoot = errors.New("collections: missing template-v1 template root")
	errTemplateV1TemplateNotFound    = errors.New("collections: template-v1 template not found")
)

type templateV1Record struct {
	id  [32]byte
	raw []byte
	tpl *templateV1Template
}

type templateV1Template struct {
	id     [32]byte
	fields []string
}

type templateV1Resolver interface {
	lookupTemplateV1(id [32]byte) (*templateV1Template, error)
}

type templateV1MemoryResolver struct {
	templates map[string]*templateV1Template
}

type templateV1CompositeResolver struct {
	memory   *templateV1MemoryResolver
	fallback templateV1Resolver
}

type templateV1SnapshotResolver struct {
	snap   *backenddb.Snapshot
	rootID uint64
	cache  map[string]*templateV1Template
}

type templateV1ObjectRef struct {
	templateID [32]byte
	values     []byte
}

func normalizedDocumentFormat(format DocumentFormat) DocumentFormat {
	switch format {
	case DocumentFormatDefault:
		return DocumentFormatJSON
	default:
		return format
	}
}

func normalizeDocumentFormat(format DocumentFormat) (DocumentFormat, error) {
	switch DocumentFormat(strings.ToLower(strings.TrimSpace(string(format)))) {
	case DocumentFormatDefault, DocumentFormatJSON:
		return DocumentFormatJSON, nil
	case DocumentFormatTemplateV1:
		return DocumentFormatTemplateV1, nil
	default:
		return DocumentFormatDefault, fmt.Errorf("collections: unsupported document format %q", format)
	}
}

func canonicalCollectionOptionDocumentFormat(format DocumentFormat) DocumentFormat {
	normalized, err := normalizeDocumentFormat(format)
	if err != nil || normalized == DocumentFormatJSON {
		return DocumentFormatDefault
	}
	return normalized
}

func prepareInsertDocuments(documents [][]byte, opts collectionOptions) ([][]byte, []templateV1Record, templateV1Resolver, error) {
	switch normalizedDocumentFormat(opts.documentFormat) {
	case DocumentFormatJSON:
		return documents, nil, nil, nil
	case DocumentFormatTemplateV1:
		return prepareTemplateV1InsertDocuments(documents, opts.templateResolver)
	default:
		return nil, nil, nil, fmt.Errorf("collections: unsupported document format %q", opts.documentFormat)
	}
}

func collectionOptionsWithTemplateV1Resolver(opts collectionOptions, snap *backenddb.Snapshot, catalog *collectionCatalog) collectionOptions {
	if normalizedDocumentFormat(opts.documentFormat) != DocumentFormatTemplateV1 || snap == nil || catalog == nil {
		return opts
	}
	opts.templateResolver = &templateV1SnapshotResolver{
		snap:   snap,
		rootID: catalog.rootID(collectionTemplateRootName(catalog.meta.Name)),
		cache:  make(map[string]*templateV1Template),
	}
	return opts
}

func prepareTemplateV1InsertDocuments(documents [][]byte, fallback templateV1Resolver) ([][]byte, []templateV1Record, templateV1Resolver, error) {
	prepared := make([][]byte, len(documents))
	records := make([]templateV1Record, 0)
	resolver := &templateV1MemoryResolver{}
	totalStoredBytes := 0
	for i, document := range documents {
		stored, docRecords, err := parseTemplateV1InsertDocument(document)
		if err != nil {
			return nil, nil, nil, err
		}
		totalStoredBytes += len(stored)
		prepared[i] = stored
		for _, record := range docRecords {
			if err := resolver.addRecord(record); err != nil {
				return nil, nil, nil, err
			}
			publish, err := shouldPublishTemplateV1Record(record, fallback)
			if err != nil {
				return nil, nil, nil, err
			}
			if publish {
				records = append(records, record)
			}
		}
	}
	arena := make([]byte, 0, totalStoredBytes)
	for i, stored := range prepared {
		start := len(arena)
		arena = append(arena, stored...)
		prepared[i] = arena[start:len(arena):len(arena)]
	}
	if fallback == nil {
		if err := validateTemplateV1PreparedDocuments(prepared, resolver); err != nil {
			return nil, nil, nil, err
		}
		return prepared, records, resolver, nil
	}
	composite := &templateV1CompositeResolver{memory: resolver, fallback: fallback}
	if err := validateTemplateV1PreparedDocuments(prepared, composite); err != nil {
		return nil, nil, nil, err
	}
	return prepared, records, composite, nil
}

func shouldPublishTemplateV1Record(record templateV1Record, fallback templateV1Resolver) (bool, error) {
	if fallback == nil {
		return true, nil
	}
	existing, err := fallback.lookupTemplateV1(record.id)
	if err == nil {
		if !equalStringSlices(existing.fields, record.tpl.fields) {
			return false, errors.New("collections: template-v1 template id collision")
		}
		return false, nil
	}
	if errors.Is(err, errTemplateV1TemplateNotFound) || errors.Is(err, errTemplateV1MissingTemplateRoot) {
		return true, nil
	}
	return false, err
}

func parseTemplateV1InsertDocument(raw []byte) ([]byte, []templateV1Record, error) {
	if bytes.HasPrefix(raw, []byte(templateV1StoredMagic)) {
		return raw, nil, nil
	}
	return parseTemplateV1InsertEnvelope(raw)
}

func parseTemplateV1InsertEnvelope(raw []byte) ([]byte, []templateV1Record, error) {
	pos := 0
	if !consumeMagic(raw, &pos, templateV1InputMagic) {
		return nil, nil, errors.New("collections: template-v1 insert requires template input envelope")
	}
	templateCount, err := readTemplateV1Uvarint(raw, &pos)
	if err != nil {
		return nil, nil, err
	}
	if templateCount > uint64(len(raw)) {
		return nil, nil, errors.New("collections: malformed template-v1 template count")
	}
	records := make([]templateV1Record, 0, int(templateCount))
	for i := uint64(0); i < templateCount; i++ {
		var id [32]byte
		if len(raw)-pos < len(id) {
			return nil, nil, errors.New("collections: malformed template-v1 template id")
		}
		copy(id[:], raw[pos:pos+len(id)])
		pos += len(id)
		recordLen, err := readTemplateV1Uvarint(raw, &pos)
		if err != nil {
			return nil, nil, err
		}
		if recordLen > uint64(len(raw)-pos) {
			return nil, nil, errors.New("collections: malformed template-v1 template record length")
		}
		recordRaw := raw[pos : pos+int(recordLen)]
		pos += int(recordLen)
		record, err := parseTemplateV1Record(recordRaw)
		if err != nil {
			return nil, nil, err
		}
		if record.id != id {
			return nil, nil, errors.New("collections: template-v1 template id does not match record")
		}
		records = append(records, record)
	}
	stored := raw[pos:]
	if !bytes.HasPrefix(stored, []byte(templateV1StoredMagic)) {
		return nil, nil, errors.New("collections: malformed template-v1 stored document")
	}
	return stored, records, nil
}

func validateTemplateV1PreparedDocuments(documents [][]byte, resolver templateV1Resolver) error {
	for _, document := range documents {
		if err := validateTemplateV1StoredDocumentTemplates(document, resolver); err != nil {
			return err
		}
	}
	return nil
}

func validateTemplateV1StoredDocumentTemplates(document []byte, resolver templateV1Resolver) error {
	if resolver == nil {
		return errTemplateV1MissingResolver
	}
	root, err := parseTemplateV1StoredDocument(document)
	if err != nil {
		return err
	}
	tpl, err := resolver.lookupTemplateV1(root.templateID)
	if err != nil {
		return err
	}
	pos := 0
	for range tpl.fields {
		if err := skipTemplateV1Value(root.values, &pos, resolver); err != nil {
			return err
		}
	}
	if pos != len(root.values) {
		return errors.New("collections: trailing template-v1 object values")
	}
	return nil
}

func (r *templateV1MemoryResolver) addRecord(record templateV1Record) error {
	if r.templates == nil {
		r.templates = make(map[string]*templateV1Template)
	}
	key := string(record.id[:])
	if existing := r.templates[key]; existing != nil {
		if !equalStringSlices(existing.fields, record.tpl.fields) {
			return errors.New("collections: template-v1 template id collision")
		}
		return nil
	}
	r.templates[key] = record.tpl
	return nil
}

func (r *templateV1MemoryResolver) lookupTemplateV1(id [32]byte) (*templateV1Template, error) {
	if r == nil {
		return nil, errTemplateV1MissingResolver
	}
	tpl := r.templates[string(id[:])]
	if tpl == nil {
		return nil, errTemplateV1TemplateNotFound
	}
	return tpl, nil
}

func (r *templateV1CompositeResolver) lookupTemplateV1(id [32]byte) (*templateV1Template, error) {
	if r == nil {
		return nil, errTemplateV1MissingResolver
	}
	if r.memory != nil && r.memory.templates != nil {
		if tpl := r.memory.templates[string(id[:])]; tpl != nil {
			return tpl, nil
		}
	}
	if r.fallback != nil {
		return r.fallback.lookupTemplateV1(id)
	}
	return nil, errTemplateV1TemplateNotFound
}

func (r *templateV1SnapshotResolver) lookupTemplateV1(id [32]byte) (*templateV1Template, error) {
	if r == nil || r.snap == nil || r.rootID == 0 {
		return nil, errTemplateV1MissingTemplateRoot
	}
	key := string(id[:])
	if tpl := r.cache[key]; tpl != nil {
		return tpl, nil
	}
	entry, err := r.snap.GetEntryAtRoot(r.rootID, id[:])
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, errTemplateV1TemplateNotFound
	}
	if err != nil {
		return nil, err
	}
	record, err := parseTemplateV1Record(entry.Value)
	if err != nil {
		return nil, err
	}
	if record.id != id {
		return nil, errors.New("collections: template-v1 template id mismatch")
	}
	if r.cache == nil {
		r.cache = make(map[string]*templateV1Template)
	}
	r.cache[key] = record.tpl
	return record.tpl, nil
}

func sortTemplateV1Records(records []templateV1Record) {
	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i].id[:], records[j].id[:]) < 0
	})
}

func dedupeTemplateV1Records(records []templateV1Record) ([]templateV1Record, error) {
	if len(records) <= 1 {
		return records, nil
	}
	out := records[:1]
	for _, record := range records[1:] {
		last := &out[len(out)-1]
		if record.id != last.id {
			out = append(out, record)
			continue
		}
		if !bytes.Equal(record.raw, last.raw) {
			return nil, errors.New("collections: template-v1 template id collision")
		}
	}
	return out, nil
}

func EncodeTemplateV1Document(fields []string, values []any) ([]byte, error) {
	if len(fields) != len(values) {
		return nil, errors.New("collections: template-v1 field/value length mismatch")
	}
	obj := make(map[string]any, len(fields))
	for i, field := range fields {
		if _, exists := obj[field]; exists {
			return nil, fmt.Errorf("collections: duplicate template-v1 field %q", field)
		}
		obj[field] = values[i]
	}
	return encodeTemplateV1ObjectMap(obj)
}

type TemplateV1Encoder struct {
	emitted map[string]struct{}
}

func (e *TemplateV1Encoder) Reset() {
	e.emitted = nil
}

func (e *TemplateV1Encoder) EncodeDocument(fields []string, values []any) ([]byte, error) {
	if len(fields) != len(values) {
		return nil, errors.New("collections: template-v1 field/value length mismatch")
	}
	obj := make(map[string]any, len(fields))
	for i, field := range fields {
		if _, exists := obj[field]; exists {
			return nil, fmt.Errorf("collections: duplicate template-v1 field %q", field)
		}
		obj[field] = values[i]
	}
	if e.emitted == nil {
		e.emitted = make(map[string]struct{})
	}
	return encodeTemplateV1ObjectMapWithRecordFilter(obj, func(record templateV1Record) bool {
		key := string(record.id[:])
		if _, exists := e.emitted[key]; exists {
			return false
		}
		e.emitted[key] = struct{}{}
		return true
	})
}

func EncodeTemplateV1DocumentJSON(raw []byte) ([]byte, error) {
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("collections: template-v1 JSON input: %w", err)
	}
	obj, ok := decoded.(map[string]any)
	if !ok {
		return nil, errors.New("collections: template-v1 JSON input must be an object")
	}
	return encodeTemplateV1ObjectMap(obj)
}

func encodeTemplateV1ObjectMap(obj map[string]any) ([]byte, error) {
	return encodeTemplateV1ObjectMapWithRecordFilter(obj, func(templateV1Record) bool { return true })
}

func encodeTemplateV1ObjectMapWithRecordFilter(obj map[string]any, includeRecord func(templateV1Record) bool) ([]byte, error) {
	state := &templateV1BuildState{records: make(map[string]templateV1Record)}
	root, err := state.encodeObject(obj)
	if err != nil {
		return nil, err
	}
	records := make([]templateV1Record, 0, len(state.records))
	for _, record := range state.records {
		if includeRecord == nil || includeRecord(record) {
			records = append(records, record)
		}
	}
	sortTemplateV1Records(records)
	if len(records) == 0 {
		return root, nil
	}

	out := make([]byte, 0, len(templateV1InputMagic)+len(root)+len(records)*48)
	out = append(out, templateV1InputMagic...)
	out = binary.AppendUvarint(out, uint64(len(records)))
	for _, record := range records {
		out = append(out, record.id[:]...)
		out = binary.AppendUvarint(out, uint64(len(record.raw)))
		out = append(out, record.raw...)
	}
	out = append(out, root...)
	return out, nil
}

type templateV1BuildState struct {
	records map[string]templateV1Record
}

func (s *templateV1BuildState) encodeObject(obj map[string]any) ([]byte, error) {
	fields := make([]string, 0, len(obj))
	for field := range obj {
		if err := validateTemplateV1FieldName(field); err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}
	sort.Strings(fields)
	record, err := buildTemplateV1Record(fields)
	if err != nil {
		return nil, err
	}
	s.records[string(record.id[:])] = record
	out := make([]byte, 0, len(templateV1StoredMagic)+32+len(fields)*8)
	out = append(out, templateV1StoredMagic...)
	out = append(out, record.id[:]...)
	for _, field := range fields {
		out, err = s.appendValue(out, obj[field])
		if err != nil {
			return nil, fmt.Errorf("collections: template-v1 field %q: %w", field, err)
		}
	}
	return out, nil
}

func (s *templateV1BuildState) appendObjectValue(dst []byte, obj map[string]any) ([]byte, error) {
	fields := make([]string, 0, len(obj))
	for field := range obj {
		if err := validateTemplateV1FieldName(field); err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}
	sort.Strings(fields)
	record, err := buildTemplateV1Record(fields)
	if err != nil {
		return nil, err
	}
	s.records[string(record.id[:])] = record
	dst = append(dst, templateV1KindObject)
	dst = append(dst, record.id[:]...)
	for _, field := range fields {
		dst, err = s.appendValue(dst, obj[field])
		if err != nil {
			return nil, fmt.Errorf("collections: template-v1 field %q: %w", field, err)
		}
	}
	return dst, nil
}

func (s *templateV1BuildState) appendValue(dst []byte, value any) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return append(dst, templateV1KindNull), nil
	case string:
		dst = append(dst, templateV1KindString)
		dst = binary.AppendUvarint(dst, uint64(len(v)))
		return append(dst, v...), nil
	case bool:
		if v {
			return append(dst, templateV1KindTrue), nil
		}
		return append(dst, templateV1KindFalse), nil
	case float64:
		dst = append(dst, templateV1KindFloat64)
		var scratch [8]byte
		binary.BigEndian.PutUint64(scratch[:], math.Float64bits(v))
		return append(dst, scratch[:]...), nil
	case int:
		return s.appendValue(dst, float64(v))
	case int64:
		return s.appendValue(dst, float64(v))
	case uint64:
		return s.appendValue(dst, float64(v))
	case json.Number:
		f, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return nil, err
		}
		return s.appendValue(dst, f)
	case []any:
		dst = append(dst, templateV1KindArray)
		dst = binary.AppendUvarint(dst, uint64(len(v)))
		var err error
		for _, item := range v {
			dst, err = s.appendValue(dst, item)
			if err != nil {
				return nil, err
			}
		}
		return dst, nil
	case []string:
		dst = append(dst, templateV1KindArray)
		dst = binary.AppendUvarint(dst, uint64(len(v)))
		var err error
		for _, item := range v {
			dst, err = s.appendValue(dst, item)
			if err != nil {
				return nil, err
			}
		}
		return dst, nil
	case map[string]any:
		return s.appendObjectValue(dst, v)
	default:
		return nil, fmt.Errorf("unsupported template-v1 value type %T", value)
	}
}

func buildTemplateV1Record(fields []string) (templateV1Record, error) {
	raw := make([]byte, 0, len(templateV1RecordMagic)+len(fields)*8)
	raw = append(raw, templateV1RecordMagic...)
	raw = binary.AppendUvarint(raw, uint64(len(fields)))
	for _, field := range fields {
		if err := validateTemplateV1FieldName(field); err != nil {
			return templateV1Record{}, err
		}
		raw = binary.AppendUvarint(raw, uint64(len(field)))
		raw = append(raw, field...)
	}
	id := sha256.Sum256(raw)
	copiedFields := append([]string(nil), fields...)
	return templateV1Record{
		id:  id,
		raw: raw,
		tpl: &templateV1Template{id: id, fields: copiedFields},
	}, nil
}

func parseTemplateV1Record(raw []byte) (templateV1Record, error) {
	pos := 0
	if !consumeMagic(raw, &pos, templateV1RecordMagic) {
		return templateV1Record{}, errors.New("collections: malformed template-v1 template record")
	}
	fieldCount, err := readTemplateV1Uvarint(raw, &pos)
	if err != nil {
		return templateV1Record{}, err
	}
	if fieldCount > uint64(len(raw)) {
		return templateV1Record{}, errors.New("collections: malformed template-v1 field count")
	}
	fields := make([]string, 0, int(fieldCount))
	for i := uint64(0); i < fieldCount; i++ {
		fieldLen, err := readTemplateV1Uvarint(raw, &pos)
		if err != nil {
			return templateV1Record{}, err
		}
		if fieldLen > uint64(len(raw)-pos) {
			return templateV1Record{}, errors.New("collections: malformed template-v1 field length")
		}
		field := string(raw[pos : pos+int(fieldLen)])
		pos += int(fieldLen)
		if err := validateTemplateV1FieldName(field); err != nil {
			return templateV1Record{}, err
		}
		if len(fields) > 0 && fields[len(fields)-1] >= field {
			return templateV1Record{}, errors.New("collections: template-v1 fields are not strictly sorted")
		}
		fields = append(fields, field)
	}
	if pos != len(raw) {
		return templateV1Record{}, errors.New("collections: trailing template-v1 template bytes")
	}
	id := sha256.Sum256(raw)
	return templateV1Record{
		id:  id,
		raw: bytes.Clone(raw),
		tpl: &templateV1Template{id: id, fields: fields},
	}, nil
}

func parseTemplateV1StoredDocument(raw []byte) (templateV1ObjectRef, error) {
	pos := 0
	if !consumeMagic(raw, &pos, templateV1StoredMagic) {
		return templateV1ObjectRef{}, errors.New("collections: malformed template-v1 stored document")
	}
	var id [32]byte
	if len(raw)-pos < len(id) {
		return templateV1ObjectRef{}, errors.New("collections: malformed template-v1 root template id")
	}
	copy(id[:], raw[pos:pos+len(id)])
	pos += len(id)
	return templateV1ObjectRef{templateID: id, values: raw[pos:]}, nil
}

func templateV1OrderedIndexStateForDocumentWithArena(document []byte, runtimes []indexRuntime, opts collectionOptions, encoder *indexEncodeArena) (orderedDocumentIndexState, error) {
	if opts.templateResolver == nil {
		return nil, errTemplateV1MissingResolver
	}
	root, err := parseTemplateV1StoredDocument(document)
	if err != nil {
		return nil, err
	}
	if state, ok, err := templateV1RootIndexStateForDocumentWithArena(root, runtimes, opts, encoder); ok || err != nil {
		return state, err
	}
	state := encoder.appendState(len(runtimes))
	for runtimeIdx, runtime := range runtimes {
		value, found, err := templateV1ExtractPathValue(root, runtime.path, opts.templateResolver)
		if err != nil {
			return nil, err
		}
		if !found || len(value) == 0 {
			continue
		}
		if err := appendTemplateV1IndexValueToState(state, runtimeIdx, runtime, value, opts, encoder); err != nil {
			return nil, err
		}
	}
	return state, nil
}

func templateV1RootIndexStateForDocumentWithArena(root templateV1ObjectRef, runtimes []indexRuntime, opts collectionOptions, encoder *indexEncodeArena) (orderedDocumentIndexState, bool, error) {
	for _, runtime := range runtimes {
		if len(runtime.path) != 1 {
			return nil, false, nil
		}
	}
	tpl, err := opts.templateResolver.lookupTemplateV1(root.templateID)
	if err != nil {
		return nil, true, err
	}
	state := encoder.appendState(len(runtimes))
	pos := 0
	for _, field := range tpl.fields {
		runtimeIdx := templateV1RootFieldFirstRuntime(field, runtimes)
		if runtimeIdx < 0 {
			if err := skipTemplateV1Value(root.values, &pos, opts.templateResolver); err != nil {
				return nil, true, err
			}
			continue
		}
		if err := appendTemplateV1RootFieldIndexValues(state, field, runtimeIdx, root.values, &pos, runtimes, opts, encoder); err != nil {
			return nil, true, err
		}
	}
	if pos != len(root.values) {
		return nil, true, errors.New("collections: trailing template-v1 object values")
	}
	return state, true, nil
}

func templateV1RootFieldFirstRuntime(field string, runtimes []indexRuntime) int {
	for runtimeIdx, runtime := range runtimes {
		if runtime.path[0] == field {
			return runtimeIdx
		}
	}
	return -1
}

func appendTemplateV1RootFieldIndexValues(state orderedDocumentIndexState, field string, firstRuntimeIdx int, raw []byte, pos *int, runtimes []indexRuntime, opts collectionOptions, encoder *indexEncodeArena) error {
	if pos == nil || *pos >= len(raw) {
		return errors.New("collections: malformed template-v1 value")
	}
	valueStart := *pos
	kind := raw[*pos]
	*pos = *pos + 1
	switch kind {
	case templateV1KindNull:
		return nil
	case templateV1KindFalse:
		return appendTemplateV1ScalarIndexValueToRootStates(state, field, firstRuntimeIdx, runtimes, encoder, "b:0")
	case templateV1KindTrue:
		return appendTemplateV1ScalarIndexValueToRootStates(state, field, firstRuntimeIdx, runtimes, encoder, "b:1")
	case templateV1KindFloat64:
		if len(raw)-*pos < 8 {
			return errors.New("collections: malformed template-v1 number")
		}
		v := math.Float64frombits(binary.BigEndian.Uint64(raw[*pos:]))
		*pos += 8
		start := len(encoder.buf)
		encoder.buf = append(encoder.buf, "n:"...)
		encoder.buf = strconv.AppendFloat(encoder.buf, v, 'g', -1, 64)
		next := encoder.buf[start:len(encoder.buf):len(encoder.buf)]
		appendTemplateV1EncodedIndexValueToRootStates(state, field, firstRuntimeIdx, runtimes, encoder, next)
		return nil
	case templateV1KindString:
		n, err := readTemplateV1Uvarint(raw, pos)
		if err != nil {
			return err
		}
		if n > uint64(len(raw)-*pos) {
			return errors.New("collections: malformed template-v1 string")
		}
		valueEnd := *pos + int(n)
		start := len(encoder.buf)
		encoder.buf = append(encoder.buf, "s:"...)
		encoder.buf = append(encoder.buf, raw[*pos:valueEnd]...)
		*pos = valueEnd
		next := encoder.buf[start:len(encoder.buf):len(encoder.buf)]
		appendTemplateV1EncodedIndexValueToRootStates(state, field, firstRuntimeIdx, runtimes, encoder, next)
		return nil
	default:
		*pos = valueStart
		if err := skipTemplateV1Value(raw, pos, opts.templateResolver); err != nil {
			return err
		}
		value := raw[valueStart:*pos:*pos]
		for runtimeIdx := firstRuntimeIdx; runtimeIdx < len(runtimes); runtimeIdx++ {
			runtime := runtimes[runtimeIdx]
			if runtimeIdx != firstRuntimeIdx && runtime.path[0] != field {
				continue
			}
			if err := appendTemplateV1IndexValueToState(state, runtimeIdx, runtime, value, opts, encoder); err != nil {
				return err
			}
		}
		return nil
	}
}

func appendTemplateV1ScalarIndexValueToRootStates(state orderedDocumentIndexState, field string, firstRuntimeIdx int, runtimes []indexRuntime, encoder *indexEncodeArena, encoded string) error {
	start := len(encoder.buf)
	encoder.buf = append(encoder.buf, encoded...)
	next := encoder.buf[start:len(encoder.buf):len(encoder.buf)]
	appendTemplateV1EncodedIndexValueToRootStates(state, field, firstRuntimeIdx, runtimes, encoder, next)
	return nil
}

func appendTemplateV1EncodedIndexValueToRootStates(state orderedDocumentIndexState, field string, firstRuntimeIdx int, runtimes []indexRuntime, encoder *indexEncodeArena, encoded []byte) {
	state[firstRuntimeIdx] = encoder.appendSingleValueRef(encoded)
	for runtimeIdx := firstRuntimeIdx + 1; runtimeIdx < len(runtimes); runtimeIdx++ {
		if runtimes[runtimeIdx].path[0] == field {
			state[runtimeIdx] = encoder.appendSingleValueRef(encoded)
		}
	}
}

func appendTemplateV1IndexValueToState(state orderedDocumentIndexState, runtimeIdx int, runtime indexRuntime, value []byte, opts collectionOptions, encoder *indexEncodeArena) error {
	if len(value) == 0 || value[0] == templateV1KindNull {
		return nil
	}
	if value[0] == templateV1KindArray {
		if !runtime.def.multiKey && !opts.allowArrayValuesInIndex {
			return errors.New("collections: array value not allowed for index")
		}
		encoded, err := templateV1AppendArrayIndexValues(value, encoder)
		if err != nil {
			return err
		}
		if len(encoded) == 1 {
			state[runtimeIdx] = encoder.appendSingleValueRef(encoded[0])
		} else if len(encoded) > 1 {
			state[runtimeIdx] = normalizeOwnedEncodedIndexValues(encoded)
		}
		return nil
	}
	var next []byte
	var err error
	encoder.buf, next, err = appendTemplateV1IndexScalar(encoder.buf, value)
	if err != nil {
		return err
	}
	state[runtimeIdx] = encoder.appendSingleValueRef(next)
	return nil
}

func templateV1ExtractPathValue(root templateV1ObjectRef, path []string, resolver templateV1Resolver) ([]byte, bool, error) {
	if len(path) == 0 {
		return nil, false, nil
	}
	current := root
	for i, segment := range path {
		value, found, err := templateV1ObjectFieldValue(current, segment, resolver)
		if err != nil || !found {
			return nil, found, err
		}
		if i == len(path)-1 {
			return value, true, nil
		}
		next, err := templateV1ObjectValue(value)
		if err != nil {
			return nil, false, nil
		}
		current = next
	}
	return nil, false, nil
}

func templateV1ObjectFieldValue(obj templateV1ObjectRef, field string, resolver templateV1Resolver) ([]byte, bool, error) {
	tpl, err := resolver.lookupTemplateV1(obj.templateID)
	if err != nil {
		return nil, false, err
	}
	ord := sort.SearchStrings(tpl.fields, field)
	if ord >= len(tpl.fields) || tpl.fields[ord] != field {
		return nil, false, nil
	}
	pos := 0
	for i := range tpl.fields {
		start := pos
		if err := skipTemplateV1Value(obj.values, &pos, resolver); err != nil {
			return nil, false, err
		}
		if i == ord {
			return obj.values[start:pos:pos], true, nil
		}
	}
	if pos != len(obj.values) {
		return nil, false, errors.New("collections: trailing template-v1 object values")
	}
	return nil, false, nil
}

func templateV1ObjectValue(raw []byte) (templateV1ObjectRef, error) {
	if len(raw) == 0 || raw[0] != templateV1KindObject {
		return templateV1ObjectRef{}, errors.New("collections: template-v1 value is not an object")
	}
	pos := 1
	var id [32]byte
	if len(raw)-pos < len(id) {
		return templateV1ObjectRef{}, errors.New("collections: malformed template-v1 object value")
	}
	copy(id[:], raw[pos:pos+len(id)])
	pos += len(id)
	return templateV1ObjectRef{templateID: id, values: raw[pos:]}, nil
}

func templateV1AppendArrayIndexValues(raw []byte, encoder *indexEncodeArena) ([][]byte, error) {
	pos := 1
	count, err := readTemplateV1Uvarint(raw, &pos)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	values := make([][]byte, 0, int(count))
	for i := uint64(0); i < count; i++ {
		start := pos
		if err := skipTemplateV1Value(raw, &pos, nil); err != nil {
			return nil, err
		}
		var next []byte
		encoder.buf, next, err = appendTemplateV1IndexScalar(encoder.buf, raw[start:pos])
		if err != nil {
			return nil, err
		}
		values = append(values, next)
	}
	if pos != len(raw) {
		return nil, errors.New("collections: trailing template-v1 array bytes")
	}
	return values, nil
}

func appendTemplateV1IndexScalar(dst []byte, raw []byte) ([]byte, []byte, error) {
	if len(raw) == 0 {
		return dst, nil, errors.New("collections: empty template-v1 value")
	}
	start := len(dst)
	switch raw[0] {
	case templateV1KindNull:
		dst = append(dst, "z:"...)
	case templateV1KindFalse:
		dst = append(dst, "b:0"...)
	case templateV1KindTrue:
		dst = append(dst, "b:1"...)
	case templateV1KindFloat64:
		if len(raw) != 1+8 {
			return dst, nil, errors.New("collections: malformed template-v1 number")
		}
		v := math.Float64frombits(binary.BigEndian.Uint64(raw[1:]))
		dst = append(dst, "n:"...)
		dst = strconv.AppendFloat(dst, v, 'g', -1, 64)
	case templateV1KindString:
		pos := 1
		n, err := readTemplateV1Uvarint(raw, &pos)
		if err != nil {
			return dst, nil, err
		}
		if n > uint64(len(raw)-pos) || int(n) != len(raw)-pos {
			return dst, nil, errors.New("collections: malformed template-v1 string")
		}
		dst = append(dst, "s:"...)
		dst = append(dst, raw[pos:]...)
	default:
		return dst, nil, fmt.Errorf("collections: unsupported indexed template-v1 value kind %d", raw[0])
	}
	return dst, dst[start:len(dst):len(dst)], nil
}

func skipTemplateV1Value(raw []byte, pos *int, resolver templateV1Resolver) error {
	if pos == nil || *pos >= len(raw) {
		return errors.New("collections: malformed template-v1 value")
	}
	kind := raw[*pos]
	*pos = *pos + 1
	switch kind {
	case templateV1KindNull, templateV1KindFalse, templateV1KindTrue:
		return nil
	case templateV1KindFloat64:
		if len(raw)-*pos < 8 {
			return errors.New("collections: malformed template-v1 number")
		}
		*pos += 8
		return nil
	case templateV1KindString:
		n, err := readTemplateV1Uvarint(raw, pos)
		if err != nil {
			return err
		}
		if n > uint64(len(raw)-*pos) {
			return errors.New("collections: malformed template-v1 string")
		}
		*pos += int(n)
		return nil
	case templateV1KindArray:
		count, err := readTemplateV1Uvarint(raw, pos)
		if err != nil {
			return err
		}
		for i := uint64(0); i < count; i++ {
			if err := skipTemplateV1Value(raw, pos, resolver); err != nil {
				return err
			}
		}
		return nil
	case templateV1KindObject:
		var id [32]byte
		if len(raw)-*pos < len(id) {
			return errors.New("collections: malformed template-v1 object")
		}
		copy(id[:], raw[*pos:*pos+len(id)])
		*pos += len(id)
		if resolver == nil {
			return errors.New("collections: cannot skip template-v1 object without resolver")
		}
		tpl, err := resolver.lookupTemplateV1(id)
		if err != nil {
			return err
		}
		for range tpl.fields {
			if err := skipTemplateV1Value(raw, pos, resolver); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("collections: unknown template-v1 value kind %d", kind)
	}
}

func consumeMagic(raw []byte, pos *int, magic string) bool {
	if pos == nil || len(raw)-*pos < len(magic) {
		return false
	}
	if string(raw[*pos:*pos+len(magic)]) != magic {
		return false
	}
	*pos += len(magic)
	return true
}

func readTemplateV1Uvarint(raw []byte, pos *int) (uint64, error) {
	if pos == nil || *pos > len(raw) {
		return 0, errors.New("collections: malformed template-v1 varint")
	}
	v, n := binary.Uvarint(raw[*pos:])
	if n <= 0 {
		return 0, errors.New("collections: malformed template-v1 varint")
	}
	*pos += n
	return v, nil
}

func validateTemplateV1FieldName(field string) error {
	if field == "" {
		return errors.New("collections: template-v1 field name cannot be empty")
	}
	if strings.ContainsAny(field, "\x00.") {
		return fmt.Errorf("collections: template-v1 field %q contains reserved punctuation", field)
	}
	return nil
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
