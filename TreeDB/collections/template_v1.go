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
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	templateV1InputMagic          = "TD1I"
	templateV1InsertDocumentMagic = "TD1H"
	templateV1StoredMagic         = "TD1D"
	templateV1RecordMagic         = "TD1T"

	templateV1KindNull byte = iota
	templateV1KindFalse
	templateV1KindTrue
	templateV1KindFloat64
	templateV1KindString
	templateV1KindObject
	templateV1KindArray

	templateV1MaxArrayElements uint64 = 1 << 20
	templateV1ArrayPreallocCap        = 4096
)

var (
	errTemplateV1MissingResolver     = errors.New("collections: missing template-v1 resolver")
	errTemplateV1MissingTemplateRoot = errors.New("collections: missing template-v1 template root")
	errTemplateV1TemplateNotFound    = errors.New("collections: template-v1 template not found")
)

type templateV1Record struct {
	hash [32]byte
	id   uint64
	raw  []byte
	tpl  *templateV1Template
}

type templateV1Template struct {
	hash   [32]byte
	id     uint64
	fields []string
}

type templateV1Resolver interface {
	lookupTemplateV1(id uint64) (*templateV1Template, error)
	lookupTemplateV1ByHash(hash [32]byte) (*templateV1Template, error)
	nextTemplateV1ID() (uint64, error)
}

type templateV1MemoryResolver struct {
	templatesByID   map[uint64]*templateV1Template
	templatesByHash map[string]*templateV1Template
}

type templateV1CompositeResolver struct {
	memory   *templateV1MemoryResolver
	fallback templateV1Resolver
}

type templateV1SnapshotResolver struct {
	snap   *backenddb.Snapshot
	rootID uint64
	byID   map[uint64]*templateV1Template
	byHash map[string]*templateV1Template
}

type templateV1BufferedRunsResolver struct {
	runs     []memtable.Table
	fallback templateV1Resolver
	byID     map[uint64]*templateV1Template
	byHash   map[string]*templateV1Template
}

type templateV1ObjectRef struct {
	templateID uint64
	values     []byte
}

type templateV1HashObjectRef struct {
	templateHash [32]byte
	values       []byte
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
	case DocumentFormatBSON:
		return DocumentFormatBSON, nil
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
	case DocumentFormatBSON:
		if opts.trustedBSONDocuments {
			return documents, nil, nil, nil
		}
		return prepareBSONInsertDocuments(documents)
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
		byID:   make(map[uint64]*templateV1Template),
		byHash: make(map[string]*templateV1Template),
	}
	return opts
}

func collectionOptionsWithBufferedTemplateV1Resolver(opts collectionOptions, domain *collectionWriteDomain, collectionName string) collectionOptions {
	if normalizedDocumentFormat(opts.documentFormat) != DocumentFormatTemplateV1 || domain == nil || collectionName == "" {
		return opts
	}
	rootName := collectionTemplateRootName(collectionName)
	domain.mu.RLock()
	runs := append([]memtable.Table(nil), pendingIndexedRootRunsLocked(domain, rootName)...)
	domain.mu.RUnlock()
	if len(runs) == 0 {
		return opts
	}
	return collectionOptionsWithBufferedTemplateV1RunsResolver(opts, runs)
}

func collectionOptionsWithBufferedTemplateV1RunsResolver(opts collectionOptions, runs []memtable.Table) collectionOptions {
	if normalizedDocumentFormat(opts.documentFormat) != DocumentFormatTemplateV1 || len(runs) == 0 {
		return opts
	}
	opts.templateResolver = &templateV1BufferedRunsResolver{
		runs:     runs,
		fallback: opts.templateResolver,
		byID:     make(map[uint64]*templateV1Template),
		byHash:   make(map[string]*templateV1Template),
	}
	return opts
}

type templateV1ParsedInsertDocument struct {
	stored       []byte
	hashDocument []byte
	records      []templateV1Record
}

func prepareTemplateV1InsertDocuments(documents [][]byte, fallback templateV1Resolver) ([][]byte, []templateV1Record, templateV1Resolver, error) {
	parsed := make([]templateV1ParsedInsertDocument, len(documents))
	byHash := make(map[string]templateV1Record)
	for i, document := range documents {
		next, err := parseTemplateV1InsertDocument(document)
		if err != nil {
			return nil, nil, nil, err
		}
		parsed[i] = next
		for _, record := range next.records {
			if err := addPendingTemplateV1Record(byHash, record); err != nil {
				return nil, nil, nil, err
			}
		}
	}

	memory, publishRecords, err := assignTemplateV1RecordIDs(byHash, fallback)
	if err != nil {
		return nil, nil, nil, err
	}
	resolver := templateV1PreparedResolver(memory, fallback)
	prepared := make([][]byte, len(documents))
	for i, document := range parsed {
		if document.stored != nil {
			prepared[i] = document.stored
			continue
		}
		stored, err := convertTemplateV1InsertDocumentToStored(document.hashDocument, resolver)
		if err != nil {
			return nil, nil, nil, err
		}
		// Keep compact stored bytes borrowed or freshly prepared like JSON
		// documents; publish consumes the prepared batch synchronously.
		prepared[i] = stored
	}
	if err := validateTemplateV1PreparedDocuments(prepared, resolver); err != nil {
		return nil, nil, nil, err
	}
	return prepared, publishRecords, resolver, nil
}

func parseTemplateV1InsertDocument(raw []byte) (templateV1ParsedInsertDocument, error) {
	switch {
	case bytes.HasPrefix(raw, []byte(templateV1StoredMagic)):
		return templateV1ParsedInsertDocument{stored: raw}, nil
	case bytes.HasPrefix(raw, []byte(templateV1InsertDocumentMagic)):
		return templateV1ParsedInsertDocument{hashDocument: raw}, nil
	default:
		return parseTemplateV1InsertEnvelope(raw)
	}
}

func parseTemplateV1InsertEnvelope(raw []byte) (templateV1ParsedInsertDocument, error) {
	pos := 0
	if !consumeMagic(raw, &pos, templateV1InputMagic) {
		return templateV1ParsedInsertDocument{}, errors.New("collections: template-v1 insert requires template input envelope")
	}
	templateCount, err := readTemplateV1Uvarint(raw, &pos)
	if err != nil {
		return templateV1ParsedInsertDocument{}, err
	}
	if templateCount > uint64(len(raw)) {
		return templateV1ParsedInsertDocument{}, errors.New("collections: malformed template-v1 template count")
	}
	records := make([]templateV1Record, 0, int(templateCount))
	for i := uint64(0); i < templateCount; i++ {
		var hash [32]byte
		if len(raw)-pos < len(hash) {
			return templateV1ParsedInsertDocument{}, errors.New("collections: malformed template-v1 template hash")
		}
		copy(hash[:], raw[pos:pos+len(hash)])
		pos += len(hash)
		recordLen, err := readTemplateV1Uvarint(raw, &pos)
		if err != nil {
			return templateV1ParsedInsertDocument{}, err
		}
		if recordLen > uint64(len(raw)-pos) {
			return templateV1ParsedInsertDocument{}, errors.New("collections: malformed template-v1 template record length")
		}
		recordRaw := raw[pos : pos+int(recordLen)]
		pos += int(recordLen)
		record, err := parseTemplateV1Record(recordRaw)
		if err != nil {
			return templateV1ParsedInsertDocument{}, err
		}
		if record.hash != hash {
			return templateV1ParsedInsertDocument{}, errors.New("collections: template-v1 template hash does not match record")
		}
		records = append(records, record)
	}
	hashDocument := raw[pos:]
	if !bytes.HasPrefix(hashDocument, []byte(templateV1InsertDocumentMagic)) {
		return templateV1ParsedInsertDocument{}, errors.New("collections: malformed template-v1 insert document")
	}
	return templateV1ParsedInsertDocument{hashDocument: hashDocument, records: records}, nil
}

func addPendingTemplateV1Record(records map[string]templateV1Record, record templateV1Record) error {
	key := string(record.hash[:])
	existing, ok := records[key]
	if !ok {
		records[key] = record
		return nil
	}
	if !bytes.Equal(record.raw, existing.raw) {
		return errors.New("collections: template-v1 template hash collision")
	}
	return nil
}

func assignTemplateV1RecordIDs(recordsByHash map[string]templateV1Record, fallback templateV1Resolver) (*templateV1MemoryResolver, []templateV1Record, error) {
	memory := &templateV1MemoryResolver{}
	if len(recordsByHash) == 0 {
		return memory, nil, nil
	}
	records := make([]templateV1Record, 0, len(recordsByHash))
	for _, record := range recordsByHash {
		records = append(records, record)
	}
	sortTemplateV1Records(records)

	var nextID uint64
	nextIDReady := false
	publish := make([]templateV1Record, 0, len(records))
	for _, record := range records {
		if fallback != nil {
			existing, err := fallback.lookupTemplateV1ByHash(record.hash)
			if err == nil {
				if !equalStringSlices(existing.fields, record.tpl.fields) {
					return nil, nil, errors.New("collections: template-v1 template hash collision")
				}
				if err := memory.addTemplate(existing); err != nil {
					return nil, nil, err
				}
				continue
			}
			if !isMissingTemplateV1Lookup(err) {
				return nil, nil, err
			}
		}
		if !nextIDReady {
			var err error
			nextID, err = nextAssignableTemplateV1ID(fallback)
			if err != nil {
				return nil, nil, err
			}
			nextIDReady = true
		}
		record.id = nextID
		nextID++
		record.tpl = &templateV1Template{
			hash:   record.hash,
			id:     record.id,
			fields: record.tpl.fields,
		}
		if err := memory.addRecord(record); err != nil {
			return nil, nil, err
		}
		publish = append(publish, record)
	}
	return memory, publish, nil
}

func nextAssignableTemplateV1ID(fallback templateV1Resolver) (uint64, error) {
	if fallback == nil {
		return 1, nil
	}
	nextID, err := fallback.nextTemplateV1ID()
	if err == nil {
		if nextID == 0 {
			return 1, nil
		}
		return nextID, nil
	}
	if isMissingTemplateV1Lookup(err) {
		return 1, nil
	}
	return 0, err
}

func templateV1PreparedResolver(memory *templateV1MemoryResolver, fallback templateV1Resolver) templateV1Resolver {
	if memory == nil || memory.empty() {
		return fallback
	}
	if fallback == nil {
		return memory
	}
	return &templateV1CompositeResolver{memory: memory, fallback: fallback}
}

func isMissingTemplateV1Lookup(err error) bool {
	return errors.Is(err, errTemplateV1TemplateNotFound) || errors.Is(err, errTemplateV1MissingTemplateRoot)
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

func templateV1NextIDKey() []byte {
	return []byte{0x00, 'n'}
}

func templateV1HashKey(hash [32]byte) []byte {
	out := make([]byte, 1+len(hash))
	out[0] = 0x01
	copy(out[1:], hash[:])
	return out
}

func templateV1RecordKey(id uint64) []byte {
	out := []byte{0x02}
	return binary.AppendUvarint(out, id)
}

func encodeTemplateV1ID(id uint64) []byte {
	return binary.AppendUvarint(nil, id)
}

func decodeTemplateV1ID(raw []byte) (uint64, error) {
	id, n := binary.Uvarint(raw)
	if n <= 0 {
		return 0, errors.New("collections: malformed template-v1 template id")
	}
	if n != len(raw) {
		return 0, errors.New("collections: trailing template-v1 template id bytes")
	}
	if id == 0 {
		return 0, errors.New("collections: template-v1 template id must be non-zero")
	}
	return id, nil
}

func readTemplateV1TemplateID(raw []byte, pos *int) (uint64, error) {
	id, err := readTemplateV1Uvarint(raw, pos)
	if err != nil {
		return 0, err
	}
	if id == 0 {
		return 0, errors.New("collections: template-v1 template id must be non-zero")
	}
	return id, nil
}

func (r *templateV1MemoryResolver) empty() bool {
	return r == nil || (len(r.templatesByID) == 0 && len(r.templatesByHash) == 0)
}

func (r *templateV1MemoryResolver) addRecord(record templateV1Record) error {
	if record.tpl == nil {
		return errors.New("collections: template-v1 record missing template")
	}
	if record.id == 0 {
		return errors.New("collections: template-v1 record missing numeric template id")
	}
	return r.addTemplate(record.tpl)
}

func (r *templateV1MemoryResolver) addTemplate(tpl *templateV1Template) error {
	if r == nil {
		return errTemplateV1MissingResolver
	}
	if tpl == nil {
		return errors.New("collections: template-v1 template is nil")
	}
	if tpl.id == 0 {
		return errors.New("collections: template-v1 template id must be non-zero")
	}
	if r.templatesByID == nil {
		r.templatesByID = make(map[uint64]*templateV1Template)
	}
	if r.templatesByHash == nil {
		r.templatesByHash = make(map[string]*templateV1Template)
	}
	hashKey := string(tpl.hash[:])
	if existing := r.templatesByID[tpl.id]; existing != nil {
		if existing.hash != tpl.hash || !equalStringSlices(existing.fields, tpl.fields) {
			return errors.New("collections: template-v1 template id collision")
		}
	}
	if existing := r.templatesByHash[hashKey]; existing != nil {
		if existing.id != tpl.id || !equalStringSlices(existing.fields, tpl.fields) {
			return errors.New("collections: template-v1 template hash collision")
		}
	}
	r.templatesByID[tpl.id] = tpl
	r.templatesByHash[hashKey] = tpl
	return nil
}

func (r *templateV1MemoryResolver) lookupTemplateV1(id uint64) (*templateV1Template, error) {
	if r == nil {
		return nil, errTemplateV1MissingResolver
	}
	tpl := r.templatesByID[id]
	if tpl == nil {
		return nil, errTemplateV1TemplateNotFound
	}
	return tpl, nil
}

func (r *templateV1MemoryResolver) lookupTemplateV1ByHash(hash [32]byte) (*templateV1Template, error) {
	if r == nil {
		return nil, errTemplateV1MissingResolver
	}
	tpl := r.templatesByHash[string(hash[:])]
	if tpl == nil {
		return nil, errTemplateV1TemplateNotFound
	}
	return tpl, nil
}

func (r *templateV1MemoryResolver) nextTemplateV1ID() (uint64, error) {
	if r == nil {
		return 0, errTemplateV1MissingResolver
	}
	var maxID uint64
	for id := range r.templatesByID {
		if id > maxID {
			maxID = id
		}
	}
	return maxID + 1, nil
}

func (r *templateV1CompositeResolver) lookupTemplateV1(id uint64) (*templateV1Template, error) {
	if r == nil {
		return nil, errTemplateV1MissingResolver
	}
	if r.memory != nil {
		if tpl, err := r.memory.lookupTemplateV1(id); err == nil {
			return tpl, nil
		} else if !errors.Is(err, errTemplateV1TemplateNotFound) {
			return nil, err
		}
	}
	if r.fallback != nil {
		return r.fallback.lookupTemplateV1(id)
	}
	return nil, errTemplateV1TemplateNotFound
}

func (r *templateV1CompositeResolver) lookupTemplateV1ByHash(hash [32]byte) (*templateV1Template, error) {
	if r == nil {
		return nil, errTemplateV1MissingResolver
	}
	if r.memory != nil {
		if tpl, err := r.memory.lookupTemplateV1ByHash(hash); err == nil {
			return tpl, nil
		} else if !errors.Is(err, errTemplateV1TemplateNotFound) {
			return nil, err
		}
	}
	if r.fallback != nil {
		return r.fallback.lookupTemplateV1ByHash(hash)
	}
	return nil, errTemplateV1TemplateNotFound
}

func (r *templateV1CompositeResolver) nextTemplateV1ID() (uint64, error) {
	if r == nil {
		return 0, errTemplateV1MissingResolver
	}
	nextID := uint64(1)
	if r.fallback != nil {
		fallbackNext, err := r.fallback.nextTemplateV1ID()
		if err != nil && !isMissingTemplateV1Lookup(err) {
			return 0, err
		}
		if err == nil && fallbackNext > nextID {
			nextID = fallbackNext
		}
	}
	if r.memory != nil && !r.memory.empty() {
		memoryNext, err := r.memory.nextTemplateV1ID()
		if err != nil {
			return 0, err
		}
		if memoryNext > nextID {
			nextID = memoryNext
		}
	}
	return nextID, nil
}

func (r *templateV1SnapshotResolver) lookupTemplateV1(id uint64) (*templateV1Template, error) {
	if r == nil || r.snap == nil || r.rootID == 0 {
		return nil, errTemplateV1MissingTemplateRoot
	}
	if tpl := r.byID[id]; tpl != nil {
		return tpl, nil
	}
	entry, err := r.snap.GetEntryAtRoot(r.rootID, templateV1RecordKey(id))
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, errTemplateV1TemplateNotFound
	}
	if err != nil {
		return nil, err
	}
	record, err := parseTemplateV1RecordWithID(id, entry.Value)
	if err != nil {
		return nil, err
	}
	r.cacheTemplate(record.tpl)
	return record.tpl, nil
}

func (r *templateV1SnapshotResolver) lookupTemplateV1ByHash(hash [32]byte) (*templateV1Template, error) {
	if r == nil || r.snap == nil || r.rootID == 0 {
		return nil, errTemplateV1MissingTemplateRoot
	}
	if tpl := r.byHash[string(hash[:])]; tpl != nil {
		return tpl, nil
	}
	entry, err := r.snap.GetEntryAtRoot(r.rootID, templateV1HashKey(hash))
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, errTemplateV1TemplateNotFound
	}
	if err != nil {
		return nil, err
	}
	id, err := decodeTemplateV1ID(entry.Value)
	if err != nil {
		return nil, err
	}
	tpl, err := r.lookupTemplateV1(id)
	if err != nil {
		return nil, err
	}
	if tpl.hash != hash {
		return nil, errors.New("collections: template-v1 template hash mismatch")
	}
	return tpl, nil
}

func (r *templateV1SnapshotResolver) nextTemplateV1ID() (uint64, error) {
	if r == nil || r.snap == nil || r.rootID == 0 {
		return 0, errTemplateV1MissingTemplateRoot
	}
	entry, err := r.snap.GetEntryAtRoot(r.rootID, templateV1NextIDKey())
	if errors.Is(err, tree.ErrKeyNotFound) {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	return decodeTemplateV1ID(entry.Value)
}

func (r *templateV1SnapshotResolver) cacheTemplate(tpl *templateV1Template) {
	if r.byID == nil {
		r.byID = make(map[uint64]*templateV1Template)
	}
	if r.byHash == nil {
		r.byHash = make(map[string]*templateV1Template)
	}
	r.byID[tpl.id] = tpl
	r.byHash[string(tpl.hash[:])] = tpl
}

func (r *templateV1BufferedRunsResolver) lookupTemplateV1(id uint64) (*templateV1Template, error) {
	if r == nil {
		return nil, errTemplateV1MissingResolver
	}
	if tpl := r.byID[id]; tpl != nil {
		return tpl, nil
	}
	key := templateV1RecordKey(id)
	for i := len(r.runs) - 1; i >= 0; i-- {
		run := r.runs[i]
		if run == nil {
			continue
		}
		value, _, flags, found := run.GetEntry(key)
		if !found || flags&node.FlagTombstone != 0 {
			continue
		}
		record, err := parseTemplateV1RecordWithID(id, value)
		if err != nil {
			return nil, err
		}
		r.cacheTemplate(record.tpl)
		return record.tpl, nil
	}
	if r.fallback != nil {
		return r.fallback.lookupTemplateV1(id)
	}
	return nil, errTemplateV1TemplateNotFound
}

func (r *templateV1BufferedRunsResolver) lookupTemplateV1ByHash(hash [32]byte) (*templateV1Template, error) {
	if r == nil {
		return nil, errTemplateV1MissingResolver
	}
	if tpl := r.byHash[string(hash[:])]; tpl != nil {
		return tpl, nil
	}
	key := templateV1HashKey(hash)
	for i := len(r.runs) - 1; i >= 0; i-- {
		run := r.runs[i]
		if run == nil {
			continue
		}
		value, _, flags, found := run.GetEntry(key)
		if !found || flags&node.FlagTombstone != 0 {
			continue
		}
		id, err := decodeTemplateV1ID(value)
		if err != nil {
			return nil, err
		}
		tpl, err := r.lookupTemplateV1(id)
		if err != nil {
			return nil, err
		}
		if tpl.hash != hash {
			return nil, errors.New("collections: template-v1 template hash mismatch")
		}
		return tpl, nil
	}
	if r.fallback != nil {
		return r.fallback.lookupTemplateV1ByHash(hash)
	}
	return nil, errTemplateV1TemplateNotFound
}

func (r *templateV1BufferedRunsResolver) nextTemplateV1ID() (uint64, error) {
	if r == nil {
		return 0, errTemplateV1MissingResolver
	}
	key := templateV1NextIDKey()
	for i := len(r.runs) - 1; i >= 0; i-- {
		run := r.runs[i]
		if run == nil {
			continue
		}
		value, _, flags, found := run.GetEntry(key)
		if !found || flags&node.FlagTombstone != 0 {
			continue
		}
		return decodeTemplateV1ID(value)
	}
	if r.fallback != nil {
		return r.fallback.nextTemplateV1ID()
	}
	return 1, nil
}

func (r *templateV1BufferedRunsResolver) cacheTemplate(tpl *templateV1Template) {
	if r.byID == nil {
		r.byID = make(map[uint64]*templateV1Template)
	}
	if r.byHash == nil {
		r.byHash = make(map[string]*templateV1Template)
	}
	r.byID[tpl.id] = tpl
	r.byHash[string(tpl.hash[:])] = tpl
}

func sortTemplateV1Records(records []templateV1Record) {
	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i].hash[:], records[j].hash[:]) < 0
	})
}

func dedupeTemplateV1Records(records []templateV1Record) ([]templateV1Record, error) {
	if len(records) <= 1 {
		return records, nil
	}
	out := records[:1]
	for _, record := range records[1:] {
		last := &out[len(out)-1]
		if record.hash != last.hash {
			out = append(out, record)
			continue
		}
		if record.id != last.id || !bytes.Equal(record.raw, last.raw) {
			return nil, errors.New("collections: template-v1 template hash collision")
		}
	}
	return out, nil
}

func EncodeTemplateV1Document(fields []string, values []any) ([]byte, error) {
	return encodeTemplateV1FieldsWithRecordFilter(fields, values, nil)
}

type TemplateV1Encoder struct {
	emitted map[string]struct{}
}

func (e *TemplateV1Encoder) Reset() {
	e.emitted = nil
}

func (e *TemplateV1Encoder) EncodeDocument(fields []string, values []any) ([]byte, error) {
	return encodeTemplateV1FieldsWithRecordFilter(fields, values, func(record templateV1Record) bool {
		if e.emitted == nil {
			e.emitted = make(map[string]struct{})
		}
		key := string(record.hash[:])
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

func templateV1StoredDocumentJSON(raw []byte, resolver templateV1Resolver) ([]byte, error) {
	root, err := parseTemplateV1StoredDocument(raw)
	if err != nil {
		return nil, err
	}
	pos := 0
	obj, err := decodeTemplateV1ObjectFields(root.templateID, root.values, &pos, resolver)
	if err != nil {
		return nil, err
	}
	if pos != len(root.values) {
		return nil, errors.New("collections: trailing template-v1 object values")
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func decodeTemplateV1ObjectFields(id uint64, raw []byte, pos *int, resolver templateV1Resolver) (map[string]any, error) {
	if resolver == nil {
		return nil, errTemplateV1MissingResolver
	}
	tpl, err := resolver.lookupTemplateV1(id)
	if err != nil {
		return nil, err
	}
	out := make(map[string]any, len(tpl.fields))
	for _, field := range tpl.fields {
		value, err := decodeTemplateV1Value(raw, pos, resolver)
		if err != nil {
			return nil, fmt.Errorf("collections: template-v1 field %q: %w", field, err)
		}
		out[field] = value
	}
	return out, nil
}

func decodeTemplateV1Value(raw []byte, pos *int, resolver templateV1Resolver) (any, error) {
	if pos == nil || *pos >= len(raw) {
		return nil, errors.New("collections: malformed template-v1 value")
	}
	kind := raw[*pos]
	*pos = *pos + 1
	switch kind {
	case templateV1KindNull:
		return nil, nil
	case templateV1KindFalse:
		return false, nil
	case templateV1KindTrue:
		return true, nil
	case templateV1KindFloat64:
		if len(raw)-*pos < 8 {
			return nil, errors.New("collections: malformed template-v1 number")
		}
		v := math.Float64frombits(binary.BigEndian.Uint64(raw[*pos:]))
		*pos += 8
		return v, nil
	case templateV1KindString:
		n, err := readTemplateV1Uvarint(raw, pos)
		if err != nil {
			return nil, err
		}
		if n > uint64(len(raw)-*pos) {
			return nil, errors.New("collections: malformed template-v1 string")
		}
		valueEnd := *pos + int(n)
		value := string(raw[*pos:valueEnd])
		*pos = valueEnd
		return value, nil
	case templateV1KindArray:
		count, err := readTemplateV1Uvarint(raw, pos)
		if err != nil {
			return nil, err
		}
		if err := validateTemplateV1ArrayCount(raw, pos, count); err != nil {
			return nil, err
		}
		values := make([]any, 0, templateV1ArrayInitialCap(count))
		for i := uint64(0); i < count; i++ {
			value, err := decodeTemplateV1Value(raw, pos, resolver)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	case templateV1KindObject:
		id, err := readTemplateV1TemplateID(raw, pos)
		if err != nil {
			return nil, fmt.Errorf("collections: malformed template-v1 object: %w", err)
		}
		return decodeTemplateV1ObjectFields(id, raw, pos, resolver)
	default:
		return nil, fmt.Errorf("collections: unknown template-v1 value kind %d", kind)
	}
}

func encodeTemplateV1ObjectMap(obj map[string]any) ([]byte, error) {
	return encodeTemplateV1ObjectMapWithRecordFilter(obj, nil)
}

func encodeTemplateV1ObjectMapWithRecordFilter(obj map[string]any, includeRecord func(templateV1Record) bool) ([]byte, error) {
	var state templateV1BuildState
	root, err := state.encodeObject(obj)
	if err != nil {
		return nil, err
	}
	return encodeTemplateV1RootWithRecords(root, &state, includeRecord)
}

func encodeTemplateV1FieldsWithRecordFilter(fields []string, values []any, includeRecord func(templateV1Record) bool) ([]byte, error) {
	if len(fields) != len(values) {
		return nil, errors.New("collections: template-v1 field/value length mismatch")
	}
	var state templateV1BuildState
	root, err := state.encodeFields(fields, values)
	if err != nil {
		return nil, err
	}
	return encodeTemplateV1RootWithRecords(root, &state, includeRecord)
}

func encodeTemplateV1RootWithRecords(root []byte, state *templateV1BuildState, includeRecord func(templateV1Record) bool) ([]byte, error) {
	if state == nil || !state.hasRecord {
		return root, nil
	}
	if len(state.records) == 0 {
		record := state.firstRecord
		if includeRecord != nil && !includeRecord(record) {
			return root, nil
		}
		return encodeTemplateV1RootWithSingleRecord(root, record), nil
	}

	var stackRecords [4]templateV1Record
	records := stackRecords[:0]
	records = append(records, state.firstRecord)
	records = append(records, state.records...)
	if includeRecord != nil {
		selected := records[:0]
		for _, record := range records {
			if includeRecord(record) {
				selected = append(selected, record)
			}
		}
		records = selected
	}
	sortTemplateV1Records(records)
	if len(records) == 0 {
		return root, nil
	}
	return encodeTemplateV1RootWithRecordSlice(root, records), nil
}

func encodeTemplateV1RootWithSingleRecord(root []byte, record templateV1Record) []byte {
	out := make([]byte, 0, len(templateV1InputMagic)+binary.MaxVarintLen64+32+binary.MaxVarintLen64+len(record.raw)+len(root))
	out = append(out, templateV1InputMagic...)
	out = binary.AppendUvarint(out, 1)
	out = append(out, record.hash[:]...)
	out = binary.AppendUvarint(out, uint64(len(record.raw)))
	out = append(out, record.raw...)
	out = append(out, root...)
	return out
}

func encodeTemplateV1RootWithRecordSlice(root []byte, records []templateV1Record) []byte {
	out := make([]byte, 0, len(templateV1InputMagic)+len(root)+len(records)*48)
	out = append(out, templateV1InputMagic...)
	out = binary.AppendUvarint(out, uint64(len(records)))
	for _, record := range records {
		out = append(out, record.hash[:]...)
		out = binary.AppendUvarint(out, uint64(len(record.raw)))
		out = append(out, record.raw...)
	}
	out = append(out, root...)
	return out
}

type templateV1BuildState struct {
	firstRecord templateV1Record
	hasRecord   bool
	records     []templateV1Record
}

func (s *templateV1BuildState) addRecord(record templateV1Record) {
	if !s.hasRecord {
		s.firstRecord = record
		s.hasRecord = true
		return
	}
	if s.firstRecord.hash == record.hash {
		return
	}
	for _, existing := range s.records {
		if existing.hash == record.hash {
			return
		}
	}
	s.records = append(s.records, record)
}

func (s *templateV1BuildState) encodeFields(fields []string, values []any) ([]byte, error) {
	var stackOrder [16]int
	order := stackOrder[:0]
	if len(fields) <= len(stackOrder) {
		order = stackOrder[:len(fields)]
	} else {
		order = make([]int, len(fields))
	}
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool {
		return fields[order[i]] < fields[order[j]]
	})

	var stackFields [16]string
	sortedFields := stackFields[:0]
	if len(fields) <= len(stackFields) {
		sortedFields = stackFields[:len(fields)]
	} else {
		sortedFields = make([]string, len(fields))
	}
	for sortedPos, sourcePos := range order {
		field := fields[sourcePos]
		if err := validateTemplateV1FieldName(field); err != nil {
			return nil, err
		}
		if sortedPos > 0 && sortedFields[sortedPos-1] == field {
			return nil, fmt.Errorf("collections: duplicate template-v1 field %q", field)
		}
		sortedFields[sortedPos] = field
	}

	record, err := buildTemplateV1Record(sortedFields)
	if err != nil {
		return nil, err
	}
	s.addRecord(record)
	out := make([]byte, 0, len(templateV1InsertDocumentMagic)+32+len(fields)*8)
	out = append(out, templateV1InsertDocumentMagic...)
	out = append(out, record.hash[:]...)
	for _, sourcePos := range order {
		field := fields[sourcePos]
		out, err = s.appendValue(out, values[sourcePos])
		if err != nil {
			return nil, fmt.Errorf("collections: template-v1 field %q: %w", field, err)
		}
	}
	return out, nil
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
	s.addRecord(record)
	out := make([]byte, 0, len(templateV1InsertDocumentMagic)+32+len(fields)*8)
	out = append(out, templateV1InsertDocumentMagic...)
	out = append(out, record.hash[:]...)
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
	s.addRecord(record)
	dst = append(dst, templateV1KindObject)
	dst = append(dst, record.hash[:]...)
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
	hash := sha256.Sum256(raw)
	return templateV1Record{
		hash: hash,
		raw:  raw,
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
	hash := sha256.Sum256(raw)
	return templateV1Record{
		hash: hash,
		raw:  bytes.Clone(raw),
		tpl:  &templateV1Template{hash: hash, fields: fields},
	}, nil
}

func parseTemplateV1RecordWithID(id uint64, raw []byte) (templateV1Record, error) {
	record, err := parseTemplateV1Record(raw)
	if err != nil {
		return templateV1Record{}, err
	}
	if id == 0 {
		return templateV1Record{}, errors.New("collections: template-v1 template id must be non-zero")
	}
	record.id = id
	record.tpl = &templateV1Template{
		hash:   record.hash,
		id:     id,
		fields: record.tpl.fields,
	}
	return record, nil
}

func parseTemplateV1InsertHashDocument(raw []byte) (templateV1HashObjectRef, error) {
	pos := 0
	if !consumeMagic(raw, &pos, templateV1InsertDocumentMagic) {
		return templateV1HashObjectRef{}, errors.New("collections: malformed template-v1 insert document")
	}
	var hash [32]byte
	if len(raw)-pos < len(hash) {
		return templateV1HashObjectRef{}, errors.New("collections: malformed template-v1 root template hash")
	}
	copy(hash[:], raw[pos:pos+len(hash)])
	pos += len(hash)
	return templateV1HashObjectRef{templateHash: hash, values: raw[pos:]}, nil
}

func convertTemplateV1InsertDocumentToStored(raw []byte, resolver templateV1Resolver) ([]byte, error) {
	if resolver == nil {
		return nil, errTemplateV1MissingResolver
	}
	root, err := parseTemplateV1InsertHashDocument(raw)
	if err != nil {
		return nil, err
	}
	tpl, err := resolver.lookupTemplateV1ByHash(root.templateHash)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(raw)-32+binary.MaxVarintLen64)
	out = append(out, templateV1StoredMagic...)
	out = binary.AppendUvarint(out, tpl.id)
	pos := 0
	for range tpl.fields {
		out, err = appendTemplateV1ConvertedValue(out, root.values, &pos, resolver)
		if err != nil {
			return nil, err
		}
	}
	if pos != len(root.values) {
		return nil, errors.New("collections: trailing template-v1 object values")
	}
	return out, nil
}

func appendTemplateV1ConvertedValue(dst []byte, raw []byte, pos *int, resolver templateV1Resolver) ([]byte, error) {
	if pos == nil || *pos >= len(raw) {
		return nil, errors.New("collections: malformed template-v1 value")
	}
	start := *pos
	kind := raw[*pos]
	*pos = *pos + 1
	switch kind {
	case templateV1KindNull, templateV1KindFalse, templateV1KindTrue:
		return append(dst, raw[start:*pos]...), nil
	case templateV1KindFloat64:
		if len(raw)-*pos < 8 {
			return nil, errors.New("collections: malformed template-v1 number")
		}
		*pos += 8
		return append(dst, raw[start:*pos]...), nil
	case templateV1KindString:
		n, err := readTemplateV1Uvarint(raw, pos)
		if err != nil {
			return nil, err
		}
		if n > uint64(len(raw)-*pos) {
			return nil, errors.New("collections: malformed template-v1 string")
		}
		*pos += int(n)
		return append(dst, raw[start:*pos]...), nil
	case templateV1KindArray:
		count, err := readTemplateV1Uvarint(raw, pos)
		if err != nil {
			return nil, err
		}
		if err := validateTemplateV1ArrayCount(raw, pos, count); err != nil {
			return nil, err
		}
		dst = append(dst, raw[start:*pos]...)
		for i := uint64(0); i < count; i++ {
			dst, err = appendTemplateV1ConvertedValue(dst, raw, pos, resolver)
			if err != nil {
				return nil, err
			}
		}
		return dst, nil
	case templateV1KindObject:
		var hash [32]byte
		if len(raw)-*pos < len(hash) {
			return nil, errors.New("collections: malformed template-v1 object")
		}
		copy(hash[:], raw[*pos:*pos+len(hash)])
		*pos += len(hash)
		tpl, err := resolver.lookupTemplateV1ByHash(hash)
		if err != nil {
			return nil, err
		}
		dst = append(dst, kind)
		dst = binary.AppendUvarint(dst, tpl.id)
		for range tpl.fields {
			dst, err = appendTemplateV1ConvertedValue(dst, raw, pos, resolver)
			if err != nil {
				return nil, err
			}
		}
		return dst, nil
	default:
		return nil, fmt.Errorf("collections: unknown template-v1 value kind %d", kind)
	}
}

func parseTemplateV1StoredDocument(raw []byte) (templateV1ObjectRef, error) {
	pos := 0
	if !consumeMagic(raw, &pos, templateV1StoredMagic) {
		return templateV1ObjectRef{}, errors.New("collections: malformed template-v1 stored document")
	}
	id, err := readTemplateV1TemplateID(raw, &pos)
	if err != nil {
		return templateV1ObjectRef{}, fmt.Errorf("collections: malformed template-v1 root template id: %w", err)
	}
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
		return appendTemplateV1RawIndexValueToRootStates(state, field, firstRuntimeIdx, runtimes, raw[valueStart:*pos], opts, encoder)
	case templateV1KindTrue:
		return appendTemplateV1RawIndexValueToRootStates(state, field, firstRuntimeIdx, runtimes, raw[valueStart:*pos], opts, encoder)
	case templateV1KindFloat64:
		if len(raw)-*pos < 8 {
			return errors.New("collections: malformed template-v1 number")
		}
		*pos += 8
		return appendTemplateV1RawIndexValueToRootStates(state, field, firstRuntimeIdx, runtimes, raw[valueStart:*pos], opts, encoder)
	case templateV1KindString:
		n, err := readTemplateV1Uvarint(raw, pos)
		if err != nil {
			return err
		}
		if n > uint64(len(raw)-*pos) {
			return errors.New("collections: malformed template-v1 string")
		}
		valueEnd := *pos + int(n)
		*pos = valueEnd
		return appendTemplateV1RawIndexValueToRootStates(state, field, firstRuntimeIdx, runtimes, raw[valueStart:*pos], opts, encoder)
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

func appendTemplateV1RawIndexValueToRootStates(state orderedDocumentIndexState, field string, firstRuntimeIdx int, runtimes []indexRuntime, raw []byte, opts collectionOptions, encoder *indexEncodeArena) error {
	for runtimeIdx := firstRuntimeIdx; runtimeIdx < len(runtimes); runtimeIdx++ {
		if runtimeIdx != firstRuntimeIdx && runtimes[runtimeIdx].path[0] != field {
			continue
		}
		if err := appendTemplateV1IndexValueToState(state, runtimeIdx, runtimes[runtimeIdx], raw, opts, encoder); err != nil {
			return err
		}
	}
	return nil
}

func appendTemplateV1IndexValueToState(state orderedDocumentIndexState, runtimeIdx int, runtime indexRuntime, value []byte, opts collectionOptions, encoder *indexEncodeArena) error {
	if len(value) == 0 || value[0] == templateV1KindNull {
		return nil
	}
	if value[0] == templateV1KindArray {
		if !runtime.def.multiKey && !opts.allowArrayValuesInIndex {
			return errors.New("collections: array value not allowed for index")
		}
		encoded, err := templateV1AppendArrayIndexValues(value, runtime.def.valueType, encoder)
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
	if nextBuf, next, ok, err := appendTemplateV1ExtendedJSONIndexScalar(encoder.buf, runtime.def.valueType, value, opts.templateResolver); ok || err != nil {
		if err != nil {
			return err
		}
		encoder.buf = nextBuf
		state[runtimeIdx] = encoder.appendSingleValueRef(next)
		return nil
	}
	var next []byte
	var err error
	encoder.buf, next, err = appendTemplateV1IndexScalar(encoder.buf, runtime.def.valueType, value)
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
	id, err := readTemplateV1TemplateID(raw, &pos)
	if err != nil {
		return templateV1ObjectRef{}, fmt.Errorf("collections: malformed template-v1 object value: %w", err)
	}
	return templateV1ObjectRef{templateID: id, values: raw[pos:]}, nil
}

func templateV1AppendArrayIndexValues(raw []byte, valueType IndexValueType, encoder *indexEncodeArena) ([][]byte, error) {
	pos := 1
	count, err := readTemplateV1Uvarint(raw, &pos)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	if err := validateTemplateV1ArrayCount(raw, &pos, count); err != nil {
		return nil, err
	}
	values := make([][]byte, 0, templateV1ArrayInitialCap(count))
	for i := uint64(0); i < count; i++ {
		start := pos
		if err := skipTemplateV1Value(raw, &pos, nil); err != nil {
			return nil, err
		}
		if len(raw[start:pos]) == 0 || raw[start] == templateV1KindNull {
			continue
		}
		var next []byte
		encoder.buf, next, err = appendTemplateV1IndexScalar(encoder.buf, valueType, raw[start:pos])
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

func appendTemplateV1ExtendedJSONIndexScalar(dst []byte, valueType IndexValueType, raw []byte, resolver templateV1Resolver) ([]byte, []byte, bool, error) {
	field, value, ok, err := templateV1ExtendedJSONNumberString(raw, resolver)
	if err != nil || !ok {
		return dst, nil, ok, err
	}
	start := len(dst)
	switch valueType {
	case IndexValueInt64:
		switch field {
		case "$numberInt", "$numberLong":
			v, err := parseJSONInt64IndexValue(value)
			if err != nil {
				return dst, nil, true, err
			}
			dst = appendIndexInt64Component(dst, v)
		default:
			return dst, nil, true, fmt.Errorf("collections: indexed extended JSON value for type %q must be $numberInt or $numberLong, got %s", valueType, field)
		}
	case IndexValueDouble:
		switch field {
		case "$numberInt", "$numberLong", "$numberDouble":
			v, err := parseJSONDoubleIndexValue(value)
			if err != nil {
				return dst, nil, true, err
			}
			dst = appendIndexDoubleComponent(dst, v)
		default:
			return dst, nil, true, fmt.Errorf("collections: indexed extended JSON value for type %q must be numeric, got %s", valueType, field)
		}
	default:
		return dst, nil, false, nil
	}
	return dst, dst[start:len(dst):len(dst)], true, nil
}

func templateV1ExtendedJSONNumberString(raw []byte, resolver templateV1Resolver) (string, string, bool, error) {
	if len(raw) == 0 || raw[0] != templateV1KindObject {
		return "", "", false, nil
	}
	obj, err := templateV1ObjectValue(raw)
	if err != nil {
		return "", "", false, err
	}
	tpl, err := resolver.lookupTemplateV1(obj.templateID)
	if err != nil {
		return "", "", false, err
	}
	if len(tpl.fields) != 1 || !isExtendedJSONNumberField(tpl.fields[0]) {
		return "", "", false, nil
	}
	value, found, err := templateV1ObjectFieldValue(obj, tpl.fields[0], resolver)
	if err != nil || !found {
		return "", "", found, err
	}
	text, ok, err := templateV1StringValue(value)
	if err != nil || !ok {
		return "", "", true, err
	}
	return tpl.fields[0], text, true, nil
}

func templateV1StringValue(raw []byte) (string, bool, error) {
	if len(raw) == 0 || raw[0] != templateV1KindString {
		return "", false, errors.New("collections: extended JSON numeric wrapper must contain a string")
	}
	pos := 1
	n, err := readTemplateV1Uvarint(raw, &pos)
	if err != nil {
		return "", false, err
	}
	if n > uint64(len(raw)-pos) || int(n) != len(raw)-pos {
		return "", false, errors.New("collections: malformed template-v1 string")
	}
	return string(raw[pos:]), true, nil
}

func appendTemplateV1IndexScalar(dst []byte, valueType IndexValueType, raw []byte) ([]byte, []byte, error) {
	if len(raw) == 0 {
		return dst, nil, errors.New("collections: empty template-v1 value")
	}
	start := len(dst)
	switch valueType {
	case IndexValueString:
		if raw[0] != templateV1KindString {
			return dst, nil, fmt.Errorf("collections: indexed template-v1 value for type %q must be string, got kind %d", valueType, raw[0])
		}
		pos := 1
		n, err := readTemplateV1Uvarint(raw, &pos)
		if err != nil {
			return dst, nil, err
		}
		if n > uint64(len(raw)-pos) || int(n) != len(raw)-pos {
			return dst, nil, errors.New("collections: malformed template-v1 string")
		}
		dst = appendIndexStringComponent(dst, raw[pos:])
	case IndexValueBool:
		switch raw[0] {
		case templateV1KindFalse:
			dst = appendIndexBoolComponent(dst, false)
		case templateV1KindTrue:
			dst = appendIndexBoolComponent(dst, true)
		default:
			return dst, nil, fmt.Errorf("collections: indexed template-v1 value for type %q must be bool, got kind %d", valueType, raw[0])
		}
	case IndexValueInt64:
		if raw[0] != templateV1KindFloat64 {
			return dst, nil, fmt.Errorf("collections: indexed template-v1 value for type %q must be number, got kind %d", valueType, raw[0])
		}
		if len(raw) != 1+8 {
			return dst, nil, errors.New("collections: malformed template-v1 number")
		}
		i, err := exactFloat64AsInt64(math.Float64frombits(binary.BigEndian.Uint64(raw[1:])))
		if err != nil {
			return dst, nil, err
		}
		dst = appendIndexInt64Component(dst, i)
	case IndexValueDouble:
		if raw[0] != templateV1KindFloat64 {
			return dst, nil, fmt.Errorf("collections: indexed template-v1 value for type %q must be number, got kind %d", valueType, raw[0])
		}
		if len(raw) != 1+8 {
			return dst, nil, errors.New("collections: malformed template-v1 number")
		}
		dst = appendIndexDoubleComponent(dst, math.Float64frombits(binary.BigEndian.Uint64(raw[1:])))
	default:
		return dst, nil, fmt.Errorf("collections: unsupported index value type %q", valueType)
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
		if err := validateTemplateV1ArrayCount(raw, pos, count); err != nil {
			return err
		}
		for i := uint64(0); i < count; i++ {
			if err := skipTemplateV1Value(raw, pos, resolver); err != nil {
				return err
			}
		}
		return nil
	case templateV1KindObject:
		id, err := readTemplateV1TemplateID(raw, pos)
		if err != nil {
			return fmt.Errorf("collections: malformed template-v1 object: %w", err)
		}
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

func validateTemplateV1ArrayCount(raw []byte, pos *int, count uint64) error {
	if pos == nil || *pos > len(raw) {
		return errors.New("collections: malformed template-v1 array length")
	}
	if count > uint64(len(raw)-*pos) {
		return errors.New("collections: malformed template-v1 array length")
	}
	if count > templateV1MaxArrayElements {
		return fmt.Errorf("collections: template-v1 array length exceeds maximum %d", templateV1MaxArrayElements)
	}
	return nil
}

func templateV1ArrayInitialCap(count uint64) int {
	if count > uint64(templateV1ArrayPreallocCap) {
		return templateV1ArrayPreallocCap
	}
	return int(count)
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
