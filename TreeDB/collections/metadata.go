package collections

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	collectionMetaVersion = 1
)

var (
	systemCollectionMetaKeyPrefix   = []byte("sys:c:")
	systemCollectionIndexKeyPrefix  = []byte("sys:i:")
	systemCollectionDataKeyPrefix   = []byte("col:d:")
	systemCollectionIndexDataPrefix = []byte("col:i:")
	systemCollectionIDSeqKeySuffix  = []byte("id_seq")
)

const (
	idModeCallerProvided IDMode = iota
	idModeAuto
)

type IDMode uint8

type CollectionStorageMode uint8

const (
	CollectionStorageModeOuterLeafInValueLog CollectionStorageMode = iota
	CollectionStorageModeInnerOnly
)

const (
	IDModeCallerProvided IDMode = idModeCallerProvided
	IDModeAuto           IDMode = idModeAuto
)

type IndexDefinition struct {
	Name     string
	Field    string
	Unique   bool
	MultiKey bool
}

type CollectionOptions struct {
	IDMode                  IDMode
	StorageMode             CollectionStorageMode
	RejectMissingFields     bool
	AllowArrayValuesInIndex bool
}

type CollectionMeta struct {
	Version uint16
	Name    string
	Options CollectionOptions
	Indexes []IndexDefinition
}

type indexDefField uint8

const (
	indexDefFieldName     indexDefField = 1
	indexDefFieldField    indexDefField = 2
	indexDefFieldUnique   indexDefField = 3
	indexDefFieldMultiKey indexDefField = 4
)

func (m *CollectionMeta) normalizeAndValidate() error {
	if m == nil {
		return errors.New("collections: nil collection metadata")
	}
	if m.Version == 0 {
		m.Version = 1
	}
	if m.Version != collectionMetaVersion {
		return fmt.Errorf("collections: unsupported collection metadata version %d", m.Version)
	}
	if err := ValidateCollectionName(m.Name); err != nil {
		return err
	}
	nameCounted := m.Name
	if strings.TrimSpace(nameCounted) != nameCounted {
		return errors.New("collections: collection name has leading or trailing spaces")
	}
	if !utf8.ValidString(nameCounted) {
		return errors.New("collections: collection name must be valid utf-8")
	}
	if m.Options.IDMode != idModeCallerProvided && m.Options.IDMode != idModeAuto {
		return errors.New("collections: unsupported id mode")
	}
	if m.Options.StorageMode != CollectionStorageModeOuterLeafInValueLog && m.Options.StorageMode != CollectionStorageModeInnerOnly {
		return errors.New("collections: unsupported storage mode")
	}
	for i := range m.Indexes {
		if err := ValidateIndexPath(m.Indexes[i].Field); err != nil {
			return fmt.Errorf("collections: invalid index[%d].field: %w", i, err)
		}
	}
	sort.SliceStable(m.Indexes, func(i, j int) bool {
		if m.Indexes[i].Name == m.Indexes[j].Name {
			return m.Indexes[i].Field < m.Indexes[j].Field
		}
		return m.Indexes[i].Name < m.Indexes[j].Name
	})
	seen := make(map[string]struct{}, len(m.Indexes))
	for _, idxDef := range m.Indexes {
		key := idxDef.Name + "\x00" + idxDef.Field
		if _, exists := seen[key]; exists {
			return fmt.Errorf("collections: duplicate index %q", idxDef.Name)
		}
		seen[key] = struct{}{}
		if strings.TrimSpace(idxDef.Name) != idxDef.Name {
			return errors.New("collections: index name has leading or trailing spaces")
		}
	}
	return nil
}

func DefaultCollectionOptions() CollectionOptions {
	return CollectionOptions{
		IDMode:                  idModeCallerProvided,
		StorageMode:             CollectionStorageModeOuterLeafInValueLog,
		RejectMissingFields:     true,
		AllowArrayValuesInIndex: false,
	}
}

func (m *CollectionMeta) copy() *CollectionMeta {
	cp := &CollectionMeta{
		Version: m.Version,
		Name:    m.Name,
		Options: m.Options,
		Indexes: append([]IndexDefinition(nil), m.Indexes...),
	}
	return cp
}

func (m *CollectionMeta) SetDefaults() {
	if m.Options.IDMode == 0 {
		m.Options.IDMode = DefaultCollectionOptions().IDMode
	}
	if m.Options.StorageMode == 0 {
		m.Options.StorageMode = DefaultCollectionOptions().StorageMode
	}
	m.Options.RejectMissingFields = true
}

func (m *CollectionMeta) Encode() ([]byte, error) {
	if err := m.normalizeAndValidate(); err != nil {
		return nil, err
	}
	memo := m.copy()
	memo.Version = collectionMetaVersion
	memo.SetDefaults()
	name := []byte(memo.Name)
	if len(name) > 65535 {
		return nil, errors.New("collections: collection name too long")
	}
	out := make([]byte, 0, 64+len(name))
	out = append(out, byte(collectionMetaVersion))
	tmp := make([]byte, 2)
	binary.BigEndian.PutUint16(tmp, uint16(len(name)))
	out = append(out, tmp...)
	out = append(out, name...)
	out = append(out, byte(memo.Options.IDMode))
	out = append(out, byte(memo.Options.StorageMode))
	if memo.Options.RejectMissingFields {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	if memo.Options.AllowArrayValuesInIndex {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	out = binary.BigEndian.AppendUint16(out, uint16(len(memo.Indexes)))
	for _, idxDef := range memo.Indexes {
		if err := encodeIndexDefinition(&out, &idxDef); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (m *CollectionMeta) Decode(raw []byte) error {
	if len(raw) < 11 {
		return errors.New("collections: truncated collection metadata")
	}
	ver := int(raw[0])
	if ver != collectionMetaVersion {
		return fmt.Errorf("collections: unsupported collection metadata version %d", ver)
	}
	cursor := 1
	nameLen := int(binary.BigEndian.Uint16(raw[cursor : cursor+2]))
	cursor += 2
	if cursor+nameLen > len(raw) {
		return errors.New("collections: malformed collection name length")
	}
	name := string(raw[cursor : cursor+nameLen])
	cursor += nameLen
	if cursor >= len(raw) {
		return errors.New("collections: malformed collection metadata payload")
	}
	opts := CollectionOptions{
		IDMode: IDMode(raw[cursor]),
	}
	cursor++
	if cursor >= len(raw) {
		return errors.New("collections: malformed collection metadata payload")
	}
	opts.StorageMode = CollectionStorageMode(raw[cursor])
	cursor++
	if cursor >= len(raw) {
		return errors.New("collections: malformed collection metadata payload")
	}
	opts.RejectMissingFields = raw[cursor] == 1
	cursor++
	if cursor >= len(raw) {
		return errors.New("collections: malformed collection metadata payload")
	}
	opts.AllowArrayValuesInIndex = raw[cursor] == 1
	cursor++
	if cursor+2 > len(raw) {
		return errors.New("collections: malformed index count")
	}
	count := int(binary.BigEndian.Uint16(raw[cursor : cursor+2]))
	cursor += 2
	indexes := make([]IndexDefinition, 0, count)
	for i := 0; i < count; i++ {
		if cursor >= len(raw) {
			return fmt.Errorf("collections: malformed index definition at %d", i)
		}
		def, c, err := decodeIndexDefinition(raw[cursor:])
		if err != nil {
			return fmt.Errorf("collections: index definition %d: %w", i, err)
		}
		cursor += c
		indexes = append(indexes, def)
	}
	if cursor != len(raw) {
		return errors.New("collections: trailing bytes in collection metadata")
	}
	candidate := CollectionMeta{
		Version: uint16(ver),
		Name:    name,
		Options: opts,
		Indexes: indexes,
	}
	if err := candidate.normalizeAndValidate(); err != nil {
		return err
	}
	*m = candidate
	return nil
}

func encodeIndexDefinition(out *[]byte, def *IndexDefinition) error {
	if def == nil {
		return errors.New("collections: nil index definition")
	}
	if err := ValidateIndexName(def.Name); err != nil {
		return fmt.Errorf("collections: invalid index name %q: %w", def.Name, err)
	}
	if err := ValidateIndexPath(def.Field); err != nil {
		return fmt.Errorf("collections: invalid index field %q: %w", def.Field, err)
	}
	flags := uint8(0)
	if def.Unique {
		flags |= 1 << 0
	}
	if def.MultiKey {
		flags |= 1 << 1
	}
	record := make([]byte, 0, 32)
	record = append(record, byte(indexDefFieldName))
	record = appendWithLenPrefix(record, def.Name)
	record = append(record, byte(indexDefFieldField))
	record = appendWithLenPrefix(record, def.Field)
	record = append(record, byte(indexDefFieldUnique))
	record = append(record, flags)
	*out = binary.BigEndian.AppendUint16(*out, uint16(len(record)))
	*out = append(*out, record...)
	return nil
}

func decodeIndexDefinition(raw []byte) (IndexDefinition, int, error) {
	if len(raw) < 2 {
		return IndexDefinition{}, 0, errors.New("collections: truncated index definition")
	}
	recordLen := int(binary.BigEndian.Uint16(raw[0:2]))
	if recordLen+2 > len(raw) {
		return IndexDefinition{}, 0, errors.New("collections: invalid index definition length")
	}
	payload := raw[2 : 2+recordLen]
	def := IndexDefinition{}
	for len(payload) > 0 {
		field := indexDefField(payload[0])
		payload = payload[1:]
		switch field {
		case indexDefFieldName, indexDefFieldField:
			_, used := readLenPrefixed(payload)
			if used == 0 {
				return IndexDefinition{}, 0, errors.New("collections: invalid index key length")
			}
			s := string(payload[2:used])
			payload = payload[used:]
			if field == indexDefFieldName {
				def.Name = s
			} else {
				def.Field = s
			}
		case indexDefFieldUnique:
			if len(payload) == 0 {
				return IndexDefinition{}, 0, errors.New("collections: missing index option byte")
			}
			flags := payload[0]
			payload = payload[1:]
			def.Unique = flags&0x1 == 0x1
			def.MultiKey = flags&0x2 == 0x2
		case indexDefFieldMultiKey:
			if len(payload) == 0 {
				return IndexDefinition{}, 0, errors.New("collections: missing multikey option byte")
			}
			def.MultiKey = payload[0] != 0
			payload = payload[1:]
		default:
			return IndexDefinition{}, 0, fmt.Errorf("collections: unknown index definition field %d", field)
		}
	}
	consumed := 2 + recordLen
	if err := ValidateIndexName(def.Name); err != nil {
		return IndexDefinition{}, 0, err
	}
	if err := ValidateIndexPath(def.Field); err != nil {
		return IndexDefinition{}, 0, err
	}
	return def, consumed, nil
}

func appendWithLenPrefix(dst []byte, value string) []byte {
	dst = append(dst, 0, 0)
	binary.BigEndian.PutUint16(dst[len(dst)-2:], uint16(len(value)))
	dst = append(dst, value...)
	return dst
}

func readLenPrefixed(raw []byte) (int, int) {
	if len(raw) < 2 {
		return 0, 0
	}
	size := int(binary.BigEndian.Uint16(raw[0:2]))
	if len(raw)-2 < size {
		return 0, 0
	}
	return size, 2 + size
}

func ValidateIndexPath(path string) error {
	if len(path) == 0 {
		return errors.New("path cannot be empty")
	}
	if strings.Contains(path, "\x00") {
		return errors.New("path cannot contain NUL")
	}
	if strings.Contains(path, "..") {
		return errors.New("path cannot contain empty segments")
	}
	if strings.HasPrefix(path, ".") || strings.HasSuffix(path, ".") {
		return errors.New("path cannot start or end with segment separator")
	}
	parts := strings.Split(path, ".")
	for _, p := range parts {
		if p == "" {
			return errors.New("path cannot contain empty segment")
		}
		if strings.HasPrefix(p, "$") {
			return errors.New("path cannot start with '$'")
		}
	}
	return nil
}

func ValidateCollectionName(name string) error {
	if len(name) == 0 {
		return errors.New("collection name cannot be empty")
	}
	if len(name) > 128 {
		return errors.New("collection name too long")
	}
	if strings.Contains(name, "\x00") {
		return errors.New("collection name contains NUL")
	}
	if strings.Contains(name, "/") || strings.Contains(name, ":") {
		return errors.New("collection name contains reserved punctuation")
	}
	if strings.TrimSpace(name) != name {
		return errors.New("collection name has leading or trailing spaces")
	}
	for _, ch := range name {
		if ch < 0x20 || ch == 0x7f {
			return errors.New("collection name contains control character")
		}
	}
	if !utf8.ValidString(name) {
		return errors.New("collection name invalid utf-8")
	}
	if strings.EqualFold(name, "_sys") || strings.EqualFold(name, "sys") {
		return errors.New("collection name is reserved")
	}
	return nil
}

func ValidateIndexName(name string) error {
	if len(name) == 0 {
		return errors.New("index name cannot be empty")
	}
	if len(name) > 128 {
		return errors.New("index name too long")
	}
	if strings.Contains(name, "\x00") {
		return errors.New("index name contains NUL")
	}
	if strings.TrimSpace(name) != name {
		return errors.New("index name has leading or trailing spaces")
	}
	for _, ch := range name {
		if ch < 0x20 || ch == 0x7f {
			return errors.New("index name contains control character")
		}
	}
	return nil
}

func normalizeKeyPart(value string) string {
	return base64.StdEncoding.EncodeToString([]byte(value))
}

func base64DecodeString(value string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(value)
}

func SystemCollectionMetaPrefix() []byte {
	return append([]byte{}, systemCollectionMetaKeyPrefix...)
}

func SystemCollectionMetaKey(collection string) ([]byte, error) {
	if err := ValidateCollectionName(collection); err != nil {
		return nil, err
	}
	return append([]byte("sys:c:"), normalizeKeyPart(collection)...), nil
}

func SystemIndexKey(collection, indexName string) ([]byte, error) {
	if err := ValidateCollectionName(collection); err != nil {
		return nil, err
	}
	if err := ValidateIndexName(indexName); err != nil {
		return nil, err
	}
	key := append([]byte("sys:i:"), normalizeKeyPart(collection)...)
	key = append(key, ':')
	key = append(key, normalizeKeyPart(indexName)...)
	return key, nil
}

func SystemIndexPrefix(collection string) ([]byte, error) {
	if err := ValidateCollectionName(collection); err != nil {
		return nil, err
	}
	key := append([]byte{}, systemCollectionIndexKeyPrefix...)
	key = append(key, normalizeKeyPart(collection)...)
	key = append(key, ':')
	return key, nil
}

func SystemCollectionPrefix(collection string) ([]byte, error) {
	if err := ValidateCollectionName(collection); err != nil {
		return nil, err
	}
	key := append([]byte{}, systemCollectionDataKeyPrefix...)
	key = append(key, normalizeKeyPart(collection)...)
	key = append(key, ':')
	return key, nil
}

// CollectionDataPrefix returns the reserved keyspace prefix for collection documents.
//
// Stored keys are user-root keys with this prefix followed by collection-local IDs.
func CollectionDataPrefix(collection string) ([]byte, error) {
	return SystemCollectionPrefix(collection)
}

func CollectionIndexDataPrefix(collection string) ([]byte, error) {
	if err := ValidateCollectionName(collection); err != nil {
		return nil, err
	}
	key := append([]byte{}, systemCollectionIndexDataPrefix...)
	key = append(key, normalizeKeyPart(collection)...)
	key = append(key, ':')
	return key, nil
}

func CollectionIndexPrefix(collection, indexName string) ([]byte, error) {
	if err := ValidateCollectionName(collection); err != nil {
		return nil, err
	}
	if err := ValidateIndexName(indexName); err != nil {
		return nil, err
	}
	key := append([]byte{}, systemCollectionIndexDataPrefix...)
	key = append(key, normalizeKeyPart(collection)...)
	key = append(key, ':')
	key = append(key, normalizeKeyPart(indexName)...)
	key = append(key, ':')
	return key, nil
}

func SystemCollectionIDSequenceKey(collection string) ([]byte, error) {
	prefix, err := SystemCollectionPrefix(collection)
	if err != nil {
		return nil, err
	}
	return append(append([]byte{}, prefix...), systemCollectionIDSeqKeySuffix...), nil
}
