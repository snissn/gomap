package nativewire

import (
	"fmt"
	"sort"
)

type CommandKind uint8

const (
	CommandKindControl CommandKind = iota + 1
	CommandKindMutation
	CommandKindRead
)

type SectionRule struct {
	ID            SectionID
	Name          string
	Required      bool
	Repeatable    bool
	Ordered       bool
	Deterministic bool
}

type CommandSchema struct {
	ID                   CommandID
	Version              uint64
	Name                 string
	Kind                 CommandKind
	AllowedCommandFlags  uint64
	Replicated           bool
	LocalOnly            bool
	RequiresIdempotency  bool
	RequiresCatalogGuard bool
	BenchmarkRequired    bool
	Sections             []SectionRule

	rules    map[SectionID]SectionRule
	required []SectionID
}

type Registry struct {
	commands map[commandKey]*CommandSchema
	schemas  []CommandSchema
}

type commandKey struct {
	id      CommandID
	version uint64
}

type ValidatedCommand struct {
	Header  CommandHeader
	Schema  *CommandSchema
	Known   []Section
	Ignored []Section
}

// CommandScratch carries reusable buffers for command-schema validation.
type CommandScratch struct {
	Known          []Section
	Ignored        []Section
	seenOverflow   map[SectionID]sectionSeenEntry
	seenGeneration uint64
}

func NewRegistry(commands ...CommandSchema) (*Registry, error) {
	r := &Registry{
		commands: make(map[commandKey]*CommandSchema, len(commands)),
		schemas:  append([]CommandSchema(nil), commands...),
	}
	for i := range r.schemas {
		c := &r.schemas[i]
		if c.Version == 0 {
			return nil, fmt.Errorf("nativewire: command %s has zero version", c.Name)
		}
		key := commandKey{id: c.ID, version: c.Version}
		if _, exists := r.commands[key]; exists {
			return nil, fmt.Errorf("nativewire: duplicate command %d version %d", c.ID, c.Version)
		}
		ruleSeen := make(map[SectionID]struct{}, len(c.Sections))
		for _, rule := range c.Sections {
			if !validSchemaSectionID(rule.ID) {
				return nil, fmt.Errorf("nativewire: command %s uses invalid section %d", c.Name, rule.ID)
			}
			if _, exists := ruleSeen[rule.ID]; exists {
				return nil, fmt.Errorf("nativewire: command %s duplicates section rule %d", c.Name, rule.ID)
			}
			ruleSeen[rule.ID] = struct{}{}
		}
		c.rules, c.required = compileCommandRules(*c)
		r.commands[key] = c
	}
	return r, nil
}

func MustV1Registry() *Registry {
	r, err := NewRegistry(v1CommandSchemas()...)
	if err != nil {
		panic(err)
	}
	return r
}

func (r *Registry) LookupCommand(id CommandID, version uint64) (*CommandSchema, bool) {
	if r == nil {
		return nil, false
	}
	c, ok := r.commands[commandKey{id: id, version: version}]
	return c, ok
}

func (r *Registry) Schemas() []CommandSchema {
	if r == nil || len(r.schemas) == 0 {
		return nil
	}
	return append([]CommandSchema(nil), r.schemas...)
}

func (r *Registry) ValidateRequestSections(sections []Section) (ValidatedCommand, error) {
	return r.ValidateRequestSectionsInto(sections, nil)
}

// ValidateRequestSectionsInto validates sections using scratch when provided.
//
// The returned command borrows section slices from sections and scratch. Callers
// may reuse scratch after they are done with the validated command view.
func (r *Registry) ValidateRequestSectionsInto(sections []Section, scratch *CommandScratch) (ValidatedCommand, error) {
	if r == nil {
		return ValidatedCommand{}, protocolError(ErrUnsupportedVersion, "nil command registry")
	}
	header, err := findCommandHeader(sections)
	if err != nil {
		return ValidatedCommand{}, err
	}
	schema, ok := r.LookupCommand(header.ID, header.Version)
	if !ok {
		return ValidatedCommand{}, protocolError(ErrUnsupportedVersion, "unsupported command %d version %d", header.ID, header.Version)
	}
	if unsupported := header.Flags &^ schema.AllowedCommandFlags; unsupported != 0 {
		return ValidatedCommand{}, protocolError(ErrUnsupportedFeature, "unsupported command flags 0x%x", unsupported)
	}
	known, ignored, err := schema.validateSections(sections, scratch)
	if err != nil {
		return ValidatedCommand{}, err
	}
	return ValidatedCommand{
		Header:  header,
		Schema:  schema,
		Known:   known,
		Ignored: ignored,
	}, nil
}

func findCommandHeader(sections []Section) (CommandHeader, error) {
	found := false
	var header CommandHeader
	for _, section := range sections {
		if section.ID != SectionCommandHeader {
			continue
		}
		if found {
			return CommandHeader{}, protocolError(ErrInvalidCommand, "duplicate command_header section")
		}
		var err error
		header, err = DecodeCommandHeader(section.Bytes)
		if err != nil {
			return CommandHeader{}, err
		}
		found = true
	}
	if !found {
		return CommandHeader{}, protocolError(ErrInvalidCommand, "missing command_header section")
	}
	return header, nil
}

func (c *CommandSchema) validateSections(sections []Section, scratch *CommandScratch) ([]Section, []Section, error) {
	rules := c.ruleMap()
	var seen sectionSeenSet
	if scratch != nil && len(sections) > sectionSeenInlineCapacity {
		scratch.seenGeneration++
		if scratch.seenGeneration == 0 {
			clear(scratch.seenOverflow)
			scratch.seenGeneration = 1
		}
		seen.reuseOverflow(scratch.seenOverflow, scratch.seenGeneration)
	}
	var known []Section
	var ignored []Section
	if scratch == nil {
		known = make([]Section, 0, len(sections))
		ignored = make([]Section, 0)
	} else {
		known = scratch.Known[:0]
		ignored = scratch.Ignored[:0]
	}

	for _, section := range sections {
		if section.Flags&^knownSectionFlags != 0 {
			return nil, nil, protocolError(ErrUnsupportedFeature, "unknown section flags 0x%x", section.Flags&^knownSectionFlags)
		}
		rule, ok := rules[section.ID]
		if !ok {
			if section.Critical() {
				return nil, nil, protocolError(ErrUnsupportedFeature, "unknown critical section %d", section.ID)
			}
			ignored = append(ignored, section)
			continue
		}
		count := seen.add(section.ID)
		if !rule.Repeatable && count > 1 {
			return nil, nil, protocolError(ErrInvalidCommand, "command %s duplicate singleton section %s (%d)", c.Name, rule.Name, section.ID)
		}
		known = append(known, section)
	}

	for _, id := range c.requiredSections() {
		if seen.get(id) == 0 {
			rule := rules[id]
			return nil, nil, protocolError(ErrInvalidCommand, "command %s missing required section %s (%d)", c.Name, rule.Name, id)
		}
	}
	if c.RequiresIdempotency && seen.get(SectionIdempotencyKey) == 0 {
		return nil, nil, protocolError(ErrInvalidCommand, "command %s missing idempotency key", c.Name)
	}
	if c.RequiresCatalogGuard && seen.get(SectionExpectedCatalogVersion) == 0 {
		return nil, nil, protocolError(ErrInvalidCommand, "command %s missing catalog guard", c.Name)
	}
	if scratch != nil {
		scratch.Known = known
		scratch.Ignored = ignored
		if seen.overflow != nil {
			scratch.seenOverflow = seen.overflow
		}
	}
	return known, ignored, nil
}

func (c *CommandSchema) ruleMap() map[SectionID]SectionRule {
	if c.rules != nil {
		return c.rules
	}
	rules, _ := compileCommandRules(*c)
	return rules
}

func (c *CommandSchema) requiredSections() []SectionID {
	if c.required != nil {
		return c.required
	}
	_, required := compileCommandRules(*c)
	return required
}

func compileCommandRules(c CommandSchema) (map[SectionID]SectionRule, []SectionID) {
	rules := map[SectionID]SectionRule{
		SectionCommandHeader:     {ID: SectionCommandHeader, Name: "command_header", Required: true},
		SectionDeadline:          {ID: SectionDeadline, Name: "deadline"},
		SectionTraceContext:      {ID: SectionTraceContext, Name: "trace_context"},
		SectionAckPolicy:         {ID: SectionAckPolicy, Name: "ack_policy"},
		SectionConsistencyPolicy: {ID: SectionConsistencyPolicy, Name: "consistency_policy"},
		SectionIdempotencyKey:    {ID: SectionIdempotencyKey, Name: "idempotency_key", Deterministic: true},
		SectionChecksum:          {ID: SectionChecksum, Name: "checksum"},
		SectionCompression:       {ID: SectionCompression, Name: "compression"},
	}
	for _, rule := range c.Sections {
		rules[rule.ID] = rule
	}
	required := make([]SectionID, 0, len(rules))
	for _, rule := range rules {
		if rule.Required {
			required = append(required, rule.ID)
		}
	}
	sort.Slice(required, func(i, j int) bool { return required[i] < required[j] })
	return rules, required
}

const sectionSeenInlineCapacity = 64

type sectionSeenSet struct {
	ids        [sectionSeenInlineCapacity]SectionID
	counts     [sectionSeenInlineCapacity]int
	n          int
	overflow   map[SectionID]sectionSeenEntry
	generation uint64
}

type sectionSeenEntry struct {
	generation uint64
	count      int
}

func (s *sectionSeenSet) reuseOverflow(overflow map[SectionID]sectionSeenEntry, generation uint64) {
	if overflow == nil {
		return
	}
	s.overflow = overflow
	s.generation = generation
}

func (s *sectionSeenSet) add(id SectionID) int {
	if s.overflow != nil {
		entry := s.overflow[id]
		if entry.generation != s.generation {
			entry = sectionSeenEntry{generation: s.generation}
		}
		entry.count++
		s.overflow[id] = entry
		return entry.count
	}
	for i := 0; i < s.n; i++ {
		if s.ids[i] == id {
			s.counts[i]++
			return s.counts[i]
		}
	}
	if s.n < len(s.ids) {
		s.ids[s.n] = id
		s.counts[s.n] = 1
		s.n++
		return 1
	}
	s.overflow = make(map[SectionID]sectionSeenEntry, s.n+1)
	for i := 0; i < s.n; i++ {
		s.overflow[s.ids[i]] = sectionSeenEntry{generation: s.generation, count: s.counts[i]}
	}
	s.overflow[id] = sectionSeenEntry{generation: s.generation, count: 1}
	return 1
}

func (s *sectionSeenSet) get(id SectionID) int {
	if s.overflow != nil {
		entry := s.overflow[id]
		if entry.generation == s.generation {
			return entry.count
		}
		return 0
	}
	for i := 0; i < s.n; i++ {
		if s.ids[i] == id {
			return s.counts[i]
		}
	}
	return 0
}

func validSchemaSectionID(id SectionID) bool {
	if id >= CommandSpecificSectionStart {
		return true
	}
	switch id {
	case SectionDeadline,
		SectionTraceContext,
		SectionAckPolicy,
		SectionConsistencyPolicy,
		SectionIdempotencyKey,
		SectionChecksum,
		SectionCompression:
		return true
	default:
		return false
	}
}

func v1CommandSchemas() []CommandSchema {
	return []CommandSchema{
		{
			ID:                   CommandCreateCollection,
			Version:              1,
			Name:                 "create_collection",
			Kind:                 CommandKindMutation,
			AllowedCommandFlags:  CommandFlagOmitResponseMeta,
			Replicated:           true,
			RequiresIdempotency:  true,
			RequiresCatalogGuard: true,
			LocalOnly:            false,
			Sections: []SectionRule{
				{ID: SectionCollectionMeta, Name: "collection_meta", Required: true, Deterministic: true},
				{ID: SectionExpectedCatalogVersion, Name: "expected_catalog_version", Deterministic: true},
			},
		},
		{
			ID:        CommandListCollections,
			Version:   1,
			Name:      "list_collections",
			Kind:      CommandKindRead,
			LocalOnly: true,
		},
		{
			ID:                   CommandCreateIndex,
			Version:              1,
			Name:                 "create_index",
			Kind:                 CommandKindMutation,
			Replicated:           true,
			RequiresIdempotency:  true,
			RequiresCatalogGuard: true,
			LocalOnly:            false,
			Sections: []SectionRule{
				{ID: SectionCollectionRef, Name: "collection_ref", Required: true, Deterministic: true},
				{ID: SectionIndexDefinition, Name: "index_definition", Required: true, Deterministic: true},
				{ID: SectionExpectedCatalogVersion, Name: "expected_catalog_version", Deterministic: true},
			},
		},
		{
			ID:      CommandListIndexes,
			Version: 1,
			Name:    "list_indexes",
			Kind:    CommandKindRead,
			Sections: []SectionRule{
				{ID: SectionCollectionRef, Name: "collection_ref", Required: true},
			},
		},
		{
			ID:                   CommandDropIndex,
			Version:              1,
			Name:                 "drop_index",
			Kind:                 CommandKindMutation,
			Replicated:           true,
			RequiresIdempotency:  true,
			RequiresCatalogGuard: true,
			LocalOnly:            false,
			Sections: []SectionRule{
				{ID: SectionCollectionRef, Name: "collection_ref", Required: true, Deterministic: true},
				{ID: SectionIndexName, Name: "index_name", Required: true, Deterministic: true},
				{ID: SectionExpectedCatalogVersion, Name: "expected_catalog_version", Deterministic: true},
			},
		},
		{
			ID:        CommandOpenCollection,
			Version:   1,
			Name:      "open_collection",
			Kind:      CommandKindRead,
			LocalOnly: true,
			Sections: []SectionRule{
				{ID: SectionCollectionRef, Name: "collection_ref", Required: true},
			},
		},
		{
			ID:        CommandCloseCollection,
			Version:   1,
			Name:      "close_collection",
			Kind:      CommandKindRead,
			LocalOnly: true,
			Sections: []SectionRule{
				{ID: SectionCollectionRef, Name: "collection_ref", Required: true},
			},
		},
		{
			ID:        CommandDropCollection,
			Version:   1,
			Name:      "drop_collection",
			Kind:      CommandKindMutation,
			LocalOnly: true,
			Sections: []SectionRule{
				{ID: SectionCollectionRef, Name: "collection_ref", Required: true},
			},
		},
		{
			ID:                   CommandInsertBatch,
			Version:              1,
			Name:                 "insert_batch",
			Kind:                 CommandKindMutation,
			AllowedCommandFlags:  commandResponseShapingFlagsMask,
			Replicated:           true,
			RequiresIdempotency:  true,
			RequiresCatalogGuard: true,
			BenchmarkRequired:    true,
			Sections: []SectionRule{
				{ID: SectionCollectionRef, Name: "collection_ref", Required: true, Deterministic: true},
				{ID: SectionDocumentFormat, Name: "document_format", Required: true, Deterministic: true},
				{ID: SectionDocumentIDs, Name: "document_ids", Required: true, Deterministic: true},
				{ID: SectionDocuments, Name: "documents", Required: true, Deterministic: true},
				{ID: SectionTemplateRecords, Name: "template_records", Deterministic: true},
				{ID: SectionExpectedCatalogVersion, Name: "expected_catalog_version", Deterministic: true},
				{ID: SectionAckPolicy, Name: "ack_policy"},
			},
		},
		{
			ID:                   CommandReplaceBatch,
			Version:              1,
			Name:                 "replace_batch",
			Kind:                 CommandKindMutation,
			Replicated:           true,
			RequiresIdempotency:  true,
			RequiresCatalogGuard: true,
			BenchmarkRequired:    true,
			Sections: []SectionRule{
				{ID: SectionCollectionRef, Name: "collection_ref", Required: true, Deterministic: true},
				{ID: SectionDocumentFormat, Name: "document_format", Required: true, Deterministic: true},
				{ID: SectionDocumentIDs, Name: "document_ids", Required: true, Deterministic: true},
				{ID: SectionDocuments, Name: "documents", Required: true, Deterministic: true},
				{ID: SectionTemplateRecords, Name: "template_records", Deterministic: true},
				{ID: SectionExpectedCatalogVersion, Name: "expected_catalog_version", Deterministic: true},
				{ID: SectionReplacementMode, Name: "replacement_mode", Required: true, Deterministic: true},
				{ID: SectionAckPolicy, Name: "ack_policy"},
			},
		},
		{
			ID:                   CommandDeleteBatch,
			Version:              1,
			Name:                 "delete_batch",
			Kind:                 CommandKindMutation,
			Replicated:           true,
			RequiresIdempotency:  true,
			RequiresCatalogGuard: true,
			BenchmarkRequired:    true,
			Sections: []SectionRule{
				{ID: SectionCollectionRef, Name: "collection_ref", Required: true, Deterministic: true},
				{ID: SectionDocumentIDs, Name: "document_ids", Required: true, Deterministic: true},
				{ID: SectionExpectedCatalogVersion, Name: "expected_catalog_version", Deterministic: true},
				{ID: SectionAckPolicy, Name: "ack_policy"},
			},
		},
		{
			ID:                   CommandUpdateBSONSet,
			Version:              1,
			Name:                 "update_bson_set",
			Kind:                 CommandKindMutation,
			Replicated:           true,
			RequiresIdempotency:  true,
			RequiresCatalogGuard: true,
			BenchmarkRequired:    true,
			Sections: []SectionRule{
				{ID: SectionCollectionRef, Name: "collection_ref", Required: true, Deterministic: true},
				{ID: SectionDocumentIDs, Name: "document_ids", Required: true, Deterministic: true},
				{ID: SectionUpdateFieldNames, Name: "update_field_names", Required: true, Deterministic: true},
				{ID: SectionUpdateFieldValues, Name: "update_field_values", Required: true, Deterministic: true},
				{ID: SectionExpectedCatalogVersion, Name: "expected_catalog_version", Deterministic: true},
				{ID: SectionAckPolicy, Name: "ack_policy"},
			},
		},
		{
			ID:        CommandFlushCollection,
			Version:   1,
			Name:      "flush_collection",
			Kind:      CommandKindMutation,
			LocalOnly: true,
			Sections: []SectionRule{
				{ID: SectionCollectionRef, Name: "collection_ref", Required: true},
				{ID: SectionAckPolicy, Name: "ack_policy"},
			},
		},
		{
			ID:        CommandFlushAll,
			Version:   1,
			Name:      "flush_all",
			Kind:      CommandKindMutation,
			LocalOnly: true,
			Sections: []SectionRule{
				{ID: SectionAckPolicy, Name: "ack_policy"},
			},
		},
		{
			ID:        CommandCheckpoint,
			Version:   1,
			Name:      "checkpoint",
			Kind:      CommandKindMutation,
			LocalOnly: true,
			Sections: []SectionRule{
				{ID: SectionAckPolicy, Name: "ack_policy"},
			},
		},
		{
			ID:                CommandGetMany,
			Version:           1,
			Name:              "get_many",
			Kind:              CommandKindRead,
			BenchmarkRequired: true,
			Sections: []SectionRule{
				{ID: SectionCollectionRef, Name: "collection_ref", Required: true},
				{ID: SectionDocumentIDs, Name: "document_ids", Required: true},
			},
		},
		{
			ID:                CommandIndexLookup,
			Version:           1,
			Name:              "index_lookup",
			Kind:              CommandKindRead,
			BenchmarkRequired: true,
			Sections: []SectionRule{
				{ID: SectionCollectionRef, Name: "collection_ref", Required: true},
				{ID: SectionIndexName, Name: "index_name", Required: true},
				{ID: SectionIndexValue, Name: "index_value", Required: true},
				{ID: SectionCursorLimits, Name: "cursor_limits"},
			},
		},
		{
			ID:                CommandIndexRange,
			Version:           1,
			Name:              "index_range",
			Kind:              CommandKindRead,
			BenchmarkRequired: true,
			Sections: []SectionRule{
				{ID: SectionCollectionRef, Name: "collection_ref", Required: true},
				{ID: SectionIndexName, Name: "index_name", Required: true},
				{ID: SectionIndexLowerBound, Name: "index_lower_bound"},
				{ID: SectionIndexUpperBound, Name: "index_upper_bound"},
				{ID: SectionCursorLimits, Name: "cursor_limits"},
			},
		},
		{
			ID:                CommandOpenScan,
			Version:           1,
			Name:              "open_scan",
			Kind:              CommandKindRead,
			BenchmarkRequired: true,
			Sections: []SectionRule{
				{ID: SectionCollectionRef, Name: "collection_ref", Required: true},
				{ID: SectionCursorLimits, Name: "cursor_limits"},
			},
		},
		{
			ID:      CommandCursorNext,
			Version: 1,
			Name:    "cursor_next",
			Kind:    CommandKindRead,
			Sections: []SectionRule{
				{ID: SectionCursorRef, Name: "cursor_ref", Required: true},
				{ID: SectionCursorLimits, Name: "cursor_limits", Required: true},
			},
		},
		{
			ID:      CommandCursorClose,
			Version: 1,
			Name:    "cursor_close",
			Kind:    CommandKindRead,
			Sections: []SectionRule{
				{ID: SectionCursorRef, Name: "cursor_ref", Required: true},
			},
		},
		{
			ID:      CommandStats,
			Version: 1,
			Name:    "stats",
			Kind:    CommandKindRead,
		},
		{
			ID:        CommandDenseVectorSearch,
			Version:   1,
			Name:      "dense_vector_search",
			Kind:      CommandKindRead,
			LocalOnly: true,
			Sections:  []SectionRule{{ID: SectionDenseSearchRequest, Name: "dense_search_request", Required: true}},
		},
		{
			ID:        CommandVectorStatus,
			Version:   1,
			Name:      "vector_status",
			Kind:      CommandKindRead,
			LocalOnly: true,
			Sections:  []SectionRule{{ID: SectionDeadline, Name: "deadline", Required: true}},
		},
		{
			ID:        CommandVectorSearchStrict,
			Version:   1,
			Name:      "vector_search_strict",
			Kind:      CommandKindRead,
			LocalOnly: true,
			Sections: []SectionRule{
				{ID: SectionDeadline, Name: "deadline", Required: true},
				{ID: SectionVectorSearchRequest, Name: "vector_search_request", Required: true},
			},
		},
		{
			ID:        CommandVectorSearchFast,
			Version:   1,
			Name:      "vector_search_fast",
			Kind:      CommandKindRead,
			LocalOnly: true,
			Sections: []SectionRule{
				{ID: SectionDeadline, Name: "deadline", Required: true},
				{ID: SectionVectorSearchRequest, Name: "vector_search_request", Required: true},
				{ID: SectionVectorFastOptions, Name: "vector_fast_options", Required: true},
			},
		},
		{
			ID:        CommandVectorPinSearchSnapshot,
			Version:   1,
			Name:      "vector_pin_search_snapshot",
			Kind:      CommandKindRead,
			LocalOnly: true,
			Sections: []SectionRule{
				{ID: SectionDeadline, Name: "deadline", Required: true},
				{ID: SectionVectorPinOptions, Name: "vector_pin_options", Required: true},
			},
		},
		{
			ID:        CommandVectorSearchPinned,
			Version:   1,
			Name:      "vector_search_pinned",
			Kind:      CommandKindRead,
			LocalOnly: true,
			Sections: []SectionRule{
				{ID: SectionDeadline, Name: "deadline", Required: true},
				{ID: SectionVectorSearchRequest, Name: "vector_search_request", Required: true},
			},
		},
		{
			ID:        CommandVectorClosePinnedSnapshot,
			Version:   1,
			Name:      "vector_close_pinned_snapshot",
			Kind:      CommandKindRead,
			LocalOnly: true,
			Sections:  []SectionRule{{ID: SectionDeadline, Name: "deadline", Required: true}},
		},
	}
}
