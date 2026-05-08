package nativewire

import "fmt"

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

func (r *Registry) ValidateRequestSections(sections []Section) (ValidatedCommand, error) {
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
	known, ignored, err := schema.validateSections(sections)
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

func (c *CommandSchema) validateSections(sections []Section) ([]Section, []Section, error) {
	rules := c.ruleMap()
	seen := make(map[SectionID]int, len(sections))
	known := make([]Section, 0, len(sections))
	ignored := make([]Section, 0)

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
		if !rule.Repeatable && seen[section.ID] > 0 {
			return nil, nil, protocolError(ErrInvalidCommand, "duplicate singleton section %d", section.ID)
		}
		seen[section.ID]++
		known = append(known, section)
	}

	for _, rule := range rules {
		if rule.Required && seen[rule.ID] == 0 {
			return nil, nil, protocolError(ErrInvalidCommand, "missing required section %d", rule.ID)
		}
	}
	if c.RequiresIdempotency && seen[SectionIdempotencyKey] == 0 {
		return nil, nil, protocolError(ErrInvalidCommand, "missing idempotency key")
	}
	if c.RequiresCatalogGuard && seen[SectionExpectedCatalogVersion] == 0 {
		return nil, nil, protocolError(ErrInvalidCommand, "missing catalog guard")
	}
	return known, ignored, nil
}

func (c *CommandSchema) ruleMap() map[SectionID]SectionRule {
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
	return rules
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
			ID:                   CommandInsertBatch,
			Version:              1,
			Name:                 "insert_batch",
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
			ID:                CommandOpenScan,
			Version:           1,
			Name:              "open_scan",
			Kind:              CommandKindRead,
			BenchmarkRequired: true,
			Sections: []SectionRule{
				{ID: SectionCollectionRef, Name: "collection_ref", Required: true},
			},
		},
		{
			ID:      CommandCursorNext,
			Version: 1,
			Name:    "cursor_next",
			Kind:    CommandKindRead,
			Sections: []SectionRule{
				{ID: SectionCursorLimits, Name: "cursor_limits", Required: true},
			},
		},
		{
			ID:      CommandCursorClose,
			Version: 1,
			Name:    "cursor_close",
			Kind:    CommandKindRead,
		},
		{
			ID:      CommandStats,
			Version: 1,
			Name:    "stats",
			Kind:    CommandKindRead,
		},
	}
}
