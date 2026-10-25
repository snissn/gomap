package collections

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unsafe"

	"github.com/buger/jsonparser"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
)

// Analyzer option normalization constructs stopword slices/maps on every
// analysis call. Bound those and field-name/runtime headers from the captured
// declaration independently of document/query bytes.
func nativeTextDefinitionCharge(def TextIndexDefinition, calls uint64) (uint64, error) {
	size := uint64(len(def.Fields)) + 1
	add := func(n uint64) bool {
		if n > ^uint64(0)-size {
			return false
		}
		size += n
		return true
	}
	for _, field := range def.Fields {
		if !add(uint64(len(field.Field))) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	if def.AnalyzerOptions != nil {
		for _, word := range def.AnalyzerOptions.StopWords {
			if !add(uint64(len(word)) + 1) {
				return 0, memtable.ErrCOWCapacity
			}
		}
	}
	unit := 16*uint64(unsafe.Sizeof(string(""))) + 2*uint64(unsafe.Sizeof(TextToken{}))
	if calls == 0 || calls > ^uint64(0)/unit || size > ^uint64(0)/(calls*unit) {
		return 0, memtable.ErrCOWCapacity
	}
	return memtable.COWAllocationCharge(size * calls * unit), nil
}

func nativeTextSourceCharge(meta CollectionMeta, rows []nativeJSONRow, opts collectionOptions, admit func(uint64) error) (uint64, error) {
	var total uint64
	add := func(n uint64) bool {
		if n > ^uint64(0)-total {
			return false
		}
		total += n
		return true
	}
	// A source byte bounds one token/term, all state/posting/position headers,
	// map entries, and simultaneous analysis/encoded images. No token expansion
	// analyzer is eligible (synonyms/stemming are rejected by normalization).
	unit := 2 * (uint64(unsafe.Sizeof(TextToken{})) + uint64(unsafe.Sizeof(textAnalyzedTerm{})) + uint64(unsafe.Sizeof(textDocumentTermState{})) + uint64(unsafe.Sizeof(textV2PostingBlockEntry{})) + uint64(unsafe.Sizeof(textV2PositionFieldValue{})) + 8*uint64(unsafe.Sizeof(string(""))))
	for _, def := range meta.TextIndexes {
		calls := uint64(len(rows)) * 2
		if len(def.Fields) != 0 && calls > (^uint64(0)-1)/uint64(len(def.Fields)) {
			return 0, memtable.ErrCOWCapacity
		}
		definitionCharge, err := nativeTextDefinitionCharge(def, 1+calls*uint64(len(def.Fields)))
		if err != nil {
			return 0, err
		}
		if !add(definitionCharge) {
			return 0, memtable.ErrCOWCapacity
		}
		// Fixed formats/status/corpus plus field stats; each row emits at most
		// one docid, docmap and norm key. Term/posting/position keys are bounded
		// by analyzed tokens below; existing block decode has its own receipt.
		operations := uint64(9+len(def.Fields)) + 3*uint64(len(rows))
		for _, row := range rows {
			for _, doc := range [][]byte{row.opening, row.current} {
				if doc == nil {
					continue
				}
				// Size the same maintained stored-to-JSON representation that the
				// reducer analyzes. Conversion cohorts remain charged: the reducer
				// converts again while the sizing buffer may await collection.
				var err error
				doc, err = materializeTextBackfillDocumentJSON(doc, opts)
				if err != nil {
					return 0, err
				}
				// JSON selection borrows source spans; ObjectEach can decode
				// escaped keys. Reserve source-sized parser/conversion scratch
				// and selected-value headers
				// before selection; unrelated values incur no token multiplier.
				source := memtable.COWAllocationCharge(uint64(len(doc)))
				valueSize := uint64(unsafe.Sizeof(jsonParserIndexValue{}))
				if source > ^uint64(0)/2 || uint64(len(def.Fields)) > ^uint64(0)/valueSize {
					return 0, memtable.ErrCOWCapacity
				}
				refs := memtable.COWAllocationCharge(uint64(len(def.Fields)) * valueSize)
				if refs > ^uint64(0)-2*source {
					return 0, memtable.ErrCOWCapacity
				}
				parse := 2*source + refs
				if err := admit(parse); err != nil {
					return 0, err
				}
				var stack [8]jsonParserIndexValue
				values := stack[:]
				if len(def.Fields) > len(stack) {
					values = make([]jsonParserIndexValue, len(def.Fields))
				} else {
					values = values[:len(def.Fields)]
				}
				handled, err := selectTextIndexDocumentJSONRootValues(def, doc, values)
				if err != nil {
					return 0, err
				}
				var selected, tokens uint64
				if handled {
					for _, value := range values {
						if value.valueType != jsonparser.String && value.valueType != jsonparser.Array {
							continue
						}
						n := uint64(len(value.raw))
						if n > ^uint64(0)-selected || (n+1)/2 > ^uint64(0)-tokens {
							return 0, memtable.ErrCOWCapacity
						}
						selected += n
						// Simple tokens need a separating byte except the final
						// token. Array syntax only increases this upper bound.
						tokens += (n + 1) / 2
					}
				} else {
					// The sizing decode uses the exact maintained fallback. Admit
					// its source/path/object scratch before allocation on this operation's
					// original lease. A second cohort remains in the final envelope: the
					// reducer decodes again, and unreachable first objects may await GC.
					decoder, err := nativeTextNestedDecoderCharge(def, uint64(len(doc)))
					if err != nil {
						return 0, err
					}
					if err = admit(decoder); err != nil {
						return 0, fmt.Errorf("collections: native text JSON decode workspace: %w", err)
					}
					object, err := decodeTextIndexDocumentJSONObject(doc)
					if err != nil {
						return 0, err
					}
					if !add(decoder) {
						return 0, memtable.ErrCOWCapacity
					}
					for _, field := range def.Fields {
						value, found := extractIndexPathValue(object, splitIndexPath(field.Field))
						if !found || value == nil {
							continue
						}
						var n uint64
						switch value := value.(type) {
						case string:
							n = uint64(len(value))
						case []any:
							// textIndexFieldText allocates len(input) string descriptors,
							// even when only a few elements are strings. Count empty strings
							// as members: they still contribute join separators.
							if uint64(len(value)) > ^uint64(0)/uint64(unsafe.Sizeof(string(""))) || !add(memtable.COWAllocationCharge(uint64(len(value))*uint64(unsafe.Sizeof(string(""))))) {
								return 0, memtable.ErrCOWCapacity
							}
							var members uint64
							for _, item := range value {
								if part, ok := item.(string); ok {
									if uint64(len(part)) > ^uint64(0)-n {
										return 0, memtable.ErrCOWCapacity
									}
									n += uint64(len(part))
									members++
								}
							}
							if members > 0 {
								if members-1 > ^uint64(0)-n {
									return 0, memtable.ErrCOWCapacity
								}
								n += members - 1
							}
						default:
							continue
						}
						if n > ^uint64(0)-selected || (n+1)/2 > ^uint64(0)-tokens {
							return 0, memtable.ErrCOWCapacity
						}
						selected += n
						tokens += (n + 1) / 2
					}
				}
				overhead := uint64(len(row.id)) + uint64(len(def.Fields)) + 1
				if selected > ^uint64(0)-overhead {
					return 0, memtable.ErrCOWCapacity
				}
				size := selected + overhead
				if size > ^uint64(0)/unit || !add(memtable.COWAllocationCharge(size*unit)) || tokens > (^uint64(0)-operations)/3 {
					return 0, memtable.ErrCOWCapacity
				}
				operations += 3 * tokens
			}
		}
		roots := uint64(7)
		if def.Version == TextIndexVersionV1 {
			roots = 3
		}
		tables, err := nativeTextTableCharge(operations, roots)
		if err != nil {
			return 0, err
		}
		if !add(tables) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	return total, nil
}

// One full-source fallback decode cohort. Containers, interface/map/slice/string
// headers and growth are bounded per source byte; parser/reader and the initial
// 512-byte decoder buffer are charged separately for even tiny documents. Path
// splitting allocates string descriptors once per declared field per decode.
func nativeTextNestedDecoderCharge(def TextIndexDefinition, source uint64) (uint64, error) {
	charge := memtable.COWAllocationCharge
	unit := 4 * (uint64(unsafe.Sizeof(any(nil))) + uint64(unsafe.Sizeof(string(""))) + uint64(unsafe.Sizeof([]any{})))
	if source > ^uint64(0)/unit {
		return 0, memtable.ErrCOWCapacity
	}
	total := charge(source * unit)
	add := func(n uint64) bool {
		if n > ^uint64(0)-total {
			return false
		}
		total += n
		return true
	}
	if !add(charge(uint64(unsafe.Sizeof(json.Decoder{})))) || !add(charge(uint64(unsafe.Sizeof(bytes.Reader{})))) || !add(charge(512)) {
		return 0, memtable.ErrCOWCapacity
	}
	for _, field := range def.Fields {
		count := uint64(strings.Count(field.Field, ".")) + 1
		if count > ^uint64(0)/uint64(unsafe.Sizeof(string(""))) || !add(charge(count*uint64(unsafe.Sizeof(string(""))))) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	return total, nil
}

func nativeTextDomainNumber(meta CollectionMeta, name string) uint64 {
	for i, def := range meta.TextIndexes {
		for j, root := range collectionTextRootNamesForDefinition(meta.Name, def) {
			if name == strings.TrimPrefix(root, meta.Name+"/") {
				return (uint64(1) << 32) + uint64(i)*7 + uint64(j)
			}
		}
	}
	return 0
}
func nativeTextTables(names []string, policies []backenddb.OrderedRootStoragePolicy, tables []memtable.Table, admit func(uint64) error) ([]nativeJSONDomainChange, error) {
	if len(names) != len(policies) || len(names) != len(tables) {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	if err := admit(memtable.COWAllocationCharge(uint64(len(names)) * uint64(unsafe.Sizeof(nativeJSONDomainChange{})))); err != nil {
		return nil, err
	}
	out := make([]nativeJSONDomainChange, len(names))
	for i, table := range tables {
		if err := admit(memtable.COWAllocationCharge(uint64(table.Len()) * uint64(unsafe.Sizeof(memtable.COWMutation{})))); err != nil {
			return nil, err
		}
		out[i] = nativeJSONDomainChange{name: names[i], policy: policies[i], entries: make([]memtable.COWMutation, 0, table.Len())}
		it := table.NewIterator(nil, nil)
		err := func() error {
			defer it.Close()
			for ; it.Valid(); it.Next() {
				key, value := it.UnsafeKey(), it.UnsafeValue()
				if err := admit(memtable.COWAllocationCharge(uint64(len(key))) + memtable.COWAllocationCharge(uint64(len(value)))); err != nil {
					return err
				}
				flags := byte(node.FlagInline)
				if it.IsDeleted() {
					flags = node.FlagTombstone
				}
				out[i].entries = append(out[i].entries, memtable.COWMutation{Key: bytes.Clone(key), Value: bytes.Clone(value), Flags: flags})
			}
			return it.Error()
		}()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
func nativeTextInitialDomains(meta CollectionMeta, admit func(uint64) error) ([]nativeJSONDomainChange, error) {
	if len(meta.TextIndexes) == 0 {
		return nil, nil
	}
	opts, err := collectionPlannerOptions(meta)
	if err != nil {
		return nil, err
	}
	opts.textMaterializationAdmit = admit
	charge, err := nativeTextSourceCharge(meta, nil, opts, admit)
	if err != nil {
		return nil, err
	}
	if err = admit(charge); err != nil {
		return nil, err
	}
	opts.textTableFactory = nativeTextTable
	catalog := &collectionCatalog{meta: meta}
	var out []nativeJSONDomainChange
	for _, def := range meta.TextIndexes {
		var plan *createTextIndexBackfillPlan
		var err error
		if def.Version == TextIndexVersionV2 {
			plan, err = buildCreateTextV2IndexBackfillPlan(nil, catalog, def, opts)
		} else {
			plan, err = buildCreateTextV1IndexBackfillPlan(nil, catalog, def, opts)
		}
		if err != nil {
			return nil, err
		}
		part, err := nativeTextTables(plan.rootNames, plan.policies, plan.tables, admit)
		resetCollectionTables(plan.tables)
		if err != nil {
			return nil, err
		}
		// V1's maintained builder omits empty posting/state tables. Native
		// captures still require all three declared domains, including empty ones.
		if def.Version == TextIndexVersionV1 {
			if err := admit(memtable.COWAllocationCharge(3 * uint64(unsafe.Sizeof(nativeJSONDomainChange{})))); err != nil {
				return nil, err
			}
			complete := make([]nativeJSONDomainChange, 0, 3)
			for _, name := range collectionTextRootNamesForDefinition(meta.Name, def) {
				found := false
				for _, domain := range part {
					if domain.name == name {
						complete = append(complete, domain)
						found = true
						break
					}
				}
				if !found {
					policy, e := collectionRootStoragePolicy(meta, name)
					if e != nil {
						return nil, e
					}
					complete = append(complete, nativeJSONDomainChange{name: name, policy: policy})
				}
			}
			part = complete
		}
		for i := range part {
			part[i].name = strings.TrimPrefix(part[i].name, meta.Name+"/")
		}
		out = append(out, part...)
	}
	return out, nil
}
func nativeTextMutationDomains(snap *backenddb.Snapshot, catalog *collectionCatalog, rows []nativeJSONRow, resolver templateV1Resolver, admit func(uint64) error) ([]nativeJSONDomainChange, error) {
	if len(catalog.meta.TextIndexes) == 0 {
		return nil, nil
	}
	opts, err := collectionPlannerOptions(catalog.meta)
	if err != nil {
		return nil, err
	}
	opts.textMaterializationAdmit = admit
	opts.templateResolver = resolver
	charge, err := nativeTextSourceCharge(catalog.meta, rows, opts, admit)
	if err != nil {
		return nil, err
	}
	if err = admit(charge); err != nil {
		return nil, fmt.Errorf("collections: native text analysis workspace: %w", err)
	}
	opts.textTableFactory = nativeTextTable
	mutations := make([]textDocumentMutation, 0, len(rows))
	for _, row := range rows {
		if row.opening == nil && row.current == nil {
			continue
		}
		mutations = append(mutations, textDocumentMutation{documentID: row.id, oldDocument: row.opening, deleteOld: row.opening != nil, newDocument: row.current, setNew: row.current != nil})
	}
	var names []string
	var policies []backenddb.OrderedRootStoragePolicy
	var tables []memtable.Table
	defer func() { resetCollectionTables(tables) }()
	if err = appendTextIndexMutationDeltas(snap, catalog, opts, mutations, &names, make(map[string]uint64), &policies, &tables); err != nil {
		return nil, err
	}
	out, err := nativeTextTables(names, policies, tables, admit)
	for i := range out {
		out[i].name = strings.TrimPrefix(out[i].name, catalog.meta.Name+"/")
	}
	return out, err
}
func (t *NativeJSONTxn) finalizeNativeText(g *nativeJSONTxnBinding) error {
	if len(g.probe.meta.TextIndexes) == 0 {
		return nil
	}
	admit := func(n uint64) error { return t.admitStage(g, n) }
	catalog, err := newNativeTextCatalog(&g.probe, admit)
	if err != nil {
		return err
	}
	for _, def := range g.probe.meta.TextIndexes {
		name := collectionTextV2GenerationsRootName(g.name, def.Name)
		kind, key := txnhistory.PointEffect, encodeTextV2StatusKey()
		if def.Version == TextIndexVersionV1 {
			// V1 has no ordinal/status record. Corpus, term and field counts
			// share one RMW basis even for otherwise independent document IDs.
			name, kind, key = collectionTextStatsRootName(g.name, def.Name), txnhistory.RangeEffect, nil
		}
		name = strings.TrimPrefix(name, g.name+"/")
		ns := txnhistory.Namespace{Kind: txnhistory.CollectionNamespace, Collection: g.incarnation, Domain: nativeTextDomainNumber(g.probe.meta, name), Interpretation: 1}
		if err = t.trackDependency(ns, kind, key, nil); err != nil {
			return err
		}
	}
	additional, err := nativeTextMutationDomains(g.probe.capture.Snapshot(), catalog, g.rows.rows, g.templates, admit)
	if err != nil {
		return err
	}
	if err = admit(memtable.COWAllocationCharge(uint64(len(g.plan.additional)+len(additional)) * uint64(unsafe.Sizeof(nativeJSONDomainChange{})))); err != nil {
		return err
	}
	g.plan.additional = append(g.plan.additional, additional...)
	var count int
	for _, change := range additional {
		count += len(change.entries)
	}
	if err = admit(memtable.COWAllocationCharge(uint64(len(g.effects)+count) * uint64(unsafe.Sizeof(txnhistory.Effect{})))); err != nil {
		return err
	}
	for _, change := range additional {
		ns := txnhistory.Namespace{Kind: txnhistory.CollectionNamespace, Collection: g.incarnation, Domain: nativeTextDomainNumber(g.probe.meta, change.name), Interpretation: 1}
		if ns.Domain == 0 {
			return backenddb.ErrNativeSemanticUnsupported
		}
		for _, row := range change.entries {
			g.effects = append(g.effects, txnhistory.Effect{Namespace: ns, Kind: txnhistory.PointEffect, Key: row.Key})
		}
	}
	return nil
}

func nativeTextDocumentFormat(format DocumentFormat) bool {
	switch normalizedDocumentFormat(format) {
	case DocumentFormatJSON, DocumentFormatBSON, DocumentFormatTemplateV1:
		return true
	default:
		return false
	}
}

func nativeTextMeta(meta CollectionMeta) bool {
	for _, def := range meta.TextIndexes {
		if (def.Version != TextIndexVersionV1 && def.Version != TextIndexVersionV2) || meta.Options.ColumnStore != nil || !nativeTextDocumentFormat(meta.Options.DocumentFormat) {
			return false
		}
	}
	return true
}

func nativeTextDomainCount(meta CollectionMeta) int {
	count := 0
	for _, def := range meta.TextIndexes {
		if def.Version == TextIndexVersionV2 {
			count += 7
		} else {
			count += 3
		}
	}
	return count
}
