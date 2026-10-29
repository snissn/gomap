package collections

import (
	"fmt"
	"strings"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Request-local output of the existing index-family owner; not a new publisher.
type nativeTextIndexDDL struct {
	create *TextIndexDefinition
	drop   string
	stats  TextIndexBackfillStats
}

func (c *Collection) nativeMutateTextIndex(request *nativeTextIndexDDL) (*CollectionMeta, error) {
	if c.manager.isClosing() {
		return nil, backenddb.ErrClosed
	}
	if c.nativeJSON.db.CommandWALEnabled() {
		return nil, fmt.Errorf("%w: collection catalog text index mutation is rejected under command_wal_v2 until catalog text index commands are supported", backenddb.ErrCommandWALRejected)
	}
	if err := c.nativeJSON.db.PrepareNativeProducer(); err != nil {
		return nil, err
	}
	unlock := c.lockVectorIndexMutation()
	defer unlock()
	return c.nativeJSON.mutateIndexFamilies(c, c.name, nil, nil, false, nil, "", request)
}

// Recognize the exact maintained root families without allocating root names in
// the publication callback. V1 and V2 declarations never share interpretations.
func nativeTextDomain(name string) (index string, v2 bool, ok bool) {
	family, index, ok := strings.Cut(name, "/")
	if !ok {
		return "", false, false
	}
	switch family {
	case "text-index", "text-state", "text-stats":
		return index, false, true
	case "text-v2-docid", "text-v2-docmap", "text-v2-terms", "text-v2-posting-blocks", "text-v2-norm-blocks", "text-v2-positions", "text-v2-generations":
		return index, true, true
	}
	return "", false, false
}

func nativeTextDomainDeclared(meta CollectionMeta, name string) bool {
	index, v2, ok := nativeTextDomain(name)
	if !ok {
		return false
	}
	def, found := findTextIndex(meta.TextIndexes, index)
	return found && (def.Version == TextIndexVersionV2) == v2
}

// One original capture supplies both sizing and the maintained backfill scan.
// All selected source, decoder, analysis and unpooled table cohorts stay charged
// on the caller's operation lease until the publisher owns immutable copies.
func nativeTextBackfill(read *NativeJSONRead, meta CollectionMeta, def TextIndexDefinition, admit func(uint64) error) ([]nativeJSONDomainChange, TextIndexBackfillStats, error) {
	var empty TextIndexBackfillStats
	catalog, err := newNativeTextCatalog(read, admit)
	if err != nil {
		return nil, empty, err
	}
	it, err := collectionIteratorAtCatalogRoot(read.capture.Snapshot(), catalog, collectionPrimaryRootName(meta.Name), nil, nil, false)
	if err != nil {
		return nil, empty, err
	}
	var rows nativeJSONRows
	if it != nil {
		defer it.Close()
		for ; it.Valid(); it.Next() {
			row, e := rows.touch(it.UnsafeKey(), func([]byte) ([]byte, error) { return nil, nil }, admit)
			if e != nil {
				return nil, empty, e
			}
			// The admitted iterator owns each immutable value through this scope.
			row.current = it.UnsafeValue()
		}
		if err = it.Error(); err != nil {
			return nil, empty, err
		}
	}
	// Analyze only the new declaration; existing text families remain untouched.
	var definition [1]TextIndexDefinition
	definition[0] = def
	selected := meta
	selected.TextIndexes = definition[:]
	opts, err := collectionPlannerOptions(meta)
	if err != nil {
		return nil, empty, err
	}
	opts.textMaterializationAdmit = admit
	if normalizedDocumentFormat(opts.documentFormat) == DocumentFormatTemplateV1 {
		opts.templateResolver, err = nativeTextCapturedTemplateResolver(read, admit)
		if err != nil {
			return nil, empty, err
		}
	}
	charge, err := nativeTextSourceCharge(selected, rows.rows, opts, admit)
	if err != nil {
		return nil, empty, err
	}
	if err = admit(charge); err != nil {
		return nil, empty, fmt.Errorf("collections: native text backfill workspace: %w", err)
	}
	opts.textTableFactory = nativeTextTable
	plan, err := buildCreateTextIndexBackfillPlan(read.capture.Snapshot(), catalog, def, opts)
	if err != nil {
		return nil, empty, err
	}
	defer resetCollectionTables(plan.tables)
	part, err := nativeTextTables(plan.rootNames, plan.policies, plan.tables, admit)
	if err != nil {
		return nil, empty, err
	}
	// V1 omits empty tables. Its declared empty posting/state domains must still
	// exist in the native inventory, just as for initial collection creation.
	if err = admit(uint64(7) * uint64(unsafe.Sizeof(nativeJSONDomainChange{}))); err != nil {
		return nil, empty, err
	}
	complete := make([]nativeJSONDomainChange, 0, 7)
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
				return nil, empty, e
			}
			complete = append(complete, nativeJSONDomainChange{name: name, policy: policy})
		}
	}
	for i := range complete {
		complete[i].name = strings.TrimPrefix(complete[i].name, meta.Name+"/")
	}
	return complete, plan.stats, nil
}
