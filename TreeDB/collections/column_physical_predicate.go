package collections

import (
	"errors"
	"fmt"
)

// ColumnPhysicalQueryPredicateKind names the narrow predicate shapes supported
// by explicit physical column queries. The first implementation intentionally
// supports only dictionary string equality and small IN lists.
type ColumnPhysicalQueryPredicateKind string

const (
	ColumnPhysicalQueryPredicateEqual  ColumnPhysicalQueryPredicateKind = "equal"
	ColumnPhysicalQueryPredicateInList ColumnPhysicalQueryPredicateKind = "in_list"
)

const columnPhysicalQueryMaxPredicateValues = 64

// ColumnPhysicalQueryPredicate describes one dictionary string predicate. Equal
// uses Value, including the empty string as a valid literal. In-list uses
// Values so callers can distinguish an empty-string member from a missing
// literal. Predicates are combined with AND.
type ColumnPhysicalQueryPredicate struct {
	Column string
	Kind   ColumnPhysicalQueryPredicateKind
	Value  string
	Values []string
}

type columnPhysicalQueryPredicateSpec struct {
	column string
	kind   ColumnPhysicalQueryPredicateKind
	values []string
}

type columnDictionaryPredicateAsset struct {
	rowCount   int
	codes      [][]uint32
	allowed    [][]uint64
	rejectsAll bool
}

type columnPhysicalQueryPredicateDiagnosticPlan struct {
	columns  []string
	kinds    []string
	count    int
	literals int
}

func columnPhysicalQueryHasPredicates(req ColumnPhysicalQueryRequest) bool {
	return len(req.Predicates) > 0
}

func columnPhysicalQueryPredicateKindOrDefault(kind ColumnPhysicalQueryPredicateKind) ColumnPhysicalQueryPredicateKind {
	if kind == "" {
		return ColumnPhysicalQueryPredicateEqual
	}
	return kind
}

func columnPhysicalQueryPredicateSpecs(cfg ColumnStoreConfig, req ColumnPhysicalQueryRequest) ([]columnPhysicalQueryPredicateSpec, error) {
	if len(req.Predicates) == 0 {
		return nil, nil
	}
	if req.AggregateMetadataName != "" {
		return nil, fmt.Errorf("%w: aggregate metadata physical predicates are not supported", ErrColumnQueryPlanUnsupported)
	}
	seen := make(map[string]struct{}, len(req.Predicates))
	specs := make([]columnPhysicalQueryPredicateSpec, 0, len(req.Predicates))
	for idx, predicate := range req.Predicates {
		if predicate.Column == "" {
			return nil, fmt.Errorf("%w: physical predicate[%d] column is required", ErrColumnQueryPlanUnsupported, idx)
		}
		if _, ok := seen[predicate.Column]; ok {
			return nil, fmt.Errorf("%w: multiple physical predicates on column %q are not supported", ErrColumnQueryPlanUnsupported, predicate.Column)
		}
		seen[predicate.Column] = struct{}{}
		col, _, ok := columnPhysicalQueryDeclaredColumn(cfg, predicate.Column)
		if !ok {
			return nil, fmt.Errorf("%w: physical predicate requested undeclared column %q", ErrColumnQueryPlanUnsupported, predicate.Column)
		}
		if col.ValueType != ColumnStoreValueString {
			return nil, fmt.Errorf("%w: physical predicate column %q has type %q, want %q", ErrColumnQueryPlanUnsupported, predicate.Column, col.ValueType, ColumnStoreValueString)
		}
		if !col.Dictionary {
			return nil, fmt.Errorf("%w: physical predicate column %q requires dictionary string sidecars", ErrColumnQueryPlanUnsupported, predicate.Column)
		}
		if col.Nullable {
			return nil, fmt.Errorf("%w: physical predicate column %q does not support nullable values", ErrColumnQueryPlanUnsupported, predicate.Column)
		}
		owner, err := columnStoreColumnOwner(col)
		if err != nil {
			return nil, err
		}
		if owner != TypedStorageOwnerRowAsset {
			return nil, fmt.Errorf("%w: physical predicate column %q owner=%q is not supported", ErrColumnQueryPlanUnsupported, predicate.Column, owner)
		}
		kind := columnPhysicalQueryPredicateKindOrDefault(predicate.Kind)
		var values []string
		switch kind {
		case ColumnPhysicalQueryPredicateEqual:
			if len(predicate.Values) != 0 {
				return nil, fmt.Errorf("%w: physical predicate column %q equal uses Value, not Values", ErrColumnQueryPlanUnsupported, predicate.Column)
			}
			values = []string{predicate.Value}
		case ColumnPhysicalQueryPredicateInList:
			if predicate.Value != "" {
				return nil, fmt.Errorf("%w: physical predicate column %q in-list uses Values, not Value", ErrColumnQueryPlanUnsupported, predicate.Column)
			}
			if len(predicate.Values) == 0 {
				return nil, fmt.Errorf("%w: physical predicate column %q in-list requires at least one value", ErrColumnQueryPlanUnsupported, predicate.Column)
			}
			if len(predicate.Values) > columnPhysicalQueryMaxPredicateValues {
				return nil, fmt.Errorf("%w: physical predicate column %q in-list values=%d exceeds limit=%d", ErrColumnQueryPlanUnsupported, predicate.Column, len(predicate.Values), columnPhysicalQueryMaxPredicateValues)
			}
			values = append([]string(nil), predicate.Values...)
		default:
			return nil, fmt.Errorf("%w: unsupported physical predicate kind %q for column %q", ErrColumnQueryPlanUnsupported, predicate.Kind, predicate.Column)
		}
		specs = append(specs, columnPhysicalQueryPredicateSpec{column: predicate.Column, kind: kind, values: values})
	}
	return specs, nil
}

func prepareColumnDictionaryPredicateAssets(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, readCache *columnPhysicalAssetReadCache) ([]columnDictionaryPredicateAsset, int64, error) {
	specs, err := columnPhysicalQueryPredicateSpecs(view.Config, req)
	if err != nil || len(specs) == 0 {
		return nil, 0, err
	}
	if view.MutationParts != 0 {
		return nil, 0, fmt.Errorf("%w: physical predicates require insert-only manifest", ErrColumnQueryPlanUnsupported)
	}
	if readCache == nil {
		return nil, 0, errors.New("collections: physical predicate query missing read cache")
	}
	byColumn := make([]map[[2]uint64]columnManifestDictionaryCodesSnapshot, len(specs))
	for idx, spec := range specs {
		byPart := columnDictionaryCodeSnapshotsByPart(view, spec.column)
		if len(byPart) == 0 || !columnDictionaryCodeSnapshotsCoverParts(view, byPart) {
			return nil, 0, fmt.Errorf("%w: physical predicate column %q requires complete dictionary code sidecars", ErrColumnQueryPlanUnsupported, spec.column)
		}
		byColumn[idx] = byPart
	}
	assets := make([]columnDictionaryPredicateAsset, 0, len(view.AssetRefs))
	var totalBytes int64
	var scratch []byte
	for _, part := range view.AssetRefs {
		if part.Reason != ColumnPublishOperationInsert {
			return nil, 0, fmt.Errorf("%w: physical predicates require insert-only manifest", ErrColumnQueryPlanUnsupported)
		}
		partKey := [2]uint64{part.Ref.Generation, part.Ref.PartID}
		asset := columnDictionaryPredicateAsset{
			rowCount: part.Rows,
			codes:    make([][]uint32, len(specs)),
			allowed:  make([][]uint64, len(specs)),
		}
		for predicateIdx, spec := range specs {
			snapshot, ok := byColumn[predicateIdx][partKey]
			if !ok {
				return nil, 0, fmt.Errorf("%w: physical predicate column %q missing dictionary code sidecar generation=%d part_id=%d", ErrColumnQueryPlanUnsupported, spec.column, part.Ref.Generation, part.Ref.PartID)
			}
			raw, err := readCache.read(snapshot.AssetRef, scratch)
			if err != nil {
				return nil, 0, fmt.Errorf("collections: physical predicate dictionary codes read generation=%d part_id=%d column=%q: %w", snapshot.AssetRef.Generation, snapshot.AssetRef.PartID, spec.column, err)
			}
			scratch = raw
			dictCur, cardinality, rowCount, err := decodeColumnDictionaryCodesAssetHeader(raw, snapshot.AssetRef, view.Config, view.CollectionName, spec.column, false)
			if err != nil {
				return nil, 0, err
			}
			if rowCount != part.Rows {
				return nil, 0, fmt.Errorf("collections: physical predicate dictionary codes asset row count=%d want manifest rows=%d generation=%d part_id=%d column=%q", rowCount, part.Rows, snapshot.AssetRef.Generation, snapshot.AssetRef.PartID, spec.column)
			}
			allowedWords := (cardinality + 63) / 64
			allowed := make([]uint64, allowedWords)
			targets := make(map[string]struct{}, len(spec.values))
			for _, value := range spec.values {
				targets[value] = struct{}{}
			}
			matchedAnyCode := false
			for localCode := 0; localCode < cardinality; localCode++ {
				value := dictCur.stringBytes()
				if _, ok := targets[unsafeStringFromBytes(value)]; ok {
					allowed[localCode/64] |= uint64(1) << uint(localCode&63)
					matchedAnyCode = true
				}
				if dictCur.err != nil {
					break
				}
			}
			if dictCur.err != nil {
				return nil, 0, dictCur.err
			}
			cur := manifestCursor{raw: raw, pos: dictCur.pos}
			codes := make([]uint32, rowCount)
			for rowIdx := range codes {
				localCode := cur.u32()
				localIdx, ok := columnDictionaryCodeIndex(localCode, cardinality)
				if !ok {
					return nil, 0, fmt.Errorf("collections: physical predicate dictionary codes asset code[%d]=%d outside cardinality=%d", rowIdx, localCode, cardinality)
				}
				codes[rowIdx] = uint32(localIdx)
			}
			if cur.err != nil {
				return nil, 0, cur.err
			}
			if cur.pos != len(raw) {
				return nil, 0, errors.New("collections: trailing bytes in physical predicate dictionary codes asset")
			}
			if !matchedAnyCode {
				asset.rejectsAll = true
			}
			asset.codes[predicateIdx] = codes
			asset.allowed[predicateIdx] = allowed
			totalBytes += snapshot.AssetRef.Length
		}
		assets = append(assets, asset)
	}
	return assets, totalBytes, nil
}

func (a columnDictionaryPredicateAsset) matches(rowIdx int) bool {
	if a.rejectsAll {
		return false
	}
	if rowIdx < 0 || rowIdx >= a.rowCount {
		return false
	}
	for predicateIdx, codes := range a.codes {
		if rowIdx >= len(codes) {
			return false
		}
		code := int(codes[rowIdx])
		allowed := a.allowed[predicateIdx]
		word := code / 64
		if word < 0 || word >= len(allowed) || allowed[word]&(uint64(1)<<uint(code&63)) == 0 {
			return false
		}
	}
	return true
}

func columnDictionaryPredicateAssetHits(assets []columnDictionaryPredicateAsset) int {
	if len(assets) == 0 {
		return 0
	}
	return len(assets) * len(assets[0].codes)
}

func newColumnPhysicalQueryPredicateDiagnosticPlan(req ColumnPhysicalQueryRequest) columnPhysicalQueryPredicateDiagnosticPlan {
	if len(req.Predicates) == 0 {
		return columnPhysicalQueryPredicateDiagnosticPlan{}
	}
	plan := columnPhysicalQueryPredicateDiagnosticPlan{
		columns: make([]string, 0, len(req.Predicates)),
		kinds:   make([]string, 0, len(req.Predicates)),
		count:   len(req.Predicates),
	}
	for _, predicate := range req.Predicates {
		plan.columns = append(plan.columns, predicate.Column)
		kind := columnPhysicalQueryPredicateKindOrDefault(predicate.Kind)
		plan.kinds = append(plan.kinds, string(kind))
		if kind == ColumnPhysicalQueryPredicateInList {
			plan.literals += len(predicate.Values)
		} else {
			plan.literals++
		}
	}
	return plan
}

func applyColumnPhysicalQueryPredicateDiagnostics(diag *ColumnPhysicalQueryDiagnostics, plan columnPhysicalQueryPredicateDiagnosticPlan, matchedRows int, dictionaryHits int) {
	if diag == nil || plan.count == 0 {
		return
	}
	diag.PredicateColumns = plan.columns
	diag.PredicateKinds = plan.kinds
	diag.PredicateLiterals = plan.literals
	diag.PredicateCount = plan.count
	diag.RowsMatched = matchedRows
	diag.PredicateDictionaryCodeHits = dictionaryHits
}

func columnPhysicalQueryProjectedColumnCount(req ColumnPhysicalQueryRequest) int {
	seen := make(map[string]struct{}, 3+len(req.Predicates))
	add := func(name string) {
		if name != "" {
			seen[name] = struct{}{}
		}
	}
	add(req.GroupColumn)
	add(req.ValueColumn)
	add(req.DistinctColumn)
	for _, predicate := range req.Predicates {
		add(predicate.Column)
	}
	return len(seen)
}
