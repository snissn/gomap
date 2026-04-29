package mongogateway

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type findPredicateOp uint8

const (
	findPredicateEq findPredicateOp = iota + 1
	findPredicateIn
	findPredicateGT
	findPredicateGTE
	findPredicateLT
	findPredicateLTE
)

type findPredicate struct {
	field  string
	op     findPredicateOp
	values []bson.RawValue
}

type findSort struct {
	field string
	desc  bool
}

func (s *Server) executeFind(col *collections.Collection, command wire.Document, filter wire.Document) (bson.A, error) {
	predicates, err := parseFindPredicates(filter)
	if err != nil {
		return nil, err
	}
	docs, err := s.findCandidateDocuments(col, predicates)
	if err != nil {
		return nil, err
	}
	filtered := docs[:0]
	for _, doc := range docs {
		match, err := documentMatchesPredicates(doc, predicates)
		if err != nil {
			return nil, err
		}
		if match {
			filtered = append(filtered, doc)
		}
	}
	docs = filtered

	sortSpec, err := parseFindSort(command)
	if err != nil {
		return nil, err
	}
	if sortSpec.field != "" {
		sort.SliceStable(docs, func(i, j int) bool {
			cmp, ok := compareDocumentField(docs[i], docs[j], sortSpec.field)
			if !ok {
				return false
			}
			if sortSpec.desc {
				return cmp > 0
			}
			return cmp < 0
		})
	}

	skip, err := optionalInt32Field(command, "skip")
	if err != nil {
		return nil, err
	}
	if skip > 0 {
		if int(skip) >= len(docs) {
			docs = nil
		} else {
			docs = docs[skip:]
		}
	}
	limit, err := optionalInt32Field(command, "limit")
	if err != nil {
		return nil, err
	}
	if limit > 0 && int(limit) < len(docs) {
		docs = docs[:limit]
	}

	projection, err := commandOptionalDocument(command, "projection")
	if err != nil {
		return nil, err
	}
	firstBatch := make(bson.A, 0, len(docs))
	for _, doc := range docs {
		projected, err := projectDocument(doc, projection)
		if err != nil {
			return nil, err
		}
		firstBatch = append(firstBatch, bson.Raw(projected))
	}
	return firstBatch, nil
}

func (s *Server) findCandidateDocuments(col *collections.Collection, predicates []findPredicate) ([]wire.Document, error) {
	meta := col.Meta()
	if pred, ok := primaryCandidatePredicate(predicates); ok {
		return documentsForPrimaryPredicate(col, pred)
	}
	if pred, idx, ok := indexedCandidatePredicate(meta, predicates); ok {
		return documentsForIndexedPredicate(col, pred, idx)
	}
	records, truncated, err := col.ScanDocuments(s.maxFindScanDocuments())
	if err != nil {
		return nil, err
	}
	if truncated {
		return nil, fmt.Errorf("Mongo gateway find requires a bounded scan and exceeded %d documents", s.maxFindScanDocuments())
	}
	out := make([]wire.Document, 0, len(records))
	for _, record := range records {
		doc, err := storedDocumentToBSON(record.Document)
		if err != nil {
			return nil, err
		}
		out = append(out, doc)
	}
	return out, nil
}

func primaryCandidatePredicate(predicates []findPredicate) (findPredicate, bool) {
	for _, pred := range predicates {
		if pred.field == "_id" && (pred.op == findPredicateEq || pred.op == findPredicateIn) {
			return pred, true
		}
	}
	return findPredicate{}, false
}

func indexedCandidatePredicate(meta collections.CollectionMeta, predicates []findPredicate) (findPredicate, collections.IndexDefinition, bool) {
	for _, pred := range predicates {
		if pred.op != findPredicateEq && pred.op != findPredicateIn {
			continue
		}
		for _, idx := range meta.Indexes {
			if idx.Field == pred.field {
				return pred, idx, true
			}
		}
	}
	return findPredicate{}, collections.IndexDefinition{}, false
}

func documentsForPrimaryPredicate(col *collections.Collection, pred findPredicate) ([]wire.Document, error) {
	out := make([]wire.Document, 0, len(pred.values))
	seen := make(map[string]struct{}, len(pred.values))
	for _, value := range pred.values {
		key, err := encodePrimaryKey(value)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[string(key)]; ok {
			continue
		}
		seen[string(key)] = struct{}{}
		stored, err := col.Get(key)
		if err != nil {
			return nil, err
		}
		if len(stored) == 0 {
			continue
		}
		doc, err := storedDocumentToBSON(stored)
		if err != nil {
			return nil, err
		}
		out = append(out, doc)
	}
	return out, nil
}

func documentsForIndexedPredicate(col *collections.Collection, pred findPredicate, idx collections.IndexDefinition) ([]wire.Document, error) {
	out := make([]wire.Document, 0)
	seen := make(map[string]struct{})
	for _, value := range pred.values {
		scalar, ok := indexScalarForBSONValue(value)
		if !ok {
			continue
		}
		ids, err := col.FindByIndexValue(idx.Name, scalar)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			if _, ok := seen[string(id)]; ok {
				continue
			}
			seen[string(id)] = struct{}{}
			stored, err := col.Get(id)
			if err != nil {
				return nil, err
			}
			if len(stored) == 0 {
				continue
			}
			doc, err := storedDocumentToBSON(stored)
			if err != nil {
				return nil, err
			}
			out = append(out, doc)
		}
	}
	return out, nil
}

func parseFindPredicates(filter wire.Document) ([]findPredicate, error) {
	if filter == nil {
		return nil, nil
	}
	return parseFindPredicateDocument(filter)
}

func parseFindPredicateDocument(doc wire.Document) ([]findPredicate, error) {
	elements, err := bson.Raw(doc).Elements()
	if err != nil {
		return nil, err
	}
	var out []findPredicate
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return nil, err
		}
		value := elem.Value()
		if key == "$and" {
			preds, err := parseAndPredicates(value)
			if err != nil {
				return nil, err
			}
			out = append(out, preds...)
			continue
		}
		preds, err := parseFieldPredicate(key, value)
		if err != nil {
			return nil, err
		}
		out = append(out, preds...)
	}
	return out, nil
}

func parseAndPredicates(value bson.RawValue) ([]findPredicate, error) {
	array, ok := value.ArrayOK()
	if !ok {
		return nil, errors.New("Mongo gateway $and must be an array")
	}
	values, err := array.Values()
	if err != nil {
		return nil, err
	}
	var out []findPredicate
	for i, value := range values {
		doc, ok := value.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("Mongo gateway $and[%d] must be a document", i)
		}
		preds, err := parseFindPredicateDocument(wire.Document(doc))
		if err != nil {
			return nil, err
		}
		out = append(out, preds...)
	}
	return out, nil
}

func parseFieldPredicate(field string, value bson.RawValue) ([]findPredicate, error) {
	if field == "" || strings.HasPrefix(field, "$") {
		return nil, fmt.Errorf("Mongo gateway unsupported find predicate %q", field)
	}
	if doc, ok := value.DocumentOK(); ok && operatorDocument(doc) {
		elements, err := doc.Elements()
		if err != nil {
			return nil, err
		}
		out := make([]findPredicate, 0, len(elements))
		for _, elem := range elements {
			op, err := elem.KeyErr()
			if err != nil {
				return nil, err
			}
			opValue := elem.Value()
			switch op {
			case "$in":
				array, ok := opValue.ArrayOK()
				if !ok {
					return nil, fmt.Errorf("Mongo gateway find field %q $in must be an array", field)
				}
				values, err := array.Values()
				if err != nil {
					return nil, err
				}
				out = append(out, findPredicate{field: field, op: findPredicateIn, values: values})
			case "$gt", "$gte", "$lt", "$lte":
				out = append(out, findPredicate{field: field, op: rangeOperator(op), values: []bson.RawValue{opValue}})
			default:
				return nil, fmt.Errorf("Mongo gateway unsupported find operator %q", op)
			}
		}
		return out, nil
	}
	return []findPredicate{{field: field, op: findPredicateEq, values: []bson.RawValue{value}}}, nil
}

func operatorDocument(doc bson.Raw) bool {
	elements, err := doc.Elements()
	if err != nil || len(elements) == 0 {
		return false
	}
	key, err := elements[0].KeyErr()
	return err == nil && strings.HasPrefix(key, "$")
}

func rangeOperator(op string) findPredicateOp {
	switch op {
	case "$gt":
		return findPredicateGT
	case "$gte":
		return findPredicateGTE
	case "$lt":
		return findPredicateLT
	default:
		return findPredicateLTE
	}
}

func documentMatchesPredicates(doc wire.Document, predicates []findPredicate) (bool, error) {
	for _, pred := range predicates {
		value, ok := lookupDocumentValue(doc, pred.field)
		if !ok {
			return false, nil
		}
		match, err := valueMatchesPredicate(value, pred)
		if err != nil || !match {
			return match, err
		}
	}
	return true, nil
}

func valueMatchesPredicate(value bson.RawValue, pred findPredicate) (bool, error) {
	switch pred.op {
	case findPredicateEq:
		return rawValuesEqual(value, pred.values[0]), nil
	case findPredicateIn:
		for _, candidate := range pred.values {
			if rawValuesEqual(value, candidate) {
				return true, nil
			}
		}
		return false, nil
	case findPredicateGT, findPredicateGTE, findPredicateLT, findPredicateLTE:
		cmp, ok := compareRawValues(value, pred.values[0])
		if !ok {
			return false, nil
		}
		switch pred.op {
		case findPredicateGT:
			return cmp > 0, nil
		case findPredicateGTE:
			return cmp >= 0, nil
		case findPredicateLT:
			return cmp < 0, nil
		default:
			return cmp <= 0, nil
		}
	default:
		return false, errors.New("Mongo gateway internal unknown predicate")
	}
}

func parseFindSort(command wire.Document) (findSort, error) {
	sortDoc, err := commandOptionalDocument(command, "sort")
	if err != nil || sortDoc == nil {
		return findSort{}, err
	}
	elements, err := bson.Raw(sortDoc).Elements()
	if err != nil {
		return findSort{}, err
	}
	if len(elements) == 0 {
		return findSort{}, nil
	}
	if len(elements) != 1 {
		return findSort{}, errors.New("Mongo gateway find sort currently supports one field")
	}
	field, err := elements[0].KeyErr()
	if err != nil {
		return findSort{}, err
	}
	value := elements[0].Value()
	if isAscendingIndexKey(value) {
		return findSort{field: field}, nil
	}
	if v, ok := value.Int32OK(); ok && v == -1 {
		return findSort{field: field, desc: true}, nil
	}
	if v, ok := value.Int64OK(); ok && v == -1 {
		return findSort{field: field, desc: true}, nil
	}
	if v, ok := value.DoubleOK(); ok && v == -1 {
		return findSort{field: field, desc: true}, nil
	}
	return findSort{}, errors.New("Mongo gateway find sort direction must be 1 or -1")
}

func compareDocumentField(left, right wire.Document, field string) (int, bool) {
	leftValue, leftOK := lookupDocumentValue(left, field)
	rightValue, rightOK := lookupDocumentValue(right, field)
	if !leftOK && !rightOK {
		return 0, true
	}
	if !leftOK {
		return 1, true
	}
	if !rightOK {
		return -1, true
	}
	return compareRawValues(leftValue, rightValue)
}

func projectDocument(doc wire.Document, projection wire.Document) (wire.Document, error) {
	if projection == nil {
		return doc, nil
	}
	mode, fields, includeID, err := parseProjection(projection)
	if err != nil {
		return nil, err
	}
	if mode == projectionNone {
		return doc, nil
	}
	elements, err := bson.Raw(doc).Elements()
	if err != nil {
		return nil, err
	}
	out := make(bson.D, 0, len(elements))
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return nil, err
		}
		if key == "_id" && !includeID {
			continue
		}
		_, selected := fields[key]
		if mode == projectionInclude && (selected || key == "_id") {
			out = append(out, bson.E{Key: key, Value: elem.Value()})
		}
		if mode == projectionExclude && !selected {
			out = append(out, bson.E{Key: key, Value: elem.Value()})
		}
	}
	raw, err := bson.Marshal(out)
	if err != nil {
		return nil, err
	}
	return wire.Document(raw), nil
}

type projectionMode uint8

const (
	projectionNone projectionMode = iota
	projectionInclude
	projectionExclude
)

func parseProjection(projection wire.Document) (projectionMode, map[string]struct{}, bool, error) {
	elements, err := bson.Raw(projection).Elements()
	if err != nil {
		return projectionNone, nil, true, err
	}
	fields := make(map[string]struct{}, len(elements))
	mode := projectionNone
	includeID := true
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return projectionNone, nil, true, err
		}
		include, err := projectionValueIncluded(elem.Value())
		if err != nil {
			return projectionNone, nil, true, err
		}
		if key == "_id" {
			includeID = include
			continue
		}
		if strings.Contains(key, ".") {
			return projectionNone, nil, true, errors.New("Mongo gateway projection currently supports top-level fields only")
		}
		nextMode := projectionExclude
		if include {
			nextMode = projectionInclude
		}
		if mode != projectionNone && mode != nextMode {
			return projectionNone, nil, true, errors.New("Mongo gateway projection cannot mix include and exclude fields")
		}
		mode = nextMode
		fields[key] = struct{}{}
	}
	return mode, fields, includeID, nil
}

func projectionValueIncluded(value bson.RawValue) (bool, error) {
	if v, ok := value.BooleanOK(); ok {
		return v, nil
	}
	if v, ok := value.Int32OK(); ok {
		return v != 0, nil
	}
	if v, ok := value.Int64OK(); ok {
		return v != 0, nil
	}
	if v, ok := value.DoubleOK(); ok {
		return v != 0, nil
	}
	return false, errors.New("Mongo gateway projection values must be boolean or numeric")
}

func lookupDocumentValue(doc wire.Document, field string) (bson.RawValue, bool) {
	if field == "" {
		return bson.RawValue{}, false
	}
	parts := strings.Split(field, ".")
	current := bson.Raw(doc)
	for i, part := range parts {
		value := current.Lookup(part)
		if value.IsZero() {
			return bson.RawValue{}, false
		}
		if i == len(parts)-1 {
			return value, true
		}
		next, ok := value.DocumentOK()
		if !ok {
			return bson.RawValue{}, false
		}
		current = next
	}
	return bson.RawValue{}, false
}

func rawValuesEqual(left, right bson.RawValue) bool {
	if left.IsNumber() && right.IsNumber() {
		leftNumber, leftOK := left.AsFloat64OK()
		rightNumber, rightOK := right.AsFloat64OK()
		return leftOK && rightOK && leftNumber == rightNumber
	}
	return left.Equal(right)
}

func compareRawValues(left, right bson.RawValue) (int, bool) {
	if left.IsNumber() && right.IsNumber() {
		leftNumber, leftOK := left.AsFloat64OK()
		rightNumber, rightOK := right.AsFloat64OK()
		if !leftOK || !rightOK {
			return 0, false
		}
		switch {
		case leftNumber < rightNumber:
			return -1, true
		case leftNumber > rightNumber:
			return 1, true
		default:
			return 0, true
		}
	}
	if left.Type != right.Type {
		return 0, false
	}
	switch left.Type {
	case bson.TypeString:
		return strings.Compare(left.StringValue(), right.StringValue()), true
	case bson.TypeBoolean:
		leftBool := left.Boolean()
		rightBool := right.Boolean()
		switch {
		case leftBool == rightBool:
			return 0, true
		case !leftBool && rightBool:
			return -1, true
		default:
			return 1, true
		}
	case bson.TypeNull:
		return 0, true
	case bson.TypeObjectID:
		return bytes.Compare(left.Value, right.Value), true
	default:
		return bytes.Compare(left.Value, right.Value), true
	}
}

func indexScalarForBSONValue(value bson.RawValue) (any, bool) {
	switch value.Type {
	case bson.TypeString:
		out, ok := value.StringValueOK()
		return out, ok
	case bson.TypeBoolean:
		out, ok := value.BooleanOK()
		return out, ok
	case bson.TypeNull:
		return nil, true
	case bson.TypeDouble:
		out, ok := value.DoubleOK()
		return out, ok
	case bson.TypeInt32, bson.TypeInt64:
		out, ok := value.AsFloat64OK()
		return out, ok
	default:
		return nil, false
	}
}
