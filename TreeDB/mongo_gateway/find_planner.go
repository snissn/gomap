package mongogateway

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"
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
			cmp := compareDocumentField(docs[i], docs[j], sortSpec.field)
			if sortSpec.desc {
				return cmp > 0
			}
			return cmp < 0
		})
	}

	skip, limit, err := parseFindPagination(command)
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
	if limit > 0 && int(limit) < len(docs) {
		docs = docs[:limit]
	}

	projectionDoc, err := commandOptionalDocument(command, "projection")
	if err != nil {
		return nil, err
	}
	projection, err := compileProjection(projectionDoc)
	if err != nil {
		return nil, err
	}
	firstBatch := make(bson.A, 0, len(docs))
	batchBytes := 0
	maxBatchBytes := s.maxFindBatchBytes()
	for _, doc := range docs {
		projected, err := projectDocumentWithProjection(doc, projection)
		if err != nil {
			return nil, err
		}
		docBytes := len(projected) + 16
		if docBytes > maxBatchBytes {
			return nil, fmt.Errorf("Mongo gateway find result document exceeds max message size")
		}
		if batchBytes+docBytes > maxBatchBytes {
			return nil, fmt.Errorf("Mongo gateway find results exceed max message size")
		}
		firstBatch = append(firstBatch, bson.Raw(projected))
		batchBytes += docBytes
	}
	return firstBatch, nil
}

func validateFindCommandOptions(command wire.Document, filter wire.Document) error {
	if _, err := parseFindPredicates(filter); err != nil {
		return err
	}
	if _, err := parseFindSort(command); err != nil {
		return err
	}
	if _, _, err := parseFindPagination(command); err != nil {
		return err
	}
	projection, err := commandOptionalDocument(command, "projection")
	if err != nil {
		return err
	}
	if projection != nil {
		if _, _, _, _, err := parseProjection(projection); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) maxFindBatchBytes() int {
	max := int(s.maxMessageLength()) - 4096
	if max < 0 {
		return 0
	}
	return max
}

func (s *Server) findCandidateDocuments(col *collections.Collection, predicates []findPredicate) ([]wire.Document, error) {
	meta := col.Meta()
	if pred, ok := primaryCandidatePredicate(predicates); ok {
		docs, err := documentsForPrimaryPredicate(col, pred)
		if err != nil {
			return nil, err
		}
		return s.limitCandidateDocuments(docs)
	}
	if pred, idx, ok := indexedCandidatePredicate(meta, predicates); ok {
		docs, err := documentsForIndexedPredicate(col, pred, idx)
		if err != nil {
			return nil, err
		}
		return s.limitCandidateDocuments(docs)
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

func (s *Server) limitCandidateDocuments(docs []wire.Document) ([]wire.Document, error) {
	if len(docs) > s.maxFindScanDocuments() {
		return nil, fmt.Errorf("Mongo gateway find candidate set exceeded %d documents", s.maxFindScanDocuments())
	}
	return docs, nil
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
		if predicateContainsNull(pred) {
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
			return nil, fmt.Errorf("Mongo gateway find value cannot be represented in index %q", idx.Name)
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
	if len(values) == 0 {
		return nil, errors.New("Mongo gateway $and must contain at least one expression")
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
	if doc, ok := value.DocumentOK(); ok {
		isOperatorDoc, err := operatorDocument(doc)
		if err != nil {
			return nil, err
		}
		if !isOperatorDoc {
			return []findPredicate{{field: field, op: findPredicateEq, values: []bson.RawValue{value}}}, nil
		}
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

func operatorDocument(doc bson.Raw) (bool, error) {
	elements, err := doc.Elements()
	if err != nil {
		return false, err
	}
	if len(elements) == 0 {
		return false, nil
	}
	sawOperator := false
	sawNonOperator := false
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return false, err
		}
		if strings.HasPrefix(key, "$") {
			sawOperator = true
		} else {
			sawNonOperator = true
		}
		if sawOperator && sawNonOperator {
			return false, errors.New("Mongo gateway find field predicate document cannot mix operator and non-operator keys")
		}
	}
	return sawOperator, nil
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
			if missingValueMatchesPredicate(pred) {
				continue
			}
			return false, nil
		}
		match, err := valueMatchesPredicate(value, pred)
		if err != nil || !match {
			return match, err
		}
	}
	return true, nil
}

func missingValueMatchesPredicate(pred findPredicate) bool {
	switch pred.op {
	case findPredicateEq, findPredicateIn:
		return predicateContainsNull(pred)
	default:
		return false
	}
}

func predicateContainsNull(pred findPredicate) bool {
	for _, value := range pred.values {
		if rawValueIsNull(value) {
			return true
		}
	}
	return false
}

func rawValueIsNull(value bson.RawValue) bool {
	return value.Type == bson.TypeNull
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
		if rawValueIsNaN(value) || rawValueIsNaN(pred.values[0]) {
			return false, nil
		}
		cmp := compareRawValues(value, pred.values[0])
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

func parseFindPagination(command wire.Document) (int32, int32, error) {
	skip, err := optionalInt32Field(command, "skip")
	if err != nil {
		return 0, 0, err
	}
	if skip < 0 {
		return 0, 0, errors.New("Mongo gateway find skip must be non-negative")
	}
	limit, err := optionalInt32Field(command, "limit")
	if err != nil {
		return 0, 0, err
	}
	if limit < 0 {
		return 0, 0, errors.New("Mongo gateway find limit must be non-negative")
	}
	return skip, limit, nil
}

func compareDocumentField(left, right wire.Document, field string) int {
	leftValue, leftOK := lookupDocumentValue(left, field)
	rightValue, rightOK := lookupDocumentValue(right, field)
	if !leftOK {
		leftValue = bson.RawValue{Type: bson.TypeNull}
	}
	if !rightOK {
		rightValue = bson.RawValue{Type: bson.TypeNull}
	}
	return compareRawValues(leftValue, rightValue)
}

type compiledProjection struct {
	present     bool
	mode        projectionMode
	fields      map[string]struct{}
	includeID   bool
	idSpecified bool
}

func compileProjection(projection wire.Document) (compiledProjection, error) {
	if projection == nil {
		return compiledProjection{}, nil
	}
	mode, fields, includeID, idSpecified, err := parseProjection(projection)
	if err != nil {
		return compiledProjection{}, err
	}
	return compiledProjection{
		present:     true,
		mode:        mode,
		fields:      fields,
		includeID:   includeID,
		idSpecified: idSpecified,
	}, nil
}

func projectDocument(doc wire.Document, projection wire.Document) (wire.Document, error) {
	compiled, err := compileProjection(projection)
	if err != nil {
		return nil, err
	}
	return projectDocumentWithProjection(doc, compiled)
}

func projectDocumentWithProjection(doc wire.Document, projection compiledProjection) (wire.Document, error) {
	if !projection.present {
		return doc, nil
	}
	mode := projection.mode
	if mode == projectionNone {
		if !projection.idSpecified {
			return doc, nil
		}
		if projection.includeID {
			mode = projectionInclude
		} else {
			mode = projectionExclude
		}
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
		if key == "_id" && !projection.includeID {
			continue
		}
		_, selected := projection.fields[key]
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

func parseProjection(projection wire.Document) (projectionMode, map[string]struct{}, bool, bool, error) {
	elements, err := bson.Raw(projection).Elements()
	if err != nil {
		return projectionNone, nil, true, false, err
	}
	fields := make(map[string]struct{}, len(elements))
	mode := projectionNone
	includeID := true
	idSpecified := false
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return projectionNone, nil, true, false, err
		}
		include, err := projectionValueIncluded(elem.Value())
		if err != nil {
			return projectionNone, nil, true, false, err
		}
		if key == "_id" {
			includeID = include
			idSpecified = true
			continue
		}
		if strings.Contains(key, ".") {
			return projectionNone, nil, true, false, errors.New("Mongo gateway projection currently supports top-level fields only")
		}
		nextMode := projectionExclude
		if include {
			nextMode = projectionInclude
		}
		if mode != projectionNone && mode != nextMode {
			return projectionNone, nil, true, false, errors.New("Mongo gateway projection cannot mix include and exclude fields")
		}
		mode = nextMode
		fields[key] = struct{}{}
	}
	return mode, fields, includeID, idSpecified, nil
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
		if rawValueIsNaN(left) || rawValueIsNaN(right) {
			return false
		}
		return compareRawValues(left, right) == 0
	}
	return left.Equal(right)
}

func compareRawValues(left, right bson.RawValue) int {
	if left.IsNumber() && right.IsNumber() {
		return compareRawNumbers(left, right)
	}
	if left.Type != right.Type {
		return compareInt(bsonTypeSortRank(left.Type), bsonTypeSortRank(right.Type))
	}
	switch left.Type {
	case bson.TypeString:
		return strings.Compare(left.StringValue(), right.StringValue())
	case bson.TypeBoolean:
		leftBool := left.Boolean()
		rightBool := right.Boolean()
		switch {
		case leftBool == rightBool:
			return 0
		case !leftBool && rightBool:
			return -1
		default:
			return 1
		}
	case bson.TypeNull:
		return 0
	case bson.TypeObjectID:
		return bytes.Compare(left.Value, right.Value)
	default:
		return bytes.Compare(left.Value, right.Value)
	}
}

func compareRawNumbers(left, right bson.RawValue) int {
	leftRat, leftOK := rawNumberRat(left)
	rightRat, rightOK := rawNumberRat(right)
	if leftOK && rightOK {
		return leftRat.Cmp(rightRat)
	}
	leftRank := numberSortRank(left, leftOK)
	rightRank := numberSortRank(right, rightOK)
	if leftRank != rightRank {
		return compareInt(leftRank, rightRank)
	}
	return bytes.Compare(left.Value, right.Value)
}

func numberSortRank(value bson.RawValue, finite bool) int {
	if finite {
		return 1
	}
	if value.Type == bson.TypeDouble {
		v, ok := value.DoubleOK()
		if ok {
			switch {
			case math.IsInf(v, -1):
				return 0
			case math.IsInf(v, 1):
				return 2
			case math.IsNaN(v):
				return 3
			}
		}
	}
	return 4
}

func rawValueIsNaN(value bson.RawValue) bool {
	if value.Type != bson.TypeDouble {
		return false
	}
	v, ok := value.DoubleOK()
	return ok && math.IsNaN(v)
}

func rawNumberRat(value bson.RawValue) (*big.Rat, bool) {
	switch value.Type {
	case bson.TypeInt32:
		v, ok := value.Int32OK()
		if !ok {
			return nil, false
		}
		return big.NewRat(int64(v), 1), true
	case bson.TypeInt64:
		v, ok := value.Int64OK()
		if !ok {
			return nil, false
		}
		return big.NewRat(v, 1), true
	case bson.TypeDouble:
		v, ok := value.DoubleOK()
		if !ok {
			return nil, false
		}
		rat := new(big.Rat)
		if rat.SetFloat64(v) == nil {
			return nil, false
		}
		return rat, true
	default:
		return nil, false
	}
}

func compareInt(left, right int) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func bsonTypeSortRank(t bson.Type) int {
	switch t {
	case bson.TypeMinKey:
		return 0
	case bson.TypeNull:
		return 1
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble:
		return 2
	case bson.TypeString:
		return 3
	case bson.TypeEmbeddedDocument:
		return 4
	case bson.TypeArray:
		return 5
	case bson.TypeBinary:
		return 6
	case bson.TypeObjectID:
		return 7
	case bson.TypeBoolean:
		return 8
	case bson.TypeDateTime:
		return 9
	case bson.TypeTimestamp:
		return 10
	case bson.TypeMaxKey:
		return 100
	default:
		return 50 + int(t)
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
