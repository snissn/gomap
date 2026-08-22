package collections

import (
	"bytes"
	"errors"
	"fmt"
	"hash/maphash"
	"strings"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

// BSONSetField describes one top-level BSON field assignment for UpdateBSONSet.
// Nested dotted paths are intentionally not accepted yet; keeping this path
// top-level lets the planner know exactly which secondary indexes can change.
type BSONSetField struct {
	Key   string
	Value bson.RawValue
}

// BSONSetUpdateBatchItem describes one structured top-level BSON $set update
// in a batch. DocumentID must be non-empty and unique within the batch.
type BSONSetUpdateBatchItem struct {
	DocumentID []byte
	Fields     []BSONSetField
}

type bsonSetUpdate struct {
	fields       []BSONSetField
	fieldIndexes map[string]int
}

var errBSONSetRequiresBSONFormat = errors.New("collections: BSON $set update requires BSON document format")

// bsonSetReplacementSlackBytes reserves expected growth beyond the current BSON
// document size so most changed documents append without a second grow.
const (
	bsonSetFieldIndexMapThreshold = 8
	bsonSetReplacementSlackBytes  = 64
)

var bsonSetBatchDocumentIDHashSeed = maphash.MakeSeed()

// UpdateBSONSet applies a structured top-level BSON $set update to one
// document. The collection must use DocumentFormatBSON. Missing documents
// return matched=false. If all assigned values already match the stored
// document, modified=false. Callers must not mutate fields or RawValue byte
// slices until UpdateBSONSet returns.
//
// For no-index collections, this path may stage buffered root runs in WAL-off
// relaxed and WAL-on relaxed modes. In command-WAL (WAL-on) modes, staged
// updates append their deterministic command frame before returning, and
// Flush/Close later publishes the covered roots.
func (c *Collection) UpdateBSONSet(documentID []byte, fields []BSONSetField) (bool, bool, error) {
	if err := validateCollectionUpdateDocumentInput(c, documentID); err != nil {
		return false, false, err
	}
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	if err := c.validateBSONSetDocumentFormat(); err != nil {
		return false, false, err
	}
	spec, err := newBSONSetUpdate(fields)
	if err != nil {
		return false, false, err
	}
	var matched, modified bool
	if combiner, domain := c.updateFastPathWithoutCreatingCombiner(); combiner != nil {
		matched, modified, err = combiner.update(c, documentID, nil, spec, true)
	} else if domain != nil {
		defer domain.finishInlineUpdateWithoutCombiner()
		domain.observeUpdateCombineInline()
		matched, modified, err = c.updateSingleInlineWithoutCombiner(domain, documentID, nil, spec, true)
	} else if combiner := c.updateCombiner(); combiner != nil {
		matched, modified, err = combiner.update(c, documentID, nil, spec, true)
	} else {
		matched, modified, err = c.updateBSONSetDirect(documentID, spec)
	}
	if err == nil && modified {
		err = commitAmbiguousError("UpdateBSONSet vector index maintenance", c.notifyVectorIndexesUpsert([][]byte{documentID}))
	}
	return matched, modified, err
}

func (c *Collection) validateBSONSetDocumentFormat() error {
	if c == nil {
		return errCollectionNil
	}
	if normalizedDocumentFormat(c.meta.Options.DocumentFormat) != DocumentFormatBSON {
		return errBSONSetRequiresBSONFormat
	}
	return nil
}

func (c *Collection) updateBSONSetDirect(documentID []byte, spec bsonSetUpdate) (bool, bool, error) {
	if err := c.validateBSONSetDocumentFormat(); err != nil {
		return false, false, err
	}
	var itemStorage [1]updateBatchItem
	itemStorage[0] = updateBatchItem{
		UpdateBatchItem: UpdateBatchItem{
			DocumentID: documentID,
		},
		bsonSet:    spec,
		hasBSONSet: true,
	}
	items := itemStorage[:]
	mode := updateBatchModeNoSecondaryUniqueIndexChanges
	results, batched, err := c.updateBatchOwnedItems(items, mode)
	if c.commandWALActive(nil) && err == nil && !batched {
		// updateBatchOwnedItems reports batched=false for this mode only after a
		// planning-time secondary/unique-index rejection, before staging or
		// publishing any write. Retrying in ordinary command-WAL mode therefore
		// cannot double-apply the update.
		results, batched, err = c.updateBatchOwnedItems(items, updateBatchModeAny)
	}
	if !batched && err == nil {
		if c.commandWALActive(nil) {
			return false, false, errors.New("collections: command WAL BSON $set fallback unexpectedly unbatched")
		}
		return c.updateDirectBSONSet(documentID, spec)
	}
	if err != nil {
		var itemErr *UpdateBatchItemError
		if errors.As(err, &itemErr) && itemErr.Index == 0 && itemErr.Err != nil {
			return false, false, itemErr.Err
		}
		return false, false, err
	}
	if len(results) != 1 {
		return false, false, fmt.Errorf("collections: BSON $set result count %d for single update", len(results))
	}
	return results[0].Matched, results[0].Modified, nil
}

// UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges applies a batch of
// structured top-level BSON $set updates only when no secondary unique index
// value changes in the planning snapshot. This is the BSON-set equivalent of
// UpdateBatchIfNoSecondaryUniqueIndexChanges.
func (c *Collection) UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges(items []BSONSetUpdateBatchItem) ([]UpdateBatchResult, bool, error) {
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	results, batched, err := c.updateBSONSetBatch(items, updateBatchModeNoSecondaryUniqueIndexChanges)
	if err == nil && batched {
		err = commitAmbiguousError("UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges vector index maintenance", c.notifyVectorIndexesBSONSetUpdateBatch(items, results))
	}
	return results, batched, err
}

// PrepareBSONSetUpdateBatchCommandWAL plans a BSON $set batch without applying
// it and returns the exact replacement documents that must be staged in an
// externally-owned collection update command-WAL frame. It is reserved for R3a
// deterministic apply.
func (c *Collection) PrepareBSONSetUpdateBatchCommandWAL(items []BSONSetUpdateBatchItem) ([]UpdateBatchResult, []commitlog.CollectionDocument, error) {
	if c == nil {
		return nil, nil, errCollectionNil
	}
	if c.db == nil {
		return nil, nil, errCollectionDBNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return nil, nil, err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	if err := c.validateBSONSetDocumentFormat(); err != nil {
		return nil, nil, err
	}
	if len(items) == 0 {
		c.setLastUpdateStats(CollectionUpdateStats{})
		return nil, nil, nil
	}
	ownedItems, err := prepareBSONSetUpdateBatchItems(items)
	if err != nil {
		return nil, nil, err
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if err := c.flushBufferedWrites(); err != nil {
		return nil, nil, err
	}
	plan, err := c.buildUpdateBatchPlan(ownedItems, updateBatchModeAny, false, nil)
	if err != nil {
		return nil, nil, err
	}
	defer plan.close()
	return cloneBSONSetUpdateBatchResults(plan.results), cloneBSONSetUpdateCommandWALDocuments(plan.commandWALDocuments), nil
}

// UpdateBSONSetBatchWithCommandWALIntent applies the exact BSON $set
// replacement documents that were precomputed and encoded into an
// already-appended collection update command-WAL frame. It is reserved for R3a
// deterministic apply; ordinary callers should use UpdateBSONSet or
// UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges.
func (c *Collection) UpdateBSONSetBatchWithCommandWALIntent(setItems []BSONSetUpdateBatchItem, commandWALDocuments []commitlog.CollectionDocument, commandWALIntent *backenddb.CommandWALIntent) ([]UpdateBatchResult, error) {
	if commandWALIntent == nil {
		return nil, errors.New("collections: UpdateBSONSetBatchWithCommandWALIntent requires command WAL intent")
	}
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return nil, err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	if err := c.validateBSONSetDocumentFormat(); err != nil {
		return nil, err
	}
	if _, err := prepareBSONSetUpdateBatchItems(setItems); err != nil {
		return nil, err
	}
	if err := validateBSONSetCommandWALDocumentsForItems(setItems, commandWALDocuments); err != nil {
		return nil, err
	}
	ids, documents := bsonSetCommandWALDocumentsBatchInput(commandWALDocuments)
	replacementItems, err := replaceBatchUpdateItems(ids, documents)
	if err != nil {
		return nil, err
	}
	ownedItems, err := prepareUpdateBatchItems(replacementItems)
	if err != nil {
		return nil, err
	}
	if err := c.requireColumnStoreCommandWAL(c.meta, commandWALIntent); err != nil {
		return nil, err
	}
	if err := requireColumnStoreWriteOperationSupported(c.meta, ColumnPublishOperationUpdate); err != nil {
		return nil, err
	}
	results, batched, err := c.updateBatchOwnedItemsWithCommandWALIntent(ownedItems, updateBatchModeAny, commandWALIntent)
	if err == nil && !batched {
		err = errors.New("collections: command WAL BSON $set update unexpectedly unbatched")
	}
	if err == nil {
		err = commitAmbiguousError("UpdateBSONSetBatchWithCommandWALIntent vector index maintenance", c.notifyVectorIndexesUpdateBatch(replacementItems, results))
	}
	return results, err
}

func validateBSONSetCommandWALDocumentsForItems(items []BSONSetUpdateBatchItem, docs []commitlog.CollectionDocument) error {
	itemIDs := make(map[string]struct{}, len(items))
	for _, item := range items {
		itemIDs[string(item.DocumentID)] = struct{}{}
	}
	seenDocs := make(map[string]struct{}, len(docs))
	for i, doc := range docs {
		if len(doc.ID) == 0 {
			return fmt.Errorf("collections: BSON $set command WAL document id cannot be empty at index %d", i)
		}
		if len(doc.Document) == 0 {
			return fmt.Errorf("collections: BSON $set command WAL document cannot be empty at index %d", i)
		}
		key := string(doc.ID)
		if _, ok := itemIDs[key]; !ok {
			return fmt.Errorf("collections: BSON $set command WAL document id %q was not preflighted", string(doc.ID))
		}
		if _, ok := seenDocs[key]; ok {
			return fmt.Errorf("%w at command WAL document index %d", ErrDuplicateDocumentID, i)
		}
		seenDocs[key] = struct{}{}
	}
	return nil
}

func bsonSetCommandWALDocumentsBatchInput(docs []commitlog.CollectionDocument) ([][]byte, [][]byte) {
	ids := make([][]byte, len(docs))
	documents := make([][]byte, len(docs))
	for i := range docs {
		ids[i] = docs[i].ID
		documents[i] = docs[i].Document
	}
	return ids, documents
}

func (c *Collection) updateBSONSetBatch(items []BSONSetUpdateBatchItem, mode updateBatchMode) ([]UpdateBatchResult, bool, error) {
	if c == nil {
		return nil, false, errCollectionNil
	}
	if c.db == nil {
		return nil, false, errCollectionDBNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return nil, false, err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	if err := c.validateBSONSetDocumentFormat(); err != nil {
		return nil, false, err
	}
	if len(items) == 0 {
		c.setLastUpdateStats(CollectionUpdateStats{})
		return nil, true, nil
	}
	ownedItems, err := prepareBSONSetUpdateBatchItems(items)
	if err != nil {
		return nil, false, err
	}
	return c.updateBatchOwnedItems(ownedItems, mode)
}

func cloneBSONSetUpdateBatchResults(results []UpdateBatchResult) []UpdateBatchResult {
	if len(results) == 0 {
		return nil
	}
	return append([]UpdateBatchResult(nil), results...)
}

func cloneBSONSetUpdateCommandWALDocuments(docs []commitlog.CollectionDocument) []commitlog.CollectionDocument {
	if len(docs) == 0 {
		return nil
	}
	out := make([]commitlog.CollectionDocument, len(docs))
	for i := range docs {
		out[i] = commitlog.CollectionDocument{
			ID:       bytes.Clone(docs[i].ID),
			Document: bytes.Clone(docs[i].Document),
		}
	}
	return out
}

func prepareBSONSetUpdateBatchItems(items []BSONSetUpdateBatchItem) ([]updateBatchItem, error) {
	out := make([]updateBatchItem, len(items))
	seen := make(map[uint64]int, len(items))
	collisions := make(map[uint64][]int)
	for i, item := range items {
		if len(item.DocumentID) == 0 {
			return nil, fmt.Errorf("collections: document id cannot be empty at index %d", i)
		}
		hash := maphash.Bytes(bsonSetBatchDocumentIDHashSeed, item.DocumentID)
		if first, ok := seen[hash]; ok {
			if bytes.Equal(items[first].DocumentID, item.DocumentID) {
				return nil, fmt.Errorf("%w at index %d", ErrDuplicateDocumentID, i)
			}
			for _, prev := range collisions[hash] {
				if bytes.Equal(items[prev].DocumentID, item.DocumentID) {
					return nil, fmt.Errorf("%w at index %d", ErrDuplicateDocumentID, i)
				}
			}
			collisions[hash] = append(collisions[hash], i)
		} else {
			seen[hash] = i
		}
		spec, err := newBSONSetUpdate(item.Fields)
		if err != nil {
			return nil, updateBatchItemError(i, err)
		}
		out[i] = newBSONSetUpdateBatchItem(item.DocumentID, spec)
	}
	return out, nil
}

func newBSONSetUpdate(fields []BSONSetField) (bsonSetUpdate, error) {
	spec := bsonSetUpdate{}
	if len(fields) == 0 {
		return spec, nil
	}
	if len(fields) > bsonSetFieldIndexMapThreshold {
		fieldIndex := make(map[string]int, len(fields))
		for i, field := range fields {
			if err := validateBSONSetFieldKey(field.Key); err != nil {
				return bsonSetUpdate{}, fmt.Errorf("collections: BSON $set field %q key: %w", field.Key, err)
			}
			if err := validateBSONSetRawValue(field.Value); err != nil {
				return bsonSetUpdate{}, fmt.Errorf("collections: BSON $set field %q value: %w", field.Key, err)
			}
			if _, exists := fieldIndex[field.Key]; exists {
				return bsonSetUpdate{}, fmt.Errorf("collections: duplicate BSON $set field %q", field.Key)
			}
			fieldIndex[field.Key] = i
		}
		spec.fields = fields
		spec.fieldIndexes = fieldIndex
		return spec, nil
	}
	for i, field := range fields {
		if err := validateBSONSetFieldKey(field.Key); err != nil {
			return bsonSetUpdate{}, fmt.Errorf("collections: BSON $set field %q key: %w", field.Key, err)
		}
		if err := validateBSONSetRawValue(field.Value); err != nil {
			return bsonSetUpdate{}, fmt.Errorf("collections: BSON $set field %q value: %w", field.Key, err)
		}
		for j := 0; j < i; j++ {
			if fields[j].Key == field.Key {
				return bsonSetUpdate{}, fmt.Errorf("collections: duplicate BSON $set field %q", field.Key)
			}
		}
	}
	spec.fields = fields
	return spec, nil
}

func validateBSONSetFieldKey(key string) error {
	if key == "" {
		return errors.New("field name cannot be empty")
	}
	if key == "_id" {
		return errors.New("cannot modify _id")
	}
	if strings.Contains(key, ".") {
		return errors.New("currently supports top-level fields only")
	}
	if strings.HasPrefix(key, "$") {
		return errors.New("field names cannot start with $")
	}
	if strings.Contains(key, "\x00") {
		return errors.New("field names cannot contain NUL")
	}
	return nil
}

func validateBSONSetRawValue(value bson.RawValue) error {
	_, rem, ok := bsoncore.ReadValue(value.Value, bsoncore.Type(value.Type))
	if !ok || len(rem) != 0 {
		return errors.New("invalid BSON raw value")
	}
	return nil
}

func (u bsonSetUpdate) fieldIndex(key string) int {
	if u.fieldIndexes != nil {
		idx, ok := u.fieldIndexes[key]
		if ok {
			return idx
		}
		return -1
	}
	for i := range u.fields {
		if u.fields[i].Key == key {
			return i
		}
	}
	return -1
}

func (u bsonSetUpdate) fieldIndexBytes(key []byte) int {
	if u.fieldIndexes != nil {
		// elem.KeyBytes returns a stable slice into current for the duration of
		// apply. The unsafe string is used only for this map lookup and is never
		// retained, so it cannot outlive or observe mutation of the BSON buffer.
		idx, ok := u.fieldIndexes[unsafeStringFromBytes(key)]
		if ok {
			return idx
		}
		return -1
	}
	for i := range u.fields {
		if bytesEqualString(key, u.fields[i].Key) {
			return i
		}
	}
	return -1
}

func unsafeStringFromBytes(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func (u bsonSetUpdate) apply(current []byte) ([]byte, bool, error) {
	_, replacement, changed, err := u.appendReplacement(nil, current)
	return replacement, changed, err
}

// appendReplacement appends changed replacements to dst and returns both the
// possibly-grown dst and the replacement document view. When unchanged,
// replacement aliases current. When changed, replacement aliases the returned
// dst. On error, the returned dst is restored to its original length while
// preserving any grown backing store for caller reuse.
func (u bsonSetUpdate) appendReplacement(dst, current []byte) (returned []byte, replacement []byte, changed bool, err error) {
	start := len(dst)
	var out []byte
	resetDst := func() []byte {
		if changed && out != nil {
			return out[:start]
		}
		return dst[:start]
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			returned = resetDst()
			replacement = nil
			changed = false
			err = collectionUpdatePanicError("structured", recovered)
		}
	}()
	if len(u.fields) == 0 {
		return dst, current, false, nil
	}
	if returned, replacement, changed, ok, fastErr := u.appendSameShapeReplacement(dst, current); ok || fastErr != nil {
		return returned, replacement, changed, fastErr
	}
	length, rem, ok := bsoncore.ReadLength(current)
	if !ok {
		return resetDst(), nil, false, bsoncore.NewInsufficientBytesError(current, rem)
	}
	if int(length) > len(current) {
		return resetDst(), nil, false, bsoncore.NewDocumentLengthError(int(length), len(current))
	}
	if length < 5 || current[length-1] != 0x00 {
		return resetDst(), nil, false, bsoncore.ErrMissingNull
	}
	length -= 4
	var usedInline [8]bool
	used := usedInline[:]
	if len(u.fields) > len(usedInline) {
		used = make([]bool, len(u.fields))
	} else {
		used = used[:len(u.fields)]
	}
	var idx int32
	initOut := func(elemStart int) {
		changed = true
		if cap(dst)-len(dst) < len(current)+bsonSetReplacementSlackBytes {
			grown := make([]byte, len(dst), len(dst)+len(current)+bsonSetReplacementSlackBytes)
			copy(grown, dst)
			out = grown
		} else {
			out = dst
		}
		idx, out = bsoncore.AppendDocumentStart(out)
		out = append(out, current[4:elemStart]...)
	}
	var elem bsoncore.Element
	for length > 1 {
		var elemOK bool
		elemStart := len(current) - len(rem)
		elem, rem, elemOK = bsoncore.ReadElement(rem)
		length -= int32(len(elem))
		if !elemOK {
			return resetDst(), nil, false, bsoncore.NewInsufficientBytesError(current, rem)
		}
		replacementFieldIndex := u.fieldIndexBytes(elem.KeyBytes())
		if replacementFieldIndex < 0 {
			if changed {
				out = append(out, elem...)
			}
			continue
		}
		used[replacementFieldIndex] = true
		field := u.fields[replacementFieldIndex]
		value := field.Value
		currentValue := elem.Value()
		if bsonCoreValueEqualRawValue(currentValue, value) {
			if changed {
				out = append(out, elem...)
			}
			continue
		}
		if !changed {
			initOut(elemStart)
		}
		out = bsoncore.AppendValueElement(out, field.Key, bsoncore.Value{
			Type: bsoncore.Type(value.Type),
			Data: value.Value,
		})
	}
	for i, field := range u.fields {
		if used[i] {
			continue
		}
		value := field.Value
		if !changed {
			initOut(len(current) - len(rem))
		}
		out = bsoncore.AppendValueElement(out, field.Key, bsoncore.Value{
			Type: bsoncore.Type(value.Type),
			Data: value.Value,
		})
	}
	if len(rem) < 1 || rem[0] != 0x00 {
		return resetDst(), nil, false, bsoncore.ErrMissingNull
	}
	if !changed {
		return dst[:start], current, false, nil
	}
	raw, err := bsoncore.AppendDocumentEnd(out, idx)
	if err != nil {
		return resetDst(), nil, false, err
	}
	return raw, raw[start:len(raw):len(raw)], true, nil
}

func (u bsonSetUpdate) appendSameShapeReplacement(dst, current []byte) (returned []byte, replacement []byte, changed bool, ok bool, err error) {
	start := len(dst)
	if len(u.fields) == 0 {
		return dst, current, false, true, nil
	}
	length, _, lengthOK := bsoncore.ReadLength(current)
	if !lengthOK {
		return dst[:start], nil, false, false, nil
	}
	if int(length) > len(current) {
		return dst[:start], nil, false, false, nil
	}
	if length < 5 || current[length-1] != 0x00 {
		return dst[:start], nil, false, false, nil
	}
	docEnd := int(length)
	var usedInline [8]bool
	used := usedInline[:]
	if len(u.fields) > len(usedInline) {
		used = make([]bool, len(u.fields))
	} else {
		used = used[:len(u.fields)]
	}
	var out []byte
	ensureOut := func() []byte {
		if out != nil {
			return out
		}
		out = append(dst, current[:docEnd]...)
		return out
	}
	remaining := current[4:docEnd]
	for len(remaining) > 1 {
		elemStart := docEnd - len(remaining)
		elem, next, elemOK := bsoncore.ReadElement(remaining)
		if !elemOK {
			return dst[:start], nil, false, false, nil
		}
		remaining = next
		fieldIdx := u.fieldIndexBytes(elem.KeyBytes())
		if fieldIdx < 0 {
			continue
		}
		used[fieldIdx] = true
		currentValue := elem.Value()
		field := u.fields[fieldIdx]
		if bsonCoreValueEqualRawValue(currentValue, field.Value) {
			continue
		}
		if currentValue.Type != bsoncore.Type(field.Value.Type) || len(currentValue.Data) != len(field.Value.Value) {
			return dst[:start], nil, false, false, nil
		}
		valueOffset := elemStart + len(elem) - len(currentValue.Data)
		copy(ensureOut()[start+valueOffset:start+valueOffset+len(field.Value.Value)], field.Value.Value)
		changed = true
	}
	if len(remaining) != 1 || remaining[0] != 0x00 {
		return dst[:start], nil, false, false, nil
	}
	for _, fieldUsed := range used {
		if !fieldUsed {
			return dst[:start], nil, false, false, nil
		}
	}
	if !changed {
		return dst[:start], current, false, true, nil
	}
	return out, out[start:len(out):len(out)], true, true, nil
}

func callBSONSetUpdateApply(update bsonSetUpdate, current []byte) (replacement []byte, changed bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			replacement = nil
			changed = false
			err = collectionUpdatePanicError("structured", recovered)
		}
	}()
	return update.apply(current)
}

func callBSONSetUpdateAppendReplacement(update bsonSetUpdate, dst, current []byte) (out []byte, replacement []byte, changed bool, err error) {
	return update.appendReplacement(dst, current)
}

func bsonCoreValueEqualRawValue(left bsoncore.Value, right bson.RawValue) bool {
	return left.Type == bsoncore.Type(right.Type) && bytes.Equal(left.Data, right.Value)
}

func bytesEqualString(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := range b {
		if b[i] != s[i] {
			return false
		}
	}
	return true
}

func (u bsonSetUpdate) affectedIndexMask(runtimes []indexRuntime, opts collectionOptions) (uint64, bool) {
	if normalizedDocumentFormat(opts.documentFormat) != DocumentFormatBSON || len(runtimes) > 64 {
		return 0, false
	}
	var mask uint64
	for runtimeIdx, runtime := range runtimes {
		paths := runtime.componentPaths
		if len(paths) == 0 {
			paths = [][]string{runtime.path}
		}
		for _, path := range paths {
			if len(path) != 0 && u.fieldIndex(path[0]) >= 0 {
				mask |= uint64(1) << uint(runtimeIdx)
				break
			}
		}
	}
	return mask, true
}

// orderedIndexStateForKnownValidDocumentRuntimeMask extracts only index states
// covered by mask. Callers must pass syntactically valid BSON documents. In the
// BSON $set update planner, current documents are validated by ID snapshot
// capture before old-state extraction, and replacements are validated by ID
// preservation checks before new-state extraction. That avoids rescanning the
// whole document when mask is zero.
func orderedIndexStateForKnownValidDocumentRuntimeMask(document []byte, runtimes []indexRuntime, mask uint64, opts collectionOptions, encoder *indexEncodeArena) (orderedDocumentIndexState, error) {
	if len(runtimes) == 0 {
		return nil, nil
	}
	if encoder == nil {
		encoder = &indexEncodeArena{
			buf:       make([]byte, 0, estimateDocumentIndexEncodeArenaBytes(len(runtimes))),
			valueRefs: make([][]byte, 0, len(runtimes)),
		}
	}
	allMask := uint64(^uint64(0))
	if len(runtimes) < 64 {
		allMask = (uint64(1) << uint(len(runtimes))) - 1
	}
	mask &= allMask
	if mask == 0 {
		return encoder.appendState(len(runtimes)), nil
	}
	if mask == allMask {
		return orderedIndexStateForDocumentWithArena(document, runtimes, opts, encoder)
	}
	state := encoder.appendState(len(runtimes))
	raw := bson.Raw(document)
	for runtimeIdx, runtime := range runtimes {
		if mask&(uint64(1)<<uint(runtimeIdx)) == 0 {
			continue
		}
		if err := appendBSONIndexRuntimeState(raw, state, runtimeIdx, runtime, opts, encoder); err != nil {
			return nil, err
		}
	}
	return state, nil
}
