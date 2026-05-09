package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math/bits"
	"strings"

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

type bsonSetUpdate struct {
	fields []BSONSetField
}

var errBSONSetRequiresBSONFormat = errors.New("collections: BSON $set update requires BSON document format")

// UpdateBSONSet applies a structured top-level BSON $set update to one
// document. The collection must use DocumentFormatBSON. Missing documents
// return matched=false. If all assigned values already match the stored
// document, modified=false. Callers must not mutate fields or RawValue byte
// slices until UpdateBSONSet returns.
func (c *Collection) UpdateBSONSet(documentID []byte, fields []BSONSetField) (bool, bool, error) {
	if err := validateCollectionUpdateDocumentInput(c, documentID); err != nil {
		return false, false, err
	}
	if err := c.validateBSONSetDocumentFormat(); err != nil {
		return false, false, err
	}
	spec, err := newBSONSetUpdate(fields)
	if err != nil {
		return false, false, err
	}
	if combiner, domain := c.updateFastPathWithoutCreatingCombiner(); combiner != nil {
		return combiner.update(c, documentID, nil, spec, true)
	} else if domain != nil {
		defer domain.finishInlineUpdateWithoutCombiner()
		domain.observeUpdateCombineInline()
		return c.updateSingleInlineWithoutCombiner(domain, documentID, nil, spec, true)
	}
	if combiner := c.updateCombiner(); combiner != nil {
		return combiner.update(c, documentID, nil, spec, true)
	}
	return c.updateBSONSetDirect(documentID, spec)
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
	items := []UpdateBatchItem{{DocumentID: documentID, bsonSet: spec, hasBSONSet: true}}
	results, batched, err := c.updateBatchOwnedItems(items, updateBatchModeNoSecondaryUniqueIndexChanges)
	if !batched && err == nil {
		return c.updateDirect(documentID, func(current []byte) ([]byte, bool, error) {
			return callBSONSetUpdateApply(spec, current)
		})
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

func newBSONSetUpdate(fields []BSONSetField) (bsonSetUpdate, error) {
	spec := bsonSetUpdate{}
	if len(fields) == 0 {
		return spec, nil
	}
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if err := validateBSONSetFieldKey(field.Key); err != nil {
			return bsonSetUpdate{}, err
		}
		if _, ok := seen[field.Key]; ok {
			return bsonSetUpdate{}, fmt.Errorf("collections: duplicate BSON $set field %q", field.Key)
		}
		seen[field.Key] = struct{}{}
	}
	spec.fields = fields
	return spec, nil
}

func validateBSONSetFieldKey(key string) error {
	if key == "" {
		return errors.New("collections: BSON $set field name cannot be empty")
	}
	if key == "_id" {
		return errBSONIDMutation
	}
	if strings.Contains(key, ".") {
		return errors.New("collections: BSON $set currently supports top-level fields only")
	}
	if strings.HasPrefix(key, "$") {
		return errors.New("collections: BSON $set field names cannot start with $")
	}
	if strings.Contains(key, "\x00") {
		return errors.New("collections: BSON $set field names cannot contain NUL")
	}
	return nil
}

func (u bsonSetUpdate) fieldIndex(key string) int {
	for i := range u.fields {
		if u.fields[i].Key == key {
			return i
		}
	}
	return -1
}

func (u bsonSetUpdate) fieldIndexBytes(key []byte) int {
	for i := range u.fields {
		if bytesEqualString(key, u.fields[i].Key) {
			return i
		}
	}
	return -1
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
func (u bsonSetUpdate) appendReplacement(dst, current []byte) ([]byte, []byte, bool, error) {
	if len(u.fields) == 0 {
		return dst, current, false, nil
	}
	length, rem, ok := bsoncore.ReadLength(current)
	if !ok {
		return dst, nil, false, bsoncore.NewInsufficientBytesError(current, rem)
	}
	length -= 4
	start := len(dst)
	var usedInline [8]bool
	used := usedInline[:]
	if len(u.fields) > len(usedInline) {
		used = make([]bool, len(u.fields))
	} else {
		used = used[:len(u.fields)]
	}
	changed := false
	var idx int32
	var out []byte
	resetDst := func() []byte {
		if changed {
			return out[:start]
		}
		return dst[:start]
	}
	initOut := func(elemStart int) {
		changed = true
		if cap(dst)-len(dst) < len(current)+64 {
			grown := make([]byte, len(dst), len(dst)+len(current)+64)
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
		replacement := u.fieldIndexBytes(elem.KeyBytes())
		if replacement < 0 {
			if changed {
				out = append(out, elem...)
			}
			continue
		}
		used[replacement] = true
		field := u.fields[replacement]
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
	if !changed {
		return dst[:start], current, false, nil
	}
	raw, err := bsoncore.AppendDocumentEnd(out, idx)
	if err != nil {
		return resetDst(), nil, false, err
	}
	return raw, raw[start:len(raw):len(raw)], true, nil
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
	defer func() {
		if recovered := recover(); recovered != nil {
			out = dst
			replacement = nil
			changed = false
			err = collectionUpdatePanicError("structured", recovered)
		}
	}()
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
		if len(runtime.path) == 0 {
			continue
		}
		if u.fieldIndex(runtime.path[0]) >= 0 {
			mask |= uint64(1) << uint(runtimeIdx)
		}
	}
	return mask, true
}

// orderedIndexStateForKnownValidDocumentRuntimeMask extracts only index states
// covered by mask. Callers must already have validated BSON documents; the BSON
// $set update path does that via ID preservation checks before using this
// helper, which avoids rescanning the whole document when mask is zero.
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
	var inline [8]indexRuntime
	subset := inline[:0]
	count := bits.OnesCount64(mask)
	if count > len(inline) {
		subset = make([]indexRuntime, 0, count)
	}
	for runtimeIdx, runtime := range runtimes {
		if mask&(uint64(1)<<uint(runtimeIdx)) == 0 {
			continue
		}
		subset = append(subset, runtime)
	}
	subsetState, err := orderedIndexStateForDocumentWithArena(document, subset, opts, encoder)
	if err != nil {
		return nil, err
	}
	subsetOffset := 0
	for runtimeIdx := range runtimes {
		if mask&(uint64(1)<<uint(runtimeIdx)) == 0 {
			continue
		}
		state[runtimeIdx] = subsetState.valuesAt(subsetOffset)
		subsetOffset++
	}
	return state, nil
}
