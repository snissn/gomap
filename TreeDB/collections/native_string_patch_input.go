package collections

import (
	"bytes"
	"math"
	"slices"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

// validateTypedStringPatchInput checks borrowed input before the first owned
// copy. The caller holds schema admission while borrowing admissionMeta.
// The uint32 wire bound is not a Go backing-allocation certificate.
func validateTypedStringPatchInput(requests []TypedStringPatch, admissionMeta CollectionMeta, expectedSchemaHash uint64) error {
	if err := validateTypedStringPatchMeta(admissionMeta, expectedSchemaHash); err != nil {
		return err
	}
	// Prove the owned-input shape against the existing uint32 WAL section before
	// allocating ID/edit/residual copies. Actual request credit adds Go backing.
	inputBytes := uint64(22)
	addInput := func(n uint64) error {
		if n > uint64(math.MaxUint32)-inputBytes {
			return commitlog.ErrRecordTooLarge
		}
		inputBytes += n
		return nil
	}
	if uint64(len(requests)) > uint64(math.MaxInt)/uint64(unsafe.Sizeof(TypedStringPatch{})) {
		return commitlog.ErrRecordTooLarge
	}
	for _, r := range requests {
		if len(r.ID) == 0 || !utf8.Valid(r.ID) || r.ResidualMode > TypedStringResidualReplace || r.ResidualMode == TypedStringResidualPreserve && len(r.Residual) != 0 {
			return ErrTypedStringPatchInvalid
		}
		if len(r.Edits) > len(admissionMeta.Options.ColumnStore.Columns) || uint64(len(r.Edits)) > uint64(math.MaxInt)/uint64(unsafe.Sizeof(TypedStringEdit{})) {
			return ErrTypedStringPatchInvalid
		}
		if r.Expected != nil && !bytes.Equal(r.Expected.DocumentID, r.ID) {
			return ErrTypedStringPatchInvalid
		}
		for _, n := range [...]uint64{45, uint64(len(r.ID)), uint64(len(r.Residual))} {
			if err := addInput(n); err != nil {
				return err
			}
		}
		for _, e := range r.Edits {
			if e.Column == "" || !utf8.ValidString(e.Column) || !utf8.ValidString(e.Value) {
				return ErrTypedStringPatchInvalid
			}
			for _, n := range [...]uint64{8, uint64(len(e.Column)), uint64(len(e.Value))} {
				if err := addInput(n); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// cloneTypedStringPatchInput is the single owned-input constructor. Canonical
// admission must reserve all backing before calling it, then retain this
// ownership through serial fallback or grouped completion.
func cloneTypedStringPatchInput(requests []TypedStringPatch) ([]TypedStringPatch, error) {
	owned := make([]TypedStringPatch, len(requests))
	for i, r := range requests {
		owned[i] = r
		owned[i].ID = make([]byte, len(r.ID))
		copy(owned[i].ID, r.ID)
		owned[i].Residual = make([]byte, len(r.Residual))
		copy(owned[i].Residual, r.Residual)
		owned[i].Edits = make([]TypedStringEdit, len(r.Edits))
		copy(owned[i].Edits, r.Edits)
		for j, e := range r.Edits {
			owned[i].Edits[j] = TypedStringEdit{strings.Clone(e.Column), strings.Clone(e.Value)}
		}
		if r.Expected != nil {
			e := *r.Expected
			e.DocumentID = make([]byte, len(r.Expected.DocumentID))
			copy(e.DocumentID, r.Expected.DocumentID)
			if !bytes.Equal(e.DocumentID, r.ID) {
				return nil, ErrTypedStringPatchInvalid
			}
			owned[i].Expected = &e
		}
	}
	slices.SortFunc(owned, func(a, b TypedStringPatch) int { return bytes.Compare(a.ID, b.ID) })
	for i := 1; i < len(owned); i++ {
		if bytes.Equal(owned[i-1].ID, owned[i].ID) {
			return nil, ErrDuplicateDocumentID
		}
	}
	return owned, nil
}
