package commitlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

// These are supported lifetime ceilings across retained colocated scopes in one collection.
// Exhaustion is a precommit refusal; no retained outcome is silently evicted.
const (
	ColocatedVectorMutationMaxOutcomesV1      = 65536
	ColocatedVectorMutationMaxMetadataBytesV1 = 32 << 20
	ColocatedVectorMutationMaxDocumentBytesV1 = 64 << 10
	ColocatedVectorMutationMaxIdentityBytesV1 = 1024
	ColocatedVectorMutationMaxPayloadBytesV1  = 256 << 10
)

// Scope contains no commit, count, or visibility authority. Only the dedicated
// producer may attach it to ReplaceBatch/DeleteBatch deterministic commands.
type ColocatedVectorMutationScopeV1 struct {
	Version           uint32
	Index, OwnerGroup string
	Generation        uint64
	Digest            [sha256.Size]byte
}

func (s ColocatedVectorMutationScopeV1) ValidateV1() error {
	if s.Version != 1 || s.Generation == 0 || len(s.Index) == 0 || len(s.Index) > ColocatedVectorMutationMaxIdentityBytesV1 || len(s.OwnerGroup) == 0 || len(s.OwnerGroup) > ColocatedVectorMutationMaxIdentityBytesV1 || !utf8.ValidString(s.Index) || !utf8.ValidString(s.OwnerGroup) || s.Digest == ([sha256.Size]byte{}) {
		return ErrCorrupt
	}
	return nil
}

func EncodeColocatedVectorMutationScopeV1(s ColocatedVectorMutationScopeV1) ([]byte, error) {
	if err := s.ValidateV1(); err != nil {
		return nil, err
	}
	return json.Marshal(s)
}

func DecodeColocatedVectorMutationScopeV1(raw []byte) (ColocatedVectorMutationScopeV1, error) {
	var s ColocatedVectorMutationScopeV1
	if err := decodeColocatedVectorJSONV1(raw, &s, 4096); err != nil {
		return s, err
	}
	return s, s.ValidateV1()
}

// WAL authority is assigned by the applying FSM and prepared source owner,
// never decoded from a public request. The extension retains the exact target
// even when the ordinary changed-document payload would be empty.
type ColocatedVectorMutationWALV1 struct {
	Scope                               ColocatedVectorMutationScopeV1
	Collection                          string
	Delete                              bool
	ID, Attempt, Document               []byte
	CommandDigest                       [sha256.Size]byte
	ExpectedCatalogVersion, Term, Index uint64
	Matched, Affected                   uint64
}

func (v ColocatedVectorMutationWALV1) ValidateV1() error {
	if v.Term == 0 || v.Index == 0 {
		return ErrCorrupt
	}
	return v.ValidateInputsV1()
}

// ValidateInputsV1 admits precommit inputs without inventing FSM authority.
func (v ColocatedVectorMutationWALV1) ValidateInputsV1() error {
	if err := v.Scope.ValidateV1(); err != nil {
		return err
	}
	if len(v.Collection) == 0 || len(v.Collection) > ColocatedVectorMutationMaxIdentityBytesV1 || len(v.ID) == 0 || len(v.ID) > ColocatedVectorMutationMaxIdentityBytesV1 || !utf8.Valid(v.ID) || len(v.Attempt) == 0 || len(v.Attempt) > ColocatedVectorMutationMaxIdentityBytesV1 || v.CommandDigest == ([sha256.Size]byte{}) || v.Matched > 1 || v.Affected > 1 {
		return ErrCorrupt
	}
	if v.Delete {
		if len(v.Document) != 0 || v.Matched != 0 {
			return ErrCorrupt
		}
	} else if len(v.Document) == 0 || len(v.Document) > ColocatedVectorMutationMaxDocumentBytesV1 || v.Affected > v.Matched {
		return ErrCorrupt
	}
	return nil
}

// Outcome is the fixed-size original authority stored with source/live roots.
// Its collection-wide attempt key retains ScopeDigest; CommandDigest binds the exact ID/payload.
type ColocatedVectorMutationOutcomeV1 struct {
	ScopeDigest, CommandDigest                                                                             [sha256.Size]byte
	ExpectedCatalogVersion, Term, Index, Coverage, Revision, Matched, Affected, Ordinal, AppliedCommandLSN uint64
}

const ColocatedVectorMutationOutcomeBytesV1 = 8 + 2*sha256.Size + 9*8

func validateColocatedVectorMutationOutcomeV1(v ColocatedVectorMutationOutcomeV1) error {
	if v.ScopeDigest == ([sha256.Size]byte{}) || v.CommandDigest == ([sha256.Size]byte{}) || v.Term == 0 || v.Index == 0 || v.Coverage == 0 || v.AppliedCommandLSN == 0 || v.Ordinal == 0 || v.Ordinal > ColocatedVectorMutationMaxOutcomesV1 || v.Matched > 1 || v.Affected > 1 || (v.Affected != 0 && v.Revision == 0) {
		return ErrCorrupt
	}
	return nil
}

func EncodeColocatedVectorMutationOutcomeV1(v ColocatedVectorMutationOutcomeV1) ([]byte, error) {
	if err := validateColocatedVectorMutationOutcomeV1(v); err != nil {
		return nil, err
	}
	b := make([]byte, 0, ColocatedVectorMutationOutcomeBytesV1)
	b = binary.LittleEndian.AppendUint64(b, 1)
	b = append(b, v.ScopeDigest[:]...)
	b = append(b, v.CommandDigest[:]...)
	for _, n := range []uint64{v.ExpectedCatalogVersion, v.Term, v.Index, v.Coverage, v.Revision, v.Matched, v.Affected, v.Ordinal, v.AppliedCommandLSN} {
		b = binary.LittleEndian.AppendUint64(b, n)
	}
	return b, nil
}

func DecodeColocatedVectorMutationOutcomeV1(b []byte) (ColocatedVectorMutationOutcomeV1, error) {
	var v ColocatedVectorMutationOutcomeV1
	if len(b) != ColocatedVectorMutationOutcomeBytesV1 || binary.LittleEndian.Uint64(b) != 1 {
		return v, ErrCorrupt
	}
	copy(v.ScopeDigest[:], b[8:40])
	copy(v.CommandDigest[:], b[40:72])
	p := []*uint64{&v.ExpectedCatalogVersion, &v.Term, &v.Index, &v.Coverage, &v.Revision, &v.Matched, &v.Affected, &v.Ordinal, &v.AppliedCommandLSN}
	for i, n := range p {
		*n = binary.LittleEndian.Uint64(b[72+i*8:])
	}
	return v, validateColocatedVectorMutationOutcomeV1(v)
}

func encodeColocatedVectorMutationPayloadV2(base []byte, v ColocatedVectorMutationWALV1) ([]byte, error) {
	if err := v.ValidateV1(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(v)
	if err != nil || len(raw) > ColocatedVectorMutationMaxPayloadBytesV1 || len(base) > ColocatedVectorMutationMaxPayloadBytesV1 || len(base)+len(raw)+6 > ColocatedVectorMutationMaxPayloadBytesV1 {
		return nil, errors.Join(ErrRecordTooLarge, err)
	}
	b := binary.LittleEndian.AppendUint16(nil, 2)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(base)))
	b = append(b, base...)
	return append(b, raw...), nil
}

func decodeColocatedVectorMutationPayloadV2(raw []byte) ([]byte, *ColocatedVectorMutationWALV1, error) {
	if len(raw) < 2 || binary.LittleEndian.Uint16(raw) != 2 {
		return raw, nil, nil
	}
	if len(raw) < 6 || len(raw) > ColocatedVectorMutationMaxPayloadBytesV1 {
		return nil, nil, ErrCorrupt
	}
	n := uint64(binary.LittleEndian.Uint32(raw[2:6]))
	if n > uint64(len(raw)-6) || n < 2 || binary.LittleEndian.Uint16(raw[6:]) != 1 {
		return nil, nil, ErrCorrupt
	}
	var v ColocatedVectorMutationWALV1
	if err := decodeColocatedVectorJSONV1(raw[6+int(n):], &v, ColocatedVectorMutationMaxPayloadBytesV1); err != nil {
		return nil, nil, err
	}
	if err := v.ValidateV1(); err != nil {
		return nil, nil, err
	}
	return raw[6 : 6+int(n)], &v, nil
}

func decodeColocatedVectorJSONV1(raw []byte, value any, limit int) error {
	if len(raw) == 0 || len(raw) > limit {
		return ErrCorrupt
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return errors.Join(ErrCorrupt, err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return ErrCorrupt
	}
	return nil
}

func EncodeColocatedVectorReplacePayloadV2(v ColocatedVectorMutationWALV1) ([]byte, error) {
	if v.Delete {
		return nil, ErrCorrupt
	}
	base, err := EncodeCollectionUpdateBatchByIDPayload(v.Collection, []CollectionDocument{{ID: v.ID, Document: v.Document}})
	if err != nil {
		return nil, err
	}
	return encodeColocatedVectorMutationPayloadV2(base, v)
}

func EncodeColocatedVectorDeletePayloadV2(v ColocatedVectorMutationWALV1) ([]byte, error) {
	if !v.Delete {
		return nil, ErrCorrupt
	}
	base, err := EncodeCollectionDeleteBatchByIDPayload(v.Collection, [][]byte{v.ID})
	if err != nil {
		return nil, err
	}
	return encodeColocatedVectorMutationPayloadV2(base, v)
}
