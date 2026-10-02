package commitlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
)

// VectorPrepareV1 accepts only a bounded first, single-group partition.
// Applying FSMs assign Term/Index; source rebuild and prepare are distinct entries.
type VectorPrepareV1 struct {
	Version                                                            uint32
	Operation, Collection, Index, Group, IndexDefinitionDigest         string
	Generation, MaxSourceRows                                          uint64
	SourceGeneration, SourceChecksum, SourceSchemaHash, SourceRowCount uint64
	Term, IndexPosition                                                uint64
	CommandDigest                                                      string
}

const VectorPrepareMaxPayloadBytesV1 = 8192

func (v VectorPrepareV1) ValidateV1() error {
	bounded := func(s string) bool { return len(s) > 0 && len(s) <= 1024 }
	if v.Version != 1 || !bounded(v.Collection) || !bounded(v.Index) || !bounded(v.Group) || v.Generation == 0 || v.MaxSourceRows == 0 || v.MaxSourceRows > 512 {
		return errors.New("commitlog: invalid bounded vector prepare identity")
	}
	digest, err := hex.DecodeString(v.IndexDefinitionDigest)
	if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != v.IndexDefinitionDigest {
		return errors.New("commitlog: invalid vector prepare index digest")
	}
	if (v.Term == 0) != (v.IndexPosition == 0) || (v.Term == 0) != (v.CommandDigest == "") {
		return errors.New("commitlog: vector prepare positions must belong to one applying FSM")
	}
	if v.CommandDigest != "" {
		digest, err = hex.DecodeString(v.CommandDigest)
		if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != v.CommandDigest {
			return errors.New("commitlog: invalid vector prepare command digest")
		}
	}
	switch v.Operation {
	case "rebuild":
		if v.SourceGeneration != 0 || v.SourceChecksum != 0 || v.SourceSchemaHash != 0 || v.SourceRowCount != 0 {
			return errors.New("commitlog: rebuild cannot claim a prepared source")
		}
	case "prepare":
		if v.SourceGeneration == 0 || v.SourceChecksum == 0 || v.SourceSchemaHash == 0 || v.SourceRowCount == 0 || v.SourceRowCount > v.MaxSourceRows {
			return errors.New("commitlog: invalid vector prepare frozen source")
		}
	default:
		return errors.New("commitlog: unsupported vector prepare operation")
	}
	return nil
}

func EncodeVectorPreparePayloadV1(v VectorPrepareV1) ([]byte, error) {
	if err := v.ValidateV1(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(v)
	if len(raw) > VectorPrepareMaxPayloadBytesV1 {
		return nil, errors.New("commitlog: vector prepare payload exceeds cap")
	}
	return raw, err
}
func DecodeVectorPreparePayloadV1(raw []byte) (VectorPrepareV1, error) {
	var v VectorPrepareV1
	if len(raw) == 0 || len(raw) > VectorPrepareMaxPayloadBytesV1 {
		return v, errors.New("commitlog: vector prepare payload size")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		return v, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return v, errors.New("commitlog: vector prepare trailing data")
	}
	canonical, err := EncodeVectorPreparePayloadV1(v)
	if err != nil {
		return v, err
	}
	if !bytes.Equal(raw, canonical) {
		return v, errors.New("commitlog: noncanonical vector prepare")
	}
	return v, nil
}
