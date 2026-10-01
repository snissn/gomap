package commitlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
)

// The first split-source checkpoint is deliberately one exact insert, one
// canonical source and one separate mutable ANN group. Bounds apply before
// decoding or retaining the command; this is not a general mutation queue.
const (
	SplitVectorInsertMaxPayloadBytesV1  = 128 << 10
	SplitVectorInsertMaxDocumentBytesV1 = 64 << 10
	SplitVectorInsertMaxDimensionsV1    = 1024
	SplitVectorInsertMaxIdentityBytesV1 = 1024
	SplitVectorInsertMaxAttemptsV1      = 64
)

type SplitVectorInsertV1 struct {
	Version        uint32
	Operation      string
	Collection     string
	Index          string
	Generation     uint64
	SourceGroup    string
	TargetGroup    string
	CatalogEpoch   uint64
	CatalogDigest  string
	ReadySetDigest string
	ModelDigest    string
	PartitionID    uint32
	Attempt        []byte
	ID             []byte
	Vector         []float32
	Document       []byte
	DocumentDigest string
	SourceTerm     uint64
	SourceIndex    uint64
	TargetTerm     uint64
	TargetIndex    uint64
	LiveRevision   uint64
}

// DigestV1 identifies the immutable semantic insert, excluding transport phase
// and the consensus positions assigned only after their respective commits.
func (v SplitVectorInsertV1) DigestV1() ([sha256.Size]byte, error) {
	if err := v.ValidateV1(); err != nil {
		return [sha256.Size]byte{}, err
	}
	v.Operation = "semantic-insert"
	v.Document = nil
	v.SourceTerm, v.SourceIndex = 0, 0
	v.TargetTerm, v.TargetIndex, v.LiveRevision = 0, 0, 0
	raw, err := json.Marshal(v)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(raw), nil
}

func (v SplitVectorInsertV1) ValidateV1() error {
	bounded := func(s string) bool { return len(s) > 0 && len(s) <= SplitVectorInsertMaxIdentityBytesV1 }
	if v.Version != 1 || !bounded(v.Collection) || !bounded(v.Index) ||
		!bounded(v.SourceGroup) || !bounded(v.TargetGroup) || v.SourceGroup == v.TargetGroup ||
		v.Generation == 0 || v.CatalogEpoch == 0 ||
		len(v.Attempt) == 0 || len(v.Attempt) > SplitVectorInsertMaxIdentityBytesV1 ||
		len(v.ID) == 0 || len(v.ID) > SplitVectorInsertMaxIdentityBytesV1 ||
		len(v.Vector) == 0 || len(v.Vector) > SplitVectorInsertMaxDimensionsV1 {
		return errors.New("commitlog: split vector insert identity or size is invalid")
	}
	for _, d := range []string{v.CatalogDigest, v.ReadySetDigest, v.ModelDigest, v.DocumentDigest} {
		decoded, err := hex.DecodeString(d)
		if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != d {
			return errors.New("commitlog: split vector insert digest is invalid")
		}
	}
	for _, f := range v.Vector {
		if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
			return errors.New("commitlog: split vector insert has non-finite vector")
		}
	}
	switch v.Operation {
	case "source":
		docDigest := sha256.Sum256(v.Document)
		if len(v.Document) == 0 || len(v.Document) > SplitVectorInsertMaxDocumentBytesV1 || hex.EncodeToString(docDigest[:]) != v.DocumentDigest {
			return errors.New("commitlog: split source document digest or size is invalid")
		}
		if (v.SourceTerm == 0) != (v.SourceIndex == 0) || v.TargetTerm != 0 || v.TargetIndex != 0 || v.LiveRevision != 0 {
			return errors.New("commitlog: split source insert has invalid commit positions")
		}
	case "project":
		if len(v.Document) != 0 {
			return errors.New("commitlog: split projection must not contain canonical document bytes")
		}
		if v.SourceTerm == 0 || v.SourceIndex == 0 || (v.TargetTerm == 0) != (v.TargetIndex == 0) || v.LiveRevision != 0 {
			return errors.New("commitlog: split projection lacks original source commit")
		}
	case "clear":
		if len(v.Document) != 0 {
			return errors.New("commitlog: split completion must not contain canonical document bytes")
		}
		if v.SourceTerm == 0 || v.SourceIndex == 0 || v.TargetTerm == 0 || v.TargetIndex == 0 || v.LiveRevision == 0 {
			return errors.New("commitlog: split completion lacks target receipt")
		}
	default:
		return errors.New("commitlog: unsupported split vector insert operation")
	}
	return nil
}

func EncodeSplitVectorInsertPayloadV1(v SplitVectorInsertV1) ([]byte, error) {
	if err := v.ValidateV1(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(raw) > SplitVectorInsertMaxPayloadBytesV1 {
		return nil, errors.New("commitlog: split vector insert payload is too large")
	}
	return raw, nil
}

func DecodeSplitVectorInsertPayloadV1(raw []byte) (SplitVectorInsertV1, error) {
	var v SplitVectorInsertV1
	if len(raw) == 0 || len(raw) > SplitVectorInsertMaxPayloadBytesV1 {
		return v, errors.New("commitlog: split vector insert payload is too large or empty")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		return SplitVectorInsertV1{}, err
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return SplitVectorInsertV1{}, errors.New("commitlog: split vector insert trailing data")
	}
	if err := v.ValidateV1(); err != nil {
		return SplitVectorInsertV1{}, err
	}
	canonical, err := EncodeSplitVectorInsertPayloadV1(v)
	if err != nil || !bytes.Equal(raw, canonical) {
		return SplitVectorInsertV1{}, errors.New("commitlog: split vector insert payload is not canonical")
	}
	return v, nil
}
