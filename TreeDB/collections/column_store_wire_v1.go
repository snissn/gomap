package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// Limits of the bounded physical-schema nativewire checkpoint.
const ColumnStoreWireMaxBytesV1 = 16 << 10
const ColumnStoreWireMaxColumnsV1 = 32

// DecodeColumnStoreWireConfigV1 shares normalization across metadata readers,
// deterministic create validation and actual Raft create lowering. Responses
// preserve durable state; create requests may only supply schema.
func DecodeColumnStoreWireConfigV1(collection string, raw []byte, create bool) (*ColumnStoreConfig, error) {
	if len(raw) == 0 || len(raw) > ColumnStoreWireMaxBytesV1 {
		return nil, errors.New("collections: column_store wire schema exceeds bounded byte limit")
	}
	var cfg *ColumnStoreConfig
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, errors.New("collections: column_store wire schema has trailing JSON")
	}
	if cfg == nil || !cfg.Enabled || len(cfg.Columns) == 0 || len(cfg.Columns) > ColumnStoreWireMaxColumnsV1 {
		return nil, errors.New("collections: column_store wire schema requires 1..32 enabled columns")
	}
	if create && (cfg.ActiveManifest != nil || cfg.RecoveryAuthoritativeManifest != nil || cfg.RecoveryAuthoritativeAppliedCommandLSN != 0 || cfg.PhysicalMutationParts != 0) {
		return nil, errors.New("collections: column_store create cannot supply durable physical authority")
	}
	normalized, err := normalizeColumnStoreConfig(collection, cfg)
	if err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	if len(canonical) > ColumnStoreWireMaxBytesV1 {
		return nil, errors.New("collections: normalized column_store wire schema exceeds bounded byte limit")
	}
	return normalized, nil
}

// EncodeColumnStoreWireConfigV1 reports normalization and bounds errors through
// the existing metadata error path. It does not drop configuration on failure.
func EncodeColumnStoreWireConfigV1(collection string, cfg *ColumnStoreConfig, create bool) ([]byte, error) {
	raw, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	normalized, err := DecodeColumnStoreWireConfigV1(collection, raw, create)
	if err != nil {
		return nil, err
	}
	raw, err = json.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	if len(raw) > ColumnStoreWireMaxBytesV1 {
		return nil, errors.New("collections: normalized column_store wire schema exceeds bounded byte limit")
	}
	return raw, nil
}
