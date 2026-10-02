package collections

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestColumnStoreWireConfigV1StrictBoundedCreate(t *testing.T) {
	valid := []byte(`{"enabled":true,"columns":[{"name":"embedding","path":"embedding","owner":"typed_column_part","value_type":"float32_vector","vector_dims":2}]}`)
	cfg, err := DecodeColumnStoreWireConfigV1("docs", valid, true)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AssetManager == nil || cfg.AssetManager.Kind != ColumnAssetManagerValueLogShaped || !cfg.AssetManager.IsolatedNamespace || cfg.SchemaHash == 0 {
		t.Fatalf("defaults absent: %+v", cfg)
	}
	for _, raw := range [][]byte{
		[]byte("null"), []byte("{}"), append(append([]byte(nil), valid...), []byte("{}")...),
		[]byte(strings.Replace(string(valid), `"enabled":true`, `"enabled":true,"unknown":1`, 1)),
		[]byte(strings.Replace(string(valid), `"enabled":true`, `"enabled":true,"physical_mutation_parts":1`, 1)),
		[]byte(strings.Replace(string(valid), `"enabled":true`, `"enabled":true,"recovery_authoritative_applied_command_lsn":1`, 1)),
		[]byte(strings.Replace(string(valid), `"enabled":true`, `"enabled":true,"active_manifest":{}`, 1)),
		[]byte(strings.Replace(string(valid), `"enabled":true`, `"enabled":true,"recovery_authoritative_manifest":{}`, 1)),
		[]byte(strings.Replace(string(valid), `"enabled":true`, `"enabled":true,"asset_manager":{"kind":"invalid-kind"}`, 1)),
		[]byte(strings.Replace(string(valid), `"enabled":true`, `"enabled":true,"asset_manager":{"namespace":"../escape"}`, 1)),
		[]byte(strings.Replace(string(valid), "typed_column_part", "invalid-owner", 1)),
		[]byte(strings.Replace(string(valid), "float32_vector", "invalid-type", 1)),
		[]byte(strings.Replace(string(valid), `"vector_dims":2`, `"vector_dims":-1`, 1)),
		bytes.Repeat([]byte(" "), ColumnStoreWireMaxBytesV1+1),
	} {
		if _, err := DecodeColumnStoreWireConfigV1("docs", raw, true); err == nil {
			t.Fatalf("invalid schema accepted: %.100s", raw)
		}
	}
	many := cfg.copy()
	many.Columns = make([]ColumnStoreColumn, ColumnStoreWireMaxColumnsV1+1)
	raw, err := json.Marshal(many)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeColumnStoreWireConfigV1("docs", raw, true); err == nil {
		t.Fatal("column bound accepted")
	}
}

func TestColumnStoreWireConfigV1PreservesRealResponseAuthority(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "x", vector: []float32{1, 0}}}
	_, db, c, _ := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, rows)
	defer db.Close()
	cfg := c.Meta().Options.ColumnStore
	raw, err := EncodeColumnStoreWireConfigV1(c.name, cfg, false)
	if err != nil {
		t.Fatal(err)
	}
	response, err := DecodeColumnStoreWireConfigV1(c.name, raw, false)
	if err != nil {
		t.Fatal(err)
	}
	if response.ActiveManifest == nil || response.RecoveryAuthoritativeManifest == nil || response.RecoveryAuthoritativeAppliedCommandLSN != cfg.RecoveryAuthoritativeAppliedCommandLSN || !columnManifestIdentityValueEqual(*response.ActiveManifest, *cfg.ActiveManifest) {
		t.Fatalf("durable response state lost: %+v", response)
	}
	if _, err := DecodeColumnStoreWireConfigV1(c.name, raw, true); err == nil {
		t.Fatal("response authority accepted in create")
	}
}
