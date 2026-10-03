package collections

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func TestColumnStoreWireConfigV1DurableResponseExceedsCreateBounds(t *testing.T) {
	for _, arm := range []string{"columns", "bytes"} {
		t.Run(arm, func(t *testing.T) {
			cfg := &ColumnStoreConfig{Enabled: true}
			if arm == "columns" {
				for i := 0; i <= ColumnStoreWireMaxColumnsV1; i++ {
					name := fmt.Sprintf("value_%d", i)
					cfg.Columns = append(cfg.Columns, ColumnStoreColumn{Name: name, Path: name, ValueType: ColumnStoreValueString})
				}
			} else {
				cfg.Columns = []ColumnStoreColumn{{Name: "value", Path: strings.Repeat("x", ColumnStoreWireMaxBytesV1), ValueType: ColumnStoreValueString}}
			}
			raw, err := EncodeColumnStoreWireConfigV1("docs", cfg, false)
			if err != nil {
				t.Fatalf("encode durable response: %v", err)
			}
			if arm == "bytes" && len(raw) <= ColumnStoreWireMaxBytesV1 {
				t.Fatal("fixture does not exceed the create byte limit")
			}
			decoded, err := DecodeColumnStoreWireConfigV1("docs", raw, false)
			if err != nil {
				t.Fatalf("decode durable response: %v", err)
			}
			again, err := EncodeColumnStoreWireConfigV1("docs", decoded, false)
			if err != nil || !bytes.Equal(raw, again) {
				t.Fatalf("durable response did not round-trip: err=%v", err)
			}
			if len(decoded.Columns) != len(cfg.Columns) {
				t.Fatal("durable response changed column count")
			}
			for i := range cfg.Columns {
				if decoded.Columns[i].Name != cfg.Columns[i].Name || decoded.Columns[i].Path != cfg.Columns[i].Path || decoded.Columns[i].ValueType != cfg.Columns[i].ValueType {
					t.Fatal("durable response changed column semantics")
				}
			}
			if _, err := EncodeColumnStoreWireConfigV1("docs", cfg, true); err == nil {
				t.Fatal("create encoder accepted an oversized schema")
			}
			if _, err := DecodeColumnStoreWireConfigV1("docs", raw, true); err == nil {
				t.Fatal("create decoder accepted an oversized schema")
			}
			for _, invalid := range [][]byte{
				append(append([]byte(nil), raw...), []byte(" {}")...),
				append([]byte("{\"unknown\":true,"), raw[1:]...),
			} {
				if _, err := DecodeColumnStoreWireConfigV1("docs", invalid, false); err == nil {
					t.Fatal("response decoder accepted malformed metadata")
				}
			}
		})
	}
}
