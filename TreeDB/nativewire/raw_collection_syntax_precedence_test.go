package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

// Build legacy metadata directly: supported encoders deliberately cannot emit
// the retired strategy, and client normalization must not mask ingress bugs.
func rawCollectionPrecedenceFixture(t *testing.T, version uint64, malformed string, strategy uint64) []byte {
	t.Helper()
	meta := collections.CollectionMeta{Name: "raw_precedence"}
	if malformed == "scalar_collision" {
		meta.Indexes = []collections.IndexDefinition{{Name: "embedding", Field: "tag", ValueType: collections.IndexValueInt64}}
	}
	raw, err := encodeCollectionMeta(meta)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] = byte(version)
	raw = raw[:len(raw)-1] // Replace the empty vector count.
	count := uint64(1)
	switch malformed {
	case "numeric", "dimensions", "name", "field", "duplicate", "unknown_strategy":
		count++
	}
	raw = binary.AppendUvarint(raw, count)
	def := collections.VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: collections.VectorMetricCosine, Encoding: collections.VectorIndexEncodingFloat32}
	if malformed == "modern_l2" {
		def.Metric = collections.VectorMetricL2
	}
	if malformed == "modern_int8" {
		def.Encoding = collections.VectorIndexEncodingInt8
	}
	raw = appendVectorIndexDefinitionFields(raw, def)
	raw = binary.AppendUvarint(raw, strategy)
	qcount := uint64(0)
	if malformed == "quantized" || malformed == "invalid_bool" {
		qcount = 1
	}
	raw = binary.AppendUvarint(raw, qcount)
	if qcount != 0 {
		qname := "quantized"
		if malformed == "quantized" {
			qname = "" // Quantized names are required.
		}
		raw = appendString(raw, qname)
		raw = appendString(raw, "scalar_u8")
		raw = binary.AppendUvarint(raw, 1)
		if version >= 4 {
			if malformed == "invalid_bool" {
				raw = append(raw, 2)
			} else {
				raw = appendBool(raw, false)
			}
		}
	}
	if count == 2 {
		def.Name, def.Field = "later", "other"
		switch malformed {
		case "numeric":
			def.M = -1
		case "dimensions":
			def.Dimensions = 0
		case "name":
			def.Name = "bad/name"
		case "field":
			def.Field = "bad..path"
		case "duplicate":
			def.Name = "embedding"
		}
		raw = appendVectorIndexDefinitionFields(raw, def)
		laterStrategy := uint64(2)
		if malformed == "unknown_strategy" {
			laterStrategy = 99
		}
		raw = binary.AppendUvarint(raw, laterStrategy)
		raw = binary.AppendUvarint(raw, 0)
	}
	if version >= 6 {
		schema, err := collections.EncodeColumnStoreWireConfigV1("raw_precedence", &collections.ColumnStoreConfig{
			Enabled: true,
			Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2}},
		}, true)
		if err != nil {
			t.Fatal(err)
		}
		if malformed == "column" {
			schema = []byte(`{"enabled":true}`)
		}
		raw = appendString(raw, string(schema))
	}
	switch malformed {
	case "truncated":
		raw = raw[:len(raw)-1]
	case "trailing":
		raw = append(raw, 0)
	}
	return raw
}

func TestRawCreateCollectionWholeRequestSyntaxBeforeCapability(t *testing.T) {
	for _, version := range []uint64{3, 4, 5, 6} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			client, mgr, db := serveCommandWALCollectionPipe(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := client.Hello(ctx); err != nil {
				t.Fatal(err)
			}
			cases := []string{"numeric", "dimensions", "name", "field", "duplicate", "scalar_collision", "quantized", "unknown_strategy", "truncated", "trailing", ""}
			if version >= 4 {
				cases = append(cases, "invalid_bool")
			}
			if version == 6 {
				cases = append(cases, "column")
			}
			for _, malformed := range cases {
				t.Run("retired_"+malformed, func(t *testing.T) {
					raw := rawCollectionPrecedenceFixture(t, version, malformed, 1)
					want := iwire.ErrInvalidCommand
					switch malformed {
					case "":
						want = iwire.ErrUnsupportedFeature
					case "truncated", "trailing", "invalid_bool":
						want = iwire.ErrMalformedFrame
					}
					sections := []iwire.Section{
						{ID: iwire.SectionIdempotencyKey, Bytes: []byte("raw-precedence")},
						{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, clientCatalogVersion(t, client, ctx))},
						{ID: iwire.SectionCollectionMeta, Bytes: raw},
					}
					lsn, catalogVersion := db.CommandWALNextLSN(), clientCatalogVersion(t, client, ctx)
					response, err := client.commandSections(ctx, iwire.CommandCreateCollection, sections...)
					if !isRemoteError(err, want) {
						t.Errorf("raw ingress: code=%d want=%d err=%v", nativeCodeOf(err), want, err)
					}
					if len(response) != 0 || db.CommandWALNextLSN() != lsn || clientCatalogVersion(t, client, ctx) != catalogVersion {
						t.Error("rejected create changed response/WAL/catalog")
					}
					if _, err := mgr.OpenCollection("raw_precedence"); !errors.Is(err, collections.ErrCollectionNotFound) {
						t.Errorf("rejected create published collection: %v", err)
					}
					// Response/persisted metadata must still reject tag 1 immediately.
					if _, err := decodeCollectionMeta(raw); nativeCodeOf(err) != iwire.ErrUnsupportedFeature {
						t.Errorf("strict response decode: %v", err)
					}

					sections = append([]iwire.Section{{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandCreateCollection, Version: 1})}}, sections...)
					cmd, err := iwire.MustV1Registry().ValidateRequestSections(sections)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := iwire.AppendDeterministicEntry(nil, cmd); nativeCodeOf(err) != want {
						t.Errorf("deterministic append: code=%d want=%d err=%v", nativeCodeOf(err), want, err)
					}
					entry := append([]byte(nil), iwire.DeterministicEntryMagic...)
					for _, value := range []uint64{1, uint64(iwire.CommandCreateCollection), 1, 0, uint64(len(sections) - 1)} {
						entry = binary.AppendUvarint(entry, value)
					}
					for _, section := range sections[1:] {
						entry = binary.AppendUvarint(entry, uint64(section.ID))
						entry = binary.AppendUvarint(entry, uint64(len(section.Bytes)))
						entry = append(entry, section.Bytes...)
					}
					if _, err := iwire.DecodeDeterministicEntry(entry, iwire.Limits{}); nativeCodeOf(err) != want {
						t.Errorf("deterministic decode: code=%d want=%d err=%v", nativeCodeOf(err), want, err)
					}
				})
			}
			raw := rawCollectionPrecedenceFixture(t, version, "", 2)
			if _, err := client.commandSections(ctx, iwire.CommandCreateCollection,
				iwire.Section{ID: iwire.SectionIdempotencyKey, Bytes: []byte("modern-precedence")},
				iwire.Section{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, clientCatalogVersion(t, client, ctx))},
				iwire.Section{ID: iwire.SectionCollectionMeta, Bytes: raw}); err != nil {
				t.Fatalf("modern raw create: %v", err)
			}
			if _, err := mgr.OpenCollection("raw_precedence"); err != nil {
				t.Fatalf("modern collection missing: %v", err)
			}
		})
	}
}

// The targeted retired-tag fix must preserve validation of modern-only legacy
// deterministic entries, including capabilities that later admission refuses.
func TestLegacyModernDeterministicCollectionMetaValidation(t *testing.T) {
	for _, version := range []uint64{3, 4, 5, 6} {
		for _, capability := range []string{"", "modern_l2", "modern_int8"} {
			t.Run(fmt.Sprintf("v%d/%s", version, capability), func(t *testing.T) {
				raw := rawCollectionPrecedenceFixture(t, version, capability, 2)
				sections := []iwire.Section{
					{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandCreateCollection, Version: 1})},
					{ID: iwire.SectionIdempotencyKey, Bytes: []byte("modern-contract")},
					{ID: iwire.SectionExpectedCatalogVersion, Bytes: []byte{0}},
					{ID: iwire.SectionCollectionMeta, Bytes: raw},
				}
				cmd, err := iwire.MustV1Registry().ValidateRequestSections(sections)
				if err != nil {
					t.Fatal(err)
				}
				entry, err := iwire.AppendDeterministicEntry(nil, cmd)
				if err != nil {
					t.Fatalf("modern append: %v", err)
				}
				decoded, err := iwire.DecodeDeterministicEntry(entry, iwire.Limits{})
				if err != nil {
					t.Fatalf("modern decode: %v", err)
				}
				for _, section := range decoded.Sections {
					if section.ID == iwire.SectionCollectionMeta && !bytes.Equal(section.Bytes, raw) {
						t.Fatal("deterministic validation rewrote metadata")
					}
				}
			})
		}
	}
}
