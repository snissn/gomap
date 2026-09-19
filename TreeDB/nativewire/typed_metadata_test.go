package nativewire

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

const typedMetadataUpdateGolden = `{"expected_generation":7,"ids":["a","missing"],"index":"docs","set":{"meta.acl":"new","meta.rank":2},"unset":["meta.old"]}`

func TestTypedMetadataUpdateGoldenBoundsAndRegistry(t *testing.T) {
	limits := iwire.DefaultLimits()
	limits.MaxSectionLen = uint64(len(typedMetadataUpdateGolden))
	req, err := decodeTypedMetadataUpdate([]byte(typedMetadataUpdateGolden), limits)
	if err != nil || req.Index != "docs" || req.ExpectedGeneration != 7 || len(req.IDs) != 2 || req.Set["meta.acl"] != "new" || req.Set["meta.rank"] != json.Number("2") {
		t.Fatalf("decoded=%+v err=%v", req, err)
	}
	for i := 0; i < len(typedMetadataUpdateGolden); i++ {
		if _, err := decodeTypedMetadataUpdate([]byte(typedMetadataUpdateGolden[:i]), limits); err == nil {
			t.Fatalf("accepted prefix %d", i)
		}
	}
	for _, raw := range [][]byte{
		append([]byte(typedMetadataUpdateGolden), 0),
		[]byte(`{"expected_generation":7,"ids":["a"],"index":"docs","set":{}}`),
		[]byte(`{"expected_generation":7,"ids":["a"],"index":"docs","set":{},"unset":[],"unknown":true}`),
		[]byte(`{"expected_generation":7,"ids":["a"],"index":"docs","set":{},"unset":[]} {}`),
	} {
		malformedLimits := iwire.DefaultLimits()
		malformedLimits.MaxSectionLen = uint64(len(raw))
		if _, err := decodeTypedMetadataUpdate(raw, malformedLimits); err == nil {
			t.Fatalf("accepted malformed request %q", raw)
		}
	}
	limits.MaxSectionLen--
	if _, err := decodeTypedMetadataUpdate([]byte(typedMetadataUpdateGolden), limits); err == nil {
		t.Fatal("accepted request over bound")
	}

	schema, ok := iwire.MustV1Registry().LookupCommand(iwire.CommandTypedMetadataUpdate, 1)
	if !ok || schema.Name != "typed_metadata_update" || schema.Kind != iwire.CommandKindMutation || !schema.LocalOnly || schema.Replicated {
		t.Fatalf("missing local typed metadata mutation: %+v", schema)
	}
	if _, err := iwire.AppendDeterministicEntry(nil, iwire.ValidatedCommand{Schema: schema}); err == nil {
		t.Fatal("typed metadata mutation was accepted as deterministic entry")
	}
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandTypedMetadataUpdate, Version: 1})},
		{ID: iwire.SectionDeadline},
		{ID: iwire.SectionTypedMetadataUpdateRequest, Bytes: []byte(typedMetadataUpdateGolden)},
	}
	registry := iwire.MustV1Registry()
	if _, err := registry.ValidateRequestSections(sections); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(sections); i++ {
		missing := append(append([]iwire.Section(nil), sections[:i]...), sections[i+1:]...)
		if _, err := registry.ValidateRequestSections(missing); err == nil {
			t.Fatalf("missing section %d accepted", i)
		}
	}
}

func TestTypedMetadataUpdateCapabilityIsConfigurationBound(t *testing.T) {
	server := NewServer(ServerOptions{})
	defer server.Close()
	if _, err := server.handleTypedMetadataUpdate(context.Background(), nil); err == nil {
		t.Fatal("unconfigured mutation accepted")
	}
	var output bytes.Buffer
	if err := server.writeHelloOK(&output, iwire.Header{}, nil); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(output.Bytes(), []byte("typed_metadata_update_versions")) {
		t.Fatal("unconfigured capability advertised")
	}
	configured := NewServer(ServerOptions{DocumentService: &documentservice.Service{}})
	defer configured.Close()
	output.Reset()
	if err := configured.writeHelloOK(&output, iwire.Header{}, nil); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(output.Bytes(), []byte("typed_metadata_update_versions")) {
		t.Fatal("configured capability missing")
	}
}
