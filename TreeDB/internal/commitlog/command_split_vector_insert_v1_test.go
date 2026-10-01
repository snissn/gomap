package commitlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestSplitVectorInsertSemanticEnvelopeV1(t *testing.T) {
	document := []byte(`{"embedding":[0,1],"canonical":"source-only"}`)
	documentDigest := sha256.Sum256(document)
	v := SplitVectorInsertV1{Version: 1, Operation: "source", Collection: "docs", Index: "embedding", Generation: 1, SourceGroup: "source", TargetGroup: "ann", CatalogEpoch: 1,
		CatalogDigest: strings.Repeat("a", 64), ReadySetDigest: strings.Repeat("b", 64), ModelDigest: strings.Repeat("c", 64), DocumentDigest: hex.EncodeToString(documentDigest[:]),
		Attempt: []byte("operation"), ID: []byte("document"), Vector: []float32{0, 1}, Document: document}
	semantic, err := v.DigestV1()
	if err != nil {
		t.Fatal(err)
	}
	source, err := EncodeSplitVectorInsertPayloadV1(v)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeSplitVectorInsertPayloadV1(source)
	if err != nil {
		t.Fatal(err)
	}
	source[0] = 'x'
	if !bytes.Equal(decoded.Document, document) {
		t.Fatal("decode retained caller input")
	}
	project := v
	project.Operation = "project"
	project.Document = nil
	project.SourceTerm = 3
	project.SourceIndex = 9
	raw, err := EncodeSplitVectorInsertPayloadV1(project)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, document) || len(project.Document) != 0 {
		t.Fatal("projection retained canonical document")
	}
	got, err := project.DigestV1()
	if err != nil || got != semantic {
		t.Fatalf("phase changed semantic identity %x %v", got, err)
	}
	complete := project
	complete.Operation = "clear"
	complete.TargetTerm = 4
	complete.TargetIndex = 12
	complete.LiveRevision = 2
	got, err = complete.DigestV1()
	if err != nil || got != semantic {
		t.Fatalf("receipt changed semantic identity %x %v", got, err)
	}
	changed := complete
	changed.Vector = []float32{1, 0}
	got, err = changed.DigestV1()
	if err != nil || got == semantic {
		t.Fatal("changed projection vector lost semantic conflict")
	}
	project.Document = document
	if _, err := EncodeSplitVectorInsertPayloadV1(project); err == nil {
		t.Fatal("projection with canonical bytes accepted")
	}
	if _, err := DecodeSplitVectorInsertPayloadV1(make([]byte, SplitVectorInsertMaxPayloadBytesV1+1)); err == nil {
		t.Fatal("oversized envelope accepted")
	}
	if _, err := DecodeSplitVectorInsertPayloadV1(append(raw, ' ')); err == nil {
		t.Fatal("noncanonical envelope accepted")
	}
}
