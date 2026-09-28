package raftplacement

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func replicaReplacementAuthorityForTestV1(t *testing.T) (*CatalogMetaAuthorityV1, ReplicaReplacementBeginV1) {
	t.Helper()
	record, err := NewCatalogMetaRecordV1(1, validCatalog())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	authority := NewCatalogMetaAuthorityV1()
	if _, err := authority.applyCommittedCatalogMetaV1(raw, 1); err != nil {
		t.Fatal(err)
	}
	group := record.Catalog.Groups[0]
	return authority, ReplicaReplacementBeginV1{Format: 1, Kind: ReplicaReplacementBeginKindV1, OperationID: "replace-one", ConfigDigest: strings.Repeat("a", 64), ExpectedEpoch: 1, CatalogDigest: record.Digest, GroupID: group.ID, OldNodeID: group.Members[0], NewPeer: raftcluster.Peer{ID: "spare", Address: "127.0.0.1:19001"}}
}

func TestCatalogReplicaReplacementBeginSnapshotAndExactRetryV1(t *testing.T) {
	authority, command := replicaReplacementAuthorityForTestV1(t)
	before, err := authority.ExportCatalogMetaSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		t.Fatal(err)
	}
	status, err := authority.applyCommittedCatalogMetaV1(raw, 2)
	if err != nil {
		t.Fatal(err)
	}
	if status.Epoch != 1 || status.AppliedIndex != 2 {
		t.Fatalf("status=%+v", status)
	}
	retry, err := authority.applyCommittedCatalogMetaV1(raw, 3)
	if err != nil || !reflect.DeepEqual(retry, status) {
		t.Fatalf("retry=%+v err=%v", retry, err)
	}
	after, err := authority.ExportCatalogMetaSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before.Record, after.Record) || !bytes.Equal(before.LastCommand, after.LastCommand) {
		t.Fatal("BEGIN changed catalog authority")
	}
	encoded, err := authority.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewCatalogMetaAuthorityV1()
	if err := restored.installCatalogMetaSnapshotBytesV1(encoded); err != nil {
		t.Fatal(err)
	}
	restoredBytes, err := restored.ExportCatalogMetaSnapshotBytesV1()
	if err != nil || !bytes.Equal(encoded, restoredBytes) {
		t.Fatalf("restore bytes mismatch: %v", err)
	}
	values, err := restored.ReplicaReplacementBeginsV1()
	if err != nil || len(values) != 1 || values[0].OperationID != command.OperationID {
		t.Fatalf("values=%+v err=%v", values, err)
	}
	values[0].OperationID = "changed-copy"
	if _, err := restored.applyCommittedCatalogMetaV1(raw, 4); err != nil {
		t.Fatal(err)
	}
	command.NewPeer.Address = "127.0.0.1:19002"
	conflicting, err := EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restored.applyCommittedCatalogMetaV1(conflicting, 5); !errors.Is(err, ErrCatalogMetaConflict) {
		t.Fatalf("conflicting retry=%v", err)
	}
	var record CatalogMetaRecordV1
	if err := json.Unmarshal(after.Record, &record); err != nil {
		t.Fatal(err)
	}
	next, err := NewCatalogMetaRecordV1(2, record.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	nextRaw, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: 1, Record: next})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restored.applyCommittedCatalogMetaV1(nextRaw, 6); !errors.Is(err, ErrCatalogMetaConflict) {
		t.Fatalf("pending replacement allowed catalog mutation: %v", err)
	}
}

func TestCatalogReplicaReplacementRejectsUnboundInputsV1(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*ReplicaReplacementBeginV1)
	}{
		{"stale", func(c *ReplicaReplacementBeginV1) { c.ExpectedEpoch++ }},
		{"wrong-digest", func(c *ReplicaReplacementBeginV1) { c.CatalogDigest = strings.Repeat("b", 64) }},
		{"unknown-group", func(c *ReplicaReplacementBeginV1) { c.GroupID = "missing" }},
		{"unknown-old", func(c *ReplicaReplacementBeginV1) { c.OldNodeID = "missing" }},
		{"existing-target", func(c *ReplicaReplacementBeginV1) { c.NewPeer.ID = c.OldNodeID }},
		{"bad-address", func(c *ReplicaReplacementBeginV1) { c.NewPeer.Address = "not-a-port" }},
		{"unknown-kind", func(c *ReplicaReplacementBeginV1) { c.Kind = "replica-replacement-ready-v1" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			authority, command := replicaReplacementAuthorityForTestV1(t)
			tc.change(&command)
			raw, err := json.Marshal(command)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := authority.applyCommittedCatalogMetaV1(raw, 2); err == nil {
				t.Fatal("invalid BEGIN accepted")
			}
			values, err := authority.ReplicaReplacementBeginsV1()
			if err != nil || len(values) != 0 {
				t.Fatalf("refusal mutated state: %v %v", values, err)
			}
		})
	}
	authority, command := replicaReplacementAuthorityForTestV1(t)
	raw, _ := EncodeReplicaReplacementBeginV1(command)
	for _, malformed := range [][]byte{append([]byte(" "), raw...), append(bytes.Clone(raw), raw...), bytes.Replace(raw, []byte(`"format":1`), []byte(`"format":1,"format":1`), 1)} {
		if _, err := authority.applyCommittedCatalogMetaV1(malformed, 2); err == nil {
			t.Fatal("noncanonical BEGIN accepted")
		}
	}
	// Feature refusal is independent of whether any generation is currently
	// active. P3 runtime assembly must qualify that profile before activation.
	authority.record.Catalog.Features.Required = append(authority.record.Catalog.Features.Required, raftcluster.RequiredFeature{Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.Version{Major: 1}})
	if _, err := authority.applyCommittedCatalogMetaV1(raw, 2); !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("lifecycle refusal=%v", err)
	}
}

func TestCatalogReplicaReplacementSnapshotRefusesDuplicatesAndOmissionsV1(t *testing.T) {
	authority, command := replicaReplacementAuthorityForTestV1(t)
	raw, _ := EncodeReplicaReplacementBeginV1(command)
	if _, err := authority.applyCommittedCatalogMetaV1(raw, 2); err != nil {
		t.Fatal(err)
	}
	snapshot, err := authority.ExportCatalogMetaSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	duplicate := append([]byte{'['}, raw...)
	duplicate = append(duplicate, ',')
	duplicate = append(duplicate, raw...)
	duplicate = append(duplicate, ']')
	snapshot.ReplicaReplacements = duplicate
	if _, err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotV1(snapshot); err == nil {
		t.Fatal("duplicate operation accepted")
	}
	snapshot.ReplicaReplacements = nil
	if _, err := authority.installCatalogMetaSnapshotV1(snapshot); !errors.Is(err, ErrCatalogMetaConflict) {
		t.Fatalf("same-index omission=%v", err)
	}
	snapshot.AppliedIndex++
	if _, err := authority.installCatalogMetaSnapshotV1(snapshot); !errors.Is(err, ErrCatalogMetaConflict) {
		t.Fatalf("newer-index omission=%v", err)
	}
}
