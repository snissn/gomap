package raftplacement

import (
	"context"
	"errors"
	"testing"
)

func TestCatalogMetaCanonicalCommandAndExactRetry(t *testing.T) {
	record, err := NewCatalogMetaRecordV1(1, validCatalog())
	if err != nil {
		t.Fatalf("NewCatalogMetaRecordV1: %v", err)
	}
	command, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: 0, Record: record})
	if err != nil {
		t.Fatalf("EncodeCatalogMetaCommandV1: %v", err)
	}
	again, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: 0, Record: record})
	if err != nil {
		t.Fatalf("EncodeCatalogMetaCommandV1 again: %v", err)
	}
	if string(command) != string(again) {
		t.Fatalf("command bytes are not deterministic")
	}

	a := NewCatalogMetaAuthorityV1()
	first, err := a.ApplyCommittedCatalogMetaV1(command, 11)
	if err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if first.Epoch != 1 || first.AppliedIndex != 11 || first.Digest != record.Digest {
		t.Fatalf("first status=%+v", first)
	}
	retry, err := a.ApplyCommittedCatalogMetaV1(command, 12)
	if err != nil {
		t.Fatalf("exact retry: %v", err)
	}
	if retry.Epoch != 1 || retry.AppliedIndex != 11 {
		t.Fatalf("retry status=%+v", retry)
	}
}

func TestCatalogMetaRejectsStaleSkippedAndConflictingEpoch(t *testing.T) {
	a := NewCatalogMetaAuthorityV1()
	first := mustCatalogMetaCommand(t, 0, 1, validCatalog())
	if _, err := a.ApplyCommittedCatalogMetaV1(first, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ApplyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 0, 2, validCatalog()), 2); !errors.Is(err, ErrCatalogMetaStaleEpoch) {
		t.Fatalf("stale err=%v", err)
	}
	if _, err := a.ApplyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 1, 3, validCatalog()), 3); !errors.Is(err, ErrCatalogMetaSkippedEpoch) {
		t.Fatalf("skipped err=%v", err)
	}
	c := validCatalog()
	c.Groups[0].LeaderHint = ""
	if _, err := a.ApplyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 0, 1, c), 3); !errors.Is(err, ErrCatalogMetaConflict) {
		t.Fatalf("conflict err=%v", err)
	}
}

func TestCatalogMetaRouteAdmissionFailsClosed(t *testing.T) {
	a := NewCatalogMetaAuthorityV1()
	command := mustCatalogMetaCommand(t, 0, 1, validCatalog())
	if _, err := a.ApplyCommittedCatalogMetaV1(command, 7); err != nil {
		t.Fatal(err)
	}
	status, ok := a.Status()
	if !ok {
		t.Fatal("missing status")
	}
	request := RouteRequestV1{Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "users"}, Shape: RouteShapeCollectionV1}
	if _, err := a.Route(context.Background(), CatalogProofV1{}, request); !errors.Is(err, ErrCatalogMetaProofMissing) {
		t.Fatalf("missing proof err=%v", err)
	}
	if _, err := a.Route(context.Background(), CatalogProofV1{Epoch: 2, Digest: status.Digest}, request); !errors.Is(err, ErrCatalogMetaStaleEpoch) {
		t.Fatalf("stale proof err=%v", err)
	}
	if _, err := a.Route(context.Background(), CatalogProofV1{Epoch: 1, Digest: "00"}, request); !errors.Is(err, ErrCatalogMetaDigestMismatch) {
		t.Fatalf("digest proof err=%v", err)
	}
	decision, err := a.Route(context.Background(), CatalogProofV1{Epoch: 1, Digest: status.Digest}, request)
	if err != nil || decision.GroupID() != "group-a" {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
}

func TestCatalogMetaSnapshotInstallNeverMovesBackward(t *testing.T) {
	source := NewCatalogMetaAuthorityV1()
	if _, err := source.ApplyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 0, 1, validCatalog()), 9); err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.ExportCatalogMetaSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	target := NewCatalogMetaAuthorityV1()
	if _, err := target.InstallCatalogMetaSnapshotV1(snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := target.InstallCatalogMetaSnapshotV1(snapshot); err != nil {
		t.Fatalf("same snapshot: %v", err)
	}
	if _, err := target.ApplyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 1, 2, validCatalog()), 10); err != nil {
		t.Fatal(err)
	}
	if _, err := target.InstallCatalogMetaSnapshotV1(snapshot); !errors.Is(err, ErrCatalogMetaStaleEpoch) {
		t.Fatalf("rollback snapshot err=%v", err)
	}
}

func TestCatalogMetaDecodeRejectsUnknownAndOversized(t *testing.T) {
	record, err := NewCatalogMetaRecordV1(1, validCatalog())
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: 0, Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCatalogMetaCommandV1(append(append([]byte{}, b[:len(b)-1]...), []byte(`,"unknown":true}`)...)); !errors.Is(err, ErrInvalidCatalogMeta) {
		t.Fatalf("unknown err=%v", err)
	}
	if _, err := DecodeCatalogMetaCommandV1(make([]byte, MaxCatalogMetaCommandBytesV1+1)); !errors.Is(err, ErrCatalogMetaLimit) {
		t.Fatalf("limit err=%v", err)
	}
}

func mustCatalogMetaCommand(t testing.TB, expected, epoch uint64, catalog CatalogV1) []byte {
	t.Helper()
	record, err := NewCatalogMetaRecordV1(epoch, catalog)
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	b, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: expected, Record: record})
	if err != nil {
		t.Fatalf("command: %v", err)
	}
	return b
}
