package valuelog

import (
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestRotateToWithStableResourcesRetainsClosedAndActiveIdentities(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	writer, err := NewWriter(firstPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Append(0, nil, 1, []byte("first")); err != nil {
		t.Fatal(err)
	}
	rotation, err := writer.RotateToWithStableResources(filepath.Join(dir, "000002.vlog"), 2, true,
		StableResourceRegistration{
			LogicalLane: "main", Generation: 1, DiagnosticPath: "maindb/value_vlog/000001.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer,
		},
		StableResourceRegistration{
			LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog",
			Reachability:     rootpublication.ReachabilityValueLogPointer,
			ParentGeneration: 1, NamespaceOperation: rootpublication.NamespaceCreate,
		})
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Release()
	if rotation.Closed == nil || rotation.Active == nil {
		t.Fatalf("rotation=%+v want closed and active tokens", rotation)
	}
	if rotation.Closed.Identity() == rotation.Active.Identity() {
		t.Fatal("rotation collapsed distinct file identities")
	}
	if rotation.Closed.Frontier().Bytes == 0 {
		t.Fatal("closed segment lost accepted byte frontier")
	}
	if rotation.Closed.ResourceID() != "1" || rotation.Active.ResourceID() != "2" {
		t.Fatalf("resource IDs closed=%s active=%s", rotation.Closed.ResourceID(), rotation.Active.ResourceID())
	}
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityValueLogPointer)
	if err := builder.Add(rotation.TakeClosed()); err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(rotation.TakeActive()); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if set.Len() != 2 {
		t.Fatalf("rotated set len=%d want 2", set.Len())
	}
}

func TestStableValueLogRotationNamespaceFailureKeepsOldWriterActive(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	writer, err := NewWriter(firstPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Append(0, nil, 1, []byte("before-failure")); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected namespace failure")
	originalFactory := newValueLogStableNamespaceToken
	newValueLogStableNamespaceToken = func(rootpublication.StableNamespaceSpec) (*rootpublication.StableNamespaceToken, error) {
		return nil, injected
	}
	defer func() { newValueLogStableNamespaceToken = originalFactory }()
	rotation, err := writer.RotateToWithStableResources(filepath.Join(dir, "000002.vlog"), 2, true,
		StableResourceRegistration{
			LogicalLane: "main", Generation: 1, DiagnosticPath: "maindb/value_vlog/000001.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer,
		},
		StableResourceRegistration{
			LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: 1,
			NamespaceOperation: rootpublication.NamespaceCreate,
		})
	if !errors.Is(err, injected) {
		t.Fatalf("rotation error=%v want injected namespace failure", err)
	}
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed rotation returned owned resources")
	}
	if writer.FileID() != 1 || writer.f == nil || writer.f.Name() != firstPath {
		t.Fatalf("failed rotation changed active writer: id=%d file=%v", writer.FileID(), writer.f)
	}
	if _, err := writer.Append(0, nil, 2, []byte("after-failure")); err != nil {
		t.Fatalf("old writer append after failed rotation: %v", err)
	}
}

func TestStableValueLogTokenCarriesCanonicalExternalRIDFence(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(filepath.Join(dir, "000007.vlog"), 7)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Append(0, nil, 9, []byte("rid")); err != nil {
		t.Fatal(err)
	}
	token, err := writer.StableResourceToken(StableResourceRegistration{
		Kind:        rootpublication.ResourceCommandWALExternalRID,
		LogicalLane: "main", Generation: 7, DiagnosticPath: "maindb/value_vlog/000007.vlog",
		Reachability: rootpublication.ReachabilityCommandWALExternalRIDFence,
		ExternalRIDs: []uint64{9, 2, 9, 4}, Digest: sha256.Sum256([]byte("segment-header")),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()
	frontier := token.Frontier()
	if frontier.RIDCount != 3 || frontier.RIDMin != 2 || frontier.RIDMax != 9 || frontier.RIDSetDigest == [32]byte{} {
		t.Fatalf("RID frontier=%+v", frontier)
	}
	if token.Kind() != rootpublication.ResourceCommandWALExternalRID {
		t.Fatalf("token kind=%q want command WAL external RID", token.Kind())
	}
}

func TestStableValueLogRegistrationSupportsOuterLeafProducerKinds(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(filepath.Join(dir, "outer-leaf.log"), 9)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Append(0, nil, 1, []byte("outer-leaf")); err != nil {
		t.Fatal(err)
	}
	token, err := writer.StableResourceToken(StableResourceRegistration{
		Kind: rootpublication.ResourceOuterLeafLog, LogicalLane: "outer-leaf", Generation: 9,
		DiagnosticPath: "maindb/outer_leaf/raw/000009.log", Reachability: rootpublication.ReachabilityOuterLeafRawPointer,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()
	if token.Kind() != rootpublication.ResourceOuterLeafLog {
		t.Fatalf("token kind=%q", token.Kind())
	}
}
