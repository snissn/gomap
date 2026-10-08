package valuelog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestFiniteStableMetadataRefusesBeforeSerializerOrRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "current.log")
	writer, err := NewWriter(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Append(0, nil, 1, []byte("pending")); err != nil {
		t.Fatal(err)
	}
	owner, err := NewFiniteStableMetadata(7, 3, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	registration := StableResourceRegistration{
		Kind: rootpublication.ResourceOuterLeafLog, LogicalLane: "outer-leaf", Generation: 1,
		DiagnosticPath: "leaf_vlog/current.log", Reachability: rootpublication.ReachabilityOuterLeafRawPointer,
		PinRegistry: rootpublication.NewIdentityPinRegistry(),
	}
	beforeSize := writer.Size()
	beforeBuffer := bytes.Clone(writer.appendBuf)
	beforeInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.StableResourceTokenWithFiniteMetadata(registration, owner); !errors.Is(err, ErrFiniteStableMetadataHooksUnavailable) {
		t.Fatalf("token refusal: %v", err)
	}
	if err := writer.CertifyStableCreationNamespaceWithFiniteMetadata(owner); !errors.Is(err, ErrFiniteStableMetadataHooksUnavailable) {
		t.Fatalf("certify refusal: %v", err)
	}
	successor := filepath.Join(filepath.Dir(path), "successor.log")
	active := registration
	active.Generation = 2
	active.NamespaceOperation = rootpublication.NamespaceCreate
	if _, err := writer.RotateToWithStableResourcesWithFiniteMetadata(successor, 2, true, registration, active, owner); !errors.Is(err, ErrFiniteStableMetadataHooksUnavailable) {
		t.Fatalf("rotation refusal: %v", err)
	}
	afterInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if beforeInfo.Size() != afterInfo.Size() || writer.Size() != beforeSize ||
		!bytes.Equal(writer.appendBuf, beforeBuffer) || writer.pendingStableSuccessor != nil || owner.rotations != 0 || owner.tokens != 0 {
		t.Fatal("closed hooks changed serializer, credits or successor state")
	}
	if _, err := os.Stat(successor); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("created successor: %v", err)
	}
	registration.ExternalRIDs = []uint64{1}
	if _, err := writer.StableResourceTokenWithFiniteMetadata(registration, owner); !errors.Is(err, ErrFiniteWriterLoan) {
		t.Fatalf("unsupported external frontier: %v", err)
	}
	owner.Close()
	if _, err := writer.StableResourceTokenWithFiniteMetadata(active, owner); !errors.Is(err, ErrFiniteWriterLoan) {
		t.Fatalf("closed owner: %v", err)
	}
}

func TestFiniteStableMetadataCreditAndGlobalCount(t *testing.T) {
	denied := errors.New("deny metadata")
	if _, err := NewFiniteStableMetadata(7, 3, func(uint64) error { return denied }); !errors.Is(err, denied) {
		t.Fatalf("constructor debit: %v", err)
	}
	owner, err := NewFiniteStableMetadata(2, 1, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.AdmitCapturedToken(); err != nil {
		t.Fatal(err)
	}
	if err := owner.AdmitCapturedToken(); err != nil {
		t.Fatal(err)
	}
	if err := owner.AdmitCapturedToken(); !errors.Is(err, ErrFiniteWriterLoan) {
		t.Fatalf("global token ceiling: %v", err)
	}
	owner.Close()
	if err := owner.AdmitCapturedToken(); !errors.Is(err, ErrFiniteWriterLoan) {
		t.Fatalf("closed count: %v", err)
	}
}
