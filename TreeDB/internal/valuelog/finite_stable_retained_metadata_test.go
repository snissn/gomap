package valuelog

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestFiniteStableMetadataClosePreservesInstalledAndPendingProofDebit(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" || runtime.Version() != "go1.26.3" {
		t.Skip("finite component source-bound platform")
	}
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "installed", true: "pending"}[pending], func(t *testing.T) {
			m, err := NewFiniteStableMetadata(7, 3, func(uint64) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			parent, err := os.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer parent.Close()
			file, err := os.Create(filepath.Join(dir, "leaf"))
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			proof, err := rootpublication.NewStableNamespaceCreationProofWithMetadataAccount(parent, file, "leaf", m)
			if err != nil {
				t.Fatal(err)
			}
			w := &Writer{}
			if pending {
				if err := m.RetainStableMetadata(); err != nil {
					t.Fatal(err)
				}
				w.pendingStableSuccessor = &pendingValueLogSuccessor{parent: parent, file: file, creationProof: proof, finiteMetadata: m, finiteRetained: true}
			} else {
				w.creationProof = proof
			}
			if err := m.Close(); !errors.Is(err, ErrFiniteWriterLoan) {
				t.Fatalf("Close discarded retained proof debit: %v", err)
			}
			if m.closed || m.reserve == nil {
				t.Fatal("refused Close changed accounting owner")
			}
			// An empty synthetic writer may report its ordinary no-file flush error;
			// Close must nevertheless drain exact pending/installed proof ownership.
			_ = w.Close()
			if err := m.Close(); err != nil {
				t.Fatalf("actual writer Close retained metadata: %v", err)
			}
		})
	}
}
func TestPendingFiniteMetadataReleaseIsExactlyOnce(t *testing.T) {
	m, err := NewFiniteStableMetadata(7, 3, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := m.RetainStableMetadata(); err != nil {
		t.Fatal(err)
	}
	p := &pendingValueLogSuccessor{finiteMetadata: m, finiteRetained: true}
	if err := m.Close(); !errors.Is(err, ErrFiniteWriterLoan) {
		t.Fatal("pending-only owner was discarded")
	}
	p.releaseFiniteMetadata()
	p.releaseFiniteMetadata()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFiniteStableMetadataAccountedSetRefusalPreservesCloseBoundary(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" || runtime.Version() != "go1.26.3" {
		t.Skip("finite component source-bound platform")
	}
	m, err := NewFiniteStableMetadata(7, 3, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "leaf")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	token, err := rootpublication.NewStableResourceTokenWithMetadataAccount(rootpublication.StableResourceSpec{Kind: rootpublication.ResourceOuterLeafLog, LogicalLane: "leaf", ResourceID: "1", Generation: 1, DiagnosticPath: "leaf/1", File: file, Reachability: rootpublication.ReachabilityOuterLeafRawPointer}, m)
	if err != nil {
		t.Fatal(err)
	}
	builder := rootpublication.NewStableResourceSetBuilder()
	defer builder.Abandon()
	if err := builder.Add(token); !errors.Is(err, rootpublication.ErrStableMetadataShapeUnsupported) {
		token.Release()
		t.Fatalf("ordinary builder admitted accounted backing: %v", err)
	}
	if err := m.Close(); !errors.Is(err, ErrFiniteWriterLoan) {
		t.Fatalf("refused Add consumed caller token ownership: %v", err)
	}
	if token.Kind() != "" || token.DiagnosticPath() != "" || token.Namespace() != nil {
		t.Fatal("generic getter exported finite backing")
	}
	if err := token.RequireMetadataExport(); !errors.Is(err, rootpublication.ErrStableMetadataShapeUnsupported) {
		t.Fatalf("export eligibility: %v", err)
	}
	token.Release()
	if err := m.Close(); err != nil {
		t.Fatalf("actual token Release did not permit Close: %v", err)
	}
	if token.Kind() != "" || token.DiagnosticPath() != "" || token.LogicalObligations() != nil {
		t.Fatal("post-Close getters exported retired backing")
	}
}
