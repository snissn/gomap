package collections

import (
	"errors"
	"os"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Component controls for failure-only context, not a reproduction of the
// intermittent public lifecycle refusal or a change to its acceptance guards.
func TestNativeBasisRefreshDiagnosticPreservesCause(t *testing.T) {
	original := &os.PathError{Op: "diagnostic-control", Path: "no-io", Err: backenddb.ErrRootPublicationBasisMismatch}
	before := backenddb.StateToken{CommitSeq: 11, RootPageID: 12, SystemRootPageID: 13, AppliedCommandLSN: 14, MaxEntryRevision: 15, LeafGenerationStateVersion: 16}
	after := backenddb.StateToken{CommitSeq: 21, RootPageID: 22, SystemRootPageID: 23, AppliedCommandLSN: 24, MaxEntryRevision: 25, LeafGenerationStateVersion: 26}
	wrapped := nativeBasisRefreshDiagnosticError("count", before, after, original)
	var typed *os.PathError
	if errors.Unwrap(wrapped) != original || !errors.Is(wrapped, backenddb.ErrRootPublicationBasisMismatch) || !errors.As(wrapped, &typed) || typed != original {
		t.Fatalf("failure context lost original cause/type: %v", wrapped)
	}
	for _, label := range []string{"phase=count", "before=(seq=11 root=12 system=13 lsn=14 revision=15 leaf_version=16)", "after=(seq=21 root=22 system=23 lsn=24 revision=25 leaf_version=26)"} {
		if !strings.Contains(wrapped.Error(), label) {
			t.Fatalf("missing original scalar context %q: %v", label, wrapped)
		}
	}
}

func TestNativeBasisRefreshDiagnosticIdentifiesFirstGuard(t *testing.T) {
	t.Run("snapshot_pair", func(t *testing.T) {
		reader := &leafPackMismatchedReaderControl{}
		state, err := prepareNativeEnginePhysicalBasisRefresh(nil, nil, nil, reader, false)
		if state != nil || !errors.Is(err, backenddb.ErrRootPublicationBasisMismatch) || !strings.Contains(err.Error(), "guard=snapshot-pair") || reader.calls != 1 {
			t.Fatalf("first pairing predicate repeated or lost: calls=%d state=%p err=%v", reader.calls, state, err)
		}
	})
	t.Run("old_domain_root", func(t *testing.T) {
		var scratch nativeBasisRefreshScratch
		collection := &nativeCollectionState{schema: &nativeSchemaState{meta: &CollectionMeta{Name: "diagnostic"}}}
		domain := &nativeDomainState{name: "primary", catalogRoot: 7}
		got, err := scratch.root(collection, domain)
		if got != 0 || !errors.Is(err, backenddb.ErrRootPublicationBasisMismatch) || !strings.Contains(err.Error(), "guard=old-domain-root decoded=0 semantic_catalog=7") || domain.catalogRoot != 7 {
			t.Fatalf("original old/domain mismatch not retained: got=%d err=%v", got, err)
		}
	})
}
