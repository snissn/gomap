//go:build darwin || linux || freebsd || netbsd || openbsd

package valuelog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func stablePendingRegistrations(closedID, activeID uint32) (StableResourceRegistration, StableResourceRegistration) {
	return StableResourceRegistration{
			LogicalLane: "main", Generation: uint64(closedID),
			DiagnosticPath: fmt.Sprintf("maindb/value_vlog/%06d.vlog", closedID),
			Reachability:   rootpublication.ReachabilityValueLogPointer,
		}, StableResourceRegistration{
			LogicalLane: "main", Generation: uint64(activeID),
			DiagnosticPath:     fmt.Sprintf("maindb/value_vlog/%06d.vlog", activeID),
			Reachability:       rootpublication.ReachabilityValueLogPointer,
			ParentGeneration:   uint64(activeID),
			NamespaceOperation: rootpublication.NamespaceCreate,
		}
}

func appendStablePendingValue(t *testing.T, writer *Writer, rid uint64) {
	t.Helper()
	if _, err := writer.Append(0, nil, rid, []byte("stable-pending-value")); err != nil {
		t.Fatal(err)
	}
}

func valueLogOpenDescriptorCount(t *testing.T) (int, bool) {
	t.Helper()
	entries, err := os.ReadDir("/dev/fd")
	if err != nil {
		return 0, false
	}
	return len(entries), true
}

func TestStableValueLogObserverFailureRetriesExactPendingSuccessor(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	secondPath := filepath.Join(dir, "000002.vlog")
	writer, err := NewWriter(firstPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	appendStablePendingValue(t, writer, 1)
	closed, active := stablePendingRegistrations(1, 2)
	oldFile := writer.f

	injected := errors.New("injected value-log create observer failure")
	createCalls := 0
	var createdInfo os.FileInfo
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource != durabilitycut.ResourceValueLog || event.Namespace != durabilitycut.NamespaceCreate {
			return nil
		}
		createCalls++
		if createCalls == 1 {
			var statErr error
			createdInfo, statErr = os.Stat(event.NewPath)
			if statErr != nil {
				t.Fatalf("stat created successor inside observer: %v", statErr)
			}
			return injected
		}
		return nil
	})
	defer restore()

	rotation, err := writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed rotation returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("first rotation error=%v want observer failure", err)
	}
	if writer.f != oldFile || writer.FileID() != 1 {
		t.Fatalf("observer failure changed old authority: file=%p/%p id=%d", writer.f, oldFile, writer.FileID())
	}
	rotation, err = writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	defer rotation.Release()
	if createCalls != 1 {
		t.Fatalf("create observer calls=%d want 1", createCalls)
	}
	installedInfo, err := writer.f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if createdInfo == nil || !os.SameFile(createdInfo, installedInfo) {
		t.Fatal("retry did not install the exact successor created before observer failure")
	}
}

func TestStableOuterLeafRotationClassifiesNamespaceCreateWithDirectorySync(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "leaf_vlog")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	firstPath := filepath.Join(dir, "000001.vlog")
	secondPath := filepath.Join(dir, "000002.vlog")
	writer, err := NewWriter(firstPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	appendStablePendingValue(t, writer, 1)
	closed := StableResourceRegistration{
		Kind: rootpublication.ResourceOuterLeafLog, LogicalLane: "outer-leaf", Generation: 1,
		DiagnosticPath: "maindb/leaf_vlog/000001.vlog", Reachability: rootpublication.ReachabilityOuterLeafRawPointer,
	}
	active := StableResourceRegistration{
		Kind: rootpublication.ResourceOuterLeafLog, LogicalLane: "outer-leaf", Generation: 2,
		DiagnosticPath: "maindb/leaf_vlog/000002.vlog", Reachability: rootpublication.ReachabilityOuterLeafRawPointer,
		ParentGeneration: 2, NamespaceOperation: rootpublication.NamespaceCreate,
	}

	var outerCreates, valueCreates, outerBeforeSyncs, outerAfterSyncs int
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		switch {
		case event.Namespace == durabilitycut.NamespaceCreate && event.NewPath == secondPath:
			if event.Resource == durabilitycut.ResourceOuterLeaf {
				outerCreates++
			}
			if event.Resource == durabilitycut.ResourceValueLog {
				valueCreates++
			}
		case event.Resource == durabilitycut.ResourceOuterLeaf && event.Point == durabilitycut.BeforeNewFileDirectorySync:
			outerBeforeSyncs++
		case event.Resource == durabilitycut.ResourceOuterLeaf && event.Point == durabilitycut.AfterNewFileDirectorySync:
			outerAfterSyncs++
		}
		return nil
	})
	defer restore()

	rotation, err := writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Release()
	if outerCreates != 1 || valueCreates != 0 || outerBeforeSyncs != 1 || outerAfterSyncs != 1 {
		t.Fatalf("outer-leaf creation cuts create(value/outer)=%d/%d directory(before/after)=%d/%d want 0/1 and 1/1",
			valueCreates, outerCreates, outerBeforeSyncs, outerAfterSyncs)
	}
}

func TestStableValueLogPendingSuccessorRejectsOrdinaryAndMismatchedRotation(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*Writer, string, StableResourceRegistration, StableResourceRegistration) error
	}{
		{name: "ordinary", run: func(writer *Writer, path string, _, _ StableResourceRegistration) error {
			return writer.RotateToWithSync(path, 2, false)
		}},
		{name: "mismatched-path", run: func(writer *Writer, _ string, closed, active StableResourceRegistration) error {
			rotation, err := writer.RotateToWithStableResources(filepath.Join(filepath.Dir(writer.f.Name()), "000003.vlog"), 3, false, closed, active)
			if rotation != nil {
				rotation.Release()
			}
			return err
		}},
		{name: "mismatched-active-registration", run: func(writer *Writer, path string, closed, active StableResourceRegistration) error {
			active.DiagnosticPath = "maindb/value_vlog/mismatched.vlog"
			rotation, err := writer.RotateToWithStableResources(path, 2, false, closed, active)
			if rotation != nil {
				rotation.Release()
			}
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			firstPath := filepath.Join(dir, "000001.vlog")
			secondPath := filepath.Join(dir, "000002.vlog")
			writer, err := NewWriter(firstPath, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			appendStablePendingValue(t, writer, 1)
			closed, active := stablePendingRegistrations(1, 2)
			oldFile := writer.f

			injected := errors.New("injected value-log namespace-token failure")
			originalFactory := bindValueLogStableNamespaceCreationProof
			bindValueLogStableNamespaceCreationProof = func(proof *rootpublication.StableNamespaceCreationProof, parent *os.File, generation uint64, name, diagnosticPath string) (*rootpublication.StableNamespaceToken, error) {
				if name == filepath.Base(secondPath) {
					return nil, injected
				}
				return originalFactory(proof, parent, generation, name, diagnosticPath)
			}
			rotation, err := writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
			bindValueLogStableNamespaceCreationProof = originalFactory
			if rotation != nil {
				rotation.Release()
				t.Fatal("failed rotation returned owned resources")
			}
			if !errors.Is(err, injected) {
				t.Fatalf("first rotation error=%v want token failure", err)
			}
			if err := tc.run(writer, secondPath, closed, active); !errors.Is(err, rootpublication.ErrResourceOwnership) {
				t.Fatalf("competing rotation error=%v want ErrResourceOwnership", err)
			}
			if writer.f != oldFile || writer.FileID() != 1 {
				t.Fatalf("competing rotation changed authority: file=%p/%p id=%d", writer.f, oldFile, writer.FileID())
			}
		})
	}
}

func TestStableValueLogTokenFailureRetriesExactPendingSuccessor(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	secondPath := filepath.Join(dir, "000002.vlog")
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(firstPath, 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	appendStablePendingValue(t, writer, 1)
	closed, active := stablePendingRegistrations(1, 2)
	active.ExternalRIDs = []uint64{101, 202}

	createCalls := 0
	beforeDirectoryCuts := 0
	afterDirectoryCuts := 0
	beforeDirectorySyncs := writer.DurabilityStats().DirectorySyncCalls
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceValueLog && event.Namespace == durabilitycut.NamespaceCreate {
			createCalls++
		}
		if event.Resource == durabilitycut.ResourceValueLog && event.Point == durabilitycut.BeforeNewFileDirectorySync {
			beforeDirectoryCuts++
		}
		if event.Resource == durabilitycut.ResourceValueLog && event.Point == durabilitycut.AfterNewFileDirectorySync {
			afterDirectoryCuts++
		}
		return nil
	})
	defer restore()
	injected := errors.New("injected value-log namespace-token failure")
	originalFactory := bindValueLogStableNamespaceCreationProof
	factoryCalls := 0
	bindValueLogStableNamespaceCreationProof = func(proof *rootpublication.StableNamespaceCreationProof, parent *os.File, generation uint64, name, diagnosticPath string) (*rootpublication.StableNamespaceToken, error) {
		if name == filepath.Base(secondPath) {
			factoryCalls++
			if factoryCalls == 1 {
				return nil, injected
			}
		}
		return originalFactory(proof, parent, generation, name, diagnosticPath)
	}
	defer func() { bindValueLogStableNamespaceCreationProof = originalFactory }()

	rotation, err := writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed rotation returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("first rotation error=%v want token failure", err)
	}
	if got := writer.DurabilityStats().DirectorySyncCalls; got != beforeDirectorySyncs+1 || beforeDirectoryCuts != 1 || afterDirectoryCuts != 1 {
		t.Fatalf("failed bind proof sync evidence calls=%d before/after=%d/%d want %d and 1/1", got, beforeDirectoryCuts, afterDirectoryCuts, beforeDirectorySyncs+1)
	}
	pending := writer.pendingStableSuccessor
	if pending == nil || !pending.stableObserved || pending.active.PinRegistry != registry {
		t.Fatalf("retry did not retain one registry-owned successor: %+v", pending)
	}
	if pending.parent != writer.stableParent || pending.path != secondPath || pending.fileID != 2 ||
		pending.active.NamespaceParent != writer.stableParent || pending.active.NewName != filepath.Base(secondPath) {
		t.Fatalf("retry did not retain the exact parent/path registration: %+v", pending)
	}
	active.ExternalRIDs[0] = 999
	if got := pending.active.ExternalRIDs; len(got) != 2 || got[0] != 101 || got[1] != 202 {
		t.Fatalf("pending frontier aliases caller storage: %v", got)
	}
	active.ExternalRIDs = []uint64{101, 202}
	pendingIdentity := pending.stableIdentity
	if got := registry.ActiveIdentities(); got != 2 {
		t.Fatalf("pending retry identities=%d want old active plus exact successor", got)
	}
	createdInfo, err := os.Stat(secondPath)
	if err != nil {
		t.Fatal(err)
	}
	rotation, err = writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	installedInfo, err := writer.f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if createCalls != 1 || factoryCalls != 2 || !os.SameFile(createdInfo, installedInfo) {
		t.Fatalf("retry createCalls=%d factoryCalls=%d sameFile=%t, want 1/2/true",
			createCalls, factoryCalls, os.SameFile(createdInfo, installedInfo))
	}
	if got := writer.DurabilityStats().DirectorySyncCalls; got != beforeDirectorySyncs+1 || beforeDirectoryCuts != 1 || afterDirectoryCuts != 1 {
		t.Fatalf("identical retry resynced proof: calls=%d before/after=%d/%d", got, beforeDirectoryCuts, afterDirectoryCuts)
	}
	if writer.pendingStableSuccessor != nil || !writer.stableResourceObserved ||
		!rootpublication.SamePhysicalIdentity(writer.stableResourceIdentity, pendingIdentity) {
		t.Fatal("successful retry did not transfer the pending observation into the active writer")
	}
	if got := registry.ActivePins(); got != 2 {
		t.Fatalf("successful retry pins=%d want closed and active tokens", got)
	}
	rotation.Release()
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("released rotation pins=%d want 0", got)
	}
	if got := registry.ActiveIdentities(); got != 1 {
		t.Fatalf("post-transfer identities=%d want active writer only", got)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("closed writer retained %d identities", got)
	}
}

func TestStableValueLogClosedCreationProofBindFailurePreservesCurrentAuthority(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	secondPath := filepath.Join(dir, "000002.vlog")
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(firstPath, 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	appendStablePendingValue(t, writer, 1)
	closed, active := stablePendingRegistrations(1, 2)
	originalFile := writer.f
	originalProof := writer.creationProof
	if originalProof == nil {
		t.Fatal("new current segment lacks retained creation proof")
	}

	injected := errors.New("injected closed creation-proof bind failure")
	originalFactory := bindRetainedValueLogStableNamespaceCreationProof
	bindRetainedValueLogStableNamespaceCreationProof = func(proof *rootpublication.StableNamespaceCreationProof, parent *os.File, generation uint64, name, diagnosticPath string) (*rootpublication.StableNamespaceToken, error) {
		if name == filepath.Base(firstPath) {
			return nil, injected
		}
		return originalFactory(proof, parent, generation, name, diagnosticPath)
	}
	rotation, err := writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	bindRetainedValueLogStableNamespaceCreationProof = originalFactory
	if rotation != nil {
		rotation.Release()
		t.Fatal("closed-proof bind failure returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("rotation error=%v want injected failure", err)
	}
	if writer.f != originalFile || writer.FileID() != 1 || writer.creationProof != originalProof || writer.pendingStableSuccessor != nil {
		t.Fatalf("closed-proof failure changed authority: file=%p/%p id=%d proof=%p/%p pending=%+v", writer.f, originalFile, writer.FileID(), writer.creationProof, originalProof, writer.pendingStableSuccessor)
	}
	if got := registry.ActiveIdentities(); got != 1 {
		t.Fatalf("closed-proof failure identities=%d want current writer only", got)
	}
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("closed-proof failure pins=%d want 0", got)
	}

	rotation, err = writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if err != nil {
		t.Fatalf("retry after closed-proof failure: %v", err)
	}
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityValueLogPointer)
	if err := builder.Add(rotation.TakeClosed()); err != nil {
		rotation.Release()
		t.Fatalf("add closed rotation token: %v", err)
	}
	if err := builder.Add(rotation.TakeActive()); err != nil {
		builder.Abandon()
		rotation.Release()
		t.Fatalf("add active rotation token: %v", err)
	}
	rotation.Release()
	resources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		t.Fatalf("freeze rotation resources: %v", err)
	}
	stats := resources.Stats(time.Now())
	if len(stats) != 1 || stats[0].NamespaceSyncs != 2 {
		resources.Release()
		t.Fatalf("rotation resource stats=%+v want two namespace syncs", stats)
	}
	resources.Release()
}

func TestStableValueLogAfterDirectoryCutRetryKeepsSyncedProof(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(filepath.Join(dir, "000001.vlog"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	appendStablePendingValue(t, writer, 1)
	closed, active := stablePendingRegistrations(1, 2)
	secondPath := filepath.Join(dir, "000002.vlog")
	beforeSyncs := writer.DurabilityStats().DirectorySyncCalls
	injected := errors.New("injected after-directory-sync cut")
	beforeCuts := 0
	afterCuts := 0
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource != durabilitycut.ResourceValueLog {
			return nil
		}
		switch event.Point {
		case durabilitycut.BeforeNewFileDirectorySync:
			beforeCuts++
		case durabilitycut.AfterNewFileDirectorySync:
			afterCuts++
			return injected
		}
		return nil
	})
	defer restore()
	rotation, err := writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed after-cut rotation returned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("after-cut error=%v want injected", err)
	}
	if writer.pendingStableSuccessor == nil || writer.pendingStableSuccessor.creationProof == nil {
		t.Fatal("after-cut failure did not retain the already-synced exact proof")
	}
	if got := writer.DurabilityStats().DirectorySyncCalls; got != beforeSyncs+1 || beforeCuts != 1 || afterCuts != 1 {
		t.Fatalf("first attempt sync evidence calls=%d cuts=%d/%d", got, beforeCuts, afterCuts)
	}
	rotation, err = writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if err != nil {
		t.Fatalf("retry after completed directory sync: %v", err)
	}
	rotation.Release()
	if got := writer.DurabilityStats().DirectorySyncCalls; got != beforeSyncs+1 || beforeCuts != 1 || afterCuts != 1 {
		t.Fatalf("retry repeated completed directory sync: calls=%d cuts=%d/%d", got, beforeCuts, afterCuts)
	}
}

func TestStableValueLogPendingSuccessorRejectsRegistryMismatch(t *testing.T) {
	dir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "000001.vlog"), 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	appendStablePendingValue(t, writer, 1)
	closed, active := stablePendingRegistrations(1, 2)
	secondPath := filepath.Join(dir, "000002.vlog")

	injected := errors.New("injected namespace-token failure before registry mismatch")
	originalFactory := bindValueLogStableNamespaceCreationProof
	bindValueLogStableNamespaceCreationProof = func(proof *rootpublication.StableNamespaceCreationProof, parent *os.File, generation uint64, name, diagnosticPath string) (*rootpublication.StableNamespaceToken, error) {
		if name == filepath.Base(secondPath) {
			return nil, injected
		}
		return originalFactory(proof, parent, generation, name, diagnosticPath)
	}
	rotation, err := writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	bindValueLogStableNamespaceCreationProof = originalFactory
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed rotation returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("first rotation error=%v want token failure", err)
	}
	pending := writer.pendingStableSuccessor
	if pending == nil || !pending.stableObserved {
		t.Fatal("token failure did not retain the observed exact successor")
	}
	identity := pending.stableIdentity
	mismatched := active
	mismatched.PinRegistry = rootpublication.NewIdentityPinRegistry()
	rotation, err = writer.RotateToWithStableResources(secondPath, 2, false, closed, mismatched)
	if rotation != nil {
		rotation.Release()
		t.Fatal("registry-mismatched retry returned owned resources")
	}
	if !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("registry-mismatched retry error=%v want ErrResourceConflict", err)
	}
	if writer.pendingStableSuccessor != pending || !pending.stableObserved ||
		!rootpublication.SamePhysicalIdentity(pending.stableIdentity, identity) {
		t.Fatal("registry-mismatched retry changed the retained exact successor")
	}
	if got := registry.ActiveIdentities(); got != 2 {
		t.Fatalf("registry mismatch identities=%d want old active plus pending successor", got)
	}
	rotation, err = writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if err != nil {
		t.Fatalf("valid retry: %v", err)
	}
	rotation.Release()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("valid retry cleanup retained %d identities", got)
	}
}

func TestStableValueLogPendingSuccessorCloseReleasesExactOwnership(t *testing.T) {
	dir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "000001.vlog"), 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	appendStablePendingValue(t, writer, 1)
	closed, active := stablePendingRegistrations(1, 2)
	originalFactory := bindValueLogStableNamespaceCreationProof
	injected := errors.New("injected token failure before close")
	secondPath := filepath.Join(dir, "000002.vlog")
	bindValueLogStableNamespaceCreationProof = func(proof *rootpublication.StableNamespaceCreationProof, parent *os.File, generation uint64, name, diagnosticPath string) (*rootpublication.StableNamespaceToken, error) {
		if name == filepath.Base(secondPath) {
			return nil, injected
		}
		return originalFactory(proof, parent, generation, name, diagnosticPath)
	}
	rotation, err := writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	bindValueLogStableNamespaceCreationProof = originalFactory
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed rotation returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("rotation error=%v want token failure", err)
	}
	oldFile := writer.f
	pendingFile := writer.pendingStableSuccessor.file
	parent := writer.stableParent
	if got := registry.ActiveIdentities(); got != 2 {
		t.Fatalf("pending close setup identities=%d want 2", got)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	for name, file := range map[string]*os.File{"old": oldFile, "pending": pendingFile, "parent": parent} {
		if err := file.Close(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("%s handle second close=%v want os.ErrClosed", name, err)
		}
	}
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("pending close retained %d pins", got)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("pending close retained %d identities", got)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("second writer close: %v", err)
	}
}

func TestStableValueLogPendingSuccessorTransfersRegistryObservationOnRetry(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	secondPath := filepath.Join(dir, "000002.vlog")
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(firstPath, 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	appendStablePendingValue(t, writer, 1)
	closed, active := stablePendingRegistrations(1, 2)

	injected := errors.New("injected value-log namespace-token failure")
	originalFactory := bindValueLogStableNamespaceCreationProof
	factoryCalls := 0
	bindValueLogStableNamespaceCreationProof = func(proof *rootpublication.StableNamespaceCreationProof, parent *os.File, generation uint64, name, diagnosticPath string) (*rootpublication.StableNamespaceToken, error) {
		if name == filepath.Base(secondPath) {
			factoryCalls++
			if factoryCalls == 1 {
				return nil, injected
			}
		}
		return originalFactory(proof, parent, generation, name, diagnosticPath)
	}
	defer func() { bindValueLogStableNamespaceCreationProof = originalFactory }()

	rotation, err := writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed rotation returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("first rotation error=%v want token failure", err)
	}
	if got := registry.ActiveIdentities(); got != 2 {
		t.Fatalf("pending rotation identities=%d want old and exact successor", got)
	}
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("pending rotation pins=%d want 0 after failed token capture", got)
	}

	rotation, err = writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	if got := registry.ActiveIdentities(); got != 2 {
		rotation.Release()
		t.Fatalf("installed rotation identities=%d want active successor plus pinned closed segment", got)
	}
	if got := registry.ActivePins(); got != 2 {
		rotation.Release()
		t.Fatalf("installed rotation pins=%d want closed and active tokens", got)
	}
	rotation.Release()
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("released rotation pins=%d want 0", got)
	}
	if got := registry.ActiveIdentities(); got != 1 {
		t.Fatalf("released rotation identities=%d want active successor only", got)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("closed writer identities=%d want 0", got)
	}
}

func TestStableValueLogPendingSuccessorReleasesRegistryObservationOnClose(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	secondPath := filepath.Join(dir, "000002.vlog")
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(firstPath, 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	appendStablePendingValue(t, writer, 1)
	closed, active := stablePendingRegistrations(1, 2)

	injected := errors.New("injected value-log namespace-token failure")
	originalFactory := bindValueLogStableNamespaceCreationProof
	bindValueLogStableNamespaceCreationProof = func(proof *rootpublication.StableNamespaceCreationProof, parent *os.File, generation uint64, name, diagnosticPath string) (*rootpublication.StableNamespaceToken, error) {
		if name == filepath.Base(secondPath) {
			return nil, injected
		}
		return originalFactory(proof, parent, generation, name, diagnosticPath)
	}
	rotation, err := writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	bindValueLogStableNamespaceCreationProof = originalFactory
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed rotation returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("rotation error=%v want token failure", err)
	}
	if got := registry.ActiveIdentities(); got != 2 {
		t.Fatalf("pending rotation identities=%d want old and exact successor", got)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("closed pending writer identities=%d want 0", got)
	}
	if _, err := os.Stat(secondPath); err != nil {
		t.Fatalf("close removed persistent pending successor: %v", err)
	}
}

func TestStableValueLogPendingSuccessorReplacementFailsClosedWithoutUnlink(t *testing.T) {
	beforeFDs, checkFDs := valueLogOpenDescriptorCount(t)
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	secondPath := filepath.Join(dir, "000002.vlog")
	displacedPath := filepath.Join(dir, "000002-displaced.vlog")
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(firstPath, 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	appendStablePendingValue(t, writer, 1)
	closed, active := stablePendingRegistrations(1, 2)
	injected := errors.New("injected value-log observer failure before replacement")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceValueLog && event.Namespace == durabilitycut.NamespaceCreate {
			return injected
		}
		return nil
	})
	rotation, err := writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	restore()
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed rotation returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("first rotation error=%v want observer failure", err)
	}
	if err := os.Rename(secondPath, displacedPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondPath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	rotation, err = writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if rotation != nil {
		rotation.Release()
		t.Error("conflicting retry returned owned resources")
	}
	if !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Errorf("conflicting retry error=%v want ErrResourceConflict", err)
	}
	if err := writer.Close(); err != nil {
		t.Errorf("close writer: %v", err)
	}
	if got := registry.ActivePins(); got != 0 {
		t.Errorf("replacement cleanup retained %d pins", got)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Errorf("replacement cleanup retained %d identities", got)
	}
	if got, err := os.ReadFile(secondPath); err != nil || string(got) != "replacement" {
		t.Errorf("replacement changed: data=%q err=%v", got, err)
	}
	if _, err := os.Stat(displacedPath); err != nil {
		t.Errorf("displaced exact successor removed: %v", err)
	}
	if afterFDs, ok := valueLogOpenDescriptorCount(t); checkFDs && ok && afterFDs > beforeFDs+1 {
		t.Errorf("close retained descriptors: before=%d after=%d", beforeFDs, afterFDs)
	}
}

func TestStableValueLogOldCloseFailureIsFailStop(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	secondPath := filepath.Join(dir, "000002.vlog")
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(firstPath, 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	appendStablePendingValue(t, writer, 1)
	closed, active := stablePendingRegistrations(1, 2)
	injected := errors.New("injected value-log old close failure")
	writer.closeRotateFn = func(file *os.File) error {
		writer.closeRotateFn = nil
		return errors.Join(file.Close(), injected)
	}
	rotation, err := writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if rotation != nil {
		rotation.Release()
		t.Fatal("old-close failure returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("rotation error=%v want old-close failure", err)
	}
	if pending := writer.pendingStableSuccessor; pending == nil || !pending.failStop || !pending.stableObserved || pending.active.PinRegistry != registry {
		t.Fatalf("old-close failure did not retain observed fail-stop successor: %+v", pending)
	}
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("old-close failure retained %d token pins", got)
	}
	if got := registry.ActiveIdentities(); got != 1 {
		t.Fatalf("old-close failure identities=%d want retained successor only", got)
	}
	if err := writer.RotateTo(firstPath, 1); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("ordinary rotation after fail-stop error=%v want ErrResourceOwnership", err)
	}
	rotation, err = writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if rotation != nil {
		rotation.Release()
		t.Fatal("stable retry after fail-stop returned owned resources")
	}
	if !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("stable retry after fail-stop error=%v want ErrResourceOwnership", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("fail-stop close retained %d identities", got)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, err := os.Stat(secondPath); err != nil {
		t.Fatalf("fail-stop close removed created successor: %v", err)
	}
}

func TestStableValueLogRetryDescriptorPlateau(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(filepath.Join(dir, "000001.vlog"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	appendStablePendingValue(t, writer, 1)
	baseline, checkFDs := valueLogOpenDescriptorCount(t)
	injected := errors.New("injected value-log retry cycle")
	createCalls := 0
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceValueLog && event.Namespace == durabilitycut.NamespaceCreate {
			createCalls++
			return injected
		}
		return nil
	})
	defer restore()

	for cycle := uint32(0); cycle < 64; cycle++ {
		closedID := writer.FileID()
		activeID := closedID + 1
		closed, active := stablePendingRegistrations(closedID, activeID)
		path := filepath.Join(dir, fmt.Sprintf("%06d.vlog", activeID))
		rotation, err := writer.RotateToWithStableResources(path, activeID, false, closed, active)
		if rotation != nil {
			rotation.Release()
			t.Fatalf("cycle %d first attempt returned resources", cycle)
		}
		if !errors.Is(err, injected) {
			t.Fatalf("cycle %d first attempt error=%v", cycle, err)
		}
		rotation, err = writer.RotateToWithStableResources(path, activeID, false, closed, active)
		if err != nil {
			t.Fatalf("cycle %d retry: %v", cycle, err)
		}
		rotation.Release()
		appendStablePendingValue(t, writer, uint64(activeID))
		if got, ok := valueLogOpenDescriptorCount(t); checkFDs && ok && got > baseline+1 {
			t.Fatalf("cycle %d descriptors=%d baseline=%d", cycle, got, baseline)
		}
	}
	if createCalls != 64 {
		t.Fatalf("create observer calls=%d want 64", createCalls)
	}
	if writer.FileID() != 65 {
		t.Fatalf("final file id=%d want 65", writer.FileID())
	}
}
