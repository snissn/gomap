package db

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// These checks serve both original resource admission and the read-only
// completed-COMMIT proof. Handles remain borrowed; this helper grants no token,
// registry enrollment, namespace effect, or lifetime authority.
func validateDurableDependencyDescriptorV1(dir string, parent, file *os.File, entry rootpublication.DependencyManifestEntryV1, physicalOnly bool) error {
	if parent == nil || file == nil {
		return rootpublication.ErrUnresolvedResource
	}
	path, err := durableDependencyPathForKindV1(dir, "", entry.Kind, entry.DiagnosticPath)
	if err != nil {
		return err
	}
	name := filepath.Base(path)
	if entry.Namespace != nil {
		if entry.Namespace.NewName != name || entry.Namespace.ParentIdentity.Generation == 0 {
			return rootpublication.ErrResourceConflict
		}
		if err = rootpublication.ValidateRecoveredStableNamespaceDescriptorV1(rootpublication.StableNamespaceSpec{Parent: parent, LinkedResource: file, ParentGeneration: entry.Namespace.ParentIdentity.Generation, Operation: entry.Namespace.Operation, OldName: entry.Namespace.OldName, NewName: entry.Namespace.NewName, DiagnosticPath: entry.Namespace.DiagnosticPath}, entry.Namespace.ParentIdentity); err != nil {
			return err
		}
	} else {
		// Original flat raw-log manifests carry identity/frontier without a
		// namespace descriptor. Their original producer derives the exact link.
		if entry.Kind != rootpublication.ResourceValueLog && entry.Kind != rootpublication.ResourceOuterLeafLog {
			return rootpublication.ErrUnresolvedResource
		}
		if err = rootpublication.ValidateStableChildLink(parent, file, name); err != nil {
			return err
		}
	}
	return validateDurableDependencyContentDescriptorV1(dir, file, entry, physicalOnly)
}

// Both original registered raw files and retained temporary proof handles use
// these same physical/content checks. Namespace provenance stays at their
// respective original producer: no extra parent open or namespace is invented.
func validateDurableDependencyContentDescriptorV1(dir string, file *os.File, entry rootpublication.DependencyManifestEntryV1, physicalOnly bool) error {
	if file == nil || entry.Generation == 0 || entry.Identity.Generation != entry.Generation ||
		entry.LogicalLane == "" || entry.ResourceID == "" || len(entry.Reachability) == 0 {
		return fmt.Errorf("%w: incomplete durable dependency descriptor", rootpublication.ErrUnresolvedResource)
	}
	if err := rootpublication.ValidateRecoveredResourceDescriptorV1(entry); err != nil {
		return err
	}
	if namespace := entry.Namespace; namespace != nil {
		path, err := durableDependencyPathForKindV1(dir, "", entry.Kind, entry.DiagnosticPath)
		if err != nil {
			return err
		}
		if namespace.NewName != filepath.Base(path) {
			return rootpublication.ErrResourceConflict
		}
		if err := rootpublication.ValidateRecoveredStableNamespaceMetadataV1(rootpublication.StableNamespaceSpec{ParentGeneration: namespace.ParentIdentity.Generation, Operation: namespace.Operation, OldName: namespace.OldName, NewName: namespace.NewName, DiagnosticPath: namespace.DiagnosticPath}); err != nil {
			return err
		}
	}
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		return err
	}
	if !rootpublication.SamePhysicalIdentity(identity, entry.Identity) {
		return rootpublication.ErrResourceConflict
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < 0 || uint64(info.Size()) < entry.Frontier.Bytes {
		return rootpublication.ErrFrontierBeyondResource
	}
	for _, field := range entry.Reachability {
		policy, ok := rootpublication.StableResourcePolicyFor(field)
		if !ok || !policy.Registerable || policy.Kind != entry.Kind {
			return rootpublication.ErrResourceConflict
		}
	}
	switch entry.Kind {
	case rootpublication.ResourceValueLog, rootpublication.ResourceOuterLeafLog:
		fileID64, err := strconv.ParseUint(entry.ResourceID, 10, 32)
		if err != nil || fileID64 == 0 || entry.Generation != fileID64 || len(entry.Reachability) != 1 || len(entry.LogicalObligations) != 0 {
			return rootpublication.ErrResourceConflict
		}
		fileID := uint32(fileID64)
		lane, _ := valuelog.DecodeFileID(fileID)
		outer := entry.Kind == rootpublication.ResourceOuterLeafLog
		field, logicalLane, scanDir := rootpublication.ReachabilityValueLogPointer, "db/value-log", ValueLogDirPath(dir)
		if outer {
			field, logicalLane, scanDir = rootpublication.ReachabilityOuterLeafRawPointer, "db/outer-leaf-raw", LeafLogDirPath(dir)
		}
		if outer != (lane == valuelog.ReservedLeafLogLaneID) || entry.Reachability[0] != field || entry.LogicalLane != logicalLane {
			return rootpublication.ErrResourceConflict
		}
		diagnostic, err := durableDiagnosticPathV1(dir, valuelog.SegmentPath(scanDir, fileID))
		if err != nil || diagnostic != entry.DiagnosticPath {
			return rootpublication.ErrResourceConflict
		}
		// Original ordinary registration has no external RID fence or content
		// digest for raw pointer logs; exact physical identity and byte frontier
		// remain its authority. Do not fabricate a whole-file content checksum.
		if entry.Frontier.MaxLSN != 0 || len(entry.Frontier.RIDs()) != 0 || entry.Digest != ([32]byte{}) {
			return rootpublication.ErrResourceConflict
		}
		return nil
	default:
		return validateDurableDependencyContent(file, entry, physicalOnly)
	}
}
