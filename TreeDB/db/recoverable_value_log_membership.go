package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// RecoverableValueLogMembershipStats reports physical certificate reuse and
// actual fallback projection work. Membership is conservative presence, never
// an exact logical reference count or a claim that a file was reclaimed.
type RecoverableValueLogMembershipStats struct {
	CertifiedRoots      uint64
	UncoveredRoots      uint64
	PhysicalDescriptors uint64
	FullRootScans       uint64
	RecordsScanned      uint64
	PointerProjections  uint64
	PhysicalBytesRead   uint64
	LastFallbackReason  string
}

// RecoverableValueLogMembership captures complete recovery-root membership for
// this snapshot's exact publication and index. Callers must fence their own
// detached roots, and destructive GC always captures and validates fresh
// recovery authority again. No physical pin escapes this method.
func (s *Snapshot) RecoverableValueLogMembership(ctx context.Context) (map[uint32]struct{}, RecoverableValueLogMembershipStats, error) {
	var work RecoverableValueLogMembershipStats
	if err := s.beginRead(); err != nil {
		return nil, work, err
	}
	defer s.endRead()
	roots, err := s.db.CaptureRecoverableRootSetForInspection(ctx)
	if err != nil {
		return nil, work, err
	}
	defer roots.Release()
	token, ok := s.StateToken()
	if !ok || roots.visible != token || roots.idx != s.idx {
		return nil, work, ErrRecoverableRootSetStale
	}
	live, work, err := s.db.recoverableValueLogMembership(ctx, roots)
	if err != nil {
		return nil, work, err
	}
	if err := roots.Revalidate(); err != nil {
		return nil, work, err
	}
	return live, work, nil
}

// The index and registered reachability policies certify completeness of the
// root-bound closure. Exact manager registrations and current canonical
// bindings certify each file projection. Any attached namespace obligation is
// validated as well. A numeric ID from a dictionary/template producer is never a
// value-log membership proof. Raw leaf resources remain in the closure even
// though ValueLogGC's existing adapter excludes leaf_vlog from its candidates.
func (db *DB) recoverableValueLogMembership(ctx context.Context, roots *RecoverableRootSet) (map[uint32]struct{}, RecoverableValueLogMembershipStats, error) {
	var work RecoverableValueLogMembershipStats
	if ctx == nil {
		ctx = context.Background()
	}
	if roots == nil || roots.released.Load() || roots.idx == nil || roots.idx.pager == nil || len(roots.Roots()) == 0 {
		return nil, work, ErrRecoverableRootSetStale
	}
	if err := ctx.Err(); err != nil {
		return nil, work, err
	}
	var indexIdentity rootpublication.StableIdentity
	var indexParent rootpublication.StableIdentity
	var indexName string
	err := roots.idx.pager.WithStableResourceFile(func(file *os.File) error {
		var err error
		indexIdentity, err = rootpublication.StableIdentityFromFile(file)
		if err != nil {
			return err
		}
		indexName = filepath.Base(file.Name())
		indexParent, err = membershipDirectoryIdentity(filepath.Dir(file.Name()))
		return err
	})
	if err != nil {
		return nil, work, err
	}
	parents := make(map[string]rootpublication.StableIdentity, 2)
	for _, dir := range []string{ValueLogDirPath(db.dir), LeafLogDirPath(db.dir)} {
		identity, err := membershipDirectoryIdentity(dir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, work, err
		}
		parents[dir] = identity
	}
	files := db.valueLogManager.CurrentSetNoRefresh()
	defer db.valueLogManager.Release(files)
	live := make(map[uint32]struct{})
	// All pending, ambiguous and replay-debt resources contribute presence;
	// only rootResources' exact five-scalar binding can certify absence.
	for _, resources := range roots.resources {
		for _, descriptor := range resources.PhysicalDescriptors() {
			if err := ctx.Err(); err != nil {
				return nil, work, err
			}
			work.PhysicalDescriptors++
			id, owned, _ := db.membershipFileDescriptor(descriptor, parents, files)
			if owned {
				live[id] = struct{}{}
			}
		}
	}
	// An invalid exact-root certificate can be projected completely below.
	// Pending/ambiguous/replay debt has no such certificate: silently dropping
	// an unresolved physical member here would turn uncertainty into absence.
	for _, resources := range roots.debtResources {
		for _, descriptor := range resources.PhysicalDescriptors() {
			if err := ctx.Err(); err != nil {
				return nil, work, err
			}
			work.PhysicalDescriptors++
			if _, _, valid := db.membershipFileDescriptor(descriptor, parents, files); !valid {
				work.LastFallbackReason = "unresolved-debt-resource"
				return nil, work, fmt.Errorf("%w: recovery debt %s/%s has no current manager authority", rootpublication.ErrUnresolvedResource, descriptor.Kind, descriptor.ResourceID())
			}
		}
	}
	for _, root := range roots.Roots() {
		if err := ctx.Err(); err != nil {
			return nil, work, err
		}
		source, sourceIndexID, sourceIndex, exact := roots.resourcesForRootExact(root)
		reason := "unbound-root"
		certified := exact && source != nil
		if exact && source == nil {
			reason = "nil-root-resources"
		}
		if certified && (sourceIndexID != roots.idx.id || !rootpublication.SamePhysicalIdentity(sourceIndex, indexIdentity)) {
			certified, reason = false, "index-identity"
		}
		if certified {
			for _, descriptor := range source.PhysicalDescriptors() {
				if err := ctx.Err(); err != nil {
					return nil, work, err
				}
				work.PhysicalDescriptors++
				fields := descriptor.ReachabilityFields()
				if len(fields) == 0 {
					certified, reason = false, "reachability-policy"
					break
				}
				for _, field := range fields {
					policy, ok := rootpublication.StableResourcePolicyFor(field)
					if !ok || !policy.Registerable || policy.Kind != descriptor.Kind {
						certified, reason = false, "reachability-policy"
						break
					}
				}
				if !certified {
					break
				}
				// CaptureRecoverableRootSet pins the index separately from the
				// external closure. An optional index descriptor must agree;
				// an empty external closure is still certified by that pin.
				if descriptor.Kind == rootpublication.ResourceIndex {
					namespace, bound := descriptor.Namespace()
					if descriptor.Generation != roots.idx.id || !rootpublication.SamePhysicalIdentity(descriptor.Identity(), indexIdentity) || (bound && (!membershipSameParent(namespace.ParentIdentity, indexParent) || namespace.NewName != indexName)) {
						certified, reason = false, "index-identity"
						break
					}
				}
				id, owned, valid := db.membershipFileDescriptor(descriptor, parents, files)
				if owned {
					live[id] = struct{}{}
				}
				if !valid {
					certified, reason = false, "value-log-identity"
					break
				}
			}
		}
		if certified {
			work.CertifiedRoots++
			continue
		}
		work.UncoveredRoots++
		work.FullRootScans++
		work.LastFallbackReason = reason
		snap := roots.AcquireSnapshotForRoot(root)
		if snap == nil {
			return nil, work, ErrRecoverableRootSetStale
		}
		// This seam observes actual uncovered-root collector work. Certified
		// roots never invoke it, and it cannot change certificate admission.
		if hook := db.testValueLogMembershipBeforeFallbackHook; hook != nil {
			hook()
		}
		result, scanErr := db.maintenanceReachabilityScan(ctx, snap, maintenanceReachabilityScanOptions{Collectors: maintenanceReachabilityValueLogRefCounts})
		closeErr := snap.Close()
		work.RecordsScanned += result.recordsScanned
		work.PointerProjections += result.counters.PointerProjections
		work.PhysicalBytesRead += result.counters.PhysicalBytesRead
		if scanErr != nil {
			return nil, work, scanErr
		}
		if closeErr != nil {
			return nil, work, closeErr
		}
		for id := range result.valueLogReferencedSegments {
			live[id] = struct{}{}
		}
	}
	return live, work, nil
}

func membershipDirectoryIdentity(dir string) (rootpublication.StableIdentity, error) {
	file, err := os.Open(dir)
	if err != nil {
		return rootpublication.StableIdentity{}, err
	}
	defer file.Close()
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		return identity, err
	}
	identity.Generation, err = rootpublication.StableNamespaceParentGeneration(file)
	return identity, err
}

func membershipSameParent(a, b rootpublication.StableIdentity) bool {
	return rootpublication.SamePhysicalIdentity(a, b) && a.Generation == b.Generation
}

// owned is a namespace-scoped conservative ID presence. valid additionally
// proves that the physical manager registration still matches the certificate.
func (db *DB) membershipFileDescriptor(descriptor rootpublication.StableResourcePhysicalDescriptor, parents map[string]rootpublication.StableIdentity, files *valuelog.Set) (id uint32, owned, valid bool) {
	var dir string
	for _, field := range descriptor.ReachabilityFields() {
		switch field {
		case rootpublication.ReachabilityValueLogPointer, rootpublication.ReachabilityCommandWALExternalRIDFence:
			dir = ValueLogDirPath(db.dir)
		case rootpublication.ReachabilityOuterLeafRawPointer:
			dir = LeafLogDirPath(db.dir)
		}
	}
	if dir == "" {
		return 0, false, true
	}
	namespace, bound := descriptor.Namespace()
	if bound && !membershipSameParent(namespace.ParentIdentity, parents[dir]) {
		return 0, false, false
	}
	scalar, err := strconv.ParseUint(descriptor.ResourceID(), 10, 32)
	if err != nil || scalar == 0 || !page.IsValueLogFileID(uint32(scalar)) || descriptor.Generation != scalar {
		return 0, false, false
	}
	id = uint32(scalar)
	lane, _ := valuelog.DecodeFileID(id)
	if (dir == LeafLogDirPath(db.dir)) != (lane == valuelog.ReservedLeafLogLaneID) {
		return 0, false, false
	}
	expectedName := filepath.Base(valuelog.SegmentPath(dir, id))
	if bound && namespace.NewName != expectedName {
		return 0, false, false
	}
	// Ordinary manager recapture has NamespaceNone: it pins a producer-
	// registered handle, not a new namespace operation. Without a namespace
	// obligation, a numeric ID contributes presence only after matching the
	// exact physical manager registration. Diagnostic names are not authority.
	owned = bound
	if files == nil {
		return id, owned, false
	}
	file := files.Files[id]
	if file == nil || file.File == nil || filepath.Clean(file.Path) != filepath.Join(dir, expectedName) {
		return id, owned, false
	}
	identity, err := rootpublication.StableIdentityFromFile(file.File)
	if err != nil || !rootpublication.SamePhysicalIdentity(identity, descriptor.Identity()) {
		return id, owned, false
	}
	owned = true
	// The retained handle alone cannot certify a basename after namespace ABA.
	// Resolve the canonical binding independently; stale registrations fall back.
	parent, err := os.Open(dir)
	if err != nil {
		return id, owned, false
	}
	defer parent.Close()
	parentIdentity, err := rootpublication.StableIdentityFromFile(parent)
	if err != nil {
		return id, owned, false
	}
	parentIdentity.Generation, err = rootpublication.StableNamespaceParentGeneration(parent)
	if err != nil || !membershipSameParent(parentIdentity, parents[dir]) {
		return id, owned, false
	}
	boundFile, err := rootpublication.OpenStableChildFile(parent, expectedName, os.O_RDONLY, 0)
	if err != nil {
		return id, owned, false
	}
	boundIdentity, identityErr := rootpublication.StableIdentityFromFile(boundFile)
	closeErr := boundFile.Close()
	if identityErr != nil || closeErr != nil || !rootpublication.SamePhysicalIdentity(boundIdentity, identity) {
		return id, owned, false
	}
	info, err := file.File.Stat()
	if err != nil || info.Size() < 0 || uint64(info.Size()) < descriptor.Frontier().Bytes {
		return id, owned, false
	}
	return id, owned, true
}

func (work *RecoverableValueLogMembershipStats) add(other RecoverableValueLogMembershipStats) {
	work.CertifiedRoots += other.CertifiedRoots
	work.UncoveredRoots += other.UncoveredRoots
	work.PhysicalDescriptors += other.PhysicalDescriptors
	work.FullRootScans += other.FullRootScans
	work.RecordsScanned += other.RecordsScanned
	work.PointerProjections += other.PointerProjections
	work.PhysicalBytesRead += other.PhysicalBytesRead
	if other.LastFallbackReason != "" {
		work.LastFallbackReason = other.LastFallbackReason
	}
}
