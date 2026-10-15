package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

const primaryNewFileName = primaryIndexFileName + ".new"
const jointSwapMagicV6 = "TDJCOM06"

var testHookPrimaryJointSwapV6 func(string) error

// This immutable decision selects two physical files, never a capsule digest.
// The two eligible slots retain their ordinary independent corruption fallback.
type primaryJointDecisionV6 struct {
	Version                                            uint32
	Parent, Data, Primary                              rootpublication.StableIdentity
	UUID                                               [16]byte
	Generation                                         freelist.GenerationRefV1
	DataName, PrimaryName, DataStaging, PrimaryStaging string
}

func jointSwapHookV6(phase string) error {
	if testHookPrimaryJointSwapV6 != nil {
		return testHookPrimaryJointSwapV6(phase)
	}
	return nil
}
func encodePrimaryJointDecisionV6(d primaryJointDecisionV6) ([]byte, error) {
	payload, e := json.Marshal(d)
	if e != nil {
		return nil, e
	}
	if len(payload) > 4096 {
		return nil, ErrRecoveryRequired
	}
	image := make([]byte, 44+len(payload))
	copy(image, jointSwapMagicV6)
	binary.LittleEndian.PutUint32(image[8:12], uint32(len(payload)))
	copy(image[44:], payload)
	sum := sha256.Sum256(image[44:])
	copy(image[12:44], sum[:])
	return image, nil
}
func decodePrimaryJointDecisionV6(image []byte) (d primaryJointDecisionV6, err error) {
	bad := func() (primaryJointDecisionV6, error) {
		return d, errors.Join(ErrRecoveryRequired, errors.New("invalid joint DATA/PRIMARY COMMIT"))
	}
	if len(image) < 44 || len(image) > 4140 || string(image[:8]) != jointSwapMagicV6 || int(binary.LittleEndian.Uint32(image[8:12])) != len(image)-44 {
		return bad()
	}
	sum := sha256.Sum256(image[44:])
	if !bytes.Equal(sum[:], image[12:44]) {
		return bad()
	}
	decoder := json.NewDecoder(bytes.NewReader(image[44:]))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&d) != nil || decoder.Decode(new(any)) != io.EOF || d.Version != 6 ||
		d.DataName != indexFileName || d.PrimaryName != primaryIndexFileName || d.DataStaging != indexNewFileName || d.PrimaryStaging != primaryNewFileName ||
		d.UUID == ([16]byte{}) || d.Generation.GenerationID == 0 || d.Generation.Digest == ([32]byte{}) ||
		!rootpublication.SamePhysicalIdentity(d.Parent, d.Parent) || !rootpublication.SamePhysicalIdentity(d.Data, d.Data) ||
		!rootpublication.SamePhysicalIdentity(d.Primary, d.Primary) || rootpublication.SamePhysicalIdentity(d.Data, d.Primary) {
		return bad()
	}
	return d, nil
}
func primaryJointDecisionForV6(parent *os.File, idx *indexGen, selected durableRootSelectionV1) (d primaryJointDecisionV6, e error) {
	d.Version = 6
	d.DataName = indexFileName
	d.PrimaryName = primaryIndexFileName
	d.DataStaging = indexNewFileName
	d.PrimaryStaging = primaryNewFileName
	d.Parent, e = rootpublication.StableIdentityFromFile(parent)
	if e != nil {
		return d, e
	}
	e = idx.pager.WithStableResourceFile(func(f *os.File) error { d.Data, e = rootpublication.StableIdentityFromFile(f); return e })
	if e != nil {
		return d, e
	}
	e = idx.primary.Pager().WithStableResourceFile(func(f *os.File) error { d.Primary, e = rootpublication.StableIdentityFromFile(f); return e })
	d.UUID = idx.primary.UUID()
	d.Generation = selected.Record.Freelist
	return d, e
}
func syncJointParentV6(dir string, parent *os.File, phase string) error {
	if e := jointSwapHookV6("before-" + phase); e != nil {
		return e
	}
	if e := durabilitycut.EmitPath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceIndex, dir, dir); e != nil {
		return e
	}
	if e := rootpublication.SyncStableNamespace(parent); e != nil {
		return e
	}
	if e := durabilitycut.EmitPath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceIndex, dir, dir); e != nil {
		return e
	}
	return jointSwapHookV6("after-" + phase)
}

// started becomes true before the decision's first possible persistent byte.
// Its caller then retains both exact owners on every uncertainty/error.
func writePrimaryJointCommitV6(dir string, parent *os.File, d primaryJointDecisionV6) (started bool, e error) {
	image, e := encodePrimaryJointDecisionV6(d)
	if e != nil {
		return false, e
	}
	if e = syncJointParentV6(dir, parent, "staging-barrier"); e != nil {
		return false, e
	}
	if e = jointSwapHookV6("before-commit"); e != nil {
		return false, e
	}
	f, e := rootpublication.OpenStableChildFile(parent, indexReadyFileName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return false, e
	}
	started = true
	defer func() { e = errors.Join(e, f.Close()) }()
	if e = observeStableNamespaceMutation(durabilitycut.NamespaceCreate, durabilitycut.ResourceIndex, dir, "", filepath.Join(dir, indexReadyFileName), parent, f, "", indexReadyFileName); e != nil {
		return true, e
	}
	if _, e = f.Write(image); e != nil {
		return true, e
	}
	if e = rootpublication.SyncStableFile(f); e != nil {
		return true, e
	}
	if e = syncJointParentV6(dir, parent, "commit-barrier"); e != nil {
		return true, e
	}
	return true, jointSwapHookV6("after-commit")
}
func jointFileByIdentityV6(parent *os.File, canonical, staging string, want rootpublication.StableIdentity) (*os.File, string, error) {
	return jointFileByIdentityWithProofV6(parent, canonical, staging, want, nil, nil)
}
func jointFileByIdentityWithProofV6(parent *os.File, canonical, staging string, want rootpublication.StableIdentity, proof *primaryJointRecoveryProofV6, slot **os.File) (*os.File, string, error) {
	for _, name := range []string{canonical, staging} {
		f, e := rootpublication.OpenStableChildFile(parent, name, os.O_RDONLY, 0)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return nil, "", e
		}
		if proof != nil {
			*slot = f
		}
		id, e := rootpublication.StableIdentityFromFile(f)
		if e == nil && rootpublication.SamePhysicalIdentity(id, want) {
			return f, name, nil
		}
		var closeErr error
		if proof == nil {
			closeErr = f.Close()
		} else {
			closeErr = proof.closeFile(slot)
		}
		if e != nil || closeErr != nil {
			return nil, "", errors.Join(e, closeErr)
		}
		if e != nil {
			return nil, "", e
		}
	}
	return nil, "", fmt.Errorf("%w: committed pair identity missing: %s", ErrRecoveryRequired, canonical)
}
func validateJointPairV6(data, primary *os.File, d primaryJointDecisionV6) error {
	ds, e := newSnapshotIndexPageStoreV1(data)
	if e != nil {
		return e
	}
	ps, e := newSnapshotIndexPageStoreV1(primary)
	if e != nil {
		return e
	}
	h, e := ps.ReadPage(0)
	if e != nil || !page.VerifyChecksumNonMutating(h) || string(h[16:24]) != "TDPRCAPB" || binary.LittleEndian.Uint16(h[24:26]) != 6 || !bytes.Equal(h[32:48], d.UUID[:]) {
		return ErrRecoveryRequired
	}
	// Exactly two candidates. At least one complete current+embedded-parent
	// closure must validate; a damaged alternate is not a pair-identity failure.
	for slot := uint64(0); slot < 2; slot++ {
		image := make([]byte, rootpublication.PrimaryCapsuleSizeV6)
		if _, e = primary.ReadAt(image, int64(2+slot*3)*page.PageSize); e != nil {
			continue
		}
		v, e := rootpublication.DecodePrimaryCapsuleV6(image, slot, d.UUID)
		if e != nil {
			continue
		}
		if v.Current().Record.Freelist != d.Generation {
			continue
		}
		if _, e = v.ValidatePhysicalProjectionV6(ds, ps, ds.pageCount, ps.pageCount); e == nil {
			return nil
		}
	}
	return fmt.Errorf("%w: committed pair has no complete eligible capsule", ErrRecoveryRequired)
}

// Close verification handles before each namespace mutation. Every reopen is
// relative to the retained parent and must match the COMMIT identity; pathname
// reopening and unsupported-platform fallbacks cannot supply this authority.
func rollForwardPrimaryJointV6(ctx context.Context, dir string, parent *os.File, d primaryJointDecisionV6) error {
	return rollForwardPrimaryJointWithProofV6(ctx, dir, parent, d, nil)
}
func rollForwardPrimaryJointWithProofV6(ctx context.Context, dir string, parent *os.File, d primaryJointDecisionV6, proof *primaryJointRecoveryProofV6) error {
	closeFile := func(f *os.File) error {
		if proof == nil {
			return f.Close()
		}
		if proof.failure != nil {
			return &proof.receipt
		}
		switch f {
		case proof.data:
			return proof.closeFile(&proof.data)
		case proof.primary:
			return proof.closeFile(&proof.primary)
		case proof.dependency:
			return proof.closeFile(&proof.dependency)
		}
		return proof.refuse(rootpublication.ErrResourceOwnership)
	}
	openFile := func(canonical, staging string, want rootpublication.StableIdentity, which int) (*os.File, string, error) {
		if proof == nil {
			return jointFileByIdentityV6(parent, canonical, staging, want)
		}
		if proof.failure != nil {
			return nil, "", &proof.receipt
		}
		slot := &proof.dependency
		if which == 0 {
			slot = &proof.data
		}
		if which == 1 {
			slot = &proof.primary
		}
		return jointFileByIdentityWithProofV6(parent, canonical, staging, want, proof, slot)
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	parentID, err := rootpublication.StableIdentityFromFile(parent)
	if err != nil || !rootpublication.SamePhysicalIdentity(parentID, d.Parent) {
		return errors.Join(err, ErrRecoveryRequired)
	}
	data, dname, err := openFile(d.DataName, d.DataStaging, d.Data, 0)
	if err != nil {
		return err
	}
	primary, pname, err := openFile(d.PrimaryName, d.PrimaryStaging, d.Primary, 1)
	if err != nil {
		return errors.Join(err, closeFile(data))
	}
	err = errors.Join(validateJointPairV6(data, primary, d), closeFile(data), closeFile(primary))
	if err != nil {
		return err
	}
	for _, entry := range []struct {
		from, to, phase string
		identity        rootpublication.StableIdentity
	}{{dname, d.DataName, "data", d.Data}, {pname, d.PrimaryName, "primary", d.Primary}} {
		if err = ctx.Err(); err != nil {
			return err
		}
		if entry.from != entry.to {
			if err = jointSwapHookV6("before-" + entry.phase + "-rename"); err != nil {
				return err
			}
			f, name, e := openFile(entry.from, entry.from, entry.identity, 2)
			if e != nil {
				return e
			}
			e = errors.Join(rootpublication.ValidateStableChildLink(parent, f, name), closeFile(f))
			if e != nil {
				return e
			}
			if e = rootpublication.RenameStableChildFile(parent, entry.from, entry.to); e != nil {
				return e
			}
			f, name, e = openFile(entry.to, entry.to, entry.identity, 2)
			if e != nil {
				return e
			}
			e = errors.Join(rootpublication.ValidateStableChildLink(parent, f, name), observeStableNamespaceMutation(durabilitycut.NamespaceRename, durabilitycut.ResourceIndex, dir, filepath.Join(dir, entry.from), filepath.Join(dir, entry.to), parent, f, entry.from, entry.to), closeFile(f))
			if e != nil {
				return e
			}
			if e = jointSwapHookV6("after-" + entry.phase + "-rename"); e != nil {
				return e
			}
		}
		if err = syncJointParentV6(dir, parent, entry.phase+"-barrier"); err != nil {
			return err
		}
	}
	// Both canonical files must still form the complete committed pair before
	// removing its recovery decision. A successful first rename is insufficient.
	data, dname, err = openFile(d.DataName, d.DataName, d.Data, 0)
	if err != nil {
		return err
	}
	primary, pname, err = openFile(d.PrimaryName, d.PrimaryName, d.Primary, 1)
	if err != nil {
		return errors.Join(err, closeFile(data))
	}
	return errors.Join(validateJointPairV6(data, primary, d), closeFile(data), closeFile(primary))
}

func deletePrimaryJointCommitV6(ctx context.Context, dir string, parent *os.File) (err error) {
	if e := ctx.Err(); e != nil {
		return e
	}
	decision, e := rootpublication.OpenStableChildFile(parent, indexReadyFileName, os.O_RDONLY, 0)
	if e != nil && !os.IsNotExist(e) {
		return e
	}
	if decision != nil {
		defer func() { err = errors.Join(err, decision.Close()) }()
	}
	if e := jointSwapHookV6("before-decision-delete"); e != nil {
		return e
	}
	if e := rootpublication.RemoveStableChildFile(parent, indexReadyFileName); e != nil && !os.IsNotExist(e) {
		return e
	}
	if e := observeStableNamespaceMutation(durabilitycut.NamespaceUnlink, durabilitycut.ResourceIndex, dir, filepath.Join(dir, indexReadyFileName), "", parent, decision, indexReadyFileName, ""); e != nil {
		return e
	}
	if e := jointSwapHookV6("after-decision-delete"); e != nil {
		return e
	}
	return syncJointParentV6(dir, parent, "deletion-barrier")
}

// Called under LOCK, before legacy cleanup or any writable pager.Open.
// A marker other than the exact old "ready\n" cannot authorize legacy cleanup.
func recoverPrimaryJointSwapV6(dir string, readOnly bool) (bool, error) {
	registry := rootpublication.NewIdentityPinRegistry()
	defer registry.Close()
	return recoverPrimaryJointSwapWithRegistryV6(dir, readOnly, registry, nil)
}

func recoverPrimaryJointSwapWithRegistryV6(dir string, readOnly bool, registry *rootpublication.IdentityPinRegistry, lock *lockfile.Lock) (handled bool, e error) {
	proof, e := newPrimaryJointRecoveryProofV6(registry, lock)
	if e != nil {
		return false, e
	}
	defer func() { e = proof.finish(e) }()
	proof.parent, e = rootpublication.OpenStableParent(dir)
	if e != nil {
		return false, e
	}
	proof.marker, e = rootpublication.OpenStableChildFile(proof.parent, indexReadyFileName, os.O_RDONLY, 0)
	if os.IsNotExist(e) {
		return false, nil
	}
	if e != nil {
		return true, e
	}
	// One fixed preadmitted backing, including the oversize sentinel byte.
	// ReadAll may retain chunks and a final copy simultaneously on Go1.26.
	proof.image = make([]byte, primaryJointMarkerReadLimitV6)
	n, e := io.ReadFull(proof.marker, proof.image)
	if errors.Is(e, io.EOF) || errors.Is(e, io.ErrUnexpectedEOF) {
		e = nil
	}
	proof.image = proof.image[:n]
	image := proof.image
	if testHookPrimaryJointRecoveryProofV6 != nil {
		testHookPrimaryJointRecoveryProofV6("before-marker-close", proof)
	}
	if closeErr := proof.closeFile(&proof.marker); closeErr != nil {
		return true, errors.Join(e, closeErr)
	}
	if e != nil {
		return true, e
	}
	if bytes.Equal(image, []byte("ready\n")) {
		return false, nil
	}
	if readOnly {
		return true, fmt.Errorf("%w: joint COMMIT requires writable recovery", ErrRecoveryRequired)
	}
	d, e := decodePrimaryJointDecisionV6(image)
	if e != nil {
		return true, e
	}
	completed, e := proof.completedCanonical(dir, d)
	if e != nil {
		return true, errors.Join(e, ErrRecoveryRequired)
	}
	if !completed {
		if e = rollForwardPrimaryJointWithProofV6(context.Background(), dir, proof.parent, d, proof); e != nil {
			return true, errors.Join(e, ErrRecoveryRequired)
		}
	}
	e = proof.deleteDecision(dir)
	if e != nil {
		return true, errors.Join(e, ErrRecoveryRequired)
	}
	return true, nil
}

func (g *indexGen) rebindPrimaryJointNamespaceV6(dir string) error {
	for _, entry := range []struct {
		name string
		p    *pager.Pager
	}{{indexFileName, g.pager}, {primaryIndexFileName, g.primary.Pager()}} {
		token, e := g.rebindStablePagerNamespaceToken(dir, entry.name, entry.p)
		if e != nil {
			if token != nil {
				e = errors.Join(e, token.ReleaseWithError())
			}
			return e
		}
		e = errors.Join(token.Stabilize(), token.ReleaseWithError())
		if e != nil {
			g.closeFailure.retainCause(2, e)
			g.metadata.CleanupFailed()
			return &g.closeFailure
		}
	}
	return nil
}
