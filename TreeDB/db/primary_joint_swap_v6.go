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
	for _, name := range []string{canonical, staging} {
		f, e := rootpublication.OpenStableChildFile(parent, name, os.O_RDONLY, 0)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return nil, "", e
		}
		id, e := rootpublication.StableIdentityFromFile(f)
		if e == nil && rootpublication.SamePhysicalIdentity(id, want) {
			return f, name, nil
		}
		f.Close()
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
func rollForwardPrimaryJointV6(ctx context.Context, dir string, parent *os.File, d primaryJointDecisionV6) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	parentID, e := rootpublication.StableIdentityFromFile(parent)
	if e != nil || !rootpublication.SamePhysicalIdentity(parentID, d.Parent) {
		return errors.Join(e, ErrRecoveryRequired)
	}
	data, dname, e := jointFileByIdentityV6(parent, d.DataName, d.DataStaging, d.Data)
	if e != nil {
		return e
	}
	defer data.Close()
	primary, pname, e := jointFileByIdentityV6(parent, d.PrimaryName, d.PrimaryStaging, d.Primary)
	if e != nil {
		return e
	}
	defer primary.Close()
	if e = validateJointPairV6(data, primary, d); e != nil {
		return e
	}
	for _, entry := range []struct {
		from, to, phase string
		f               *os.File
	}{{dname, d.DataName, "data", data}, {pname, d.PrimaryName, "primary", primary}} {
		if e = ctx.Err(); e != nil {
			return e
		}
		if entry.from != entry.to {
			if e = jointSwapHookV6("before-" + entry.phase + "-rename"); e != nil {
				return e
			}
			if e = rootpublication.ValidateStableChildLink(parent, entry.f, entry.from); e != nil {
				return e
			}
			if e = rootpublication.RenameStableChildFile(parent, entry.from, entry.to); e != nil {
				return e
			}
			if e = observeStableNamespaceMutation(durabilitycut.NamespaceRename, durabilitycut.ResourceIndex, dir, filepath.Join(dir, entry.from), filepath.Join(dir, entry.to), parent, entry.f, entry.from, entry.to); e != nil {
				return e
			}
			if e = rootpublication.ValidateStableChildLink(parent, entry.f, entry.to); e != nil {
				return e
			}
			if e = jointSwapHookV6("after-" + entry.phase + "-rename"); e != nil {
				return e
			}
		}
		if e = syncJointParentV6(dir, parent, entry.phase+"-barrier"); e != nil {
			return e
		}
	}
	return nil
}
func deletePrimaryJointCommitV6(ctx context.Context, dir string, parent *os.File) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	decision, e := rootpublication.OpenStableChildFile(parent, indexReadyFileName, os.O_RDONLY, 0)
	if e != nil && !os.IsNotExist(e) {
		return e
	}
	if decision != nil {
		defer decision.Close()
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
func recoverPrimaryJointSwapV6(dir string, readOnly bool) (handled bool, e error) {
	parent, e := rootpublication.OpenStableParent(dir)
	if e != nil {
		return false, e
	}
	defer parent.Close()
	f, e := rootpublication.OpenStableChildFile(parent, indexReadyFileName, os.O_RDONLY, 0)
	if os.IsNotExist(e) {
		return false, nil
	}
	if e != nil {
		return true, e
	}
	image, e := io.ReadAll(io.LimitReader(f, 4141))
	e = errors.Join(e, f.Close())
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
	if e = rollForwardPrimaryJointV6(context.Background(), dir, parent, d); e != nil {
		return true, errors.Join(e, ErrRecoveryRequired)
	}
	e = deletePrimaryJointCommitV6(context.Background(), dir, parent)
	return true, errors.Join(e, func() error {
		if e != nil {
			return ErrRecoveryRequired
		}
		return nil
	}())
}

func (g *indexGen) rebindPrimaryJointNamespaceV6(dir string) error {
	g.stableNamespaceMu.Lock()
	if g.stableNamespaceProof != nil {
		g.stableNamespaceProof.Release()
		g.stableNamespaceProof = nil
	}
	if g.stableNamespaceParent != nil {
		g.stableNamespaceParent.Close()
		g.stableNamespaceParent = nil
	}
	g.stableNamespaceMu.Unlock()
	owner := g.primaryOwner
	if err := owner.clearNamespaceV5(); err != nil {
		return err
	}
	for _, entry := range []struct {
		name string
		p    *pager.Pager
	}{{indexFileName, g.pager}, {primaryIndexFileName, g.primary.Pager()}} {
		token, e := g.stablePagerNamespaceToken(dir, entry.name, entry.p)
		if e != nil {
			return e
		}
		e = token.Stabilize()
		token.Release()
		if e != nil {
			return e
		}
	}
	return nil
}
