package collections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	source "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

const (
	MaxVectorPartitionDirectoryPageBytesV2   = 64 << 10
	MaxVectorPartitionDirectoryPageEntriesV2 = 128
	maxVectorPartitionDirectoryDepthV2       = 16
)

// VectorPartitionSourceRowIdentityV2 preserves semantic source provenance.
// Ordinal is scoped to the exact shard snapshot, never to a global row array.
type VectorPartitionSourceRowIdentityV2 = source.ANNSourceRowIdentityV2

// VectorPartitionDirectoryRecordV2 is one bounded leaf record. Source pages
// contain snapshots; metadata pages contain domain members or graph assets.
// Exactly one payload is present. Owner is a source owner for snapshots and an
// ANN placement owner for metadata; these authorities need not have equal sets.
type VectorPartitionDirectoryRecordV2 struct {
	Owner    string
	DomainID uint64
	Snapshot *VectorPartitionSourceSnapshotV2    `json:",omitempty"`
	Member   *VectorPartitionSourceRowIdentityV2 `json:",omitempty"`
	Asset    *VectorPartitionAssetV1             `json:",omitempty"`
}

type VectorPartitionDirectoryChildV2 struct {
	First, Last string
	Count       uint64
	Asset       VectorPartitionAssetV1
}

// VectorPartitionDirectoryPageV2 has bounded fanout and depth. Child ranges
// permit owner/domain selection without opening remote membership pages. Count
// is the exact number of leaf records below this page, not an allocation hint.
type VectorPartitionDirectoryPageV2 struct {
	Version     uint32
	Kind        string // "source" or "metadata"
	Level       uint32
	Generation  uint64
	First, Last string
	Count       uint64
	Children    []VectorPartitionDirectoryChildV2  `json:",omitempty"`
	Records     []VectorPartitionDirectoryRecordV2 `json:",omitempty"`
}

func vectorPartitionDirectoryOwnerPrefixV2(owner string) string { return owner + "\x00" }
func vectorPartitionDirectoryDomainPrefixV2(owner string, domain uint64) string {
	return fmt.Sprintf("%s\x00%016x\x00", owner, domain)
}

func (r VectorPartitionDirectoryRecordV2) keyV2(kind string) (string, error) {
	bad := func() (string, error) {
		return "", fmt.Errorf("%w: directory record", ErrVectorPartitionManifestInvalid)
	}
	if r.Owner == "" || len(r.Owner) > 1024 || strings.ContainsRune(r.Owner, 0) {
		return bad()
	}
	n := 0
	if r.Snapshot != nil {
		n++
	}
	if r.Member != nil {
		n++
	}
	if r.Asset != nil {
		n++
	}
	if n != 1 {
		return bad()
	}
	if kind == "source" {
		if r.Snapshot == nil || r.DomainID != 0 {
			return bad()
		}
		s := *r.Snapshot
		sealed, err := source.SealSourceSnapshotV2(s, s.MerkleRoot)
		if err != nil || sealed != s || strings.ContainsRune(s.ShardID, 0) {
			return bad()
		}
		return vectorPartitionDirectoryOwnerPrefixV2(r.Owner) + s.ShardID + "\x00", nil
	}
	if kind != "metadata" || r.Snapshot != nil {
		return bad()
	}
	prefix := vectorPartitionDirectoryDomainPrefixV2(r.Owner, r.DomainID)
	if r.Member != nil {
		m := r.Member
		if m.SourceOwner == "" || len(m.SourceOwner) > 1024 || strings.ContainsRune(m.SourceOwner, 0) || m.ShardID == "" || len(m.ShardID) > 1024 || strings.ContainsRune(m.ShardID, 0) || m.SnapshotRevision == 0 || m.SnapshotDigest == ([sha256.Size]byte{}) || m.DocumentRevision == 0 {
			return bad()
		}
		return fmt.Sprintf("%sm\x00%s\x00%016x", prefix, m.SourceOwner+"\x00"+m.ShardID, m.Ordinal), nil
	}
	if err := validateAssetVPM(*r.Asset, DefaultVectorPartitionManifestLimits()); err != nil {
		return "", err
	}
	if strings.ContainsRune(r.Asset.ID, 0) {
		return bad()
	}
	return prefix + "a\x00" + r.Asset.ID, nil
}

func (p VectorPartitionDirectoryPageV2) validateV2() error {
	bad := func() error { return fmt.Errorf("%w: directory page", ErrVectorPartitionManifestInvalid) }
	if p.Version != 2 || (p.Kind != "source" && p.Kind != "metadata") || p.Generation == 0 || p.Level >= maxVectorPartitionDirectoryDepthV2 || p.Count == 0 || p.First == "" || p.First > p.Last || len(p.First) > 4096 || len(p.Last) > 4096 {
		return bad()
	}
	var first, last string
	var count uint64
	if p.Level == 0 {
		if len(p.Children) != 0 || len(p.Records) == 0 || len(p.Records) > MaxVectorPartitionDirectoryPageEntriesV2 {
			return bad()
		}
		for _, r := range p.Records {
			key, err := r.keyV2(p.Kind)
			if err != nil {
				return err
			}
			if count != 0 && key <= last {
				return bad()
			}
			if count == 0 {
				first = key
			}
			last = key
			count++
			if r.Asset != nil && r.Asset.Ref.Generation != p.Generation {
				return bad()
			}
		}
	} else {
		if len(p.Records) != 0 || len(p.Children) < 1 || len(p.Children) > MaxVectorPartitionDirectoryPageEntriesV2 {
			return bad()
		}
		for _, c := range p.Children {
			if c.Count == 0 || c.First == "" || c.First > c.Last || len(c.First) > 4096 || len(c.Last) > 4096 || c.Count > ^uint64(0)-count || (count != 0 && c.First <= last) || c.Asset.Ref.Generation != p.Generation || c.Asset.Bytes > MaxVectorPartitionDirectoryPageBytesV2 || c.Asset.Bytes == 0 {
				return bad()
			}
			if err := validateAssetVPM(c.Asset, DefaultVectorPartitionManifestLimits()); err != nil {
				return err
			}
			if count == 0 {
				first = c.First
			}
			last = c.Last
			count += c.Count
		}
	}
	if first != p.First || last != p.Last || count != p.Count {
		return bad()
	}
	return nil
}

func EncodeVectorPartitionDirectoryPageV2(p VectorPartitionDirectoryPageV2) ([]byte, error) {
	if err := p.validateV2(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	if len(payload) > MaxVectorPartitionDirectoryPageBytesV2-8 {
		return nil, fmt.Errorf("%w: directory page byte cap", ErrVectorPartitionManifestInvalid)
	}
	raw := make([]byte, 8+len(payload))
	copy(raw, "VDP2")
	binary.BigEndian.PutUint32(raw[4:8], uint32(len(payload)))
	copy(raw[8:], payload)
	return raw, nil
}

func DecodeVectorPartitionDirectoryPageV2(raw []byte) (VectorPartitionDirectoryPageV2, error) {
	var p VectorPartitionDirectoryPageV2
	if len(raw) < 8 || len(raw) > MaxVectorPartitionDirectoryPageBytesV2 || string(raw[:4]) != "VDP2" || uint64(binary.BigEndian.Uint32(raw[4:8])) != uint64(len(raw)-8) {
		return p, fmt.Errorf("%w: directory page envelope", ErrVectorPartitionManifestInvalid)
	}
	dec := json.NewDecoder(bytes.NewReader(raw[8:]))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return VectorPartitionDirectoryPageV2{}, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return VectorPartitionDirectoryPageV2{}, fmt.Errorf("%w: trailing directory page", ErrVectorPartitionManifestInvalid)
	}
	canonical, err := EncodeVectorPartitionDirectoryPageV2(p)
	if err != nil {
		return VectorPartitionDirectoryPageV2{}, err
	}
	if !bytes.Equal(raw, canonical) {
		return VectorPartitionDirectoryPageV2{}, fmt.Errorf("%w: noncanonical directory page", ErrVectorPartitionManifestInvalid)
	}
	return p, nil
}

// walkVectorPartitionDirectoryV2 is the common bounded traversal for local
// validation and transitive asset closure. An empty prefix visits all owners;
// a nonempty canonical owner/domain prefix never opens disjoint child pages.
// Callbacks must not retain page-backed values. No count-derived allocation or
// corpus-sized visited set is used; strictly decreasing levels bound cycles.
func walkVectorPartitionDirectoryV2(ctx context.Context, root VectorPartitionAssetV1, kind, prefix string, read func(VectorPartitionAssetV1) ([]byte, error), asset func(VectorPartitionAssetV1) error, record func(VectorPartitionDirectoryRecordV2) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var walk func(VectorPartitionAssetV1, *VectorPartitionDirectoryChildV2, uint32) error
	walk = func(ref VectorPartitionAssetV1, expected *VectorPartitionDirectoryChildV2, parentLevel uint32) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := validateAssetVPM(ref, DefaultVectorPartitionManifestLimits()); err != nil {
			return err
		}
		if ref.Bytes == 0 || ref.Bytes > MaxVectorPartitionDirectoryPageBytesV2 {
			return fmt.Errorf("%w: directory reference bytes", ErrVectorPartitionManifestInvalid)
		}
		raw, err := read(ref)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(raw)
		if uint64(len(raw)) != ref.Bytes || hex.EncodeToString(sum[:]) != ref.Checksum {
			return fmt.Errorf("%w: directory asset checksum", ErrVectorPartitionManifestInvalid)
		}
		p, err := DecodeVectorPartitionDirectoryPageV2(raw)
		if err != nil {
			return err
		}
		if p.Kind != kind || p.Generation != root.Ref.Generation || (expected != nil && (p.Level+1 != parentLevel || p.First != expected.First || p.Last != expected.Last || p.Count != expected.Count)) {
			return fmt.Errorf("%w: directory child binding", ErrVectorPartitionManifestInvalid)
		}
		if asset != nil {
			if err := asset(ref); err != nil {
				return err
			}
		}
		for _, c := range p.Children {
			if prefix != "" && (c.Last < prefix || c.First >= prefix+"\xff") {
				continue
			}
			if err := walk(c.Asset, &c, p.Level); err != nil {
				return err
			}
		}
		for _, r := range p.Records {
			key, err := r.keyV2(kind)
			if err != nil {
				return err
			}
			if prefix != "" && !strings.HasPrefix(key, prefix) {
				continue
			}
			if r.Asset != nil && asset != nil {
				if err := asset(*r.Asset); err != nil {
					return err
				}
			}
			if record != nil {
				if err := record(r); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if read == nil {
		return errors.New("collections: directory reader required")
	}
	return walk(root, nil, maxVectorPartitionDirectoryDepthV2)
}

// Reading one asset per cache lifetime avoids retaining segment maps for the
// whole owner corpus. The caller's snapshot/producer pins cover the traversal.
func readVectorPartitionDirectoryAssetV2(root string, ref VectorPartitionAssetV1) ([]byte, error) {
	cache, err := newColumnPhysicalAssetReadCache(root, ref.Ref.Namespace)
	if err != nil {
		return nil, err
	}
	raw, err := cache.read(ref.Ref, nil)
	// cache.read can return a mapped view; detach before closing its handle.
	if err == nil {
		raw = bytes.Clone(raw)
	}
	return raw, errors.Join(err, cache.close())
}

// walkVectorPartitionManifestAssetsV2 enumerates the complete local physical
// closure. Local owner scope never filters reclamation or snapshot traversal.
// Source TCD2 records live in the pinned DB root and are copied with that root;
// directory pages and graph/router assets are explicit physical dependencies.
func walkVectorPartitionManifestAssetsV2(ctx context.Context, assetRoot, namespace string, m VectorPartitionManifestV1, visit func(VectorPartitionAssetV1) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := m.Validate(DefaultVectorPartitionManifestLimits()); err != nil {
		return err
	}
	check := func(a VectorPartitionAssetV1) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if a.Ref.Namespace != namespace || a.Ref.Generation != m.Generation {
			return fmt.Errorf("%w: foreign local physical dependency", ErrVectorPartitionManifestInvalid)
		}
		if err := verifyVectorPartitionAssetsWithContextV1(ctx, assetRoot, namespace, []VectorPartitionAssetV1{a}); err != nil {
			return err
		}
		if visit != nil {
			return visit(a)
		}
		return nil
	}
	if !m.isPagedRootV2() {
		for _, a := range m.Assets {
			if err := check(a); err != nil {
				return err
			}
		}
		if m.RouterAsset.Ref.Kind != "" {
			return check(m.RouterAsset)
		}
		return nil
	}
	r := m.PagedRootV2
	read := func(a VectorPartitionAssetV1) ([]byte, error) {
		if a.Ref.Namespace != namespace {
			return nil, fmt.Errorf("%w: foreign directory namespace", ErrVectorPartitionManifestInvalid)
		}
		return readVectorPartitionDirectoryAssetV2(assetRoot, a)
	}
	var shards, rows, domains, packs, members uint64
	var sourceOwner, annOwner, domain string
	var sourceOwners, annOwners int
	add := func(total *uint64, n uint64) error {
		if n > ^uint64(0)-*total {
			return fmt.Errorf("%w: local count overflow", ErrVectorPartitionManifestInvalid)
		}
		*total += n
		return nil
	}
	if r.SourceShardDirectory.Ref.Kind != "" {
		err := walkVectorPartitionDirectoryV2(ctx, r.SourceShardDirectory, "source", "", read, check, func(rec VectorPartitionDirectoryRecordV2) error {
			if rec.Owner != sourceOwner {
				if sourceOwners >= len(r.SourceOwners) || r.SourceOwners[sourceOwners] != rec.Owner {
					return fmt.Errorf("%w: source owner coverage", ErrVectorPartitionManifestInvalid)
				}
				sourceOwners++
				sourceOwner = rec.Owner
			}
			if err := add(&shards, 1); err != nil {
				return err
			}
			return add(&rows, rec.Snapshot.RowCount)
		})
		if err != nil {
			return err
		}
	}
	if r.MetadataDirectory.Ref.Kind != "" {
		err := walkVectorPartitionDirectoryV2(ctx, r.MetadataDirectory, "metadata", "", read, check, func(rec VectorPartitionDirectoryRecordV2) error {
			if rec.Owner != annOwner {
				if annOwners >= len(r.ANNOwners) || r.ANNOwners[annOwners] != rec.Owner {
					return fmt.Errorf("%w: ANN owner coverage", ErrVectorPartitionManifestInvalid)
				}
				annOwners++
				annOwner = rec.Owner
			}
			key := vectorPartitionDirectoryDomainPrefixV2(rec.Owner, rec.DomainID)
			if key != domain {
				if err := add(&domains, 1); err != nil {
					return err
				}
				domain = key
			}
			if rec.Member != nil {
				return add(&members, 1)
			}
			return add(&packs, 1)
		})
		if err != nil {
			return err
		}
	}
	if sourceOwners != len(r.SourceOwners) || annOwners != len(r.ANNOwners) || shards != r.LocalSourceShardCount || rows != r.LocalSourceRowCount || domains != r.LocalDomainCount || packs != r.LocalPhysicalPackCount || members != r.LocalMembershipCount {
		return fmt.Errorf("%w: exact local directory counts", ErrVectorPartitionManifestInvalid)
	}
	if m.RouterAsset.Ref.Kind != "" {
		return check(m.RouterAsset)
	}
	return nil
}
