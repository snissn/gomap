package collections

// This file owns the M1 durable *identity* record.  It intentionally does not
// build packs or route queries; later milestones consume this record.

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

const VectorPartitionManifestFormatV1 = "vector_partition_manifest_v1"
const vectorPartitionManifestMagicV1 uint32 = 0x56504d31 // VPM1

var ErrVectorPartitionManifestInvalid = errors.New("collections: invalid vector partition manifest")

// VectorPartitionManifestLimits caps decoded state before any count-derived
// allocation. Defaults deliberately leave ample headroom below int limits.
type VectorPartitionManifestLimits struct{ MaxBytes, MaxPartitions, MaxMemberships, MaxAssets, MaxStringBytes, MaxMembershipsPerVector, MaxRepresentativesPerPartition int }

func DefaultVectorPartitionManifestLimits() VectorPartitionManifestLimits {
	return VectorPartitionManifestLimits{16 << 20, 1 << 16, 1 << 20, 1 << 18, 4096, 16, 1 << 16}
}

type VectorPartitionAssetV1 struct {
	ID, Path, Checksum string
	Bytes              uint64
}
type VectorPartitionPlacementV1 struct {
	PartitionID uint32
	GroupID     string
}
type VectorPartitionMembershipV1 struct {
	VectorOrdinal uint64
	PartitionID   uint32
}

// VectorPartitionManifestV1 is canonical only when its variable-length lists
// are sorted. ReadySetDigest is SHA-256 of its required assets and placements.
type VectorPartitionManifestV1 struct {
	Format, State                                                      string
	Collection, IndexName, IndexDefinitionDigest                       string
	SourceGeneration, SourceChecksum, SourceSchemaHash, SourceRowCount uint64
	Generation, RouterGeneration                                       uint64
	PartitionCount                                                     uint32
	BalancePolicy                                                      string
	Placements                                                         []VectorPartitionPlacementV1
	Memberships                                                        []VectorPartitionMembershipV1
	OverlapMemberships                                                 []VectorPartitionMembershipV1
	Representatives                                                    []VectorPartitionMembershipV1
	Assets                                                             []VectorPartitionAssetV1
	RouterAsset                                                        VectorPartitionAssetV1
	ReadySetDigest                                                     string
}

func VectorIndexDefinitionDigestV1(d VectorIndexDefinition) string {
	// Fixed field order avoids JSON/map ambiguity and binds the declared index,
	// not a loose duplicate of its schema.
	b := bytes.NewBuffer(nil)
	for _, s := range []string{d.Name, d.Field, d.Metric.String(), d.Encoding.String(), string(d.Strategy)} {
		putStringVPM(b, s)
	}
	for _, n := range []uint64{uint64(d.Dimensions), uint64(d.M), uint64(d.EfConstruction), uint64(d.EfSearch), d.SchemaGeneration} {
		var x [8]byte
		binary.BigEndian.PutUint64(x[:], n)
		b.Write(x[:])
	}
	h := sha256.Sum256(b.Bytes())
	return hex.EncodeToString(h[:])
}

func (m VectorPartitionManifestV1) Validate(l VectorPartitionManifestLimits) error {
	if l.MaxBytes <= 0 {
		l = DefaultVectorPartitionManifestLimits()
	}
	if m.Format != VectorPartitionManifestFormatV1 || (m.State != "building" && m.State != "ready") || m.Collection == "" || m.IndexName == "" || !isSHA256VPM(m.IndexDefinitionDigest) || m.Generation == 0 || m.SourceGeneration == 0 || m.SourceRowCount == 0 || m.PartitionCount == 0 || int(m.PartitionCount) > l.MaxPartitions {
		return fmt.Errorf("%w: identity or partition bounds", ErrVectorPartitionManifestInvalid)
	}
	if len(m.Placements) != int(m.PartitionCount) || len(m.Assets) == 0 || len(m.Assets) > l.MaxAssets || len(m.Memberships) != int(m.SourceRowCount) || len(m.OverlapMemberships) > l.MaxMemberships || len(m.Representatives) > l.MaxMemberships || (m.State == "ready" && !isSHA256VPM(m.ReadySetDigest)) {
		return fmt.Errorf("%w: incomplete ready set or capped list", ErrVectorPartitionManifestInvalid)
	}
	if m.State == "ready" {
		if m.RouterGeneration != m.Generation {
			return fmt.Errorf("%w: router generation", ErrVectorPartitionManifestInvalid)
		}
		if err := validateAssetVPM(m.RouterAsset, l); err != nil {
			return err
		}
	} else if m.RouterGeneration != 0 || m.ReadySetDigest != "" || m.RouterAsset != (VectorPartitionAssetV1{}) {
		return fmt.Errorf("%w: building ready references", ErrVectorPartitionManifestInvalid)
	}
	seenP := make(map[uint32]struct{}, len(m.Placements))
	lastP := uint32(0)
	for i, p := range m.Placements {
		if p.GroupID == "" || int(p.PartitionID) >= int(m.PartitionCount) || (i > 0 && p.PartitionID <= lastP) {
			return fmt.Errorf("%w: noncanonical placement", ErrVectorPartitionManifestInvalid)
		}
		if _, ok := seenP[p.PartitionID]; ok {
			return fmt.Errorf("%w: duplicate partition", ErrVectorPartitionManifestInvalid)
		}
		seenP[p.PartitionID] = struct{}{}
		lastP = p.PartitionID
	}
	for i := uint32(0); i < m.PartitionCount; i++ {
		if _, ok := seenP[i]; !ok {
			return fmt.Errorf("%w: missing partition %d", ErrVectorPartitionManifestInvalid, i)
		}
	}
	if err := validateMembershipsVPM(m.Memberships, seenP, "membership", m.SourceRowCount, true); err != nil {
		return err
	}
	if err := validateMembershipsVPM(m.OverlapMemberships, seenP, "overlap", m.SourceRowCount, false); err != nil {
		return err
	}
	if err := validateMembershipsVPM(m.Representatives, seenP, "representative", m.SourceRowCount, false); err != nil {
		return err
	}
	lastID := ""
	for _, a := range m.Assets {
		if err := validateAssetVPM(a, l); err != nil {
			return err
		}
		if a.ID <= lastID {
			return fmt.Errorf("%w: noncanonical assets", ErrVectorPartitionManifestInvalid)
		}
		lastID = a.ID
	}
	if m.State == "ready" && m.ReadySetDigest != m.readyDigest() {
		return fmt.Errorf("%w: ready-set digest mismatch", ErrVectorPartitionManifestInvalid)
	}
	return nil
}
func validateAssetVPM(a VectorPartitionAssetV1, l VectorPartitionManifestLimits) error {
	if a.ID == "" || a.Path == "" || len(a.ID) > l.MaxStringBytes || len(a.Path) > l.MaxStringBytes || !isSHA256VPM(a.Checksum) {
		return fmt.Errorf("%w: asset", ErrVectorPartitionManifestInvalid)
	}
	return nil
}
func validateMembershipsVPM(ms []VectorPartitionMembershipV1, ps map[uint32]struct{}, kind string, rows uint64, base bool) error {
	var prev VectorPartitionMembershipV1
	counts := make(map[uint64]int)
	for i, x := range ms {
		if x.VectorOrdinal >= rows || counts[x.VectorOrdinal] >= 16 {
			return fmt.Errorf("%w: %s ordinal/cap", ErrVectorPartitionManifestInvalid, kind)
		}
		counts[x.VectorOrdinal]++
		if _, ok := ps[x.PartitionID]; !ok {
			return fmt.Errorf("%w: %s unknown partition", ErrVectorPartitionManifestInvalid, kind)
		}
		if i > 0 && (x.VectorOrdinal < prev.VectorOrdinal || x.VectorOrdinal == prev.VectorOrdinal && x.PartitionID <= prev.PartitionID) {
			return fmt.Errorf("%w: noncanonical %s", ErrVectorPartitionManifestInvalid, kind)
		}
		prev = x
	}
	if base {
		for i := uint64(0); i < rows; i++ {
			if counts[i] != 1 {
				return fmt.Errorf("%w: disjoint coverage", ErrVectorPartitionManifestInvalid)
			}
		}
	}
	return nil
}
func isSHA256VPM(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, e := hex.DecodeString(s)
	return e == nil
}
func (m VectorPartitionManifestV1) readyDigest() string {
	b := bytes.NewBuffer(nil)
	for _, p := range m.Placements {
		putU32VPM(b, p.PartitionID)
		putStringVPM(b, p.GroupID)
	}
	for _, a := range append(append([]VectorPartitionAssetV1(nil), m.Assets...), m.RouterAsset) {
		putStringVPM(b, a.ID)
		putStringVPM(b, a.Path)
		putStringVPM(b, a.Checksum)
		putU64VPM(b, a.Bytes)
	}
	h := sha256.Sum256(b.Bytes())
	return hex.EncodeToString(h[:])
}

// Canonicalize sorts caller-provided lists then fills the ready-set digest.
func (m *VectorPartitionManifestV1) Canonicalize() {
	if m.Format == "" {
		m.Format = VectorPartitionManifestFormatV1
	}
	if m.State == "" {
		m.State = "building"
	}
	sort.Slice(m.Placements, func(i, j int) bool { return m.Placements[i].PartitionID < m.Placements[j].PartitionID })
	sort.Slice(m.Memberships, func(i, j int) bool {
		a, b := m.Memberships[i], m.Memberships[j]
		return a.VectorOrdinal < b.VectorOrdinal || a.VectorOrdinal == b.VectorOrdinal && a.PartitionID < b.PartitionID
	})
	sort.Slice(m.Representatives, func(i, j int) bool {
		a, b := m.Representatives[i], m.Representatives[j]
		return a.VectorOrdinal < b.VectorOrdinal || a.VectorOrdinal == b.VectorOrdinal && a.PartitionID < b.PartitionID
	})
	sort.Slice(m.OverlapMemberships, func(i, j int) bool {
		a, b := m.OverlapMemberships[i], m.OverlapMemberships[j]
		return a.VectorOrdinal < b.VectorOrdinal || a.VectorOrdinal == b.VectorOrdinal && a.PartitionID < b.PartitionID
	})
	sort.Slice(m.Assets, func(i, j int) bool { return m.Assets[i].ID < m.Assets[j].ID })
	if m.State == "ready" {
		m.ReadySetDigest = m.readyDigest()
	}
}

// EncodeVectorPartitionManifestV1 is a strict, stable binary record. There
// are no tagged optional fields: newer records fail closed on this decoder.
func EncodeVectorPartitionManifestV1(m VectorPartitionManifestV1) ([]byte, error) {
	m.Canonicalize()
	if err := m.Validate(DefaultVectorPartitionManifestLimits()); err != nil {
		return nil, err
	}
	b := bytes.NewBuffer(nil)
	var x [4]byte
	binary.BigEndian.PutUint32(x[:], vectorPartitionManifestMagicV1)
	b.Write(x[:])
	putU32VPM(b, 1)
	for _, s := range []string{m.Format, m.State, m.Collection, m.IndexName, m.IndexDefinitionDigest, m.BalancePolicy, m.ReadySetDigest} {
		putStringVPM(b, s)
	}
	for _, n := range []uint64{m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash, m.SourceRowCount, m.Generation, m.RouterGeneration} {
		putU64VPM(b, n)
	}
	putU32VPM(b, m.PartitionCount)
	putAssetsVPM(b, []VectorPartitionAssetV1{m.RouterAsset})
	putPlacementsVPM(b, m.Placements)
	putMembershipsVPM(b, m.Memberships)
	putMembershipsVPM(b, m.OverlapMemberships)
	putMembershipsVPM(b, m.Representatives)
	putAssetsVPM(b, m.Assets)
	return b.Bytes(), nil
}
func DecodeVectorPartitionManifestV1(raw []byte, l VectorPartitionManifestLimits) (VectorPartitionManifestV1, error) {
	if l.MaxBytes <= 0 {
		l = DefaultVectorPartitionManifestLimits()
	}
	if len(raw) > l.MaxBytes {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: encoded bytes cap", ErrVectorPartitionManifestInvalid)
	}
	r := vpmReader{b: raw, l: l}
	if r.u32() != vectorPartitionManifestMagicV1 || r.u32() != 1 {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: magic/version", ErrVectorPartitionManifestInvalid)
	}
	m := VectorPartitionManifestV1{}
	ss := []*string{&m.Format, &m.State, &m.Collection, &m.IndexName, &m.IndexDefinitionDigest, &m.BalancePolicy, &m.ReadySetDigest}
	for _, p := range ss {
		*p = r.str()
	}
	m.SourceGeneration = r.u64()
	m.SourceChecksum = r.u64()
	m.SourceSchemaHash = r.u64()
	m.SourceRowCount = r.u64()
	m.Generation = r.u64()
	m.RouterGeneration = r.u64()
	m.PartitionCount = r.u32()
	ra := r.assets()
	if len(ra) == 1 {
		m.RouterAsset = ra[0]
	}
	m.Placements = r.placements()
	m.Memberships = r.memberships()
	m.OverlapMemberships = r.memberships()
	m.Representatives = r.memberships()
	m.Assets = r.assets()
	if r.err != nil || r.off != len(raw) {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: truncated, over-cap, or trailing record: %v", ErrVectorPartitionManifestInvalid, r.err)
	}
	if err := m.Validate(l); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	return m, nil
}

type vpmReader struct {
	b   []byte
	off int
	l   VectorPartitionManifestLimits
	err error
}

func (r *vpmReader) u32() uint32 {
	if r.off+4 > len(r.b) {
		r.err = errors.New("truncated")
		return 0
	}
	v := binary.BigEndian.Uint32(r.b[r.off:])
	r.off += 4
	return v
}
func (r *vpmReader) u64() uint64 {
	if r.off+8 > len(r.b) {
		r.err = errors.New("truncated")
		return 0
	}
	v := binary.BigEndian.Uint64(r.b[r.off:])
	r.off += 8
	return v
}
func (r *vpmReader) str() string {
	n := r.u32()
	if r.err != nil || n > uint32(r.l.MaxStringBytes) || uint64(r.off)+uint64(n) > uint64(len(r.b)) {
		r.err = errors.New("string cap/truncated")
		return ""
	}
	s := string(r.b[r.off : r.off+int(n)])
	r.off += int(n)
	return s
}
func (r *vpmReader) count(max int) int {
	n := r.u32()
	if r.err != nil || uint64(n) > uint64(max) {
		r.err = errors.New("count cap")
		return 0
	}
	return int(n)
}
func (r *vpmReader) assets() []VectorPartitionAssetV1 {
	n := r.count(r.l.MaxAssets)
	if r.err != nil {
		return nil
	}
	x := make([]VectorPartitionAssetV1, n)
	for i := range x {
		x[i] = VectorPartitionAssetV1{r.str(), r.str(), r.str(), r.u64()}
	}
	return x
}
func (r *vpmReader) placements() []VectorPartitionPlacementV1 {
	n := r.count(r.l.MaxPartitions)
	if r.err != nil {
		return nil
	}
	x := make([]VectorPartitionPlacementV1, n)
	for i := range x {
		x[i] = VectorPartitionPlacementV1{r.u32(), r.str()}
	}
	return x
}
func (r *vpmReader) memberships() []VectorPartitionMembershipV1 {
	n := r.count(r.l.MaxMemberships)
	if r.err != nil {
		return nil
	}
	x := make([]VectorPartitionMembershipV1, n)
	for i := range x {
		x[i] = VectorPartitionMembershipV1{r.u64(), r.u32()}
	}
	return x
}
func putU32VPM(b *bytes.Buffer, n uint32) {
	var x [4]byte
	binary.BigEndian.PutUint32(x[:], n)
	b.Write(x[:])
}
func putU64VPM(b *bytes.Buffer, n uint64) {
	var x [8]byte
	binary.BigEndian.PutUint64(x[:], n)
	b.Write(x[:])
}
func putStringVPM(b *bytes.Buffer, s string) { putU32VPM(b, uint32(len(s))); b.WriteString(s) }
func putAssetsVPM(b *bytes.Buffer, x []VectorPartitionAssetV1) {
	putU32VPM(b, uint32(len(x)))
	for _, a := range x {
		putStringVPM(b, a.ID)
		putStringVPM(b, a.Path)
		putStringVPM(b, a.Checksum)
		putU64VPM(b, a.Bytes)
	}
}
func putPlacementsVPM(b *bytes.Buffer, x []VectorPartitionPlacementV1) {
	putU32VPM(b, uint32(len(x)))
	for _, p := range x {
		putU32VPM(b, p.PartitionID)
		putStringVPM(b, p.GroupID)
	}
}
func putMembershipsVPM(b *bytes.Buffer, x []VectorPartitionMembershipV1) {
	putU32VPM(b, uint32(len(x)))
	for _, m := range x {
		putU64VPM(b, m.VectorOrdinal)
		putU32VPM(b, m.PartitionID)
	}
}

// VectorPartitionStoreV1 uses write-sync-rename-sync publication. The active
// pointer changes only after its complete generation has been made durable.
type VectorPartitionStoreV1 struct{ dir string }

func OpenVectorPartitionStoreV1(root string) (*VectorPartitionStoreV1, error) {
	if root == "" {
		return nil, errors.New("collections: empty vector partition store root")
	}
	d := filepath.Join(root, "vector_partitions")
	if err := os.MkdirAll(d, 0700); err != nil {
		return nil, err
	}
	return &VectorPartitionStoreV1{d}, nil
}
func (s *VectorPartitionStoreV1) Publish(m VectorPartitionManifestV1) error {
	raw, e := EncodeVectorPartitionManifestV1(m)
	if e != nil {
		return e
	}
	name := fmt.Sprintf("%s-%s-%d.vpm", safeVPM(m.Collection), safeVPM(m.IndexName), m.Generation)
	tmp := filepath.Join(s.dir, "."+name+".tmp")
	if e = os.WriteFile(tmp, raw, 0600); e != nil {
		return e
	}
	if e = syncFileVPM(tmp); e != nil {
		return e
	}
	if e = os.Rename(tmp, filepath.Join(s.dir, name)); e != nil {
		return e
	}
	if m.State == "ready" {
		active := filepath.Join(s.dir, safeVPM(m.Collection)+"-"+safeVPM(m.IndexName)+".active")
		atmp := active + ".tmp"
		if e = os.WriteFile(atmp, []byte(fmt.Sprintf("%d\n", m.Generation)), 0600); e != nil {
			return e
		}
		if e = syncFileVPM(atmp); e != nil {
			return e
		}
		if e = os.Rename(atmp, active); e != nil {
			return e
		}
	}
	return syncDirVPM(s.dir)
}
func (s *VectorPartitionStoreV1) Open(collection, index string, generation uint64) (VectorPartitionManifestV1, error) {
	p := filepath.Join(s.dir, fmt.Sprintf("%s-%s-%d.vpm", safeVPM(collection), safeVPM(index), generation))
	info, e := os.Stat(p)
	if e != nil {
		return VectorPartitionManifestV1{}, e
	}
	if info.Size() < 0 || info.Size() > int64(DefaultVectorPartitionManifestLimits().MaxBytes) {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: stored bytes cap", ErrVectorPartitionManifestInvalid)
	}
	raw, e := os.ReadFile(p)
	if e != nil {
		return VectorPartitionManifestV1{}, e
	}
	return DecodeVectorPartitionManifestV1(raw, DefaultVectorPartitionManifestLimits())
}
func (s *VectorPartitionStoreV1) OpenActive(collection, index string) (VectorPartitionManifestV1, error) {
	p := filepath.Join(s.dir, safeVPM(collection)+"-"+safeVPM(index)+".active")
	raw, e := os.ReadFile(p)
	if e != nil {
		return VectorPartitionManifestV1{}, e
	}
	var generation uint64
	if _, e = fmt.Sscanf(string(raw), "%d\n", &generation); e != nil || generation == 0 {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: active pointer", ErrVectorPartitionManifestInvalid)
	}
	return s.Open(collection, index, generation)
}

// VectorPartitionCleanupEligibilityV1 makes every external reachability
// condition explicit. M1 never infers catalog or backup references.
type VectorPartitionCleanupEligibilityV1 struct {
	Active                                            bool
	ReaderPins, SnapshotReferences, CatalogReferences uint64
}

func (e VectorPartitionCleanupEligibilityV1) Deletable() bool {
	return !e.Active && e.ReaderPins == 0 && e.SnapshotReferences == 0 && e.CatalogReferences == 0
}

func (s *VectorPartitionStoreV1) Delete(collection, index string, generation uint64, eligibility VectorPartitionCleanupEligibilityV1) error {
	if !eligibility.Deletable() {
		return fmt.Errorf("collections: vector partition generation %d is still reachable", generation)
	}
	p := filepath.Join(s.dir, fmt.Sprintf("%s-%s-%d.vpm", safeVPM(collection), safeVPM(index), generation))
	if e := os.Remove(p); e != nil {
		return e
	}
	return syncDirVPM(s.dir)
}

type VectorPartitionStatusV1 struct {
	Manifest                                                VectorPartitionManifestV1
	Ready                                                   bool
	StaleReason                                             string
	PartitionCount, GroupCount                              uint32
	Memberships, OverlapMemberships, AssetBytes, ReaderPins uint64
}

// PublishVectorPartitionManifestV1 binds the durable generation to this
// collection's currently declared vector-index definition before publication.
func (c *Collection) PublishVectorPartitionManifestV1(m VectorPartitionManifestV1) error {
	if c == nil || c.db == nil {
		return errors.New("collections: closed collection")
	}
	c.catalogMu.RLock()
	defer c.catalogMu.RUnlock()
	if m.Collection != c.name {
		return fmt.Errorf("collections: vector partition collection %q does not match %q", m.Collection, c.name)
	}
	var def *VectorIndexDefinition
	for i := range c.meta.VectorIndexes {
		if c.meta.VectorIndexes[i].Name == m.IndexName {
			def = &c.meta.VectorIndexes[i]
			break
		}
	}
	if def == nil {
		return fmt.Errorf("collections: unknown vector index %q", m.IndexName)
	}
	if m.IndexDefinitionDigest != VectorIndexDefinitionDigestV1(*def) {
		return errors.New("collections: vector partition index definition digest mismatch")
	}
	s, e := OpenVectorPartitionStoreV1(c.db.Dir())
	if e != nil {
		return e
	}
	return s.Publish(m)
}

func (c *Collection) VectorPartitionStatusV1(index string, generation uint64) (VectorPartitionStatusV1, error) {
	if c == nil || c.db == nil {
		return VectorPartitionStatusV1{}, errors.New("collections: closed collection")
	}
	s, e := OpenVectorPartitionStoreV1(c.db.Dir())
	if e != nil {
		return VectorPartitionStatusV1{}, e
	}
	m, e := s.Open(c.name, index, generation)
	if e != nil {
		return VectorPartitionStatusV1{}, e
	}
	groups, seen := map[string]struct{}{}, map[uint64]struct{}{}
	var total, overlap uint64
	for _, p := range m.Placements {
		groups[p.GroupID] = struct{}{}
	}
	for _, a := range m.Assets {
		total += a.Bytes
	}
	total += m.RouterAsset.Bytes
	for _, x := range m.Memberships {
		if _, ok := seen[x.VectorOrdinal]; ok {
			overlap++
		}
		seen[x.VectorOrdinal] = struct{}{}
	}
	return VectorPartitionStatusV1{Manifest: m, Ready: true, PartitionCount: m.PartitionCount, GroupCount: uint32(len(groups)), Memberships: uint64(len(m.Memberships)), OverlapMemberships: overlap, AssetBytes: total}, nil
}
func safeVPM(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func syncFileVPM(p string) error {
	f, e := os.OpenFile(p, os.O_RDWR, 0)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func syncDirVPM(p string) error {
	f, e := os.Open(p)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
