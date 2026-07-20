package collections

// This file owns the M1 durable *identity* record.  It intentionally does not
// build packs or route queries; later milestones consume this record.

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
	ID, Checksum string
	Bytes        uint64
	Ref          ColumnAssetRef
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
	if m.SourceRowCount > uint64(l.MaxMemberships) || len(m.Collection) > l.MaxStringBytes || len(m.IndexName) > l.MaxStringBytes || len(m.State) > l.MaxStringBytes || len(m.BalancePolicy) > l.MaxStringBytes || len(m.IndexDefinitionDigest) > l.MaxStringBytes {
		return fmt.Errorf("%w: source/string cap", ErrVectorPartitionManifestInvalid)
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
		if p.GroupID == "" || len(p.GroupID) > l.MaxStringBytes || int(p.PartitionID) >= int(m.PartitionCount) || (i > 0 && p.PartitionID <= lastP) {
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
	if err := validateMembershipsVPM(m.Memberships, seenP, "membership", m.SourceRowCount, true, l.MaxMembershipsPerVector); err != nil {
		return err
	}
	if err := validateMembershipsVPM(m.OverlapMemberships, seenP, "overlap", m.SourceRowCount, false, l.MaxMembershipsPerVector); err != nil {
		return err
	}
	if err := validateMembershipsVPM(m.Representatives, seenP, "representative", m.SourceRowCount, false, l.MaxRepresentativesPerPartition); err != nil {
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
	if a.ID == "" || len(a.ID) > l.MaxStringBytes || !isSHA256VPM(a.Checksum) || a.Ref.Offset < 0 || a.Ref.Length < 0 || uint64(a.Ref.Length) != a.Bytes {
		return fmt.Errorf("%w: asset", ErrVectorPartitionManifestInvalid)
	}
	if err := validateColumnAssetRefForPlan(a.Ref); err != nil {
		return fmt.Errorf("%w: asset ref", ErrVectorPartitionManifestInvalid)
	}
	return nil
}

func encodedSizeVPM(m VectorPartitionManifestV1, l VectorPartitionManifestLimits) (int, error) {
	// Every addition is checked before allocating the encoder buffer. The
	// decoder's byte cap is also the encoder's hard cap.
	n := uint64(8) // magic + version
	add := func(v uint64) error {
		if v > uint64(l.MaxBytes) || n > uint64(l.MaxBytes)-v {
			return fmt.Errorf("%w: encoded bytes cap", ErrVectorPartitionManifestInvalid)
		}
		n += v
		return nil
	}
	str := func(s string) error { return add(4 + uint64(len(s))) }
	asset := func(a VectorPartitionAssetV1) error {
		if err := str(a.ID); err != nil {
			return err
		}
		if err := str(a.Checksum); err != nil {
			return err
		}
		if err := str(string(a.Ref.Kind)); err != nil {
			return err
		}
		if err := str(a.Ref.Namespace); err != nil {
			return err
		}
		return add(8*5 + 4 + 4) // bytes + generation + part + offset + length + file + checksum
	}
	for _, s := range []string{m.Format, m.State, m.Collection, m.IndexName, m.IndexDefinitionDigest, m.BalancePolicy, m.ReadySetDigest} {
		if err := str(s); err != nil {
			return 0, err
		}
	}
	if err := add(8*6 + 4 + 4); err != nil {
		return 0, err
	} // fixed fields and router asset count
	if err := asset(m.RouterAsset); err != nil {
		return 0, err
	}
	if err := add(4 + uint64(len(m.Placements))*4); err != nil {
		return 0, err
	}
	for _, p := range m.Placements {
		if err := str(p.GroupID); err != nil {
			return 0, err
		}
	}
	for _, ms := range [][]VectorPartitionMembershipV1{m.Memberships, m.OverlapMemberships, m.Representatives} {
		if err := add(4 + uint64(len(ms))*12); err != nil {
			return 0, err
		}
	}
	if err := add(4); err != nil {
		return 0, err
	}
	for _, a := range m.Assets {
		if err := asset(a); err != nil {
			return 0, err
		}
	}
	return int(n), nil
}
func validateMembershipsVPM(ms []VectorPartitionMembershipV1, ps map[uint32]struct{}, kind string, rows uint64, base bool, capPer int) error {
	var prev VectorPartitionMembershipV1
	counts := make(map[uint64]int)
	for i, x := range ms {
		if x.VectorOrdinal >= rows || capPer <= 0 || counts[x.VectorOrdinal] >= capPer {
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
	if len(s) != 64 || s != strings.ToLower(s) {
		return false
	}
	_, e := hex.DecodeString(s)
	return e == nil
}
func (m VectorPartitionManifestV1) readyDigest() string {
	h := sha256.New()
	for _, p := range m.Placements {
		writeU32VPM(h, p.PartitionID)
		writeStringVPM(h, p.GroupID)
	}
	for _, a := range append(append([]VectorPartitionAssetV1(nil), m.Assets...), m.RouterAsset) {
		writeStringVPM(h, a.ID)
		writeStringVPM(h, a.Checksum)
		writeU64VPM(h, a.Bytes)
		writeColumnAssetRefVPM(h, a.Ref)
	}
	return hex.EncodeToString(h.Sum(nil))
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
	size, err := encodedSizeVPM(m, DefaultVectorPartitionManifestLimits())
	if err != nil {
		return nil, err
	}
	b := bytes.NewBuffer(make([]byte, 0, size))
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
		x[i] = VectorPartitionAssetV1{ID: r.str(), Checksum: r.str(), Bytes: r.u64(), Ref: r.columnRef()}
	}
	return x
}
func (r *vpmReader) columnRef() ColumnAssetRef {
	k, n, g, p, f, o, l, c := r.str(), r.str(), r.u64(), r.u64(), r.u32(), r.u64(), r.u64(), r.u32()
	if o > uint64(math.MaxInt64) || l > uint64(math.MaxInt64) {
		r.err = errors.New("column ref int64 overflow")
	}
	return ColumnAssetRef{Kind: ColumnAssetKind(k), Namespace: n, Generation: g, PartID: p, FileID: f, Offset: int64(o), Length: int64(l), Checksum: c}
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
func writeU32VPM(h interface{ Write([]byte) (int, error) }, n uint32) {
	var x [4]byte
	binary.BigEndian.PutUint32(x[:], n)
	_, _ = h.Write(x[:])
}
func writeU64VPM(h interface{ Write([]byte) (int, error) }, n uint64) {
	var x [8]byte
	binary.BigEndian.PutUint64(x[:], n)
	_, _ = h.Write(x[:])
}
func writeStringVPM(h interface{ Write([]byte) (int, error) }, s string) {
	writeU32VPM(h, uint32(len(s)))
	_, _ = h.Write([]byte(s))
}
func writeColumnAssetRefVPM(h interface{ Write([]byte) (int, error) }, r ColumnAssetRef) {
	writeStringVPM(h, string(r.Kind))
	writeStringVPM(h, r.Namespace)
	writeU64VPM(h, r.Generation)
	writeU64VPM(h, r.PartID)
	writeU32VPM(h, r.FileID)
	writeU64VPM(h, uint64(r.Offset))
	writeU64VPM(h, uint64(r.Length))
	writeU32VPM(h, r.Checksum)
}
func putStringVPM(b *bytes.Buffer, s string) { putU32VPM(b, uint32(len(s))); b.WriteString(s) }
func putAssetsVPM(b *bytes.Buffer, x []VectorPartitionAssetV1) {
	putU32VPM(b, uint32(len(x)))
	for _, a := range x {
		putStringVPM(b, a.ID)
		putStringVPM(b, a.Checksum)
		putU64VPM(b, a.Bytes)
		putColumnAssetRefVPM(b, a.Ref)
	}
}
func putColumnAssetRefVPM(b *bytes.Buffer, r ColumnAssetRef) {
	putStringVPM(b, string(r.Kind))
	putStringVPM(b, r.Namespace)
	putU64VPM(b, r.Generation)
	putU64VPM(b, r.PartID)
	putU32VPM(b, r.FileID)
	putU64VPM(b, uint64(r.Offset))
	putU64VPM(b, uint64(r.Length))
	putU32VPM(b, r.Checksum)
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

const (
	vectorPartitionStoreMaxEntriesV1 = 4096
	vectorPartitionStoreMaxBytesV1   = 64 << 20
)

func (c *Collection) vectorPartitionReachabilityRefsV1() ([]ColumnAssetRef, []ColumnAssetRef, error) {
	if c == nil || c.db == nil {
		return nil, nil, nil
	}
	s, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, nil, err
	}
	byIndex := make(map[string][]VectorPartitionManifestV1)
	prefix := safeVPM(c.name) + "-"
	var totalBytes int64
	var retained int
	activeNames := make(map[string]struct{})
	retiredNames := make(map[string]struct{})
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".active") {
			activeNames[entry.Name()] = struct{}{}
			continue
		}
		if strings.HasSuffix(entry.Name(), ".retired") {
			retiredNames[entry.Name()] = struct{}{}
			continue
		}
		if filepath.Ext(entry.Name()) != ".vpm" {
			return nil, nil, fmt.Errorf("collections: unexpected vector partition entry %q", entry.Name())
		}
		retained++
		if retained > vectorPartitionStoreMaxEntriesV1 {
			return nil, nil, fmt.Errorf("collections: vector partition retained manifest cap")
		}
		info, err := entry.Info()
		if err != nil {
			return nil, nil, err
		}
		if info.Size() < 0 || info.Size() > int64(DefaultVectorPartitionManifestLimits().MaxBytes) || totalBytes > vectorPartitionStoreMaxBytesV1-info.Size() {
			return nil, nil, fmt.Errorf("collections: vector partition retained bytes cap")
		}
		totalBytes += info.Size()
		raw, err := os.ReadFile(filepath.Join(s.dir, entry.Name()))
		if err != nil {
			return nil, nil, err
		}
		m, err := DecodeVectorPartitionManifestV1(raw, DefaultVectorPartitionManifestLimits())
		if err != nil {
			return nil, nil, fmt.Errorf("collections: vector partition manifest %q: %w", entry.Name(), err)
		}
		if m.Collection == c.name {
			byIndex[m.IndexName] = append(byIndex[m.IndexName], m)
		}
	}
	var prepared, pinned []ColumnAssetRef
	expectedActive := make(map[string]struct{})
	expectedRetired := make(map[string]struct{})
	for index, manifests := range byIndex {
		ready := false
		for _, m := range manifests {
			ready = ready || m.State == "ready"
			for _, asset := range append(append([]VectorPartitionAssetV1(nil), m.Assets...), m.RouterAsset) {
				if asset.Ref.Kind != "" {
					prepared = append(prepared, asset.Ref)
				}
			}
		}
		if !ready {
			continue
		}
		expectedActive[safeVPM(c.name)+"-"+safeVPM(index)+".active"] = struct{}{}
		active, err := s.OpenActive(c.name, index)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return nil, nil, fmt.Errorf("collections: vector partition active pointer for %q: %w", index, err)
			}
			retired, retiredErr := s.OpenRetired(c.name, index)
			if retiredErr != nil {
				return nil, nil, fmt.Errorf("collections: vector partition active/retired pointer for %q: %w", index, retiredErr)
			}
			expectedRetired[safeVPM(c.name)+"-"+safeVPM(index)+".retired"] = struct{}{}
			if !vectorPartitionManifestGenerationRetained(manifests, retired.Generation) {
				return nil, nil, fmt.Errorf("collections: retired vector partition generation %d for %q is not retained", retired.Generation, index)
			}
			continue
		}
		if active.State != "ready" {
			return nil, nil, fmt.Errorf("collections: vector partition active pointer for %q targets non-ready generation", index)
		}
		found := false
		for _, m := range manifests {
			if m.Generation != active.Generation {
				continue
			}
			found = true
			for _, asset := range append(append([]VectorPartitionAssetV1(nil), m.Assets...), m.RouterAsset) {
				if asset.Ref.Kind != "" {
					pinned = append(pinned, asset.Ref)
				}
			}
		}
		if !found {
			return nil, nil, fmt.Errorf("collections: vector partition active generation %d for %q is not retained", active.Generation, index)
		}
	}
	for activeName := range activeNames {
		if _, ok := expectedActive[activeName]; !ok {
			return nil, nil, fmt.Errorf("collections: orphan vector partition active pointer %q", activeName)
		}
	}
	for retiredName := range retiredNames {
		if _, ok := expectedRetired[retiredName]; !ok {
			return nil, nil, fmt.Errorf("collections: orphan vector partition retired pointer %q", retiredName)
		}
	}
	return prepared, pinned, nil
}
func vectorPartitionManifestGenerationRetained(manifests []VectorPartitionManifestV1, generation uint64) bool {
	for _, m := range manifests {
		if m.Generation == generation {
			return true
		}
	}
	return false
}

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

// OpenExistingVectorPartitionStoreV1 is read-only with respect to directory
// creation. Status and reclamation planning use it so observation cannot
// create durable state.
func OpenExistingVectorPartitionStoreV1(root string) (*VectorPartitionStoreV1, error) {
	if root == "" {
		return nil, errors.New("collections: empty vector partition store root")
	}
	d := filepath.Join(root, "vector_partitions")
	info, err := os.Stat(d)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%w: store is not a directory", ErrVectorPartitionManifestInvalid)
	}
	return &VectorPartitionStoreV1{d}, nil
}
func (s *VectorPartitionStoreV1) Publish(m VectorPartitionManifestV1) error {
	raw, e := EncodeVectorPartitionManifestV1(m)
	if e != nil {
		return e
	}
	name := fmt.Sprintf("%s-%s-%d.vpm", safeVPM(m.Collection), safeVPM(m.IndexName), m.Generation)
	tmp, e := s.uniqueTemp(name)
	if e != nil {
		return e
	}
	defer os.Remove(tmp)
	if e = os.WriteFile(tmp, raw, 0600); e != nil {
		return e
	}
	if e = syncFileVPM(tmp); e != nil {
		return e
	}
	if e = os.Rename(tmp, filepath.Join(s.dir, name)); e != nil {
		return e
	}
	if e = syncDirVPM(s.dir); e != nil {
		return e
	}
	if m.State == "ready" {
		active := filepath.Join(s.dir, safeVPM(m.Collection)+"-"+safeVPM(m.IndexName)+".active")
		retired := filepath.Join(s.dir, safeVPM(m.Collection)+"-"+safeVPM(m.IndexName)+".retired")
		if err := os.Remove(retired); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if e = syncDirVPM(s.dir); e != nil {
			return e
		}
		atmp, e := s.uniqueTemp(filepath.Base(active))
		if e != nil {
			return e
		}
		defer os.Remove(atmp)
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
func (s *VectorPartitionStoreV1) uniqueTemp(name string) (string, error) {
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return filepath.Join(s.dir, "."+name+"."+hex.EncodeToString(nonce[:])+".tmp"), nil
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
	return s.openPointer(collection, index, ".active")
}
func (s *VectorPartitionStoreV1) OpenRetired(collection, index string) (VectorPartitionManifestV1, error) {
	return s.openPointer(collection, index, ".retired")
}
func (s *VectorPartitionStoreV1) openPointer(collection, index, suffix string) (VectorPartitionManifestV1, error) {
	p := filepath.Join(s.dir, safeVPM(collection)+"-"+safeVPM(index)+suffix)
	info, e := os.Stat(p)
	if e != nil {
		return VectorPartitionManifestV1{}, e
	}
	if info.Size() < 2 || info.Size() > 32 {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: active pointer size", ErrVectorPartitionManifestInvalid)
	}
	raw, e := os.ReadFile(p)
	if e != nil {
		return VectorPartitionManifestV1{}, e
	}
	if raw[len(raw)-1] != '\n' || bytes.IndexByte(raw[:len(raw)-1], '\n') >= 0 {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: active pointer", ErrVectorPartitionManifestInvalid)
	}
	generation, e := strconv.ParseUint(string(raw[:len(raw)-1]), 10, 64)
	if e != nil || generation == 0 {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: active pointer", ErrVectorPartitionManifestInvalid)
	}
	return s.Open(collection, index, generation)
}

// Deactivate replaces the active pointer with a durable retired marker. The
// marker identifies the last active generation for audit/cleanup while making
// its assets prepared-only rather than query-pinned after the directory sync.
func (s *VectorPartitionStoreV1) Deactivate(collection, index string) error {
	active, err := s.OpenActive(collection, index)
	if err != nil {
		return err
	}
	retired := filepath.Join(s.dir, safeVPM(collection)+"-"+safeVPM(index)+".retired")
	tmp, err := s.uniqueTemp(filepath.Base(retired))
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	if err = os.WriteFile(tmp, []byte(fmt.Sprintf("%d\n", active.Generation)), 0600); err != nil {
		return err
	}
	if err = syncFileVPM(tmp); err != nil {
		return err
	}
	if err = os.Rename(tmp, retired); err != nil {
		return err
	}
	if err = syncDirVPM(s.dir); err != nil {
		return err
	}
	if err = os.Remove(filepath.Join(s.dir, safeVPM(collection)+"-"+safeVPM(index)+".active")); err != nil {
		return err
	}
	return syncDirVPM(s.dir)
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
	if active, err := s.OpenActive(collection, index); err == nil {
		if active.Generation == generation {
			return fmt.Errorf("collections: vector partition generation %d is active", generation)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("collections: vector partition cleanup active pointer: %w", err)
	}
	if retired, err := s.OpenRetired(collection, index); err == nil {
		if retired.Generation == generation {
			if err := os.Remove(filepath.Join(s.dir, safeVPM(collection)+"-"+safeVPM(index)+".retired")); err != nil {
				return err
			}
			if err := syncDirVPM(s.dir); err != nil {
				return err
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("collections: vector partition cleanup retired pointer: %w", err)
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
	// The source validation and active-pointer rename must share the collection
	// mutation barrier. A catalog read lock alone does not prevent a column
	// publication from advancing TVIS between validation and activation.
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
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
	if err := c.validateVectorPartitionSourceIdentityV1(m); err != nil {
		return err
	}
	if m.State == "ready" {
		if err := verifyVectorPartitionAssetsV1(c.db.ColumnAssetRootDir(), append(append([]VectorPartitionAssetV1(nil), m.Assets...), m.RouterAsset)); err != nil {
			return err
		}
	}
	s, e := OpenVectorPartitionStoreV1(c.db.Dir())
	if e != nil {
		return e
	}
	return s.Publish(m)
}

// validateVectorPartitionSourceIdentityV1 deliberately obtains the source
// identity from the live TVIS/column-manifest authority, rather than trusting a
// builder-supplied copy. VectorIndexStatus validates TVIS against the active
// manifest and its typed assets before this snapshot is inspected.
func (c *Collection) validateVectorPartitionSourceIdentityV1(m VectorPartitionManifestV1) error {
	status, err := c.VectorIndexStatus(m.IndexName)
	if err != nil {
		return fmt.Errorf("collections: vector partition source status: %w", err)
	}
	if !status.Loaded || status.State != VectorIndexStateColumnGraphLoaded {
		return fmt.Errorf("collections: vector partition source index %q is not a loaded TVIS generation", m.IndexName)
	}
	_, graph, _, err := c.columnVectorGraphPhysicalRowReaderSnapshotView(m.IndexName)
	if err != nil {
		return fmt.Errorf("collections: vector partition source identity: %w", err)
	}
	if m.SourceGeneration != graph.BaseManifestGeneration || m.SourceChecksum != graph.BaseManifestChecksum || m.SourceSchemaHash != graph.BaseSchemaHash || m.SourceRowCount != uint64(graph.RowCount) {
		return fmt.Errorf("collections: vector partition source identity mismatch")
	}
	return nil
}

func verifyVectorPartitionAssetsV1(root string, assets []VectorPartitionAssetV1) error {
	for _, a := range assets {
		if err := validateColumnAssetRefForPlan(a.Ref); err != nil {
			return fmt.Errorf("collections: vector partition asset %q ref: %w", a.ID, err)
		}
		if a.Ref.Length < 0 || uint64(a.Ref.Length) != a.Bytes {
			return fmt.Errorf("collections: vector partition asset %q ref bytes mismatch", a.ID)
		}
		raw, err := readColumnPhysicalAssetFromManager(root, a.Ref)
		if err != nil {
			return fmt.Errorf("collections: vector partition asset %q: %w", a.ID, err)
		}
		if uint64(len(raw)) != a.Bytes {
			return fmt.Errorf("collections: vector partition asset %q size mismatch", a.ID)
		}
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != a.Checksum {
			return fmt.Errorf("collections: vector partition asset %q sha256 mismatch", a.ID)
		}
	}
	return nil
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
	groups := map[string]struct{}{}
	var total uint64
	for _, p := range m.Placements {
		groups[p.GroupID] = struct{}{}
	}
	for _, a := range m.Assets {
		if math.MaxUint64-total < a.Bytes {
			return VectorPartitionStatusV1{}, fmt.Errorf("%w: asset byte overflow", ErrVectorPartitionManifestInvalid)
		}
		total += a.Bytes
	}
	if math.MaxUint64-total < m.RouterAsset.Bytes {
		return VectorPartitionStatusV1{}, fmt.Errorf("%w: asset byte overflow", ErrVectorPartitionManifestInvalid)
	}
	total += m.RouterAsset.Bytes
	return VectorPartitionStatusV1{Manifest: m, Ready: m.State == "ready", StaleReason: map[bool]string{true: "", false: "generation_building"}[m.State == "ready"], PartitionCount: m.PartitionCount, GroupCount: uint32(len(groups)), Memberships: uint64(len(m.Memberships)), OverlapMemberships: uint64(len(m.OverlapMemberships)), AssetBytes: total}, nil
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
