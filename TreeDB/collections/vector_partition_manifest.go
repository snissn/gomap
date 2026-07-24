package collections

// This file owns the M1 durable *identity* record.  It intentionally does not
// build packs or route queries; later milestones consume this record.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const VectorPartitionManifestFormatV1 = "vector_partition_manifest_v1"
const vectorPartitionManifestMagicV1 uint32 = 0x56504d31 // VPM1
const vectorPartitionMaxAssetBytesV1 uint64 = 1 << 33
const vectorPartitionMaxReferencedBytesV1 uint64 = 1 << 34

var (
	ErrVectorPartitionManifestInvalid             = errors.New("collections: invalid vector partition manifest")
	ErrVectorPartitionCollectionAuthorityRequired = errors.New("collections: vector partition lifecycle mutation requires collection authority")
)

const (
	vectorPartitionMutationOperationPublishV1 = "publish"
	vectorPartitionMutationOperationReclaimV1 = "reclaim"
)

var vectorPartitionBarrierBeforeMutationHookForTest struct {
	sync.RWMutex
	fn func(string)
}

// setVectorPartitionBarrierBeforeMutationHookForTestV1 exposes the structural
// storage-barrier-before-mutation order to deterministic concurrency tests.
func setVectorPartitionBarrierBeforeMutationHookForTestV1(fn func(string)) func() {
	vectorPartitionBarrierBeforeMutationHookForTest.Lock()
	old := vectorPartitionBarrierBeforeMutationHookForTest.fn
	vectorPartitionBarrierBeforeMutationHookForTest.fn = fn
	vectorPartitionBarrierBeforeMutationHookForTest.Unlock()
	return func() {
		vectorPartitionBarrierBeforeMutationHookForTest.Lock()
		vectorPartitionBarrierBeforeMutationHookForTest.fn = old
		vectorPartitionBarrierBeforeMutationHookForTest.Unlock()
	}
}

// SetVectorPartitionBarrierBeforeMutationHookForTestingV1 is the cross-package
// deterministic-concurrency seam for Raft FSM tests.
func SetVectorPartitionBarrierBeforeMutationHookForTestingV1(fn func(string)) func() {
	return setVectorPartitionBarrierBeforeMutationHookForTestV1(fn)
}

// VectorPartitionNamespacePersistenceSupportedForTestingV1 reports whether
// this platform can durably create and remove the VPM namespace. It is a
// testing capability seam; production mutations fail closed when unavailable.
func VectorPartitionNamespacePersistenceSupportedForTestingV1() bool {
	return vpmNamespacePersistenceSupported()
}

func (c *Collection) withVectorPartitionStorageMutationV1(operation string, fn func() error) error {
	return WithVectorPartitionStorageBarrierV1(c.db.Dir(), func() error {
		vectorPartitionBarrierBeforeMutationHookForTest.RLock()
		hook := vectorPartitionBarrierBeforeMutationHookForTest.fn
		vectorPartitionBarrierBeforeMutationHookForTest.RUnlock()
		if hook != nil {
			hook(operation)
		}
		unlock := c.lockMutation()
		defer unlock.Unlock()
		return fn()
	})
}

// VectorPartitionManifestLimits caps decoded state before any count-derived
// allocation. Defaults deliberately leave ample headroom below int limits.
type VectorPartitionManifestLimits struct {
	MaxBytes, MaxPartitions, MaxMemberships, MaxAssets, MaxStringBytes, MaxMembershipsPerVector, MaxRepresentativesPerPartition int
	// MaxSourceRows and MaxTotalMemberships separate the source cardinality
	// from aggregate primary/overlap/representative membership storage. Zero
	// retains the legacy MaxMemberships cap for callers that configured it.
	MaxSourceRows, MaxTotalMemberships int
}

func DefaultVectorPartitionManifestLimits() VectorPartitionManifestLimits {
	return VectorPartitionManifestLimits{MaxBytes: 16 << 20, MaxPartitions: 1 << 16, MaxMemberships: 2 << 20, MaxAssets: 1 << 18, MaxStringBytes: 4096, MaxMembershipsPerVector: 16, MaxRepresentativesPerPartition: 1 << 16, MaxSourceRows: 1 << 20}
}

func (l VectorPartitionManifestLimits) sourceRowLimit() int {
	if l.MaxSourceRows > 0 {
		return l.MaxSourceRows
	}
	return l.MaxMemberships
}
func (l VectorPartitionManifestLimits) totalMembershipLimit() int {
	if l.MaxTotalMemberships > 0 {
		return l.MaxTotalMemberships
	}
	return l.MaxMemberships
}

type VectorPartitionAssetV1 struct {
	ID, Checksum string
	PartitionID  uint32 // logical partition; RouterAsset is separate and remains zero.
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
	Collection, IndexName, IndexDefinitionDigest, IntegrityDigest      string
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

// VectorPartitionSourceIdentityV1 identifies the loaded vector-index
// generation a ready partition manifest must bind. Builders obtain this from
// the collection immediately before publication rather than reconstructing it
// from untrusted inputs.
type VectorPartitionSourceIdentityV1 struct {
	Generation uint64
	Checksum   uint64
	SchemaHash uint64
	RowCount   uint64
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
	putU32VPM(b, uint32(len(d.QuantizedIndexes)))
	for _, q := range d.QuantizedIndexes {
		putStringVPM(b, q.Name)
		putStringVPM(b, q.Codec)
		putU32VPM(b, q.Version)
		if q.ScalarU8Calibration == nil {
			putU32VPM(b, 0)
		} else {
			putU32VPM(b, 1)
			putStringVPM(b, string(q.ScalarU8Calibration.Mode))
			putStringVPM(b, string(q.ScalarU8Calibration.Grouping))
			putStringVPM(b, string(q.ScalarU8Calibration.AlphaPolicy.Name))
			putU32VPM(b, q.ScalarU8Calibration.AlphaPolicy.QuantilePPM)
		}
	}
	h := sha256.Sum256(b.Bytes())
	return hex.EncodeToString(h[:])
}

func (m VectorPartitionManifestV1) Validate(l VectorPartitionManifestLimits) error {
	if l.MaxBytes <= 0 {
		l = DefaultVectorPartitionManifestLimits()
	}
	if m.Format != VectorPartitionManifestFormatV1 || (m.State != "building" && m.State != "ready") || m.Collection == "" || m.IndexName == "" || !isSHA256VPM(m.IndexDefinitionDigest) || !isSHA256VPM(m.IntegrityDigest) || m.Generation == 0 || m.SourceGeneration == 0 || m.SourceRowCount == 0 || m.PartitionCount == 0 || int(m.PartitionCount) > l.MaxPartitions {
		return fmt.Errorf("%w: identity or partition bounds", ErrVectorPartitionManifestInvalid)
	}
	if m.SourceRowCount > uint64(l.sourceRowLimit()) || len(m.Collection) > l.MaxStringBytes || len(m.IndexName) > l.MaxStringBytes || len(m.State) > l.MaxStringBytes || len(m.BalancePolicy) > l.MaxStringBytes || len(m.IndexDefinitionDigest) > l.MaxStringBytes || len(m.IntegrityDigest) > l.MaxStringBytes {
		return fmt.Errorf("%w: source/string cap", ErrVectorPartitionManifestInvalid)
	}
	if len(m.Placements) != int(m.PartitionCount) || len(m.Assets) == 0 || len(m.Assets) > l.MaxAssets || len(m.Memberships) != int(m.SourceRowCount) || len(m.OverlapMemberships) > l.MaxMemberships || len(m.Representatives) > l.MaxMemberships || totalMembershipsVPM(m.Memberships, m.OverlapMemberships, m.Representatives) > l.totalMembershipLimit() || (m.State == "ready" && !isSHA256VPM(m.ReadySetDigest)) {
		return fmt.Errorf("%w: incomplete ready set or capped list", ErrVectorPartitionManifestInvalid)
	}
	if m.State == "ready" {
		if m.RouterGeneration != m.Generation {
			return fmt.Errorf("%w: router generation", ErrVectorPartitionManifestInvalid)
		}
		if err := validateAssetVPM(m.RouterAsset, l); err != nil {
			return err
		}
		if m.RouterAsset.PartitionID != 0 {
			return fmt.Errorf("%w: router partition must be zero", ErrVectorPartitionManifestInvalid)
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
	lastAssetPartition := uint32(0)
	assetCoverage := make(map[uint32]struct{}, m.PartitionCount)
	assetRefs := make(map[ColumnAssetRef]struct{}, len(m.Assets)+1)
	var referencedBytes uint64
	for _, a := range m.Assets {
		if err := validateAssetVPM(a, l); err != nil {
			return err
		}
		if a.PartitionID >= m.PartitionCount || (lastID != "" && (a.PartitionID < lastAssetPartition || a.PartitionID == lastAssetPartition && a.ID <= lastID)) {
			return fmt.Errorf("%w: noncanonical assets", ErrVectorPartitionManifestInvalid)
		}
		assetCoverage[a.PartitionID] = struct{}{}
		if a.Bytes > vectorPartitionMaxAssetBytesV1 || referencedBytes > vectorPartitionMaxReferencedBytesV1-a.Bytes {
			return fmt.Errorf("%w: asset byte cap", ErrVectorPartitionManifestInvalid)
		}
		referencedBytes += a.Bytes
		if _, dup := assetRefs[a.Ref]; dup {
			return fmt.Errorf("%w: duplicate asset ref", ErrVectorPartitionManifestInvalid)
		}
		assetRefs[a.Ref] = struct{}{}
		lastAssetPartition = a.PartitionID
		lastID = a.ID
	}
	if m.State == "ready" {
		if m.RouterAsset.Bytes > vectorPartitionMaxAssetBytesV1 || referencedBytes > vectorPartitionMaxReferencedBytesV1-m.RouterAsset.Bytes {
			return fmt.Errorf("%w: asset byte cap", ErrVectorPartitionManifestInvalid)
		}
		if _, dup := assetRefs[m.RouterAsset.Ref]; dup {
			return fmt.Errorf("%w: duplicate router ref", ErrVectorPartitionManifestInvalid)
		}
	}
	for partitionID := uint32(0); partitionID < m.PartitionCount; partitionID++ {
		if _, ok := assetCoverage[partitionID]; !ok {
			return fmt.Errorf("%w: missing partition asset %d", ErrVectorPartitionManifestInvalid, partitionID)
		}
	}
	if m.State == "ready" && m.ReadySetDigest != m.readyDigest() {
		return fmt.Errorf("%w: ready-set digest mismatch", ErrVectorPartitionManifestInvalid)
	}
	if m.IntegrityDigest != m.integrityDigest() {
		return fmt.Errorf("%w: record integrity digest mismatch", ErrVectorPartitionManifestInvalid)
	}
	return nil
}

func totalMembershipsVPM(lists ...[]VectorPartitionMembershipV1) int {
	total := 0
	for _, list := range lists {
		if len(list) > math.MaxInt-total {
			return math.MaxInt
		}
		total += len(list)
	}
	return total
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
		return add(8*5 + 4 + 4 + 4) // logical partition + bytes + generation + part + offset + length + file + checksum
	}
	for _, s := range []string{m.Format, m.State, m.Collection, m.IndexName, m.IndexDefinitionDigest, m.IntegrityDigest, m.BalancePolicy, m.ReadySetDigest} {
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
	var ordinalCount int
	partitionCounts := make(map[uint32]int)
	for i, x := range ms {
		if x.VectorOrdinal >= rows || capPer <= 0 {
			return fmt.Errorf("%w: %s ordinal/cap", ErrVectorPartitionManifestInvalid, kind)
		}
		if _, ok := ps[x.PartitionID]; !ok {
			return fmt.Errorf("%w: %s unknown partition", ErrVectorPartitionManifestInvalid, kind)
		}
		if i > 0 && (x.VectorOrdinal < prev.VectorOrdinal || x.VectorOrdinal == prev.VectorOrdinal && x.PartitionID <= prev.PartitionID) {
			return fmt.Errorf("%w: noncanonical %s", ErrVectorPartitionManifestInvalid, kind)
		}
		if base && x.VectorOrdinal != uint64(i) {
			return fmt.Errorf("%w: disjoint coverage", ErrVectorPartitionManifestInvalid)
		}
		if kind == "representative" {
			if partitionCounts[x.PartitionID] >= capPer {
				return fmt.Errorf("%w: representative partition cap", ErrVectorPartitionManifestInvalid)
			}
			partitionCounts[x.PartitionID]++
		} else {
			if i == 0 || x.VectorOrdinal != prev.VectorOrdinal {
				ordinalCount = 0
			}
			if ordinalCount >= capPer {
				return fmt.Errorf("%w: %s ordinal/cap", ErrVectorPartitionManifestInvalid, kind)
			}
			ordinalCount++
		}
		prev = x
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
	for _, a := range m.Assets {
		writeU32VPM(h, a.PartitionID)
		writeStringVPM(h, a.ID)
		writeStringVPM(h, a.Checksum)
		writeU64VPM(h, a.Bytes)
		writeColumnAssetRefVPM(h, a.Ref)
	}
	a := m.RouterAsset
	// The router has no logical partition, but its required canonical zero is
	// still encoded so this digest has one unambiguous asset representation.
	writeU32VPM(h, a.PartitionID)
	writeStringVPM(h, a.ID)
	writeStringVPM(h, a.Checksum)
	writeU64VPM(h, a.Bytes)
	writeColumnAssetRefVPM(h, a.Ref)
	return hex.EncodeToString(h.Sum(nil))
}

// Canonicalize sorts caller-provided lists then fills the ready-set digest.
func (m *VectorPartitionManifestV1) Canonicalize() {
	// Normalize empty lists so the semantic digest does not distinguish an
	// in-memory nil from the decoder's zero-length allocation.
	if m.Placements == nil {
		m.Placements = []VectorPartitionPlacementV1{}
	}
	if m.Memberships == nil {
		m.Memberships = []VectorPartitionMembershipV1{}
	}
	if m.OverlapMemberships == nil {
		m.OverlapMemberships = []VectorPartitionMembershipV1{}
	}
	if m.Representatives == nil {
		m.Representatives = []VectorPartitionMembershipV1{}
	}
	if m.Assets == nil {
		m.Assets = []VectorPartitionAssetV1{}
	}
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
	sort.Slice(m.Assets, func(i, j int) bool {
		return m.Assets[i].PartitionID < m.Assets[j].PartitionID || m.Assets[i].PartitionID == m.Assets[j].PartitionID && m.Assets[i].ID < m.Assets[j].ID
	})
	if m.State == "ready" {
		m.ReadySetDigest = m.readyDigest()
	}
	m.IntegrityDigest = m.integrityDigest()
}

func (m VectorPartitionManifestV1) integrityDigest() string {
	m.IntegrityDigest = ""
	raw, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// EncodeVectorPartitionManifestV1 is a strict, stable binary record. There
// are no tagged optional fields: newer records fail closed on this decoder.
func EncodeVectorPartitionManifestV1(m VectorPartitionManifestV1) ([]byte, error) {
	if err := preflightVectorPartitionManifestV1(m, DefaultVectorPartitionManifestLimits()); err != nil {
		return nil, err
	}
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
	putU32VPM(b, 2)
	for _, s := range []string{m.Format, m.State, m.Collection, m.IndexName, m.IndexDefinitionDigest, m.IntegrityDigest, m.BalancePolicy, m.ReadySetDigest} {
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
func preflightVectorPartitionManifestV1(m VectorPartitionManifestV1, l VectorPartitionManifestLimits) error {
	if len(m.Placements) > l.MaxPartitions || len(m.Assets) > l.MaxAssets || len(m.Memberships) > l.MaxMemberships || len(m.OverlapMemberships) > l.MaxMemberships || len(m.Representatives) > l.MaxMemberships || totalMembershipsVPM(m.Memberships, m.OverlapMemberships, m.Representatives) > l.totalMembershipLimit() {
		return fmt.Errorf("%w: list cap", ErrVectorPartitionManifestInvalid)
	}
	for _, s := range []string{m.Format, m.State, m.Collection, m.IndexName, m.IndexDefinitionDigest, m.IntegrityDigest, m.BalancePolicy, m.ReadySetDigest} {
		if len(s) > l.MaxStringBytes {
			return fmt.Errorf("%w: string cap", ErrVectorPartitionManifestInvalid)
		}
	}
	for _, p := range m.Placements {
		if len(p.GroupID) > l.MaxStringBytes {
			return fmt.Errorf("%w: string cap", ErrVectorPartitionManifestInvalid)
		}
	}
	for _, a := range m.Assets {
		if len(a.ID) > l.MaxStringBytes || len(a.Checksum) > l.MaxStringBytes || len(a.Ref.Namespace) > l.MaxStringBytes || len(a.Ref.Kind) > l.MaxStringBytes {
			return fmt.Errorf("%w: string cap", ErrVectorPartitionManifestInvalid)
		}
	}
	a := m.RouterAsset
	if len(a.ID) > l.MaxStringBytes || len(a.Checksum) > l.MaxStringBytes || len(a.Ref.Namespace) > l.MaxStringBytes || len(a.Ref.Kind) > l.MaxStringBytes {
		return fmt.Errorf("%w: string cap", ErrVectorPartitionManifestInvalid)
	}
	return nil
}
func DecodeVectorPartitionManifestV1(raw []byte, l VectorPartitionManifestLimits) (VectorPartitionManifestV1, error) {
	if l.MaxBytes <= 0 {
		l = DefaultVectorPartitionManifestLimits()
	}
	if len(raw) > l.MaxBytes {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: encoded bytes cap", ErrVectorPartitionManifestInvalid)
	}
	r := vpmReader{b: raw, l: l}
	if r.u32() != vectorPartitionManifestMagicV1 || r.u32() != 2 {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: magic/version", ErrVectorPartitionManifestInvalid)
	}
	m := VectorPartitionManifestV1{}
	ss := []*string{&m.Format, &m.State, &m.Collection, &m.IndexName, &m.IndexDefinitionDigest, &m.IntegrityDigest, &m.BalancePolicy, &m.ReadySetDigest}
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
	m.Placements = r.placements()
	m.Memberships = r.memberships()
	m.OverlapMemberships = r.memberships()
	m.Representatives = r.memberships()
	m.Assets = r.assets()
	if r.err != nil || r.off != len(raw) {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: truncated, over-cap, or trailing record: %v", ErrVectorPartitionManifestInvalid, r.err)
	}
	if len(ra) != 1 {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: router asset count", ErrVectorPartitionManifestInvalid)
	}
	m.RouterAsset = ra[0]
	if err := m.Validate(l); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	return m, nil
}

// EncodeVectorPartitionManifestJSONV1 is the canonical inspection/exchange
// form. It deliberately carries the same strict V1 object, after canonical
// ordering and validation, rather than accepting an open-ended JSON map.
func EncodeVectorPartitionManifestJSONV1(m VectorPartitionManifestV1) ([]byte, error) {
	if err := preflightVectorPartitionManifestV1(m, DefaultVectorPartitionManifestLimits()); err != nil {
		return nil, err
	}
	m.Canonicalize()
	if err := m.Validate(DefaultVectorPartitionManifestLimits()); err != nil {
		return nil, err
	}
	return json.Marshal(m)
}

// DecodeVectorPartitionManifestJSONV1 rejects unknown fields, multiple JSON
// values, trailing bytes, and every unsupported/invalid manifest state.
func DecodeVectorPartitionManifestJSONV1(raw []byte, l VectorPartitionManifestLimits) (VectorPartitionManifestV1, error) {
	if l.MaxBytes <= 0 {
		l = DefaultVectorPartitionManifestLimits()
	}
	if len(raw) > l.MaxBytes {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: JSON bytes cap", ErrVectorPartitionManifestInvalid)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var m VectorPartitionManifestV1
	if err := dec.Decode(&m); err != nil {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: JSON decode: %v", ErrVectorPartitionManifestInvalid, err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: JSON trailing value", ErrVectorPartitionManifestInvalid)
	}
	if err := m.Validate(l); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	canonical, err := EncodeVectorPartitionManifestJSONV1(m)
	if err != nil || !bytes.Equal(raw, canonical) {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: noncanonical JSON", ErrVectorPartitionManifestInvalid)
	}
	return m, nil
}

type vpmReader struct {
	b               []byte
	off             int
	membershipTotal int
	l               VectorPartitionManifestLimits
	err             error
}

func (r *vpmReader) u32() uint32 {
	if r.err != nil {
		return 0
	}
	if r.off+4 > len(r.b) {
		r.err = errors.New("truncated")
		return 0
	}
	v := binary.BigEndian.Uint32(r.b[r.off:])
	r.off += 4
	return v
}
func (r *vpmReader) u64() uint64 {
	if r.err != nil {
		return 0
	}
	if r.off+8 > len(r.b) {
		r.err = errors.New("truncated")
		return 0
	}
	v := binary.BigEndian.Uint64(r.b[r.off:])
	r.off += 8
	return v
}
func (r *vpmReader) str() string {
	if r.err != nil {
		return ""
	}
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
	if r.err != nil {
		return 0
	}
	n := r.u32()
	if r.err != nil {
		return 0
	}
	if uint64(n) > uint64(max) {
		r.err = errors.New("count cap")
		return 0
	}
	return int(n)
}

func (r *vpmReader) allocationCount(max, minItemBytes int, label string) int {
	n := r.count(max)
	if r.err != nil {
		return 0
	}
	// A declared count must fit even if every remaining item uses its shortest
	// legal encoding. Check this before make so a tiny corrupt record cannot
	// turn a permissive caller-supplied cap into a large allocation request.
	if minItemBytes <= 0 || n > (len(r.b)-r.off)/minItemBytes {
		r.err = fmt.Errorf("%s count exceeds remaining bytes", label)
		return 0
	}
	return n
}

func (r *vpmReader) assets() []VectorPartitionAssetV1 {
	// partition + two empty strings + bytes + the shortest column reference.
	const minAssetBytes = 4 + 4 + 4 + 8 + (4 + 4 + 8 + 8 + 4 + 8 + 8 + 4)
	n := r.allocationCount(r.l.MaxAssets, minAssetBytes, "asset")
	if r.err != nil {
		return nil
	}
	x := make([]VectorPartitionAssetV1, n)
	for i := range x {
		x[i] = VectorPartitionAssetV1{PartitionID: r.u32(), ID: r.str(), Checksum: r.str(), Bytes: r.u64(), Ref: r.columnRef()}
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
	const minPlacementBytes = 4 + 4 // partition + empty group string
	n := r.allocationCount(r.l.MaxPartitions, minPlacementBytes, "placement")
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
	const membershipBytes = 8 + 4
	n := r.allocationCount(r.l.MaxMemberships, membershipBytes, "membership")
	if r.err != nil {
		return nil
	}
	if n > r.l.totalMembershipLimit()-r.membershipTotal {
		r.err = errors.New("total membership cap")
		return nil
	}
	r.membershipTotal += n
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
		putU32VPM(b, a.PartitionID)
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
type VectorPartitionStoreV1 struct {
	root, dir   string
	dirIdentity rootpublication.StableIdentity
}

// vectorPartitionPublishHooksV1 is deliberately test-only fault injection at
// real persistence boundaries. It lets reopen tests verify the actual on-disk
// recovery invariant rather than infer it from a successful call return.
var vectorPartitionPublishHooksV1 struct {
	sync.RWMutex
	at func(string) error
}

var vectorPartitionDeleteAfterTombstoneForTestV1 struct {
	sync.RWMutex
	hook func()
}

func setVectorPartitionDeleteAfterTombstoneForTestV1(hook func()) func() {
	vectorPartitionDeleteAfterTombstoneForTestV1.Lock()
	old := vectorPartitionDeleteAfterTombstoneForTestV1.hook
	vectorPartitionDeleteAfterTombstoneForTestV1.hook = hook
	vectorPartitionDeleteAfterTombstoneForTestV1.Unlock()
	return func() {
		vectorPartitionDeleteAfterTombstoneForTestV1.Lock()
		vectorPartitionDeleteAfterTombstoneForTestV1.hook = old
		vectorPartitionDeleteAfterTombstoneForTestV1.Unlock()
	}
}

func vectorPartitionDeleteAfterTombstoneHookV1() func() {
	vectorPartitionDeleteAfterTombstoneForTestV1.RLock()
	defer vectorPartitionDeleteAfterTombstoneForTestV1.RUnlock()
	return vectorPartitionDeleteAfterTombstoneForTestV1.hook
}

func setVectorPartitionPublishHookForTestV1(at func(string) error) func() {
	vectorPartitionPublishHooksV1.Lock()
	old := vectorPartitionPublishHooksV1.at
	vectorPartitionPublishHooksV1.at = at
	vectorPartitionPublishHooksV1.Unlock()
	return func() {
		vectorPartitionPublishHooksV1.Lock()
		vectorPartitionPublishHooksV1.at = old
		vectorPartitionPublishHooksV1.Unlock()
	}
}

func vectorPartitionPublishFaultV1(boundary string) error {
	vectorPartitionPublishHooksV1.RLock()
	hook := vectorPartitionPublishHooksV1.at
	vectorPartitionPublishHooksV1.RUnlock()
	if hook == nil {
		return nil
	}
	return hook(boundary)
}

const (
	vectorPartitionStoreMaxEntriesV1 = 4096
	vectorPartitionStoreMaxBytesV1   = 64 << 20
	vectorPartitionReclaimMagicV1    = "VPR1"
	vectorPartitionReclaimVersionV1  = 1
	vectorPartitionReclaimMaxRefsV1  = 1 << 19
	vectorPartitionReclaimMaxBytesV1 = vectorPartitionStoreMaxBytesV1
	vectorPartitionInactiveMagicV1   = "VPI1"
	vectorPartitionInactiveVersionV1 = 1
	// magic/version, two string lengths, generation, checksum
	vectorPartitionInactiveFixedBytesV1 = 8 + 4 + 4 + 8 + sha256.Size
)

type vectorPartitionInactiveStateV1 struct {
	Collection, IndexName string
	Generation            uint64
}

// vectorPartitionInactiveStateEncodedSizeV1 is the single bounded size model
// for both writer allocation and reader admission: magic/version, two
// length-prefixed identities, generation, and checksum.
func vectorPartitionInactiveStateEncodedSizeV1(state vectorPartitionInactiveStateV1) (int, error) {
	state, err := canonicalVectorPartitionInactiveStateV1(state)
	if err != nil {
		return 0, err
	}
	return vectorPartitionInactiveFixedBytesV1 + len(state.Collection) + len(state.IndexName), nil
}

func vectorPartitionInactiveStateMaxEncodedBytesV1() int {
	limits := DefaultVectorPartitionManifestLimits()
	return vectorPartitionInactiveFixedBytesV1 + limits.MaxStringBytes*2
}

func canonicalVectorPartitionInactiveStateV1(state vectorPartitionInactiveStateV1) (vectorPartitionInactiveStateV1, error) {
	limits := DefaultVectorPartitionManifestLimits()
	if state.Collection == "" || state.IndexName == "" || state.Generation == 0 || len(state.Collection) > limits.MaxStringBytes || len(state.IndexName) > limits.MaxStringBytes {
		return vectorPartitionInactiveStateV1{}, fmt.Errorf("%w: inactive marker identity", ErrVectorPartitionManifestInvalid)
	}
	return state, nil
}

func encodeVectorPartitionInactiveStateV1(input vectorPartitionInactiveStateV1) ([]byte, error) {
	state, err := canonicalVectorPartitionInactiveStateV1(input)
	if err != nil {
		return nil, err
	}
	size, err := vectorPartitionInactiveStateEncodedSizeV1(state)
	if err != nil {
		return nil, err
	}
	payload := bytes.NewBuffer(make([]byte, 0, size-8-sha256.Size))
	putStringVPM(payload, state.Collection)
	putStringVPM(payload, state.IndexName)
	putU64VPM(payload, state.Generation)
	out := make([]byte, 0, size)
	out = append(out, vectorPartitionInactiveMagicV1...)
	var version [4]byte
	binary.BigEndian.PutUint32(version[:], vectorPartitionInactiveVersionV1)
	out = append(out, version[:]...)
	out = append(out, payload.Bytes()...)
	sum := sha256.Sum256(out)
	out = append(out, sum[:]...)
	return out, nil
}

func decodeVectorPartitionInactiveStateV1(raw []byte) (vectorPartitionInactiveStateV1, error) {
	if len(raw) < 8+4+4+8+sha256.Size || string(raw[:4]) != vectorPartitionInactiveMagicV1 || binary.BigEndian.Uint32(raw[4:8]) != vectorPartitionInactiveVersionV1 {
		return vectorPartitionInactiveStateV1{}, fmt.Errorf("%w: inactive marker header", ErrVectorPartitionManifestInvalid)
	}
	sum := sha256.Sum256(raw[:len(raw)-sha256.Size])
	if !bytes.Equal(sum[:], raw[len(raw)-sha256.Size:]) {
		return vectorPartitionInactiveStateV1{}, fmt.Errorf("%w: inactive marker checksum", ErrVectorPartitionManifestInvalid)
	}
	r := vpmReader{b: raw[8 : len(raw)-sha256.Size], l: VectorPartitionManifestLimits{MaxStringBytes: DefaultVectorPartitionManifestLimits().MaxStringBytes}}
	state := vectorPartitionInactiveStateV1{Collection: r.str(), IndexName: r.str(), Generation: r.u64()}
	if r.err != nil || r.off != len(r.b) {
		return vectorPartitionInactiveStateV1{}, fmt.Errorf("%w: inactive marker payload", ErrVectorPartitionManifestInvalid)
	}
	return canonicalVectorPartitionInactiveStateV1(state)
}

// vectorPartitionReclaimStateV1 is a bounded durable deletion journal. Original
// refs are copied from the generation manifest before that manifest is removed.
// Superseded refs are added before a mixed-segment rewrite may publish its
// remap, so a restart never loses the old live ranges that GC must retire.
type vectorPartitionReclaimStateV1 struct {
	Collection, IndexName string
	Generation            uint64
	OriginalRefs          []ColumnAssetRef
	SupersededRefs        []ColumnAssetRef
}

var vectorPartitionReclaimPersistHooksV1 struct {
	sync.RWMutex
	beforeRename func(string, vectorPartitionReclaimStateV1) error
	afterCommit  func(string, vectorPartitionReclaimStateV1) error
}

func setVectorPartitionReclaimPersistHooksForTestV1(beforeRename, afterCommit func(string, vectorPartitionReclaimStateV1) error) func() {
	vectorPartitionReclaimPersistHooksV1.Lock()
	oldBefore := vectorPartitionReclaimPersistHooksV1.beforeRename
	oldAfter := vectorPartitionReclaimPersistHooksV1.afterCommit
	vectorPartitionReclaimPersistHooksV1.beforeRename = beforeRename
	vectorPartitionReclaimPersistHooksV1.afterCommit = afterCommit
	vectorPartitionReclaimPersistHooksV1.Unlock()
	return func() {
		vectorPartitionReclaimPersistHooksV1.Lock()
		vectorPartitionReclaimPersistHooksV1.beforeRename = oldBefore
		vectorPartitionReclaimPersistHooksV1.afterCommit = oldAfter
		vectorPartitionReclaimPersistHooksV1.Unlock()
	}
}

func vectorPartitionReclaimRefsFromManifestV1(m VectorPartitionManifestV1) []ColumnAssetRef {
	refs := make([]ColumnAssetRef, 0, len(m.Assets)+1)
	for _, asset := range m.Assets {
		refs = append(refs, asset.Ref)
	}
	if m.RouterAsset.Ref.Kind != "" {
		refs = append(refs, m.RouterAsset.Ref)
	}
	return refs
}

func newVectorPartitionReclaimStateV1(m VectorPartitionManifestV1) (vectorPartitionReclaimStateV1, error) {
	if err := m.Validate(DefaultVectorPartitionManifestLimits()); err != nil {
		return vectorPartitionReclaimStateV1{}, err
	}
	return canonicalVectorPartitionReclaimStateV1(vectorPartitionReclaimStateV1{
		Collection:   m.Collection,
		IndexName:    m.IndexName,
		Generation:   m.Generation,
		OriginalRefs: vectorPartitionReclaimRefsFromManifestV1(m),
	})
}

func canonicalVectorPartitionReclaimStateV1(state vectorPartitionReclaimStateV1) (vectorPartitionReclaimStateV1, error) {
	limits := DefaultVectorPartitionManifestLimits()
	if state.Collection == "" || state.IndexName == "" || state.Generation == 0 || len(state.Collection) > limits.MaxStringBytes || len(state.IndexName) > limits.MaxStringBytes {
		return vectorPartitionReclaimStateV1{}, fmt.Errorf("%w: reclaim record identity", ErrVectorPartitionManifestInvalid)
	}
	if len(state.OriginalRefs) == 0 || len(state.OriginalRefs) > vectorPartitionReclaimMaxRefsV1 || len(state.SupersededRefs) > vectorPartitionReclaimMaxRefsV1-len(state.OriginalRefs) {
		return vectorPartitionReclaimStateV1{}, fmt.Errorf("%w: reclaim record ref count", ErrVectorPartitionManifestInvalid)
	}
	canonicalize := func(refs []ColumnAssetRef) ([]ColumnAssetRef, error) {
		out := append([]ColumnAssetRef(nil), refs...)
		for _, ref := range out {
			if err := validateColumnAssetRefForPlan(ref); err != nil {
				return nil, fmt.Errorf("%w: reclaim record ref: %v", ErrVectorPartitionManifestInvalid, err)
			}
			if len(ref.Kind) > limits.MaxStringBytes || len(ref.Namespace) > limits.MaxStringBytes || ref.Offset > math.MaxInt64-ref.Length {
				return nil, fmt.Errorf("%w: reclaim record ref bounds", ErrVectorPartitionManifestInvalid)
			}
		}
		sort.Slice(out, func(i, j int) bool { return compareColumnAssetRefs(out[i], out[j]) < 0 })
		write := 0
		for _, ref := range out {
			if write != 0 && compareColumnAssetRefs(out[write-1], ref) == 0 {
				continue
			}
			out[write] = ref
			write++
		}
		return out[:write], nil
	}
	var err error
	state.OriginalRefs, err = canonicalize(state.OriginalRefs)
	if err != nil {
		return vectorPartitionReclaimStateV1{}, err
	}
	state.SupersededRefs, err = canonicalize(state.SupersededRefs)
	if err != nil {
		return vectorPartitionReclaimStateV1{}, err
	}
	original := make(map[ColumnAssetRef]struct{}, len(state.OriginalRefs))
	namespace := state.OriginalRefs[0].Namespace
	for _, ref := range state.OriginalRefs {
		if ref.Namespace != namespace {
			return vectorPartitionReclaimStateV1{}, fmt.Errorf("%w: reclaim record namespace", ErrVectorPartitionManifestInvalid)
		}
		original[ref] = struct{}{}
	}
	write := 0
	for _, ref := range state.SupersededRefs {
		if ref.Namespace != namespace {
			return vectorPartitionReclaimStateV1{}, fmt.Errorf("%w: reclaim record namespace", ErrVectorPartitionManifestInvalid)
		}
		if _, duplicate := original[ref]; duplicate {
			continue
		}
		state.SupersededRefs[write] = ref
		write++
	}
	state.SupersededRefs = state.SupersededRefs[:write]
	return state, nil
}

func vectorPartitionReclaimStateEncodedSizeV1(state vectorPartitionReclaimStateV1) (int, error) {
	total := uint64(12 + sha256.Size) // magic, version, payload length, checksum
	add := func(n uint64) error {
		if n > vectorPartitionReclaimMaxBytesV1 || total > vectorPartitionReclaimMaxBytesV1-n {
			return fmt.Errorf("%w: reclaim record bytes cap", ErrVectorPartitionManifestInvalid)
		}
		total += n
		return nil
	}
	if err := add(4 + uint64(len(state.Collection))); err != nil {
		return 0, err
	}
	if err := add(4 + uint64(len(state.IndexName)) + 8 + 4 + 4); err != nil {
		return 0, err
	}
	for _, refs := range [][]ColumnAssetRef{state.OriginalRefs, state.SupersededRefs} {
		for _, ref := range refs {
			if err := add(4 + uint64(len(ref.Kind)) + 4 + uint64(len(ref.Namespace)) + 8 + 8 + 4 + 8 + 8 + 4); err != nil {
				return 0, err
			}
		}
	}
	return int(total), nil
}

// The checksum binds the header, format version, and canonical payload before
// decoded refs are allowed to influence reachability or deletion.
func encodeVectorPartitionReclaimRecordV1(input vectorPartitionReclaimStateV1) ([]byte, error) {
	state, err := canonicalVectorPartitionReclaimStateV1(input)
	if err != nil {
		return nil, err
	}
	size, err := vectorPartitionReclaimStateEncodedSizeV1(state)
	if err != nil {
		return nil, err
	}
	payload := bytes.NewBuffer(make([]byte, 0, size-12-sha256.Size))
	putStringVPM(payload, state.Collection)
	putStringVPM(payload, state.IndexName)
	putU64VPM(payload, state.Generation)
	putU32VPM(payload, uint32(len(state.OriginalRefs)))
	for _, ref := range state.OriginalRefs {
		putColumnAssetRefVPM(payload, ref)
	}
	putU32VPM(payload, uint32(len(state.SupersededRefs)))
	for _, ref := range state.SupersededRefs {
		putColumnAssetRefVPM(payload, ref)
	}
	out := make([]byte, 0, size)
	out = append(out, vectorPartitionReclaimMagicV1...)
	var field [4]byte
	binary.BigEndian.PutUint32(field[:], vectorPartitionReclaimVersionV1)
	out = append(out, field[:]...)
	binary.BigEndian.PutUint32(field[:], uint32(payload.Len()))
	out = append(out, field[:]...)
	out = append(out, payload.Bytes()...)
	sum := sha256.Sum256(out)
	out = append(out, sum[:]...)
	return out, nil
}

func decodeVectorPartitionReclaimRecordV1(raw []byte) (vectorPartitionReclaimStateV1, error) {
	if len(raw) < 12+sha256.Size || len(raw) > vectorPartitionReclaimMaxBytesV1 || string(raw[:4]) != vectorPartitionReclaimMagicV1 {
		return vectorPartitionReclaimStateV1{}, fmt.Errorf("%w: reclaim record header", ErrVectorPartitionManifestInvalid)
	}
	if binary.BigEndian.Uint32(raw[4:8]) != vectorPartitionReclaimVersionV1 {
		return vectorPartitionReclaimStateV1{}, fmt.Errorf("%w: reclaim record version", ErrVectorPartitionManifestInvalid)
	}
	n := uint64(binary.BigEndian.Uint32(raw[8:12]))
	if n == 0 || n > uint64(vectorPartitionReclaimMaxBytesV1-12-sha256.Size) || uint64(len(raw)) != 12+n+sha256.Size {
		return vectorPartitionReclaimStateV1{}, fmt.Errorf("%w: reclaim record length", ErrVectorPartitionManifestInvalid)
	}
	sum := sha256.Sum256(raw[:12+int(n)])
	if !bytes.Equal(sum[:], raw[12+int(n):]) {
		return vectorPartitionReclaimStateV1{}, fmt.Errorf("%w: reclaim record checksum", ErrVectorPartitionManifestInvalid)
	}
	r := vpmReader{b: raw[12 : 12+int(n)], l: VectorPartitionManifestLimits{MaxStringBytes: DefaultVectorPartitionManifestLimits().MaxStringBytes}}
	state := vectorPartitionReclaimStateV1{Collection: r.str(), IndexName: r.str(), Generation: r.u64()}
	originalCount := r.count(vectorPartitionReclaimMaxRefsV1)
	const minEncodedColumnAssetRefBytes = 48
	if r.err == nil && (originalCount > (len(r.b)-r.off)/minEncodedColumnAssetRefBytes || len(r.b)-r.off-originalCount*minEncodedColumnAssetRefBytes < 4) {
		r.err = errors.New("ref count exceeds remaining bytes")
	}
	if r.err == nil {
		state.OriginalRefs = make([]ColumnAssetRef, originalCount)
		for i := range state.OriginalRefs {
			state.OriginalRefs[i] = r.columnRef()
		}
	}
	supersededCount := r.count(vectorPartitionReclaimMaxRefsV1 - len(state.OriginalRefs))
	if r.err == nil && supersededCount > (len(r.b)-r.off)/minEncodedColumnAssetRefBytes {
		r.err = errors.New("ref count exceeds remaining bytes")
	}
	if r.err == nil {
		state.SupersededRefs = make([]ColumnAssetRef, supersededCount)
		for i := range state.SupersededRefs {
			state.SupersededRefs[i] = r.columnRef()
		}
	}
	if r.err != nil || r.off != len(r.b) {
		return vectorPartitionReclaimStateV1{}, fmt.Errorf("%w: reclaim record truncated, over-cap, or trailing: %v", ErrVectorPartitionManifestInvalid, r.err)
	}
	canonical, err := canonicalVectorPartitionReclaimStateV1(state)
	if err != nil {
		return vectorPartitionReclaimStateV1{}, err
	}
	encoded, err := encodeVectorPartitionReclaimRecordV1(canonical)
	if err != nil || !bytes.Equal(encoded, raw) {
		return vectorPartitionReclaimStateV1{}, fmt.Errorf("%w: noncanonical reclaim record", ErrVectorPartitionManifestInvalid)
	}
	return canonical, nil
}

func (state vectorPartitionReclaimStateV1) debtRefs() []ColumnAssetRef {
	refs := make([]ColumnAssetRef, 0, len(state.OriginalRefs)+len(state.SupersededRefs))
	refs = append(refs, state.OriginalRefs...)
	refs = append(refs, state.SupersededRefs...)
	return refs
}

func (state vectorPartitionReclaimStateV1) clone() vectorPartitionReclaimStateV1 {
	state.OriginalRefs = append([]ColumnAssetRef(nil), state.OriginalRefs...)
	state.SupersededRefs = append([]ColumnAssetRef(nil), state.SupersededRefs...)
	return state
}

func (c *Collection) vectorPartitionReachabilityRefsV1(releaseReclaimIDs map[string]struct{}) ([]ColumnAssetRef, []ColumnAssetRef, error) {
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
	dir, err := s.openDir()
	if err != nil {
		return nil, nil, err
	}
	defer dir.Close()
	byIndex := make(map[string][]VectorPartitionManifestV1)
	prefix := safeVPM(c.name) + "-"
	var totalBytes int64
	var reclaimBytes int64
	var relevant int
	var reclaimPrepared []ColumnAssetRef
	activeNames := make(map[string]struct{})
	retiredNames := make(map[string]struct{})
	for {
		entries, readErr := dir.ReadDir(64)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, nil, readErr
		}
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), prefix) {
				continue
			}
			if len(entry.Name()) > 256 {
				return nil, nil, fmt.Errorf("collections: vector partition entry name cap")
			}
			relevant++
			if relevant > vectorPartitionStoreMaxEntriesV1 {
				return nil, nil, fmt.Errorf("collections: vector partition retained entry cap")
			}
			if err := validateVectorPartitionDurableEntryV1(entry); err != nil {
				return nil, nil, err
			}
			if strings.HasSuffix(entry.Name(), ".active") {
				activeNames[entry.Name()] = struct{}{}
				continue
			}
			if strings.HasSuffix(entry.Name(), ".retired") {
				retiredNames[entry.Name()] = struct{}{}
				continue
			}
			if strings.HasSuffix(entry.Name(), ".inactive") {
				state, err := s.readInactiveStateName(entry.Name())
				if err != nil {
					return nil, nil, fmt.Errorf("collections: invalid vector partition inactive marker %q: %w", entry.Name(), err)
				}
				if state.Collection != c.name || entry.Name() != s.inactiveName(state.Collection, state.IndexName) {
					return nil, nil, fmt.Errorf("collections: invalid vector partition inactive marker identity %q", entry.Name())
				}
				continue
			}
			if strings.HasSuffix(entry.Name(), ".deleting") {
				raw, err := s.readBounded(entry.Name(), vectorPartitionReclaimMaxBytesV1)
				if err != nil {
					return nil, nil, err
				}
				if reclaimBytes > vectorPartitionStoreMaxBytesV1-int64(len(raw)) {
					return nil, nil, fmt.Errorf("collections: vector partition reclaim bytes cap")
				}
				reclaimBytes += int64(len(raw))
				reclaim, err := decodeVectorPartitionReclaimRecordV1(raw)
				if err != nil || reclaim.Collection != c.name || entry.Name() != s.deleteTombstoneName(reclaim.Collection, reclaim.IndexName, reclaim.Generation) {
					return nil, nil, fmt.Errorf("collections: invalid vector partition reclaim record %q", entry.Name())
				}
				if _, releasing := releaseReclaimIDs[entry.Name()]; !releasing {
					reclaimPrepared = append(reclaimPrepared, reclaim.debtRefs()...)
				}
				continue
			}
			if filepath.Ext(entry.Name()) != ".vpm" {
				return nil, nil, fmt.Errorf("collections: unexpected vector partition entry %q", entry.Name())
			}
			raw, err := s.readBounded(entry.Name(), DefaultVectorPartitionManifestLimits().MaxBytes)
			if err != nil {
				return nil, nil, err
			}
			if totalBytes > vectorPartitionStoreMaxBytesV1-int64(len(raw)) {
				return nil, nil, fmt.Errorf("collections: vector partition retained bytes cap")
			}
			totalBytes += int64(len(raw))
			m, err := DecodeVectorPartitionManifestV1(raw, DefaultVectorPartitionManifestLimits())
			if err != nil {
				return nil, nil, fmt.Errorf("collections: vector partition manifest %q: %w", entry.Name(), err)
			}
			if m.Collection != c.name {
				return nil, nil, fmt.Errorf("collections: vector partition manifest filename collection mismatch")
			}
			wantName := fmt.Sprintf("%s-%s-%d.vpm", safeVPM(m.Collection), safeVPM(m.IndexName), m.Generation)
			if entry.Name() != wantName {
				return nil, nil, fmt.Errorf("collections: vector partition manifest filename does not bind decoded identity")
			}
			byIndex[m.IndexName] = append(byIndex[m.IndexName], m)
		}
		if errors.Is(readErr, io.EOF) {
			break
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
			if _, present := activeNames[safeVPM(c.name)+"-"+safeVPM(index)+".active"]; present {
				return nil, nil, fmt.Errorf("collections: vector partition active pointer for %q targets a missing generation", index)
			}
			retired, retiredErr := s.OpenRetired(c.name, index)
			if errors.Is(retiredErr, os.ErrNotExist) {
				if _, inactiveErr := s.readInactiveGeneration(c.name, index); inactiveErr == nil {
					continue
				} else if !errors.Is(inactiveErr, os.ErrNotExist) {
					return nil, nil, fmt.Errorf("collections: vector partition inactive marker for %q: %w", index, inactiveErr)
				}
				return nil, nil, fmt.Errorf("collections: vector partition active/retired pointer for %q: %w", index, retiredErr)
			}
			if retiredErr != nil {
				return nil, nil, fmt.Errorf("collections: vector partition active/retired pointer for %q: %w", index, retiredErr)
			}
			expectedRetired[safeVPM(c.name)+"-"+safeVPM(index)+".retired"] = struct{}{}
			if _, inactiveErr := s.readInactiveGeneration(c.name, index); inactiveErr != nil && !errors.Is(inactiveErr, os.ErrNotExist) {
				return nil, nil, fmt.Errorf("collections: vector partition inactive marker for %q: %w", index, inactiveErr)
			}
			if !vectorPartitionManifestGenerationRetained(manifests, retired.Generation) {
				return nil, nil, fmt.Errorf("collections: retired vector partition generation %d for %q is not retained", retired.Generation, index)
			}
			continue
		}
		if active.State != "ready" {
			return nil, nil, fmt.Errorf("collections: vector partition active pointer for %q targets non-ready generation", index)
		}
		if _, inactiveErr := s.readInactiveGeneration(c.name, index); inactiveErr != nil && !errors.Is(inactiveErr, os.ErrNotExist) {
			return nil, nil, fmt.Errorf("collections: vector partition inactive marker for %q: %w", index, inactiveErr)
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
		// Publication and deactivation deliberately tolerate a crash with both
		// markers present. The retired generation remains prepared-only until a
		// retry removes its marker, while the active one is the sole pinned set.
		if retired, retiredErr := s.OpenRetired(c.name, index); retiredErr == nil {
			expectedRetired[safeVPM(c.name)+"-"+safeVPM(index)+".retired"] = struct{}{}
			if !vectorPartitionManifestGenerationRetained(manifests, retired.Generation) {
				return nil, nil, fmt.Errorf("collections: retired vector partition generation %d for %q is not retained", retired.Generation, index)
			}
		} else if !errors.Is(retiredErr, os.ErrNotExist) {
			return nil, nil, fmt.Errorf("collections: vector partition retired pointer for %q: %w", index, retiredErr)
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
	prepared = append(prepared, reclaimPrepared...)
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
	info, existedErr := os.Lstat(d)
	existed := existedErr == nil
	if existedErr != nil && !errors.Is(existedErr, os.ErrNotExist) {
		return nil, existedErr
	}
	if existed && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
		return nil, fmt.Errorf("%w: store is not an exact directory", ErrVectorPartitionManifestInvalid)
	}
	// On Windows the namespace protocol has no proof for the initial directory
	// creation, link, and later remove transitions. Refuse before MkdirAll so
	// even a failed request cannot leave a new namespace behind.
	if !existed && !vpmNamespacePersistenceSupported() {
		return nil, fmt.Errorf("%w: vector partition store creation", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	if err := os.MkdirAll(d, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(d)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: store is not an exact directory", ErrVectorPartitionManifestInvalid)
	}
	// A first store creation changes root's directory entries. Sync it too so a
	// returned store is rooted in durable metadata, not merely a durable child.
	if !existed {
		if err := syncDirVPM(root); err != nil {
			return nil, err
		}
	}
	return newVectorPartitionStoreV1(root, d)
}

// OpenExistingVectorPartitionStoreV1 is read-only with respect to directory
// creation. Status and reclamation planning use it so observation cannot
// create durable state.
func OpenExistingVectorPartitionStoreV1(root string) (*VectorPartitionStoreV1, error) {
	if root == "" {
		return nil, errors.New("collections: empty vector partition store root")
	}
	d := filepath.Join(root, "vector_partitions")
	info, err := os.Lstat(d)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: store is not an exact directory", ErrVectorPartitionManifestInvalid)
	}
	return newVectorPartitionStoreV1(root, d)
}

func newVectorPartitionStoreV1(root, dirPath string) (*VectorPartitionStoreV1, error) {
	dir, err := rootpublication.OpenStableParent(dirPath)
	if err != nil {
		return nil, err
	}
	identity, identityErr := rootpublication.StableIdentityFromFile(dir)
	closeErr := dir.Close()
	if err := errors.Join(identityErr, closeErr); err != nil {
		return nil, err
	}
	return &VectorPartitionStoreV1{root: root, dir: dirPath, dirIdentity: identity}, nil
}

// Publish is intentionally unavailable on the raw store. A store alone cannot
// establish collection mutation, live TVIS, or asset reachability authority;
// Collection.PublishVectorPartitionManifestV1 owns both building and ready
// publication.
func (s *VectorPartitionStoreV1) Publish(m VectorPartitionManifestV1) error {
	return errors.New("collections: vector partition publication requires collection authority")
}

func (s *VectorPartitionStoreV1) publishValidatedBuilding(m VectorPartitionManifestV1) error {
	if m.State != "building" {
		return errors.New("collections: validated building publication requires building state")
	}
	return WithVectorPartitionStorageBarrierV1(s.root, func() error { return s.publishLocked(m) })
}

func (s *VectorPartitionStoreV1) publishValidatedReady(m VectorPartitionManifestV1) error {
	if m.State != "ready" {
		return errors.New("collections: validated ready publication requires ready manifest")
	}
	return WithVectorPartitionStorageBarrierV1(s.root, func() error { return s.publishLocked(m) })
}
func (s *VectorPartitionStoreV1) publishLocked(m VectorPartitionManifestV1) error {
	if !vpmNamespacePersistenceSupported() {
		return fmt.Errorf("%w: vector partition publication", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	if present, err := s.regularEntryPresent(filepath.Base(s.deleteTombstonePath(m.Collection, m.IndexName, m.Generation))); err != nil {
		return err
	} else if present {
		return fmt.Errorf("%w: generation %d is deleting", ErrVectorPartitionManifestInvalid, m.Generation)
	}
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
	if e = vectorPartitionPublishFaultV1("generation_temp_synced"); e != nil {
		return e
	}
	target := filepath.Join(s.dir, name)
	promoting := false
	if e = os.Link(tmp, target); e != nil {
		if !errors.Is(e, os.ErrExist) {
			return e
		}
		existing, readErr := s.readBounded(name, DefaultVectorPartitionManifestLimits().MaxBytes)
		if readErr != nil {
			return readErr
		}
		if bytes.Equal(existing, raw) {
			// Exact idempotent retry.
		} else {
			previous, decodeErr := DecodeVectorPartitionManifestV1(existing, DefaultVectorPartitionManifestLimits())
			if decodeErr != nil || previous.State != "building" || m.State != "ready" || !vectorPartitionBuildingPromotionIdentityV1(previous, m) {
				return fmt.Errorf("%w: generation %d already published with different bytes", ErrVectorPartitionManifestInvalid, m.Generation)
			}
			// Persist intentional no-active authority before atomically replacing a
			// retained building record. A restart can therefore observe only the
			// old building, prepared-only ready+inactive, or active ready state.
			if e = s.writeInactiveMarker(m.Collection, m.IndexName, m.Generation); e != nil {
				return e
			}
			if e = vectorPartitionPublishFaultV1("promotion_inactive_synced"); e != nil {
				return e
			}
			if e = renameVPM(tmp, target); e != nil {
				return e
			}
			promoting = true
		}
	}
	if promoting {
		if e = vectorPartitionPublishFaultV1("promotion_renamed"); e != nil {
			return e
		}
	}
	if e = vectorPartitionPublishFaultV1("generation_linked"); e != nil {
		return e
	}
	if e = syncDirVPM(s.dir); e != nil {
		return e
	}
	if promoting {
		if e = vectorPartitionPublishFaultV1("promotion_dir_synced"); e != nil {
			return e
		}
	}
	if e = vectorPartitionPublishFaultV1("generation_dir_synced"); e != nil {
		return e
	}
	if m.State == "ready" {
		active := filepath.Join(s.dir, safeVPM(m.Collection)+"-"+safeVPM(m.IndexName)+".active")
		retired := filepath.Join(s.dir, safeVPM(m.Collection)+"-"+safeVPM(m.IndexName)+".retired")
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
		if e = vectorPartitionPublishFaultV1("active_temp_synced"); e != nil {
			return e
		}
		if e = renameVPM(atmp, active); e != nil {
			return e
		}
		if e = vectorPartitionPublishFaultV1("active_renamed"); e != nil {
			return e
		}
		// The replacement pointer is durable before a stale retired marker is
		// removed. A crash may therefore retain both markers, never neither.
		if e = syncDirVPM(s.dir); e != nil {
			return e
		}
		if e = vectorPartitionPublishFaultV1("active_dir_synced"); e != nil {
			return e
		}
		// The active pointer is durable before a previous inactive marker is
		// removed. A crash may therefore retain both lifecycle records, never
		// leave the index pointerless without the inactive record.
		if err := os.Remove(s.inactivePath(m.Collection, m.IndexName)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.Remove(retired); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if e = vectorPartitionPublishFaultV1("retired_removed"); e != nil {
			return e
		}
	}
	if e = syncDirVPM(s.dir); e != nil {
		return e
	}
	return vectorPartitionPublishFaultV1("publication_complete")
}

func vectorPartitionBuildingPromotionIdentityV1(building, ready VectorPartitionManifestV1) bool {
	// A generation-bound building record already owns all non-router topology
	// and asset identity. Promotion may change only the ready-state fields.
	expected := ready
	expected.State, expected.RouterGeneration, expected.RouterAsset, expected.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	expected.Canonicalize()
	want, wantErr := EncodeVectorPartitionManifestV1(expected)
	got, gotErr := EncodeVectorPartitionManifestV1(building)
	return wantErr == nil && gotErr == nil && bytes.Equal(got, want)
}

func (s *VectorPartitionStoreV1) uniqueTemp(name string) (string, error) {
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return filepath.Join(s.dir, "."+name+"."+hex.EncodeToString(nonce[:])+".tmp"), nil
}
func (s *VectorPartitionStoreV1) Open(collection, index string, generation uint64) (VectorPartitionManifestV1, error) {
	name := fmt.Sprintf("%s-%s-%d.vpm", safeVPM(collection), safeVPM(index), generation)
	raw, e := s.readBounded(name, DefaultVectorPartitionManifestLimits().MaxBytes)
	if e != nil {
		return VectorPartitionManifestV1{}, e
	}
	m, err := DecodeVectorPartitionManifestV1(raw, DefaultVectorPartitionManifestLimits())
	if err != nil {
		return VectorPartitionManifestV1{}, err
	}
	if m.Collection != collection || m.IndexName != index || m.Generation != generation {
		return VectorPartitionManifestV1{}, fmt.Errorf("%w: stored identity mismatch", ErrVectorPartitionManifestInvalid)
	}
	return m, nil
}
func (s *VectorPartitionStoreV1) openDir() (*os.File, error) {
	dir, err := rootpublication.OpenStableParent(s.dir)
	if err != nil {
		return nil, err
	}
	identity, identityErr := rootpublication.StableIdentityFromFile(dir)
	if identityErr != nil {
		dir.Close()
		return nil, identityErr
	}
	if !rootpublication.SamePhysicalIdentity(s.dirIdentity, identity) {
		dir.Close()
		return nil, fmt.Errorf("%w: vector partition store directory identity changed", ErrVectorPartitionManifestInvalid)
	}
	return dir, nil
}

func (s *VectorPartitionStoreV1) readBounded(name string, max int) ([]byte, error) {
	if filepath.Base(name) != name {
		return nil, fmt.Errorf("%w: vector partition record name", ErrVectorPartitionManifestInvalid)
	}
	dir, err := s.openDir()
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	f, err := rootpublication.OpenStableChildFile(dir, name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: vector partition record %q is not a regular file", ErrVectorPartitionManifestInvalid, name)
	}
	raw, err := io.ReadAll(io.LimitReader(f, int64(max)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > max {
		return nil, fmt.Errorf("%w: stored bytes cap", ErrVectorPartitionManifestInvalid)
	}
	return raw, nil
}

// regularEntryPresent reports whether an exact lifecycle entry is present.
// Tombstones are a resurrection fence: a malformed entry is corruption, never
// equivalent to an absent fence.
func (s *VectorPartitionStoreV1) regularEntryPresent(name string) (bool, error) {
	dir, err := s.openDir()
	if err != nil {
		return false, err
	}
	defer dir.Close()
	entry, err := rootpublication.OpenStableChildFile(dir, name, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	info, statErr := entry.Stat()
	closeErr := entry.Close()
	if err := errors.Join(statErr, closeErr); err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("%w: vector partition record %q is not a regular file", ErrVectorPartitionManifestInvalid, name)
	}
	return true, nil
}

func validateVectorPartitionDurableEntryV1(entry os.DirEntry) error {
	if entry.Type()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: vector partition record %q is a symlink", ErrVectorPartitionManifestInvalid, entry.Name())
	}
	info, err := entry.Info()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%w: vector partition record %q is not a regular file", ErrVectorPartitionManifestInvalid, entry.Name())
	}
	return nil
}
func (s *VectorPartitionStoreV1) OpenActive(collection, index string) (VectorPartitionManifestV1, error) {
	return s.openPointer(collection, index, ".active")
}
func (s *VectorPartitionStoreV1) OpenRetired(collection, index string) (VectorPartitionManifestV1, error) {
	return s.openPointer(collection, index, ".retired")
}
func (s *VectorPartitionStoreV1) openPointer(collection, index, suffix string) (VectorPartitionManifestV1, error) {
	name := safeVPM(collection) + "-" + safeVPM(index) + suffix
	raw, e := s.readBounded(name, 32)
	if e != nil {
		return VectorPartitionManifestV1{}, e
	}
	generation, e := readVectorPartitionPointerGenerationV1(raw)
	if e != nil {
		return VectorPartitionManifestV1{}, e
	}
	return s.Open(collection, index, generation)
}
func readVectorPartitionPointerGenerationV1(raw []byte) (uint64, error) {
	if len(raw) < 2 || len(raw) > 32 {
		return 0, fmt.Errorf("%w: active pointer size", ErrVectorPartitionManifestInvalid)
	}
	if raw[len(raw)-1] != '\n' || bytes.IndexByte(raw[:len(raw)-1], '\n') >= 0 {
		return 0, fmt.Errorf("%w: active pointer", ErrVectorPartitionManifestInvalid)
	}
	generation, e := strconv.ParseUint(string(raw[:len(raw)-1]), 10, 64)
	if e != nil || generation == 0 {
		return 0, fmt.Errorf("%w: active pointer", ErrVectorPartitionManifestInvalid)
	}
	return generation, nil
}

// Deactivate is unavailable on a raw store because it has no DB maintenance
// authority and cannot reject read-only handles. Use
// Collection.DeactivateVectorPartitionV1 instead.
func (s *VectorPartitionStoreV1) Deactivate(collection, index string) error {
	return ErrVectorPartitionCollectionAuthorityRequired
}
func (s *VectorPartitionStoreV1) deactivateLocked(collection, index string) error {
	if !vpmNamespacePersistenceSupported() {
		return fmt.Errorf("%w: vector partition deactivation", rootpublication.ErrNamespacePersistenceUnsupported)
	}
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
	if err = renameVPM(tmp, retired); err != nil {
		return err
	}
	if err = syncDirVPM(s.dir); err != nil {
		return err
	}
	if err = os.Remove(s.inactivePath(collection, index)); err != nil && !errors.Is(err, os.ErrNotExist) {
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

var vectorPartitionReaderPinsV1 = struct {
	sync.Mutex
	counts map[string]uint64
}{counts: make(map[string]uint64)}

func vectorPartitionReaderPinKeyV1(root, collection, index string, generation uint64) string {
	canonical, err := canonicalVectorPartitionStorageRootV1(root)
	if err != nil {
		return ""
	}
	return canonical + "\x00" + collection + "\x00" + index + "\x00" + strconv.FormatUint(generation, 10)
}

// VectorPartitionReaderPinV1 is an explicit M1 lifecycle handle. Query
// routing is deferred, but any consumer that opens a generation can hold this
// handle across use so status and cleanup observe real in-process readers.
type VectorPartitionReaderPinV1 struct {
	key  string
	once sync.Once
}

func (p *VectorPartitionReaderPinV1) Release() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		vectorPartitionReaderPinsV1.Lock()
		defer vectorPartitionReaderPinsV1.Unlock()
		if vectorPartitionReaderPinsV1.counts[p.key] <= 1 {
			delete(vectorPartitionReaderPinsV1.counts, p.key)
		} else {
			vectorPartitionReaderPinsV1.counts[p.key]--
		}
	})
}
func vectorPartitionReaderPinCountV1(root, collection, index string, generation uint64) uint64 {
	vectorPartitionReaderPinsV1.Lock()
	defer vectorPartitionReaderPinsV1.Unlock()
	return vectorPartitionReaderPinsV1.counts[vectorPartitionReaderPinKeyV1(root, collection, index, generation)]
}
func (c *Collection) AcquireVectorPartitionReaderPinV1(index string, generation uint64) (*VectorPartitionReaderPinV1, error) {
	if c == nil || c.db == nil {
		return nil, errors.New("collections: closed collection")
	}
	var pin *VectorPartitionReaderPinV1
	err := WithVectorPartitionStorageBarrierV1(c.db.Dir(), func() error {
		s, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
		if err != nil {
			return err
		}
		if _, err := s.Open(c.name, index, generation); err != nil {
			return err
		}
		key := vectorPartitionReaderPinKeyV1(c.db.Dir(), c.name, index, generation)
		if key == "" {
			return errors.New("collections: invalid vector partition pin root")
		}
		vectorPartitionReaderPinsV1.Lock()
		vectorPartitionReaderPinsV1.counts[key]++
		vectorPartitionReaderPinsV1.Unlock()
		pin = &VectorPartitionReaderPinV1{key: key}
		return nil
	})
	return pin, err
}

func (e VectorPartitionCleanupEligibilityV1) Deletable() bool {
	return !e.Active && e.ReaderPins == 0 && e.SnapshotReferences == 0 && e.CatalogReferences == 0
}

// Delete is unavailable on a raw store because it has no DB maintenance
// authority and cannot reject read-only handles. Use
// Collection.DeleteVectorPartitionGenerationV1 instead.
func (s *VectorPartitionStoreV1) Delete(collection, index string, generation uint64, eligibility VectorPartitionCleanupEligibilityV1) error {
	return ErrVectorPartitionCollectionAuthorityRequired
}
func (s *VectorPartitionStoreV1) deleteLocked(collection, index string, generation uint64, eligibility VectorPartitionCleanupEligibilityV1) error {
	if !vpmNamespacePersistenceSupported() {
		return fmt.Errorf("%w: vector partition deletion", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	if !eligibility.Deletable() {
		return fmt.Errorf("collections: vector partition generation %d is still reachable", generation)
	}
	if pins := vectorPartitionReaderPinCountV1(s.root, collection, index, generation); pins != 0 {
		return fmt.Errorf("collections: vector partition generation %d has %d reader pins", generation, pins)
	}
	tombstone := s.deleteTombstonePath(collection, index, generation)
	tombstonePresent, tombstoneErr := s.regularEntryPresent(filepath.Base(tombstone))
	if tombstoneErr != nil {
		return tombstoneErr
	}
	if active, err := s.OpenActive(collection, index); err == nil {
		if active.Generation == generation {
			return fmt.Errorf("collections: vector partition generation %d is active", generation)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("collections: vector partition cleanup active pointer: %w", err)
	}
	retiredName := safeVPM(collection) + "-" + safeVPM(index) + ".retired"
	retiredPath := filepath.Join(s.dir, retiredName)
	retiredRaw, retiredErr := s.readBounded(retiredName, 32)
	var retiredGeneration uint64
	if retiredErr == nil {
		retiredGeneration, retiredErr = readVectorPartitionPointerGenerationV1(retiredRaw)
	}
	retiredMatches := retiredErr == nil && retiredGeneration == generation
	if retiredErr != nil && !errors.Is(retiredErr, os.ErrNotExist) {
		return fmt.Errorf("collections: vector partition cleanup retired pointer: %w", retiredErr)
	}
	if !tombstonePresent {
		m, err := s.Open(collection, index, generation)
		if err != nil {
			return err
		}
		if err := s.writeDeleteTombstone(m); err != nil {
			return err
		}
	} else if _, err := s.openDeleteTombstone(collection, index, generation); err != nil {
		return err
	}
	if hook := vectorPartitionDeleteAfterTombstoneHookV1(); hook != nil {
		hook()
	}
	// Detach the marker before the manifest. Thus any durable interrupted state
	// is either a retryable tombstone with a retained manifest, or no manifest
	// and no pointer that could resurrect it.
	if retiredMatches {
		if err := s.writeInactiveMarker(collection, index, generation); err != nil {
			return err
		}
		if err := os.Remove(retiredPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := syncDirVPM(s.dir); err != nil {
			return err
		}
	}
	p := filepath.Join(s.dir, fmt.Sprintf("%s-%s-%d.vpm", safeVPM(collection), safeVPM(index), generation))
	if e := os.Remove(p); e != nil && !errors.Is(e, os.ErrNotExist) {
		return e
	}
	if err := syncDirVPM(s.dir); err != nil {
		return err
	}
	return syncDirVPM(s.dir)
}
func (s *VectorPartitionStoreV1) deleteTombstonePath(collection, index string, generation uint64) string {
	return filepath.Join(s.dir, s.deleteTombstoneName(collection, index, generation))
}
func (s *VectorPartitionStoreV1) deleteTombstoneName(collection, index string, generation uint64) string {
	return fmt.Sprintf("%s-%s-%d.deleting", safeVPM(collection), safeVPM(index), generation)
}

func (s *VectorPartitionStoreV1) inactivePath(collection, index string) string {
	return filepath.Join(s.dir, s.inactiveName(collection, index))
}
func (s *VectorPartitionStoreV1) inactiveName(collection, index string) string {
	return safeVPM(collection) + "-" + safeVPM(index) + ".inactive"
}

// writeInactiveMarker records that exact deletion detached the last lifecycle
// pointer for this index. It is durable before the retired marker is removed,
// so historical ready manifests remain prepared-only even after reclaim later
// releases the deletion journal.
func (s *VectorPartitionStoreV1) writeInactiveMarker(collection, index string, generation uint64) error {
	path := s.inactivePath(collection, index)
	raw, err := encodeVectorPartitionInactiveStateV1(vectorPartitionInactiveStateV1{Collection: collection, IndexName: index, Generation: generation})
	if err != nil {
		return err
	}
	tmp, err := s.uniqueTemp(filepath.Base(path))
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	if err = os.WriteFile(tmp, raw, 0600); err != nil {
		return err
	}
	if err = syncFileVPM(tmp); err != nil {
		return err
	}
	if err = renameVPM(tmp, path); err != nil {
		return err
	}
	return syncDirVPM(s.dir)
}

func (s *VectorPartitionStoreV1) readInactiveGeneration(collection, index string) (uint64, error) {
	state, err := s.readInactiveStateName(s.inactiveName(collection, index))
	if err != nil {
		return 0, err
	}
	if state.Collection != collection || state.IndexName != index {
		return 0, fmt.Errorf("%w: inactive marker identity", ErrVectorPartitionManifestInvalid)
	}
	return state.Generation, nil
}

func (s *VectorPartitionStoreV1) readInactiveStateName(name string) (vectorPartitionInactiveStateV1, error) {
	raw, err := s.readBounded(name, vectorPartitionInactiveStateMaxEncodedBytesV1())
	if err != nil {
		return vectorPartitionInactiveStateV1{}, err
	}
	return decodeVectorPartitionInactiveStateV1(raw)
}
func (s *VectorPartitionStoreV1) writeDeleteTombstone(m VectorPartitionManifestV1) error {
	state, err := newVectorPartitionReclaimStateV1(m)
	if err != nil {
		return err
	}
	return s.writeDeleteTombstoneState(state)
}
func (s *VectorPartitionStoreV1) writeDeleteTombstoneState(input vectorPartitionReclaimStateV1) error {
	if !vpmNamespacePersistenceSupported() {
		return fmt.Errorf("%w: vector partition reclaim journal", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	state, err := canonicalVectorPartitionReclaimStateV1(input)
	if err != nil {
		return err
	}
	tombstone := s.deleteTombstonePath(state.Collection, state.IndexName, state.Generation)
	raw, err := encodeVectorPartitionReclaimRecordV1(state)
	if err != nil {
		return err
	}
	tmp, err := s.uniqueTemp(filepath.Base(tombstone))
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	if err = os.WriteFile(tmp, raw, 0600); err != nil {
		return err
	}
	if err = syncFileVPM(tmp); err != nil {
		return err
	}
	vectorPartitionReclaimPersistHooksV1.RLock()
	beforeRename := vectorPartitionReclaimPersistHooksV1.beforeRename
	afterCommit := vectorPartitionReclaimPersistHooksV1.afterCommit
	vectorPartitionReclaimPersistHooksV1.RUnlock()
	if beforeRename != nil {
		if err := beforeRename(tombstone, state); err != nil {
			return err
		}
	}
	if err = renameVPM(tmp, tombstone); err != nil {
		return err
	}
	if err = syncDirVPM(s.dir); err != nil {
		return err
	}
	if afterCommit != nil {
		return afterCommit(tombstone, state)
	}
	return nil
}
func (s *VectorPartitionStoreV1) openDeleteTombstone(collection, index string, generation uint64) (vectorPartitionReclaimStateV1, error) {
	raw, err := s.readBounded(s.deleteTombstoneName(collection, index, generation), vectorPartitionReclaimMaxBytesV1)
	if err != nil {
		return vectorPartitionReclaimStateV1{}, err
	}
	state, err := decodeVectorPartitionReclaimRecordV1(raw)
	if err != nil || state.Collection != collection || state.IndexName != index || state.Generation != generation {
		return vectorPartitionReclaimStateV1{}, fmt.Errorf("%w: reclaim record identity", ErrVectorPartitionManifestInvalid)
	}
	return state, nil
}

type vectorPartitionReclaimRecordV1 struct {
	id    string
	state vectorPartitionReclaimStateV1
}

func (s *VectorPartitionStoreV1) reclaimRecords(collection string) ([]vectorPartitionReclaimRecordV1, error) {
	dir, err := s.openDir()
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	prefix := safeVPM(collection) + "-"
	var records []vectorPartitionReclaimRecordV1
	var totalBytes int64
	for {
		entries, readErr := dir.ReadDir(64)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, readErr
		}
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), prefix) || !strings.HasSuffix(entry.Name(), ".deleting") {
				continue
			}
			if err := validateVectorPartitionDurableEntryV1(entry); err != nil {
				return nil, err
			}
			if len(records) >= vectorPartitionStoreMaxEntriesV1 {
				return nil, fmt.Errorf("collections: vector partition reclaim record cap")
			}
			raw, err := s.readBounded(entry.Name(), vectorPartitionReclaimMaxBytesV1)
			if err != nil {
				return nil, err
			}
			if totalBytes > vectorPartitionStoreMaxBytesV1-int64(len(raw)) {
				return nil, fmt.Errorf("collections: vector partition reclaim bytes cap")
			}
			totalBytes += int64(len(raw))
			state, err := decodeVectorPartitionReclaimRecordV1(raw)
			if err != nil || state.Collection != collection || entry.Name() != s.deleteTombstoneName(state.Collection, state.IndexName, state.Generation) {
				return nil, fmt.Errorf("%w: reclaim record", ErrVectorPartitionManifestInvalid)
			}
			records = append(records, vectorPartitionReclaimRecordV1{id: entry.Name(), state: state})
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].id < records[j].id })
	return records, nil
}

func vectorPartitionReclaimRefsEqualV1(a, b []ColumnAssetRef) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type vectorPartitionReclaimSegmentKeyV1 struct {
	namespace string
	fileID    uint32
}

func (s *VectorPartitionStoreV1) persistVectorPartitionRewriteDebtV1(records []vectorPartitionReclaimRecordV1, oldRefs []ColumnAssetRef) error {
	if len(oldRefs) == 0 {
		return nil
	}
	updates := make([]vectorPartitionReclaimStateV1, len(records))
	changed := make([]bool, len(records))
	recordsBySegment := make(map[vectorPartitionReclaimSegmentKeyV1][]int)
	for i := range records {
		seen := make(map[vectorPartitionReclaimSegmentKeyV1]struct{})
		for _, ref := range records[i].state.debtRefs() {
			key := vectorPartitionReclaimSegmentKeyV1{namespace: ref.Namespace, fileID: ref.FileID}
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			recordsBySegment[key] = append(recordsBySegment[key], i)
		}
	}
	for _, oldRef := range oldRefs {
		key := vectorPartitionReclaimSegmentKeyV1{namespace: oldRef.Namespace, fileID: oldRef.FileID}
		indexes := recordsBySegment[key]
		if len(indexes) == 0 {
			return fmt.Errorf("%w: rewrite ref has no reclaim segment", ErrVectorPartitionManifestInvalid)
		}
		for _, i := range indexes {
			if updates[i].Collection == "" {
				updates[i] = records[i].state.clone()
			}
			updates[i].SupersededRefs = append(updates[i].SupersededRefs, oldRef)
		}
	}
	for i := range records {
		if updates[i].Collection == "" {
			updates[i] = records[i].state.clone()
		}
		canonical, err := canonicalVectorPartitionReclaimStateV1(updates[i])
		if err != nil {
			return err
		}
		updates[i] = canonical
		changed[i] = !vectorPartitionReclaimRefsEqualV1(canonical.SupersededRefs, records[i].state.SupersededRefs)
	}
	var updatedBytes int64
	for i := range updates {
		raw, err := encodeVectorPartitionReclaimRecordV1(updates[i])
		if err != nil {
			return err
		}
		if updatedBytes > vectorPartitionStoreMaxBytesV1-int64(len(raw)) {
			return fmt.Errorf("%w: reclaim records exceed store bytes cap", ErrVectorPartitionManifestInvalid)
		}
		updatedBytes += int64(len(raw))
	}
	for i := range records {
		if !changed[i] {
			continue
		}
		if err := s.writeDeleteTombstoneState(updates[i]); err != nil {
			return err
		}
		records[i].state = updates[i]
	}
	return nil
}

// Use the same bounded convergence budget as RecoverableRootSet capture.
const vectorPartitionReclaimRecoverableRootAttemptsV1 = 8

func shouldRefreshVectorPartitionReclaimGCPlanV1(err error, stats ColumnAssetGCStats, attempt int) bool {
	return errors.Is(err, backenddb.ErrRecoverableRootSetStale) &&
		stats.SegmentsDeleted == 0 &&
		attempt+1 < vectorPartitionReclaimRecoverableRootAttemptsV1
}

// DeactivateVectorPartitionV1 retires the active generation under DB-owned
// maintenance authority. It is the public lifecycle entrypoint; raw stores
// deliberately cannot mutate a DB-owned namespace.
func (c *Collection) DeactivateVectorPartitionV1(index string) error {
	if c == nil || c.db == nil {
		return errors.New("collections: closed collection")
	}
	if err := c.db.CheckStorageMaintenanceReady(); err != nil {
		return err
	}
	return c.withVectorPartitionStorageMutationV1("deactivate", func() error {
		s, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
		if err != nil {
			return err
		}
		return s.deactivateLocked(c.name, index)
	})
}

// DeleteVectorPartitionGenerationV1 removes a retired manifest after the
// caller has supplied every reachability fence. Reclaiming its asset debt is a
// separate Collection.ReclaimVectorPartitionGenerationV1 operation.
func (c *Collection) DeleteVectorPartitionGenerationV1(index string, generation uint64, eligibility VectorPartitionCleanupEligibilityV1) error {
	if c == nil || c.db == nil {
		return errors.New("collections: closed collection")
	}
	if err := c.db.CheckStorageMaintenanceReady(); err != nil {
		return err
	}
	return c.withVectorPartitionStorageMutationV1("delete", func() error {
		s, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
		if err != nil {
			return err
		}
		return s.deleteLocked(c.name, index, generation, eligibility)
	})
}

// ReclaimVectorPartitionGenerationV1 is the only path that releases a
// persisted partition reclaim record. It holds the collection mutation barrier
// throughout rewrite and GC, excludes only this exact record from VPM prepared
// roots, journals every old live ref before remap publication, and removes the
// record only after every original and superseded segment is physically absent.
// All errors leave a checksummed, canonical record durable for retry.
func (c *Collection) ReclaimVectorPartitionGenerationV1(ctx context.Context, index string, generation uint64) (ColumnAssetGCStats, error) {
	var zero ColumnAssetGCStats
	if c == nil || c.db == nil {
		return zero, errors.New("collections: closed collection")
	}
	if !vpmNamespacePersistenceSupported() {
		return zero, fmt.Errorf("%w: vector partition reclaim", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if err := c.db.CheckStorageMaintenanceReady(); err != nil {
		return zero, err
	}
	err := c.withVectorPartitionStorageMutationV1(vectorPartitionMutationOperationReclaimV1, func() error {
		s, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
		if err != nil {
			return err
		}
		if _, err := s.openDeleteTombstone(c.name, index, generation); err != nil {
			return err
		}
		records, err := s.reclaimRecords(c.name)
		if err != nil {
			return err
		}
		reclaimIDs := make(map[string]struct{}, len(records))
		refs := make([]ColumnAssetRef, 0, len(records)*2)
		expectedNamespace := ""
		if cfg := c.meta.Options.ColumnStore; cfg != nil && cfg.AssetManager != nil {
			expectedNamespace = cfg.AssetManager.Namespace
		}
		if expectedNamespace == "" {
			return errors.New("collections: vector partition reclaim requires column asset namespace")
		}
		requestedID := s.deleteTombstoneName(c.name, index, generation)
		requestedComplete := false
		for _, record := range records {
			reclaimIDs[record.id] = struct{}{}
			for _, ref := range record.state.debtRefs() {
				if ref.Namespace != expectedNamespace {
					return fmt.Errorf("%w: reclaim record collection namespace", ErrVectorPartitionManifestInvalid)
				}
				refs = append(refs, ref)
			}
		}
		rewrite, err := c.columnAssetRewrite(ctx, columnAssetRewriteOptions{
			ColumnAssetRewriteOptions: ColumnAssetRewriteOptions{CandidateRefs: refs},
			beforeRemapPublish: func(oldRefs []ColumnAssetRef) error {
				return s.persistVectorPartitionRewriteDebtV1(records, oldRefs)
			},
			releaseVectorPartitionReclaimIDs: reclaimIDs,
		})
		if err != nil {
			return err
		}
		candidates := make([]ColumnAssetRef, 0, len(refs)+len(rewrite.SupersededRefs))
		for _, record := range records {
			candidates = append(candidates, record.state.debtRefs()...)
		}
		var stats ColumnAssetGCStats
		// Durable-root activation can advance while GC audits the captured
		// recovery closure. Rebuild from a fresh plan/capability only when the
		// stale pass has not deleted anything; retrying a partially applied
		// destructive pass would not preserve the original candidate frontier.
		for attempt := 0; ; attempt++ {
			stats, err = c.columnAssetGC(ctx, ColumnAssetGCOptions{
				CandidateRefs:                    candidates,
				releaseVectorPartitionReclaimIDs: reclaimIDs,
			})
			if err == nil {
				break
			}
			if !shouldRefreshVectorPartitionReclaimGCPlanV1(err, stats, attempt) {
				return err
			}
		}
		for _, record := range records {
			complete := true
			for _, ref := range record.state.debtRefs() {
				path, err := columnAssetSegmentPath(c.db.ColumnAssetRootDir(), ref)
				if err != nil {
					return err
				}
				if _, err := os.Stat(path); err == nil {
					complete = false
				} else if !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
			if complete {
				if err := os.Remove(filepath.Join(s.dir, record.id)); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
				if record.id == requestedID {
					requestedComplete = true
				}
			}
		}
		if err := syncDirVPM(s.dir); err != nil {
			return err
		}
		if !requestedComplete {
			return fmt.Errorf("collections: vector partition reclaim generation %d not physically complete", generation)
		}
		zero = stats
		return nil
	})
	return zero, err
}

type VectorPartitionStatusV1 struct {
	Manifest                                                VectorPartitionManifestV1
	Ready, Active                                           bool
	StaleReason                                             string
	PartitionCount, GroupCount                              uint32
	Memberships, OverlapMemberships, AssetBytes, ReaderPins uint64
}

// PublishVectorPartitionManifestV1 binds the durable generation to this
// collection's currently declared vector-index definition before publication.
func (c *Collection) PublishVectorPartitionManifestV1(m VectorPartitionManifestV1, resources *rootpublication.StableResourceSet) error {
	if c == nil || c.db == nil {
		if resources != nil {
			resources.Release()
		}
		return errors.New("collections: closed collection")
	}
	if m.State != "ready" && resources != nil {
		resources.Release()
		return errors.New("collections: non-ready vector partition publication must not carry stable resources")
	}
	if m.State == "ready" {
		if resources == nil {
			return errors.New("collections: ready vector partition publication requires stable resources")
		}
		// Transfer ownership before any preflight or source check: every ready
		// return path must release the producer's exact identity pins once.
		defer resources.Release()
	}
	if err := c.db.CheckStorageMaintenanceReady(); err != nil {
		return err
	}
	return c.withVectorPartitionStorageMutationV1(vectorPartitionMutationOperationPublishV1, func() error {
		if err := preflightVectorPartitionManifestV1(m, DefaultVectorPartitionManifestLimits()); err != nil {
			return err
		}
		m.Canonicalize()
		if err := m.Validate(DefaultVectorPartitionManifestLimits()); err != nil {
			return err
		}
		// The source validation and active-pointer rename must share the collection
		// mutation barrier. A catalog read lock alone does not prevent a column
		// publication from advancing TVIS between validation and activation.
		// Do not hold catalogMu across VectorPartitionSourceIdentityV1 below:
		// VectorIndexStatus may refresh the snapshot catalog and must take its write
		// lock in rememberCatalog. The mutation barrier already prevents a source
		// publication from advancing between source validation and activation.
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
			prepared := make([]ColumnPreparedAsset, 0, len(m.Assets)+1)
			for _, asset := range m.Assets {
				prepared = append(prepared, ColumnPreparedAsset{Ref: asset.Ref, Bytes: int64(asset.Bytes)})
			}
			prepared = append(prepared, ColumnPreparedAsset{Ref: m.RouterAsset.Ref, Bytes: int64(m.RouterAsset.Bytes)})
			if err := validateStableColumnResourcesMatchPrepared(prepared, resources); err != nil {
				return fmt.Errorf("collections: vector partition stable publication authority: %w", err)
			}
		}
		// Building manifests have no producer-issued stable-resource set, but they
		// must still prove every referenced asset exists and matches before their
		// raw durable reference is published. This closes the GC/rewrite-wins order:
		// a manifest may not turn a reclaimed ref into dangling durable state.
		namespace := ""
		if cfg := c.meta.Options.ColumnStore; cfg != nil && cfg.AssetManager != nil {
			namespace = cfg.AssetManager.Namespace
		}
		assets := append([]VectorPartitionAssetV1(nil), m.Assets...)
		if m.State == "ready" {
			assets = append(assets, m.RouterAsset)
		}
		if err := verifyVectorPartitionAssetsV1(c.db.ColumnAssetRootDir(), namespace, assets); err != nil {
			return err
		}
		s, e := OpenVectorPartitionStoreV1(c.db.Dir())
		if e != nil {
			return e
		}
		return s.publishLocked(m)
	})
}

// validateVectorPartitionSourceIdentityV1 deliberately obtains the source
// identity from the live TVIS/column-manifest authority, rather than trusting a
// builder-supplied copy. VectorIndexStatus validates TVIS against the active
// manifest and its typed assets before this snapshot is inspected.
func (c *Collection) validateVectorPartitionSourceIdentityV1(m VectorPartitionManifestV1) error {
	identity, err := c.VectorPartitionSourceIdentityV1(m.IndexName)
	if err != nil {
		return err
	}
	if m.SourceGeneration != identity.Generation || m.SourceChecksum != identity.Checksum || m.SourceSchemaHash != identity.SchemaHash || m.SourceRowCount != identity.RowCount {
		return fmt.Errorf("collections: vector partition source identity mismatch")
	}
	return nil
}

// VectorPartitionSourceIdentityV1 returns the authoritative identity of a
// loaded column-graph vector index for a ready partition publication.
func (c *Collection) VectorPartitionSourceIdentityV1(indexName string) (VectorPartitionSourceIdentityV1, error) {
	if c == nil {
		return VectorPartitionSourceIdentityV1{}, errCollectionNil
	}
	if c.db == nil {
		return VectorPartitionSourceIdentityV1{}, errCollectionDBNil
	}
	status, err := c.VectorIndexStatus(indexName)
	if err != nil {
		return VectorPartitionSourceIdentityV1{}, fmt.Errorf("collections: vector partition source status: %w", err)
	}
	if !status.Loaded || status.State != VectorIndexStateColumnGraphLoaded {
		return VectorPartitionSourceIdentityV1{}, fmt.Errorf("collections: vector partition source index %q is not a loaded TVIS generation", indexName)
	}
	_, graph, _, err := c.columnVectorGraphPhysicalRowReaderSnapshotView(indexName)
	if err != nil {
		return VectorPartitionSourceIdentityV1{}, fmt.Errorf("collections: vector partition source identity: %w", err)
	}
	return VectorPartitionSourceIdentityV1{Generation: graph.BaseManifestGeneration, Checksum: graph.BaseManifestChecksum, SchemaHash: graph.BaseSchemaHash, RowCount: uint64(graph.RowCount)}, nil
}

func verifyVectorPartitionAssetsV1(root, namespace string, assets []VectorPartitionAssetV1) error {
	for _, a := range assets {
		if err := validateColumnAssetRefForPlan(a.Ref); err != nil {
			return fmt.Errorf("collections: vector partition asset %q ref: %w", a.ID, err)
		}
		if a.Ref.Length < 0 || uint64(a.Ref.Length) != a.Bytes {
			return fmt.Errorf("collections: vector partition asset %q ref bytes mismatch", a.ID)
		}
		if namespace == "" || a.Ref.Namespace != namespace {
			return fmt.Errorf("collections: vector partition asset %q foreign namespace", a.ID)
		}
		path, err := columnAssetSegmentPath(root, a.Ref)
		if err != nil {
			return fmt.Errorf("collections: vector partition asset %q: %w", a.ID, err)
		}
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("collections: vector partition asset %q: %w", a.ID, err)
		}
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return err
		}
		if a.Ref.Offset < 0 || a.Ref.Length < 0 || a.Ref.Offset > info.Size() || a.Ref.Length > info.Size()-a.Ref.Offset {
			file.Close()
			return fmt.Errorf("collections: vector partition asset %q truncated", a.ID)
		}
		section := io.NewSectionReader(file, a.Ref.Offset, a.Ref.Length)
		sha := sha256.New()
		crc := crc32.NewIEEE()
		written, err := io.CopyBuffer(io.MultiWriter(sha, crc), section, make([]byte, 64<<10))
		closeErr := file.Close()
		if err != nil || closeErr != nil || uint64(written) != a.Bytes {
			return fmt.Errorf("collections: vector partition asset %q streaming read", a.ID)
		}
		if crc.Sum32() != a.Ref.Checksum {
			return fmt.Errorf("collections: vector partition asset %q CRC mismatch", a.ID)
		}
		if hex.EncodeToString(sha.Sum(nil)) != a.Checksum {
			return fmt.Errorf("collections: vector partition asset %q sha256 mismatch", a.ID)
		}
	}
	return nil
}

func (c *Collection) VectorPartitionStatusV1(index string, generation uint64) (VectorPartitionStatusV1, error) {
	if c == nil || c.db == nil {
		return VectorPartitionStatusV1{}, errors.New("collections: closed collection")
	}
	s, e := OpenExistingVectorPartitionStoreV1(c.db.Dir())
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
	active := false
	staleReason := "generation_building"
	if m.State == "ready" {
		staleReason = "inactive"
		current, activeErr := s.OpenActive(c.name, index)
		activePointerPresent := false
		if _, statErr := os.Lstat(filepath.Join(s.dir, safeVPM(c.name)+"-"+safeVPM(index)+".active")); statErr == nil {
			activePointerPresent = true
		} else if !errors.Is(statErr, os.ErrNotExist) {
			staleReason = "pointer_invalid"
		}
		switch {
		case activeErr == nil:
			if current.Generation == generation {
				active, staleReason = true, ""
			} else {
				staleReason = "replaced"
			}
		case !errors.Is(activeErr, os.ErrNotExist):
			staleReason = "pointer_invalid"
		case activePointerPresent:
			staleReason = "pointer_invalid"
		case staleReason == "pointer_invalid":
			// Lstat could not establish whether the active pointer exists.
		default:
			retired, retiredErr := s.OpenRetired(c.name, index)
			switch {
			case retiredErr == nil && retired.Generation == generation:
				staleReason = "retired"
			case retiredErr == nil:
				staleReason = "inactive"
			case !errors.Is(retiredErr, os.ErrNotExist):
				staleReason = "pointer_invalid"
			default:
				if _, inactiveErr := s.readInactiveGeneration(c.name, index); inactiveErr != nil {
					staleReason = "pointer_invalid"
				}
			}
		}
		if staleReason != "pointer_invalid" && c.validateVectorPartitionSourceIdentityV1(m) != nil {
			active = false
			staleReason = "source_stale"
		}
	}
	return VectorPartitionStatusV1{Manifest: m, Ready: m.State == "ready", Active: active, StaleReason: staleReason, PartitionCount: m.PartitionCount, GroupCount: uint32(len(groups)), Memberships: uint64(len(m.Memberships)), OverlapMemberships: uint64(len(m.OverlapMemberships)), AssetBytes: total, ReaderPins: vectorPartitionReaderPinCountV1(c.db.Dir(), c.name, index, generation)}, nil
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
