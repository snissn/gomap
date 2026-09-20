package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"reflect"
	"slices"
)

const (
	m8MembershipFeasibilityMethodV1   = "retained_membership_joint_domain_pack_v1"
	m8MembershipFeasibilityMaxBytesV1 = 32 << 20
)

type m8MembershipFeasibilityPackV1 struct {
	PackID       uint32 `json:"pack_id"`
	DomainID     uint32 `json:"domain_id"`
	PlannedBytes uint64 `json:"planned_bytes"`
	ActualBytes  uint64 `json:"actual_bytes"`
}

type m8MembershipFeasibilityQueryV1 struct {
	QueryOrdinal   int      `json:"query_ordinal"`
	TruthSHA256    string   `json:"truth_sha256"`
	TruthCount     int      `json:"truth_count"`
	Hits           int      `json:"hits"`
	Recall         float64  `json:"recall"`
	CoverageMask   uint16   `json:"coverage_mask"`
	SelectedDomain []uint32 `json:"selected_domains"`
	ExpandedPacks  []uint32 `json:"expanded_physical_packs"`
	PackCost       int64    `json:"physical_pack_cost"`
}

type m8MembershipFeasibilityV1 struct {
	SchemaVersion         int                              `json:"schema_version"`
	ResultKind            string                           `json:"result_kind"`
	Method                string                           `json:"method"`
	Status                string                           `json:"status"`
	VariantID             string                           `json:"variant_id"`
	FixtureChecksum       string                           `json:"fixture_checksum"`
	TruthIdentity         string                           `json:"truth_identity"`
	TruthArtifactSHA256   string                           `json:"truth_artifact_sha256"`
	BuildIdentityDigest   string                           `json:"build_identity_digest"`
	ManifestIntegrity     string                           `json:"manifest_integrity"`
	ReadySetDigest        string                           `json:"ready_set_digest"`
	ShardGenerationDigest string                           `json:"shard_generation_digest"`
	MembershipDigest      string                           `json:"membership_digest"`
	SourceVectors         int                              `json:"source_vectors"`
	LogicalDomains        int                              `json:"logical_domains"`
	PhysicalPacks         int                              `json:"physical_packs"`
	DomainLimit           int                              `json:"logical_domain_limit"`
	PackLimit             int64                            `json:"physical_pack_limit"`
	RequiredRecall        float64                          `json:"required_recall"`
	TotalHits             int                              `json:"total_hits"`
	PossibleHits          int                              `json:"possible_hits"`
	Ceiling               float64                          `json:"membership_recall_ceiling"`
	ActualPackBytes       uint64                           `json:"actual_pack_bytes"`
	ActualBytesPerVector  float64                          `json:"actual_pack_bytes_per_source_vector"`
	WorkBound             int64                            `json:"work_bound"`
	ScratchBytes          int64                            `json:"scratch_bytes"`
	Packs                 []m8MembershipFeasibilityPackV1  `json:"packs"`
	Queries               []m8MembershipFeasibilityQueryV1 `json:"queries"`
}

type m8MembershipFeasibilityArtifactV1 struct {
	Path           string                    `json:"path"`
	ArtifactSHA256 string                    `json:"artifact_sha256"`
	Result         m8MembershipFeasibilityV1 `json:"result"`
}

type m8MembershipFeasibilityWitnessV1 struct {
	hits     int
	packCost int64
	mask     uint16
	domains  []uint32
}

func m8MembershipWitnessBetterV1(candidate, current m8MembershipFeasibilityWitnessV1) bool {
	if candidate.hits != current.hits {
		return candidate.hits > current.hits
	}
	if candidate.packCost != current.packCost {
		return candidate.packCost < current.packCost
	}
	if len(candidate.domains) != len(current.domains) {
		return len(candidate.domains) < len(current.domains)
	}
	return slices.Compare(candidate.domains, current.domains) < 0
}

func m8MembershipFeasibilityPlanV1(queries, truthCount, domains, domainLimit int) (work, scratch int64, err error) {
	if queries < 1 || truthCount < 1 || truthCount > 10 || domains < 1 || domainLimit < 1 || domainLimit > 2 || domainLimit > domains {
		return 0, 0, errors.New("invalid membership feasibility work shape")
	}
	states := int64(1 << uint(truthCount))
	cells, err := m8CoverageCheckedMulV1(int64(domainLimit)+1, states)
	if err != nil {
		return 0, 0, err
	}
	dpWork, err := m8CoverageCheckedMulV1(cells, int64(domains)+2)
	if err != nil {
		return 0, 0, err
	}
	candidates := int64(1 + domains)
	if domainLimit == 2 {
		pairs, err := m8CoverageCheckedMulV1(int64(domains), int64(domains-1))
		if err != nil {
			return 0, 0, err
		}
		candidates, err = m8CoverageCheckedAddV1(candidates, pairs/2)
		if err != nil {
			return 0, 0, err
		}
	}
	referenceWork, err := m8CoverageCheckedMulV1(candidates, 4)
	if err != nil {
		return 0, 0, err
	}
	perQuery, err := m8CoverageCheckedAddV1(dpWork, referenceWork)
	if err != nil {
		return 0, 0, err
	}
	work, err = m8CoverageCheckedMulV1(perQuery, int64(queries))
	if err != nil {
		return 0, 0, err
	}
	dpBytes, err := m8CoverageCheckedMulV1(cells, 8)
	if err != nil {
		return 0, 0, err
	}
	referenceBytes, err := m8CoverageCheckedMulV1(int64(domains), 16)
	if err != nil {
		return 0, 0, err
	}
	scratch, err = m8CoverageCheckedAddV1(dpBytes, referenceBytes)
	return work, scratch, err
}

// m8MembershipEnumerateP2V1 is deliberately independent of the DP. The
// admitted gate is P<=2, so exhaustive singles/pairs give us both a reference
// answer and the exact selected-set witness without a second general solver.
func m8MembershipEnumerateP2V1(masks []uint16, packCosts []int64, domainLimit int, packLimit int64, limits m8CoverageLimitsV1) (m8MembershipFeasibilityWitnessV1, error) {
	if len(masks) == 0 || len(masks) != len(packCosts) || domainLimit < 1 || domainLimit > 2 || packLimit < 1 || limits.WorkUnits < 1 || limits.Bytes < 1 {
		return m8MembershipFeasibilityWitnessV1{}, errors.New("invalid P<=2 membership enumeration")
	}
	domains := int64(len(masks))
	candidates := int64(1) + domains
	if domainLimit == 2 {
		pairs, err := m8CoverageCheckedMulV1(domains, domains-1)
		if err != nil {
			return m8MembershipFeasibilityWitnessV1{}, err
		}
		candidates, err = m8CoverageCheckedAddV1(candidates, pairs/2)
		if err != nil {
			return m8MembershipFeasibilityWitnessV1{}, err
		}
	}
	work, err := m8CoverageCheckedMulV1(candidates, 4)
	if err != nil || work > limits.WorkUnits || int64(len(masks))*16 > limits.Bytes {
		return m8MembershipFeasibilityWitnessV1{}, errors.New("P<=2 membership enumeration exceeds resource limits")
	}
	best := m8MembershipFeasibilityWitnessV1{}
	var bestDomains [2]uint32
	consider := func(mask uint16, cost int64, domains ...uint32) {
		if cost > packLimit {
			return
		}
		candidate := m8MembershipFeasibilityWitnessV1{hits: bits.OnesCount16(mask), packCost: cost, mask: mask, domains: domains}
		if m8MembershipWitnessBetterV1(candidate, best) {
			best.hits, best.packCost, best.mask = candidate.hits, candidate.packCost, candidate.mask
			copy(bestDomains[:], domains)
			best.domains = bestDomains[:len(domains)]
		}
	}
	for i := range masks {
		if packCosts[i] < 1 || packCosts[i] > math.MaxInt {
			return m8MembershipFeasibilityWitnessV1{}, errors.New("invalid membership domain pack cost")
		}
		consider(masks[i], packCosts[i], uint32(i))
		if domainLimit == 2 {
			for j := i + 1; j < len(masks); j++ {
				if packCosts[j] < 1 || packCosts[i] > math.MaxInt64-packCosts[j] {
					return m8MembershipFeasibilityWitnessV1{}, errors.New("membership pack cost overflow")
				}
				consider(masks[i]|masks[j], packCosts[i]+packCosts[j], uint32(i), uint32(j))
			}
		}
	}
	best.domains = slices.Clone(best.domains)
	return best, nil
}

func m8MembershipExpandDomainsV1(domainPacks [][]uint32, domains []uint32, packLimit int64) ([]uint32, error) {
	if len(domainPacks) == 0 || packLimit < 1 {
		return nil, errors.New("invalid membership domain-pack expansion")
	}
	var expanded []uint32
	for i, domain := range domains {
		if int(domain) >= len(domainPacks) || len(domainPacks[domain]) == 0 || i > 0 && domain <= domains[i-1] {
			return nil, errors.New("invalid or noncanonical membership witness domains")
		}
		for j, pack := range domainPacks[domain] {
			if j > 0 && pack <= domainPacks[domain][j-1] {
				return nil, errors.New("noncanonical membership domain packs")
			}
			expanded = append(expanded, pack)
		}
		if int64(len(expanded)) > packLimit {
			return nil, errors.New("membership witness exceeds physical-pack limit")
		}
	}
	slices.Sort(expanded)
	for i := 1; i < len(expanded); i++ {
		if expanded[i] == expanded[i-1] {
			return nil, errors.New("physical pack belongs to multiple selected domains")
		}
	}
	return expanded, nil
}

func m8ComputeRetainedMembershipFeasibilityV1(cfg config, fixture fixtureManifest, dir string, descriptor m3VariantDescriptorV1) (_ m8MembershipFeasibilityV1, err error) {
	assets, err := openM8ProductionExistingAssetSetModeV1(dir, true)
	if err != nil {
		return m8MembershipFeasibilityV1{}, err
	}
	defer func() { err = errors.Join(err, assets.Close()) }()
	manifest := assets.status.Manifest
	if err := m3DescriptorMatchesManifestV1(descriptor, fixture, manifest, assets.status.ModelDigest, assets.status.Config); err != nil {
		return m8MembershipFeasibilityV1{}, err
	}
	record, err := m3ValidateRetainedShardPackBytesV1(dir, descriptor, manifest.Assets)
	if err != nil {
		return m8MembershipFeasibilityV1{}, err
	}
	truth, truthEvidence, err := m8LoadOrComputeTruthV1(cfg.m8TruthCache, nil, manifest, fixture, make([][]float64, fixture.Queries), cfg.topK, cfg.m8TruthCacheSHA256)
	if err != nil {
		return m8MembershipFeasibilityV1{}, fmt.Errorf("load retained feasibility truth: %w", err)
	}
	packDomains, packCosts, err := m8QualityPackOwnersV1(manifest)
	if err != nil {
		return m8MembershipFeasibilityV1{}, err
	}
	domainPacks := make([][]uint32, len(packCosts))
	for pack, domain := range packDomains {
		domainPacks[domain] = append(domainPacks[domain], uint32(pack))
	}
	_, packMemberships, err := m8TruthPartitionMembershipsByDocumentIDV1(assets, truth)
	if err != nil {
		return m8MembershipFeasibilityV1{}, err
	}
	_, memberships, membershipDomains, err := m8OracleDomainMembershipsV1(nil, packMemberships, manifest)
	if err != nil {
		return m8MembershipFeasibilityV1{}, err
	}
	if membershipDomains != len(packCosts) {
		return m8MembershipFeasibilityV1{}, errors.New("retained membership domains disagree with pack ownership")
	}
	result := m8MembershipFeasibilityV1{
		SchemaVersion: 1, ResultKind: "m8_membership_feasibility_v1", Method: m8MembershipFeasibilityMethodV1, Status: "insufficient",
		VariantID: descriptor.VariantID, FixtureChecksum: fixture.Checksum, TruthIdentity: truthEvidence.Identity, TruthArtifactSHA256: truthEvidence.ArtifactSHA256,
		BuildIdentityDigest: descriptor.BuildIdentityDigest, ManifestIntegrity: manifest.IntegrityDigest, ReadySetDigest: manifest.ReadySetDigest,
		ShardGenerationDigest: descriptor.ShardGenerationDigest, MembershipDigest: record.MembershipDigest,
		SourceVectors: fixture.Vectors, LogicalDomains: len(packCosts), PhysicalPacks: len(packDomains), DomainLimit: cfg.m8MembershipProbes, PackLimit: int64(cfg.m8MembershipPackLimit), RequiredRecall: cfg.recallTarget,
		Queries: make([]m8MembershipFeasibilityQueryV1, len(truth)), Packs: make([]m8MembershipFeasibilityPackV1, len(manifest.Assets)),
	}
	result.WorkBound, result.ScratchBytes, err = m8MembershipFeasibilityPlanV1(len(truth), min(cfg.topK, fixture.Vectors), len(packCosts), cfg.m8MembershipProbes)
	if err != nil {
		return m8MembershipFeasibilityV1{}, err
	}
	if result.WorkBound > maxBenchmarkWorkUnits || result.ScratchBytes > cfg.maxBytes {
		return m8MembershipFeasibilityV1{}, fmt.Errorf("membership feasibility exceeds resource limits: work=%d bytes=%d", result.WorkBound, result.ScratchBytes)
	}
	for _, asset := range manifest.Assets {
		pack := int(asset.PartitionID)
		if pack >= len(result.Packs) {
			return m8MembershipFeasibilityV1{}, errors.New("retained asset pack is outside feasibility layout")
		}
		result.Packs[pack] = m8MembershipFeasibilityPackV1{PackID: asset.PartitionID, DomainID: packDomains[pack], PlannedBytes: record.PackSummaries[pack].Bytes, ActualBytes: asset.Bytes}
		if asset.Bytes > math.MaxUint64-result.ActualPackBytes {
			return m8MembershipFeasibilityV1{}, errors.New("retained actual pack bytes overflow")
		}
		result.ActualPackBytes += asset.Bytes
	}
	result.ActualBytesPerVector = float64(result.ActualPackBytes) / float64(fixture.Vectors)
	limits := m8CoverageLimitsV1{WorkUnits: maxBenchmarkWorkUnits, Bytes: cfg.maxBytes}
	for query, row := range truth {
		result.PossibleHits += len(row)
		ids := m8CanonicalIDsV1(row)
		masks, err := m8CoverageMasksV1(ids, memberships, len(packCosts))
		if err != nil {
			return m8MembershipFeasibilityV1{}, err
		}
		dpHits, err := m8CoverageJointHitsV1(context.Background(), len(row), masks, packCosts, cfg.m8MembershipProbes, int64(cfg.m8MembershipPackLimit), limits)
		if err != nil {
			return m8MembershipFeasibilityV1{}, err
		}
		witness, err := m8MembershipEnumerateP2V1(masks, packCosts, cfg.m8MembershipProbes, int64(cfg.m8MembershipPackLimit), limits)
		if err != nil {
			return m8MembershipFeasibilityV1{}, err
		}
		if witness.hits != dpHits {
			return m8MembershipFeasibilityV1{}, fmt.Errorf("query %d joint DP hits=%d disagree with direct P<=2 reference=%d", query, dpHits, witness.hits)
		}
		expanded, err := m8MembershipExpandDomainsV1(domainPacks, witness.domains, int64(cfg.m8MembershipPackLimit))
		if err != nil {
			return m8MembershipFeasibilityV1{}, err
		}
		if int64(len(expanded)) != witness.packCost {
			return m8MembershipFeasibilityV1{}, errors.New("membership witness physical-pack expansion disagrees with charged cost")
		}
		result.Queries[query] = m8MembershipFeasibilityQueryV1{QueryOrdinal: query, TruthSHA256: m8QualityTruthDigestV1(row), TruthCount: len(row), Hits: witness.hits, Recall: float64(witness.hits) / float64(len(row)), CoverageMask: witness.mask, SelectedDomain: witness.domains, ExpandedPacks: expanded, PackCost: witness.packCost}
		result.TotalHits += witness.hits
	}
	if result.PossibleHits == 0 {
		return m8MembershipFeasibilityV1{}, errors.New("membership feasibility has no truth hits")
	}
	result.Ceiling = float64(result.TotalHits) / float64(result.PossibleHits)
	if result.Ceiling >= result.RequiredRecall {
		result.Status = "sufficient"
	}
	return result, m8ValidateMembershipFeasibilityV1(result)
}

func m8ValidateMembershipFeasibilityV1(result m8MembershipFeasibilityV1) error {
	if result.SchemaVersion != 1 || result.ResultKind != "m8_membership_feasibility_v1" || result.Method != m8MembershipFeasibilityMethodV1 ||
		result.Status != "sufficient" && result.Status != "insufficient" || result.VariantID == "" || result.TruthIdentity == "" ||
		!m8SHA256V1(result.FixtureChecksum) || !m8SHA256V1(result.TruthArtifactSHA256) || !m8SHA256V1(result.BuildIdentityDigest) ||
		!m8SHA256V1(result.ManifestIntegrity) || !m8SHA256V1(result.ReadySetDigest) || !m8SHA256V1(result.ShardGenerationDigest) || !m8SHA256V1(result.MembershipDigest) ||
		result.SourceVectors < 1 || result.LogicalDomains < 1 || result.PhysicalPacks < result.LogicalDomains || result.PhysicalPacks > maxPartitions ||
		result.DomainLimit < 1 || result.DomainLimit > 2 || result.DomainLimit > result.LogicalDomains || result.PackLimit < 1 || result.PackLimit > int64(result.PhysicalPacks) ||
		math.IsNaN(result.RequiredRecall) || math.IsInf(result.RequiredRecall, 0) || result.RequiredRecall < 0 || result.RequiredRecall > 1 || len(result.Packs) != result.PhysicalPacks || len(result.Queries) == 0 {
		return errors.New("invalid membership feasibility identity or shape")
	}
	domainPacks := make([][]uint32, result.LogicalDomains)
	var actualBytes uint64
	for i, pack := range result.Packs {
		if pack.PackID != uint32(i) || int(pack.DomainID) >= result.LogicalDomains || pack.PlannedBytes == 0 || pack.ActualBytes == 0 || pack.ActualBytes > pack.PlannedBytes || pack.ActualBytes > math.MaxUint64-actualBytes {
			return errors.New("invalid membership feasibility pack evidence")
		}
		actualBytes += pack.ActualBytes
		domainPacks[pack.DomainID] = append(domainPacks[pack.DomainID], pack.PackID)
	}
	for _, packs := range domainPacks {
		if len(packs) == 0 {
			return errors.New("membership feasibility logical domain has no physical pack")
		}
	}
	if actualBytes != result.ActualPackBytes || result.ActualBytesPerVector != float64(actualBytes)/float64(result.SourceVectors) || math.IsNaN(result.ActualBytesPerVector) || math.IsInf(result.ActualBytesPerVector, 0) {
		return errors.New("membership feasibility actual-byte evidence disagrees")
	}
	wantWork, wantScratch, err := m8MembershipFeasibilityPlanV1(len(result.Queries), result.Queries[0].TruthCount, result.LogicalDomains, result.DomainLimit)
	if err != nil || result.WorkBound != wantWork || result.ScratchBytes != wantScratch {
		return errors.New("membership feasibility resource evidence disagrees")
	}
	totalHits, possibleHits := 0, 0
	for i, query := range result.Queries {
		if query.QueryOrdinal != i || !m8SHA256V1(query.TruthSHA256) || query.TruthCount < 1 || query.TruthCount > 10 || query.Hits < 0 || query.Hits > query.TruthCount ||
			query.TruthCount != result.Queries[0].TruthCount ||
			int(query.CoverageMask) >= 1<<uint(query.TruthCount) || bits.OnesCount16(query.CoverageMask) != query.Hits || query.Recall != float64(query.Hits)/float64(query.TruthCount) ||
			len(query.SelectedDomain) > result.DomainLimit || query.PackCost != int64(len(query.ExpandedPacks)) || query.PackCost > result.PackLimit {
			return errors.New("invalid membership feasibility query evidence")
		}
		expanded, err := m8MembershipExpandDomainsV1(domainPacks, query.SelectedDomain, result.PackLimit)
		if err != nil || !slices.Equal(expanded, query.ExpandedPacks) {
			return errors.New("membership feasibility selected domains disagree with physical packs")
		}
		totalHits += query.Hits
		possibleHits += query.TruthCount
	}
	ceiling := float64(totalHits) / float64(possibleHits)
	status := "insufficient"
	if ceiling >= result.RequiredRecall {
		status = "sufficient"
	}
	if result.TotalHits != totalHits || result.PossibleHits != possibleHits || result.Ceiling != ceiling || result.Status != status {
		return errors.New("membership feasibility aggregate or disposition disagrees")
	}
	return nil
}

func m8MembershipFeasibilityMatchesReportV1(result m8MembershipFeasibilityV1, report m8ProductionReportV1, domainLimit, packLimit int) bool {
	if report.Variant == nil {
		return false
	}
	return result.VariantID == report.Variant.VariantID &&
		result.FixtureChecksum == report.Dataset.Checksum &&
		result.TruthIdentity == m8TruthCacheIdentityV1(report.Dataset, report.Config.TopK) &&
		result.TruthIdentity == report.TruthCache.Identity &&
		result.TruthArtifactSHA256 == report.TruthCache.ArtifactSHA256 &&
		result.BuildIdentityDigest == report.Variant.BuildIdentityDigest &&
		result.ManifestIntegrity == report.Variant.ManifestIntegrity &&
		result.ReadySetDigest == report.Variant.ReadySetDigest &&
		result.ShardGenerationDigest == report.Variant.ShardGenerationDigest &&
		result.SourceVectors == report.Dataset.Vectors &&
		result.LogicalDomains == report.Config.DomainCount &&
		result.PhysicalPacks == report.Config.Partitions &&
		result.DomainLimit == domainLimit && result.PackLimit == int64(packLimit) &&
		result.RequiredRecall == report.Config.RecallTarget
}

func m8PublishMembershipFeasibilityV1(out string, result m8MembershipFeasibilityV1) (m8MembershipFeasibilityArtifactV1, error) {
	if err := m8ValidateMembershipFeasibilityV1(result); err != nil {
		return m8MembershipFeasibilityArtifactV1{}, err
	}
	raw, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return m8MembershipFeasibilityArtifactV1{}, err
	}
	raw = append(raw, '\n')
	if len(raw) > m8MembershipFeasibilityMaxBytesV1 {
		return m8MembershipFeasibilityArtifactV1{}, errors.New("membership feasibility artifact exceeds byte cap")
	}
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	path := filepath.Join(out, fmt.Sprintf("vector_partition_m8_membership_%s_%s.json", result.VariantID, digest[:12]))
	linked, err := m8PublishProductionMatrixV1(path, func(w io.Writer) error {
		n, err := w.Write(raw)
		if err == nil && n != len(raw) {
			err = io.ErrShortWrite
		}
		return err
	})
	if err != nil {
		if linked {
			return m8MembershipFeasibilityArtifactV1{Path: path, ArtifactSHA256: digest, Result: result}, err
		}
		return m8MembershipFeasibilityArtifactV1{}, err
	}
	return m8MembershipFeasibilityArtifactV1{Path: path, ArtifactSHA256: digest, Result: result}, nil
}

func m8ReadMembershipFeasibilityV1(artifact m8MembershipFeasibilityArtifactV1) (m8MembershipFeasibilityV1, error) {
	raw, err := readBoundedRegularFileV1(artifact.Path, m8MembershipFeasibilityMaxBytesV1)
	if err != nil {
		return m8MembershipFeasibilityV1{}, err
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != artifact.ArtifactSHA256 {
		return m8MembershipFeasibilityV1{}, errors.New("membership feasibility artifact digest mismatch")
	}
	var result m8MembershipFeasibilityV1
	if err := json.Unmarshal(raw, &result); err != nil {
		return m8MembershipFeasibilityV1{}, err
	}
	if !reflect.DeepEqual(result, artifact.Result) {
		return m8MembershipFeasibilityV1{}, errors.New("membership feasibility matrix copy disagrees with immutable artifact")
	}
	return result, m8ValidateMembershipFeasibilityV1(result)
}

func m8RunMembershipFeasibilityV1(cfg config, fixture fixtureManifest, dir string, descriptor m3VariantDescriptorV1) (m8MembershipFeasibilityArtifactV1, error) {
	result, err := m8ComputeRetainedMembershipFeasibilityV1(cfg, fixture, dir, descriptor)
	if err != nil {
		return m8MembershipFeasibilityArtifactV1{}, err
	}
	if err := os.MkdirAll(cfg.out, 0o755); err != nil {
		return m8MembershipFeasibilityArtifactV1{}, err
	}
	return m8PublishMembershipFeasibilityV1(cfg.out, result)
}

func m8ReplayMembershipFeasibilityV1(cfg config, fixture fixtureManifest, dir string, descriptor m3VariantDescriptorV1, artifact m8MembershipFeasibilityArtifactV1) error {
	retained, err := m8ReadMembershipFeasibilityV1(artifact)
	if err != nil {
		return err
	}
	recomputed, err := m8ComputeRetainedMembershipFeasibilityV1(cfg, fixture, dir, descriptor)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(retained, recomputed) {
		return errors.New("membership feasibility replay disagrees with retained truth, membership, or pack assets")
	}
	return nil
}
