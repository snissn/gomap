package collections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const vectorPartitionSearchAssetMagicV1 = "VPS1"
const vectorPartitionSearchAssetVersionV1 uint32 = 1
const vectorPartitionSearchAssetMaxBytesV1 int64 = 256 << 20

type vectorPartitionMembershipSourceV1 struct {
	ordinal int
	kind    VectorPartitionMembershipKindV1
}

// VectorPartitionConstructionEvidenceV1 is offline-only, versioned HNSW
// construction provenance. It is never serialized into the search pack.
type VectorPartitionConstructionEvidenceV1 struct {
	Schema                string                                           `json:"schema"`
	Variant               string                                           `json:"variant"`
	ManifestChecksum      string                                           `json:"manifest_checksum"`
	IndexDefinitionDigest string                                           `json:"index_definition_digest"`
	Partitions            []VectorPartitionConstructionPartitionEvidenceV1 `json:"partitions"`
}
type VectorPartitionConstructionPartitionEvidenceV1 struct {
	PartitionID      uint32                                   `json:"partition_id"`
	AssetChecksum    string                                   `json:"asset_checksum"`
	MembershipDigest string                                   `json:"membership_digest"`
	AssetBytes       uint64                                   `json:"asset_bytes"`
	Selections       []VectorPartitionConstructionSelectionV1 `json:"selections"`
	Events           []VectorPartitionConstructionEdgeEventV1 `json:"events"`
	PostfillEdges    uint64                                   `json:"postfill_edges"`
}
type VectorPartitionConstructionSelectionV1 struct {
	Node              int `json:"node"`
	Layer             int `json:"layer"`
	Candidates        int `json:"candidates"`
	Selected          int `json:"selected"`
	DiversitySelected int `json:"diversity_selected"`
	BackfillSelected  int `json:"backfill_selected"`
}
type VectorPartitionConstructionEdgeEventV1 struct {
	From             int    `json:"from"`
	To               int    `json:"to"`
	Layer            int    `json:"layer"`
	InsertionOrdinal int    `json:"insertion_ordinal"`
	Origin           string `json:"origin"`
	Action           string `json:"action"`
}

func compactPartitionAdjacencyV1(in []uint32) []uint32 {
	if len(in) < 2 {
		return in
	}
	n := 1
	for _, x := range in[1:] {
		if x != in[n-1] {
			in[n] = x
			n++
		}
	}
	return in[:n]
}

const vectorPartitionLocalNavigationBranchV1 = 8

// vectorPartitionLocalAuxiliaryNavigationV1 is a separately encoded layer-0
// component bridge. Its CSR never shares the native HNSW degree budget.
type vectorPartitionLocalAuxiliaryNavigationV1 struct {
	Offsets   []uint64
	Neighbors []uint32
}

// buildVectorPartitionLocalAuxiliaryNavigationV1 connects the deterministic
// directed-reachability roots of the native layer-0 graph. The entry component
// is root zero; each later root is the least unseen ordinal. A bidirectional
// branching-factor-eight tree gives every root a path from the entry. Every
// non-root upper-layer row also points to its native reachability root, so a
// normal HNSW upper-layer descent can still reach the component bridge. The
// resulting auxiliary degree is bounded by nine.
func buildVectorPartitionLocalAuxiliaryNavigationV1(rows []columnVectorGraphAssetRow, entryOrdinal int) (vectorPartitionLocalAuxiliaryNavigationV1, error) {
	nativeOffsets := make([]uint64, len(rows)+1)
	levels := make([]uint16, len(rows))
	var nativeNeighbors []uint32
	for ordinal := range rows {
		neighbors, _, err := vectorPartitionLayer0AdjacencySplitV1(rows[ordinal].Adjacency)
		if err != nil {
			return vectorPartitionLocalAuxiliaryNavigationV1{}, err
		}
		if uint64(len(neighbors)) > math.MaxUint64-nativeOffsets[ordinal] {
			return vectorPartitionLocalAuxiliaryNavigationV1{}, errors.New("partition-local auxiliary native edge count")
		}
		nativeOffsets[ordinal+1] = nativeOffsets[ordinal] + uint64(len(neighbors))
		nativeNeighbors = append(nativeNeighbors, neighbors...)
		level, err := columnVectorGraphAdjacencyMaxLayer(rows[ordinal].Adjacency)
		if err != nil || level < 0 || level > math.MaxUint16 {
			return vectorPartitionLocalAuxiliaryNavigationV1{}, errors.New("partition-local auxiliary level")
		}
		levels[ordinal] = uint16(level)
	}
	return buildVectorPartitionLocalAuxiliaryNavigationFromNativeLayer0V1(len(rows), entryOrdinal, levels, nativeOffsets, nativeNeighbors)
}

// repairVectorPartitionLocalLayer0ReciprocityV1 restores the cheapest missing
// reverse boundary edge for each entry-unreachable region. HNSW insertion adds
// links in both directions, but later degree pruning can discard the older
// node's reciprocal link and strand the newer region. The repair swaps one
// existing edge at the reachable endpoint, so every row keeps exactly the same
// layer-0 degree and the persisted graph does not grow.
func repairVectorPartitionLocalLayer0ReciprocityV1(rows []columnVectorGraphAssetRow, entryOrdinal int) (int, error) {
	if len(rows) == 0 {
		if entryOrdinal != -1 {
			return 0, errors.New("partition-local reciprocal repair entry")
		}
		return 0, nil
	}
	if entryOrdinal < 0 || entryOrdinal >= len(rows) {
		return 0, errors.New("partition-local reciprocal repair entry")
	}
	adjacency := make([][]uint32, len(rows))
	suffixes := make([][]uint32, len(rows))
	for ordinal := range rows {
		var err error
		adjacency[ordinal], suffixes[ordinal], err = vectorPartitionLayer0AdjacencySplitV1(rows[ordinal].Adjacency)
		if err != nil {
			return 0, err
		}
	}
	reachable := func() ([]bool, int, error) {
		seen := make([]bool, len(rows))
		queue := []int{entryOrdinal}
		seen[entryOrdinal] = true
		for head := 0; head < len(queue); head++ {
			ordinal := queue[head]
			for _, neighbor := range adjacency[ordinal] {
				if int(neighbor) >= len(rows) || neighbor == uint32(ordinal) {
					return nil, 0, errors.New("partition-local reciprocal repair neighbor")
				}
				if !seen[neighbor] {
					seen[neighbor] = true
					queue = append(queue, int(neighbor))
				}
			}
		}
		return seen, len(queue), nil
	}
	similarity := func(left, right int) (float32, error) {
		if len(rows[left].Vector) == 0 || len(rows[left].Vector) != len(rows[right].Vector) {
			return 0, errors.New("partition-local reciprocal repair vector dimensions")
		}
		leftInvNorm, rightInvNorm := rows[left].InvNorm, rows[right].InvNorm
		var err error
		if leftInvNorm == 0 {
			leftInvNorm, err = columnVectorGraphInvNorm(rows[left].Vector)
			if err != nil {
				return 0, err
			}
		}
		if rightInvNorm == 0 {
			rightInvNorm, err = columnVectorGraphInvNorm(rows[right].Vector)
			if err != nil {
				return 0, err
			}
		}
		var dot float64
		for dimension := range rows[left].Vector {
			dot = canonicalVectorPartitionAccumulateV1(dot, rows[left].Vector[dimension]*leftInvNorm, rows[right].Vector[dimension]*rightInvNorm)
		}
		return float32(dot), nil
	}
	type scoredOrdinal struct {
		ordinal int
		score   float32
	}
	type scoredBoundary struct {
		source int
		target int
		score  float32
	}
	seen, seenCount, err := reachable()
	if err != nil {
		return 0, err
	}
	repairs := 0
	for seenCount < len(rows) {
		boundaries := make([]scoredBoundary, 0)
		for target := range seen {
			if seen[target] {
				continue
			}
			for _, neighbor := range adjacency[target] {
				if !seen[neighbor] {
					continue
				}
				score, scoreErr := similarity(int(neighbor), target)
				if scoreErr != nil {
					return 0, scoreErr
				}
				boundaries = append(boundaries, scoredBoundary{source: int(neighbor), target: target, score: score})
			}
		}
		if len(boundaries) == 0 {
			root := -1
			for ordinal := range seen {
				if !seen[ordinal] {
					root = ordinal
					break
				}
			}
			if root < 0 {
				return 0, errors.New("partition-local reciprocal repair reachability")
			}
			for ordinal := range seen {
				if !seen[ordinal] || len(adjacency[ordinal]) == 0 {
					continue
				}
				score, scoreErr := similarity(ordinal, root)
				if scoreErr != nil {
					return 0, scoreErr
				}
				boundaries = append(boundaries, scoredBoundary{source: ordinal, target: root, score: score})
			}
		}
		sort.Slice(boundaries, func(i, j int) bool {
			if boundaries[i].score != boundaries[j].score {
				return boundaries[i].score > boundaries[j].score
			}
			if boundaries[i].source != boundaries[j].source {
				return boundaries[i].source < boundaries[j].source
			}
			return boundaries[i].target < boundaries[j].target
		})
		repaired := false
		for _, boundary := range boundaries {
			drops := make([]scoredOrdinal, 0, len(adjacency[boundary.source]))
			for position, neighbor := range adjacency[boundary.source] {
				score, scoreErr := similarity(boundary.source, int(neighbor))
				if scoreErr != nil {
					return 0, scoreErr
				}
				drops = append(drops, scoredOrdinal{ordinal: position, score: score})
			}
			sort.Slice(drops, func(i, j int) bool {
				if drops[i].score != drops[j].score {
					return drops[i].score < drops[j].score
				}
				return adjacency[boundary.source][drops[i].ordinal] > adjacency[boundary.source][drops[j].ordinal]
			})
			for _, drop := range drops {
				prior := adjacency[boundary.source][drop.ordinal]
				adjacency[boundary.source][drop.ordinal] = uint32(boundary.target)
				nextSeen, nextCount, reachErr := reachable()
				preserved := reachErr == nil && nextCount > seenCount
				if preserved {
					for ordinal := range seen {
						if seen[ordinal] && !nextSeen[ordinal] {
							preserved = false
							break
						}
					}
				}
				if preserved {
					seen, seenCount = nextSeen, nextCount
					repairs++
					repaired = true
					break
				}
				adjacency[boundary.source][drop.ordinal] = prior
			}
			if repaired {
				break
			}
		}
		if !repaired {
			return 0, errors.New("partition-local reciprocal repair cannot preserve reachable rows")
		}
	}
	for ordinal := range rows {
		rows[ordinal].Adjacency = vectorPartitionLayer0AdjacencyJoinV1(adjacency[ordinal], suffixes[ordinal])
	}
	return repairs, nil
}

func buildVectorPartitionLocalAuxiliaryNavigationFromNativeLayer0V1(rows, entryOrdinal int, levels []uint16, nativeOffsets []uint64, nativeNeighbors []uint32) (vectorPartitionLocalAuxiliaryNavigationV1, error) {
	return buildVectorPartitionLocalAuxiliaryNavigationFromNativeLayer0WithContextV1(context.Background(), rows, entryOrdinal, levels, nativeOffsets, nativeNeighbors)
}

func buildVectorPartitionLocalAuxiliaryNavigationFromNativeLayer0WithContextV1(ctx context.Context, rows, entryOrdinal int, levels []uint16, nativeOffsets []uint64, nativeNeighbors []uint32) (vectorPartitionLocalAuxiliaryNavigationV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return vectorPartitionLocalAuxiliaryNavigationV1{}, err
	}
	if rows == 0 {
		if entryOrdinal != -1 {
			return vectorPartitionLocalAuxiliaryNavigationV1{}, errors.New("partition-local auxiliary entry")
		}
		return vectorPartitionLocalAuxiliaryNavigationV1{Offsets: []uint64{0}}, nil
	}
	if entryOrdinal < 0 || entryOrdinal >= rows {
		return vectorPartitionLocalAuxiliaryNavigationV1{}, errors.New("partition-local auxiliary entry")
	}
	if len(levels) != rows || len(nativeOffsets) != rows+1 || nativeOffsets[0] != 0 || nativeOffsets[rows] != uint64(len(nativeNeighbors)) {
		return vectorPartitionLocalAuxiliaryNavigationV1{}, errors.New("partition-local auxiliary native offsets")
	}
	seen := make([]bool, rows)
	componentRoots := make([]uint32, rows)
	roots := make([]uint32, 0, 1)
	visit := func(root int) error {
		queue := []int{root}
		seen[root] = true
		componentRoots[root] = uint32(root)
		for head := 0; head < len(queue); head++ {
			if head&255 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			ordinal := queue[head]
			start, end := nativeOffsets[ordinal], nativeOffsets[ordinal+1]
			if end < start || end > uint64(len(nativeNeighbors)) {
				return errors.New("partition-local auxiliary native offsets")
			}
			neighbors := nativeNeighbors[start:end]
			rowSeen := make(map[uint32]struct{}, len(neighbors))
			for _, neighbor := range neighbors {
				if int(neighbor) >= rows || neighbor == uint32(ordinal) {
					return errors.New("partition-local auxiliary native neighbor")
				}
				if _, duplicate := rowSeen[neighbor]; duplicate {
					return errors.New("partition-local auxiliary duplicate native neighbor")
				}
				rowSeen[neighbor] = struct{}{}
				if !seen[neighbor] {
					seen[neighbor] = true
					componentRoots[neighbor] = uint32(root)
					queue = append(queue, int(neighbor))
				}
			}
		}
		return nil
	}
	for ordinal := entryOrdinal; ; {
		roots = append(roots, uint32(ordinal))
		if err := visit(ordinal); err != nil {
			return vectorPartitionLocalAuxiliaryNavigationV1{}, err
		}
		next := -1
		for candidate := range seen {
			if candidate&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return vectorPartitionLocalAuxiliaryNavigationV1{}, err
				}
			}
			if !seen[candidate] {
				next = candidate
				break
			}
		}
		if next < 0 {
			break
		}
		ordinal = next
	}
	if len(roots) > 1 && len(roots)-1 > (math.MaxInt/2) {
		return vectorPartitionLocalAuxiliaryNavigationV1{}, errors.New("partition-local auxiliary edge count")
	}
	degrees := make([]uint8, rows)
	for child := 1; child < len(roots); child++ {
		parent := (child - 1) / vectorPartitionLocalNavigationBranchV1
		degrees[roots[parent]]++
		degrees[roots[child]]++
	}
	for ordinal, level := range levels {
		if level > 0 && componentRoots[ordinal] != uint32(ordinal) {
			degrees[ordinal]++
		}
	}
	offsets := make([]uint64, rows+1)
	for ordinal, degree := range degrees {
		if ordinal&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return vectorPartitionLocalAuxiliaryNavigationV1{}, err
			}
		}
		if degree > vectorPartitionLocalNavigationBranchV1+1 {
			return vectorPartitionLocalAuxiliaryNavigationV1{}, errors.New("partition-local auxiliary degree")
		}
		offsets[ordinal+1] = offsets[ordinal] + uint64(degree)
	}
	anchorEdges := 0
	for ordinal, level := range levels {
		if level > 0 && componentRoots[ordinal] != uint32(ordinal) {
			anchorEdges++
		}
	}
	if offsets[rows] != uint64(2*(len(roots)-1)+anchorEdges) {
		return vectorPartitionLocalAuxiliaryNavigationV1{}, errors.New("partition-local auxiliary edge count")
	}
	neighbors := make([]uint32, offsets[rows])
	next := append([]uint64(nil), offsets[:rows]...)
	for child := 1; child < len(roots); child++ {
		parent := (child - 1) / vectorPartitionLocalNavigationBranchV1
		left, right := roots[parent], roots[child]
		neighbors[next[left]] = right
		next[left]++
		neighbors[next[right]] = left
		next[right]++
	}
	for ordinal, level := range levels {
		root := componentRoots[ordinal]
		if level > 0 && root != uint32(ordinal) {
			neighbors[next[ordinal]] = root
			next[ordinal]++
		}
	}
	return vectorPartitionLocalAuxiliaryNavigationV1{Offsets: offsets, Neighbors: neighbors}, nil
}

func validateVectorPartitionLocalAuxiliaryNavigationV1(rows []columnVectorGraphAssetRow, entryOrdinal int, auxiliary vectorPartitionLocalAuxiliaryNavigationV1) error {
	expected, err := buildVectorPartitionLocalAuxiliaryNavigationV1(rows, entryOrdinal)
	if err != nil {
		return err
	}
	if !slices.Equal(auxiliary.Offsets, expected.Offsets) || !slices.Equal(auxiliary.Neighbors, expected.Neighbors) {
		return errors.New("partition-local auxiliary navigation mismatch")
	}
	return nil
}

func validateVectorPartitionLocalAuxiliaryNavigationFromNativeLayer0V1(rows, entryOrdinal int, levels []uint16, nativeOffsets []uint64, nativeNeighbors []uint32, auxiliary vectorPartitionLocalAuxiliaryNavigationV1) error {
	return validateVectorPartitionLocalAuxiliaryNavigationFromNativeLayer0WithContextV1(context.Background(), rows, entryOrdinal, levels, nativeOffsets, nativeNeighbors, auxiliary)
}

func validateVectorPartitionLocalAuxiliaryNavigationFromNativeLayer0WithContextV1(ctx context.Context, rows, entryOrdinal int, levels []uint16, nativeOffsets []uint64, nativeNeighbors []uint32, auxiliary vectorPartitionLocalAuxiliaryNavigationV1) error {
	expected, err := buildVectorPartitionLocalAuxiliaryNavigationFromNativeLayer0WithContextV1(ctx, rows, entryOrdinal, levels, nativeOffsets, nativeNeighbors)
	if err != nil {
		return err
	}
	if !slices.Equal(auxiliary.Offsets, expected.Offsets) || !slices.Equal(auxiliary.Neighbors, expected.Neighbors) {
		return errors.New("partition-local auxiliary navigation mismatch")
	}
	return nil
}

// VectorPartitionLocalGraphVariantV1 selects the partition-local graph build
// used for offline causal attribution. Callers must bind the selected variant
// into their pack identity before comparing results.
type VectorPartitionLocalGraphVariantV1 string

const (
	VectorPartitionLocalGraphVariantNativeV1         VectorPartitionLocalGraphVariantV1 = "native"
	VectorPartitionLocalGraphVariantOverlayCurrentV1 VectorPartitionLocalGraphVariantV1 = "overlay_current"
	// VectorPartitionLocalGraphVariantAuxiliaryNavigationV1 is the explicit M16
	// v3 repair: native HNSW is unchanged and component bridges live in the
	// separately encoded auxiliary channel.
	VectorPartitionLocalGraphVariantAuxiliaryNavigationV1 VectorPartitionLocalGraphVariantV1 = "auxiliary_navigation"
	// VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction256V1 is
	// an offline-only auxiliary-navigation construction candidate.
	VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction256V1 VectorPartitionLocalGraphVariantV1 = "auxiliary_navigation_ef_construction_256"
	// VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction512V1 is
	// an offline-only auxiliary-navigation construction candidate.
	VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction512V1 VectorPartitionLocalGraphVariantV1 = "auxiliary_navigation_ef_construction_512"
	// VectorPartitionLocalGraphVariantAuxiliaryNavigationM24EfConstruction256V1
	// is an offline-only auxiliary-navigation construction candidate.
	VectorPartitionLocalGraphVariantAuxiliaryNavigationM24EfConstruction256V1 VectorPartitionLocalGraphVariantV1 = "auxiliary_navigation_m24_ef_construction_256"
	// VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1
	// is an explicitly supported production auxiliary-navigation construction
	// variant.
	VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1 VectorPartitionLocalGraphVariantV1 = "auxiliary_navigation_m18_ef_construction_256"
	// VectorPartitionLocalGraphVariantAuxiliaryNavigationM20EfConstruction256V1
	// is the explicit production high-recall auxiliary-navigation profile.
	VectorPartitionLocalGraphVariantAuxiliaryNavigationM20EfConstruction256V1 VectorPartitionLocalGraphVariantV1 = "auxiliary_navigation_m20_ef_construction_256"
	// VectorPartitionLocalGraphVariantAuxiliaryNavigationM22EfConstruction256V1
	// is an offline-only auxiliary-navigation construction candidate.
	VectorPartitionLocalGraphVariantAuxiliaryNavigationM22EfConstruction256V1 VectorPartitionLocalGraphVariantV1 = "auxiliary_navigation_m22_ef_construction_256"
	// VectorPartitionLocalGraphVariantAuxiliaryNavigationM32EfConstruction256V1
	// is an offline-only auxiliary-navigation construction candidate.
	VectorPartitionLocalGraphVariantAuxiliaryNavigationM32EfConstruction256V1 VectorPartitionLocalGraphVariantV1 = "auxiliary_navigation_m32_ef_construction_256"
	// vectorPartitionLocalDefaultGraphVariantV1 is the auxiliary-navigation
	// M18/eFC256 balanced production variant used by the implicit
	// partition-local materialization APIs.
	vectorPartitionLocalDefaultGraphVariantV1 = VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1
)

func VectorPartitionLocalGraphVariantIdentityV1(variant VectorPartitionLocalGraphVariantV1) (string, error) {
	switch variant {
	case VectorPartitionLocalGraphVariantNativeV1, VectorPartitionLocalGraphVariantOverlayCurrentV1, VectorPartitionLocalGraphVariantAuxiliaryNavigationV1, VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction256V1, VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction512V1, VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1, VectorPartitionLocalGraphVariantAuxiliaryNavigationM20EfConstruction256V1, VectorPartitionLocalGraphVariantAuxiliaryNavigationM22EfConstruction256V1, VectorPartitionLocalGraphVariantAuxiliaryNavigationM24EfConstruction256V1, VectorPartitionLocalGraphVariantAuxiliaryNavigationM32EfConstruction256V1:
		return "partition_local_graph_delta_v1:" + string(variant), nil
	default:
		return "", fmt.Errorf("partition-local graph variant=%q", variant)
	}
}

func vectorPartitionLocalGraphVariantMembershipDigestV1(membership [sha256.Size]byte, variant VectorPartitionLocalGraphVariantV1) [sha256.Size]byte {
	if variant == VectorPartitionLocalGraphVariantOverlayCurrentV1 || variant == VectorPartitionLocalGraphVariantAuxiliaryNavigationV1 {
		return membership
	}
	h := sha256.New()
	h.Write([]byte("treedb_vector_partition_local_graph_variant_v1/"))
	h.Write([]byte(variant))
	h.Write(membership[:])
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

// vectorPartitionLocalProductionGraphVariantV1 permits the explicit M16
// rollback pack, the M18 balanced default, and the M20 high-recall profile.
// The canonical source definition remains the manifest identity.
func vectorPartitionLocalProductionGraphVariantV1(membership, expected [sha256.Size]byte) (VectorPartitionLocalGraphVariantV1, bool) {
	if membership == expected {
		return VectorPartitionLocalGraphVariantAuxiliaryNavigationV1, true
	}
	if vectorPartitionLocalGraphVariantMembershipDigestV1(membership, VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1) == expected {
		return VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1, true
	}
	if vectorPartitionLocalGraphVariantMembershipDigestV1(membership, VectorPartitionLocalGraphVariantAuxiliaryNavigationM20EfConstruction256V1) == expected {
		return VectorPartitionLocalGraphVariantAuxiliaryNavigationM20EfConstruction256V1, true
	}
	return "", false
}

// vectorPartitionLocalGraphVariantDefinitionV1 keeps the authoritative source
// definition unchanged while selecting the local offline builder parameters.
func vectorPartitionLocalGraphVariantDefinitionV1(def VectorIndexDefinition, variant VectorPartitionLocalGraphVariantV1) (VectorIndexDefinition, bool, error) {
	switch variant {
	case VectorPartitionLocalGraphVariantNativeV1, VectorPartitionLocalGraphVariantOverlayCurrentV1:
		return def, false, nil
	case VectorPartitionLocalGraphVariantAuxiliaryNavigationV1:
		return def, true, nil
	case VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction256V1:
		def.EfConstruction = 256
		return def, true, nil
	case VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction512V1:
		def.EfConstruction = 512
		return def, true, nil
	case VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1:
		def.M, def.EfConstruction = 18, 256
		return def, true, nil
	case VectorPartitionLocalGraphVariantAuxiliaryNavigationM20EfConstruction256V1:
		def.M, def.EfConstruction = 20, 256
		return def, true, nil
	case VectorPartitionLocalGraphVariantAuxiliaryNavigationM22EfConstruction256V1:
		def.M, def.EfConstruction = 22, 256
		return def, true, nil
	case VectorPartitionLocalGraphVariantAuxiliaryNavigationM24EfConstruction256V1:
		def.M, def.EfConstruction = 24, 256
		return def, true, nil
	case VectorPartitionLocalGraphVariantAuxiliaryNavigationM32EfConstruction256V1:
		def.M, def.EfConstruction = 32, 256
		return def, true, nil
	default:
		return VectorIndexDefinition{}, false, fmt.Errorf("partition-local graph variant=%q", variant)
	}
}

// vectorPartitionLocalGraphDeltaRowV1 is the deterministic, construction-time
// accounting needed to compare the native local graph with the current
// partition-only navigation overlay. It intentionally describes the existing
// mutation; it does not select a repair.
type vectorPartitionLocalGraphDeltaRowV1 struct {
	NativeDegree         uint32
	FinalDegree          uint32
	OverlayEdges         uint32
	OverlayDuplicates    uint32
	DisplacedNativeEdges uint32
}

func vectorPartitionLocalGraphDeltaV1(nativeRows, finalRows []columnVectorGraphAssetRow, degreeLimit int) ([]vectorPartitionLocalGraphDeltaRowV1, error) {
	if len(nativeRows) != len(finalRows) || degreeLimit < 2 {
		return nil, errors.New("partition-local graph delta input")
	}
	deltas := make([]vectorPartitionLocalGraphDeltaRowV1, len(nativeRows))
	for row := range nativeRows {
		native, nativeSuffix, err := vectorPartitionLayer0AdjacencySplitV1(nativeRows[row].Adjacency)
		if err != nil {
			return nil, err
		}
		final, finalSuffix, err := vectorPartitionLayer0AdjacencySplitV1(finalRows[row].Adjacency)
		if err != nil || !vectorPartitionUint32SlicesEqualV1(nativeSuffix, finalSuffix) || len(final) > degreeLimit {
			return nil, errors.New("partition-local graph delta adjacency")
		}
		finalSet := make(map[uint32]struct{}, len(final))
		for _, neighbor := range final {
			if int(neighbor) >= len(finalRows) || neighbor == uint32(row) {
				return nil, errors.New("partition-local graph delta neighbor")
			}
			finalSet[neighbor] = struct{}{}
		}
		overlay, err := vectorPartitionLocalNavigationEdgesV1(row, len(nativeRows), degreeLimit)
		if err != nil {
			return nil, err
		}
		nativeSet := make(map[uint32]struct{}, len(native))
		for _, neighbor := range native {
			nativeSet[neighbor] = struct{}{}
		}
		d := vectorPartitionLocalGraphDeltaRowV1{NativeDegree: uint32(len(native)), FinalDegree: uint32(len(final)), OverlayEdges: uint32(len(overlay))}
		for _, neighbor := range overlay {
			if _, ok := nativeSet[neighbor]; ok {
				d.OverlayDuplicates++
			}
		}
		for _, neighbor := range native {
			if _, ok := finalSet[neighbor]; !ok {
				d.DisplacedNativeEdges++
			}
		}
		deltas[row] = d
	}
	return deltas, nil
}

func vectorPartitionLocalNavigationEdgesV1(row, rows, degreeLimit int) ([]uint32, error) {
	if rows < 1 || row < 0 || row >= rows || degreeLimit < 2 {
		return nil, errors.New("partition-local navigation delta input")
	}
	if rows == 1 {
		return nil, nil
	}
	if degreeLimit == 2 {
		return []uint32{uint32((row + 1) % rows)}, nil
	}
	branch := min(vectorPartitionLocalNavigationBranchV1, degreeLimit-2)
	edges := make([]uint32, 0, branch+1)
	if row != 0 {
		edges = append(edges, uint32((row-1)/branch))
	}
	for child := row*branch + 1; child < min(rows, row*branch+branch+1); child++ {
		edges = append(edges, uint32(child))
	}
	return edges, nil
}

func vectorPartitionUint32SlicesEqualV1(left, right []uint32) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// buildVectorPartitionLocalGraphAdjacencyV1 retains the shared native graph
// and computes its separate partition-only reachability channel. The channel
// is encoded by the pack materializer; native adjacency is never rewritten.
func buildVectorPartitionLocalGraphAdjacencyV1(rows []columnVectorGraphAssetRow, def VectorIndexDefinition) error {
	_, err := buildVectorPartitionLocalGraphAdjacencyVariantWithAuxiliaryV1(rows, def, VectorPartitionLocalGraphVariantAuxiliaryNavigationV1)
	return err
}

func buildVectorPartitionLocalGraphAdjacencyVariantV1(rows []columnVectorGraphAssetRow, def VectorIndexDefinition, variant VectorPartitionLocalGraphVariantV1) error {
	_, err := buildVectorPartitionLocalGraphAdjacencyVariantWithAuxiliaryV1(rows, def, variant)
	return err
}

func buildVectorPartitionLocalGraphAdjacencyVariantWithAuxiliaryV1(rows []columnVectorGraphAssetRow, def VectorIndexDefinition, variant VectorPartitionLocalGraphVariantV1) (vectorPartitionLocalAuxiliaryNavigationV1, error) {
	return buildVectorPartitionLocalGraphAdjacencyVariantWithConstructionTraceV1(rows, def, variant, nil)
}

func buildVectorPartitionLocalGraphAdjacencyVariantWithConstructionTraceV1(rows []columnVectorGraphAssetRow, def VectorIndexDefinition, variant VectorPartitionLocalGraphVariantV1, trace *vectorIndexConstructionTraceV1) (vectorPartitionLocalAuxiliaryNavigationV1, error) {
	if _, err := VectorPartitionLocalGraphVariantIdentityV1(variant); err != nil {
		return vectorPartitionLocalAuxiliaryNavigationV1{}, err
	}
	if err := buildColumnVectorGraphAdjacencyWithConstructionTraceV1(rows, def, trace); err != nil {
		return vectorPartitionLocalAuxiliaryNavigationV1{}, err
	}
	switch variant {
	case VectorPartitionLocalGraphVariantNativeV1:
		return vectorPartitionLocalAuxiliaryNavigationV1{}, nil
	case VectorPartitionLocalGraphVariantOverlayCurrentV1:
		return vectorPartitionLocalAuxiliaryNavigationV1{}, addVectorPartitionLocalNavigationOverlayV1(rows, def.M*2)
	case VectorPartitionLocalGraphVariantAuxiliaryNavigationV1, VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction256V1, VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction512V1, VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1, VectorPartitionLocalGraphVariantAuxiliaryNavigationM20EfConstruction256V1, VectorPartitionLocalGraphVariantAuxiliaryNavigationM22EfConstruction256V1, VectorPartitionLocalGraphVariantAuxiliaryNavigationM24EfConstruction256V1, VectorPartitionLocalGraphVariantAuxiliaryNavigationM32EfConstruction256V1:
		if _, err := repairVectorPartitionLocalLayer0ReciprocityV1(rows, 0); err != nil {
			return vectorPartitionLocalAuxiliaryNavigationV1{}, err
		}
		return buildVectorPartitionLocalAuxiliaryNavigationV1(rows, 0)
	default:
		return vectorPartitionLocalAuxiliaryNavigationV1{}, fmt.Errorf("partition-local graph variant=%q", variant)
	}
}

func addVectorPartitionLocalNavigationOverlayV1(rows []columnVectorGraphAssetRow, degreeLimit int) error {
	if len(rows) < 2 {
		return nil
	}
	if degreeLimit < 2 {
		return fmt.Errorf("partition-local navigation degree limit=%d", degreeLimit)
	}
	if degreeLimit == 2 {
		// M=1 has room for exactly a navigation successor plus one native edge.
		// The directed ring reaches every row without widening the declared cap.
		for row := range rows {
			native, suffix, err := vectorPartitionLayer0AdjacencySplitV1(rows[row].Adjacency)
			if err != nil {
				return err
			}
			out := []uint32{uint32((row + 1) % len(rows))}
			for _, neighbor := range native {
				if uint64(neighbor) >= uint64(len(rows)) || neighbor == uint32(row) {
					return fmt.Errorf("partition-local native neighbor row=%d neighbor=%d", row, neighbor)
				}
			}
			for _, neighbor := range native {
				if neighbor != out[0] {
					out = append(out, neighbor)
					break
				}
			}
			rows[row].Adjacency = vectorPartitionLayer0AdjacencyJoinV1(out, suffix)
		}
		return nil
	}
	// Reserve one layer-0 native edge for every row. M=16 keeps the existing
	// branch=8 layout byte-for-byte; lower M reduces the overlay fanout.
	branch := min(vectorPartitionLocalNavigationBranchV1, degreeLimit-2)
	children := make([][]uint32, len(rows))
	for child := 1; child < len(rows); child++ {
		parent := (child - 1) / branch
		children[parent] = append(children[parent], uint32(child))
	}
	for row := range rows {
		native, suffix, err := vectorPartitionLayer0AdjacencySplitV1(rows[row].Adjacency)
		if err != nil {
			return err
		}
		overlay := make([]uint32, 0, len(children[row])+1)
		if row != 0 {
			overlay = append(overlay, uint32((row-1)/branch))
		}
		overlay = append(overlay, children[row]...)
		if len(overlay) > degreeLimit {
			return fmt.Errorf("partition-local navigation row=%d degree=%d exceeds limit=%d", row, len(overlay), degreeLimit)
		}
		out := append([]uint32(nil), overlay...)
		for _, neighbor := range native {
			if uint64(neighbor) >= uint64(len(rows)) || neighbor == uint32(row) {
				return fmt.Errorf("partition-local native neighbor row=%d neighbor=%d", row, neighbor)
			}
			duplicate := false
			for _, existing := range out {
				if existing == neighbor {
					duplicate = true
					break
				}
			}
			if !duplicate && len(out) < degreeLimit {
				out = append(out, neighbor)
			}
		}
		rows[row].Adjacency = vectorPartitionLayer0AdjacencyJoinV1(out, suffix)
	}
	return nil
}

// split returns the layer-0 neighbors plus the exact encoded higher-layer
// suffix. A nil suffix denotes the legacy layer-0-only representation.
func vectorPartitionLayer0AdjacencySplitV1(adjacency []uint32) ([]uint32, []uint32, error) {
	if len(adjacency) == 0 {
		return nil, nil, nil
	}
	if adjacency[0] != columnVectorGraphLayeredAdjacencyMagic {
		return append([]uint32(nil), adjacency...), nil, nil
	}
	if len(adjacency) < 3 {
		return nil, nil, errors.New("partition-local layered adjacency header")
	}
	count := int(adjacency[2])
	if count < 0 || count > len(adjacency)-3 {
		return nil, nil, errors.New("partition-local layered adjacency layer-0 count")
	}
	suffix := make([]uint32, 1, 1+len(adjacency)-(3+count))
	suffix[0] = adjacency[1]
	suffix = append(suffix, adjacency[3+count:]...)
	return append([]uint32(nil), adjacency[3:3+count]...), suffix, nil
}

func vectorPartitionLayer0AdjacencyJoinV1(layer0, suffix []uint32) []uint32 {
	if suffix == nil {
		return layer0
	}
	out := make([]uint32, 0, 3+len(layer0)+len(suffix))
	out = append(out, columnVectorGraphLayeredAdjacencyMagic, suffix[0], uint32(len(layer0)))
	out = append(out, layer0...)
	out = append(out, suffix[1:]...)
	return out
}

func vectorPartitionLocalAssetIDV1(partition uint32) string {
	return fmt.Sprintf("hnsw_search_pack_v1/partition/%d", partition)
}

func vectorPartitionMembershipsForPartitionV1(manifest VectorPartitionManifestV1, partition uint32) []vectorPartitionMembershipSourceV1 {
	members, _ := vectorPartitionMembershipsForPartitionWithContextV1(context.Background(), manifest, partition)
	return members
}

func vectorPartitionMembershipsForPartitionWithContextV1(ctx context.Context, manifest VectorPartitionManifestV1, partition uint32) ([]vectorPartitionMembershipSourceV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	count := 0
	for i, membership := range manifest.Memberships {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if membership.PartitionID == partition {
			count++
		}
	}
	for i, membership := range manifest.OverlapMemberships {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if membership.PartitionID == partition {
			count++
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	members := make([]vectorPartitionMembershipSourceV1, 0, count)
	for i, membership := range manifest.Memberships {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if membership.PartitionID == partition {
			members = append(members, vectorPartitionMembershipSourceV1{ordinal: int(membership.VectorOrdinal), kind: VectorPartitionMembershipHomeV1})
		}
	}
	for i, membership := range manifest.OverlapMemberships {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if membership.PartitionID == partition {
			members = append(members, vectorPartitionMembershipSourceV1{ordinal: int(membership.VectorOrdinal), kind: VectorPartitionMembershipOverlapV1})
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].ordinal < members[j].ordinal || members[i].ordinal == members[j].ordinal && members[i].kind < members[j].kind
	})
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return members, nil
}

// VectorPartitionGenerationSearchOpenPlanV1 is an immutable, generation-wide
// cold-open plan. Routed serving builds it once from the manifest already
// validated by the active-generation authority, then reuses its partition
// asset lookup and membership index for every cold partition open.
type VectorPartitionGenerationSearchOpenPlanV1 struct {
	collection            string
	indexName             string
	indexDefinitionDigest string
	sourceGeneration      uint64
	sourceChecksum        uint64
	sourceSchemaHash      uint64
	generation            uint64
	partitionCount        uint32
	assets                []VectorPartitionAssetV1
	assetPresent          []bool
	members               []vectorPartitionMembershipSourceV1
	memberOffsets         []int
	homeCounts            []int
	overlapCounts         []int
}

// NewVectorPartitionGenerationSearchOpenPlanWithContextV1 indexes a validated
// generation manifest once for bounded per-partition lookup. The returned plan
// does not retain or expose the caller's manifest slices.
func NewVectorPartitionGenerationSearchOpenPlanWithContextV1(ctx context.Context, manifest VectorPartitionManifestV1) (*VectorPartitionGenerationSearchOpenPlanV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if manifest.Collection == "" || manifest.IndexName == "" || manifest.IndexDefinitionDigest == "" || manifest.Generation == 0 || manifest.PartitionCount == 0 {
		return nil, fmt.Errorf("%w: incomplete generation search-open plan", ErrVectorPartitionSearchUnavailable)
	}
	limits := DefaultVectorPartitionManifestLimits()
	if uint64(manifest.PartitionCount) > uint64(limits.MaxPartitions) ||
		len(manifest.Memberships) > limits.totalMembershipLimit() ||
		len(manifest.OverlapMemberships) > limits.totalMembershipLimit()-min(len(manifest.Memberships), limits.totalMembershipLimit()) {
		return nil, fmt.Errorf("%w: generation search-open plan exceeds manifest limits", ErrVectorPartitionSearchUnavailable)
	}
	partitionCount := int(manifest.PartitionCount)
	plan := &VectorPartitionGenerationSearchOpenPlanV1{
		collection:            manifest.Collection,
		indexName:             manifest.IndexName,
		indexDefinitionDigest: manifest.IndexDefinitionDigest,
		sourceGeneration:      manifest.SourceGeneration,
		sourceChecksum:        manifest.SourceChecksum,
		sourceSchemaHash:      manifest.SourceSchemaHash,
		generation:            manifest.Generation,
		partitionCount:        manifest.PartitionCount,
		assets:                make([]VectorPartitionAssetV1, partitionCount),
		assetPresent:          make([]bool, partitionCount),
		memberOffsets:         make([]int, partitionCount+1),
		homeCounts:            make([]int, partitionCount),
		overlapCounts:         make([]int, partitionCount),
	}
	for i, asset := range manifest.Assets {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if asset.PartitionID >= manifest.PartitionCount || asset.ID != vectorPartitionLocalAssetIDV1(asset.PartitionID) {
			continue
		}
		partition := int(asset.PartitionID)
		if plan.assetPresent[partition] {
			return nil, fmt.Errorf("%w: duplicate partition search asset %d", ErrVectorPartitionSearchUnavailable, asset.PartitionID)
		}
		plan.assets[partition] = asset
		plan.assetPresent[partition] = true
	}
	countMembership := func(memberships []VectorPartitionMembershipV1, counts []int) error {
		for i, membership := range memberships {
			if i&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if membership.PartitionID >= manifest.PartitionCount {
				return fmt.Errorf("%w: membership partition %d out of range", ErrVectorPartitionSearchUnavailable, membership.PartitionID)
			}
			counts[membership.PartitionID]++
		}
		return nil
	}
	if err := countMembership(manifest.Memberships, plan.homeCounts); err != nil {
		return nil, err
	}
	if err := countMembership(manifest.OverlapMemberships, plan.overlapCounts); err != nil {
		return nil, err
	}
	for partition := 0; partition < partitionCount; partition++ {
		count := plan.homeCounts[partition] + plan.overlapCounts[partition]
		if count < plan.homeCounts[partition] {
			return nil, fmt.Errorf("%w: membership count overflow", ErrVectorPartitionSearchUnavailable)
		}
		plan.memberOffsets[partition+1] = plan.memberOffsets[partition] + count
		if plan.memberOffsets[partition+1] < plan.memberOffsets[partition] {
			return nil, fmt.Errorf("%w: membership offset overflow", ErrVectorPartitionSearchUnavailable)
		}
	}
	plan.members = make([]vectorPartitionMembershipSourceV1, plan.memberOffsets[partitionCount])
	next := append([]int(nil), plan.memberOffsets[:partitionCount]...)
	if err := appendVectorPartitionMembershipsToPlanV1(ctx, plan, manifest.Memberships, VectorPartitionMembershipHomeV1, next); err != nil {
		return nil, err
	}
	if err := appendVectorPartitionMembershipsToPlanV1(ctx, plan, manifest.OverlapMemberships, VectorPartitionMembershipOverlapV1, next); err != nil {
		return nil, err
	}
	// The digest verifier makes and canonically sorts its own bounded copy.
	// Keeping the grouped home/overlap runs here avoids another generation-wide
	// sort during plan preparation.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return plan, nil
}

func appendVectorPartitionMembershipsToPlanV1(
	ctx context.Context,
	plan *VectorPartitionGenerationSearchOpenPlanV1,
	memberships []VectorPartitionMembershipV1,
	kind VectorPartitionMembershipKindV1,
	next []int,
) error {
	partitionCount := int(plan.partitionCount)
	for i, membership := range memberships {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		partition := int(membership.PartitionID)
		if partition < 0 || partition >= partitionCount || partition+1 >= len(plan.memberOffsets) || partition >= len(next) {
			return fmt.Errorf("%w: membership partition %d out of range", ErrVectorPartitionSearchUnavailable, membership.PartitionID)
		}
		offset := next[partition]
		if offset < plan.memberOffsets[partition] || offset >= plan.memberOffsets[partition+1] || offset >= len(plan.members) {
			return fmt.Errorf("%w: membership index changed during plan preparation", ErrVectorPartitionSearchUnavailable)
		}
		plan.members[offset] = vectorPartitionMembershipSourceV1{ordinal: int(membership.VectorOrdinal), kind: kind}
		next[partition]++
	}
	return nil
}

func (p *VectorPartitionGenerationSearchOpenPlanV1) partition(partition uint32) (*VectorPartitionAssetV1, []vectorPartitionMembershipSourceV1, int, int, error) {
	if p == nil || partition >= p.partitionCount || int(partition)+1 >= len(p.memberOffsets) || int(partition) >= len(p.assets) || !p.assetPresent[partition] {
		return nil, nil, 0, 0, fmt.Errorf("%w: missing or stale partition asset", ErrVectorPartitionSearchUnavailable)
	}
	i := int(partition)
	return &p.assets[i], p.members[p.memberOffsets[i]:p.memberOffsets[i+1]], p.homeCounts[i], p.overlapCounts[i], nil
}

func vectorPartitionMembershipDigestV1(reader *columnVectorGraphPhysicalRowReader, generation uint64, partition uint32, members []vectorPartitionMembershipSourceV1) ([sha256.Size]byte, error) {
	return vectorPartitionMembershipDigestWithContextV1(context.Background(), reader, generation, partition, members)
}

func vectorPartitionMembershipDigestWithContextV1(ctx context.Context, reader *columnVectorGraphPhysicalRowReader, generation uint64, partition uint32, members []vectorPartitionMembershipSourceV1) ([sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if reader == nil || generation == 0 || len(members) == 0 {
		return zero, fmt.Errorf("%w: membership digest input", ErrVectorPartitionSearchUnavailable)
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	canonical := make([]vectorPartitionMembershipSourceV1, len(members))
	for offset := 0; offset < len(members); offset += 1024 {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		end := min(offset+1024, len(members))
		copy(canonical[offset:end], members[offset:end])
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	sort.Slice(canonical, func(i, j int) bool {
		return canonical[i].ordinal < canonical[j].ordinal || canonical[i].ordinal == canonical[j].ordinal && canonical[i].kind < canonical[j].kind
	})
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	h := sha256.New()
	h.Write([]byte("treedb/vector-partition-membership/v1"))
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], generation)
	h.Write(encoded[:])
	binary.BigEndian.PutUint32(encoded[:4], partition)
	h.Write(encoded[:4])
	binary.BigEndian.PutUint64(encoded[:], uint64(len(canonical)))
	h.Write(encoded[:])
	lastOrdinal := -1
	for i, member := range canonical {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return zero, err
			}
		}
		if member.ordinal < 0 || member.ordinal == lastOrdinal {
			return zero, fmt.Errorf("%w: duplicate membership ordinal", ErrVectorPartitionSearchUnavailable)
		}
		if member.kind != VectorPartitionMembershipHomeV1 && member.kind != VectorPartitionMembershipOverlapV1 {
			return zero, fmt.Errorf("%w: membership kind", ErrVectorPartitionSearchUnavailable)
		}
		id, ok := reader.documentIDForOrdinal(member.ordinal)
		if !ok || len(id) == 0 {
			return zero, fmt.Errorf("%w: authoritative stable ID", ErrVectorPartitionSearchUnavailable)
		}
		binary.BigEndian.PutUint64(encoded[:], uint64(member.ordinal))
		h.Write(encoded[:])
		binary.BigEndian.PutUint32(encoded[:4], uint32(len(id)))
		h.Write(encoded[:4])
		h.Write(id)
		kindByte := byte(1)
		if member.kind == VectorPartitionMembershipOverlapV1 {
			kindByte = 2
		}
		h.Write([]byte{kindByte})
		lastOrdinal = member.ordinal
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

func decodeVectorPartitionMembershipDigestV1(raw string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	decoded, err := hex.DecodeString(raw)
	if err != nil || len(decoded) != sha256.Size || raw != strings.ToLower(raw) {
		return digest, fmt.Errorf("%w: membership digest", ErrVectorPartitionSearchUnavailable)
	}
	copy(digest[:], decoded)
	return digest, nil
}

func (c *Collection) validateVectorPartitionAssetMembershipBindingsV1(manifest VectorPartitionManifestV1) error {
	hasNative := false
	for _, asset := range manifest.Assets {
		if asset.ID == vectorPartitionLocalAssetIDV1(asset.PartitionID) {
			hasNative = true
			break
		}
	}
	if !hasNative {
		return nil
	}
	def, ok := findVectorIndex(c.meta.VectorIndexes, manifest.IndexName)
	if !ok {
		return ErrVectorPartitionSearchUnavailable
	}
	reader, err := c.openColumnVectorGraphPhysicalRowReader(manifest.IndexName, columnVectorGraphPhysicalRowReaderOptions{})
	if err != nil {
		return fmt.Errorf("%w: membership source reader: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	defer reader.Close()
	for _, asset := range manifest.Assets {
		if asset.ID != vectorPartitionLocalAssetIDV1(asset.PartitionID) {
			continue
		}
		got, err := decodeVectorPartitionMembershipDigestV1(asset.MembershipDigest)
		if err != nil {
			return err
		}
		want, err := vectorPartitionMembershipDigestV1(reader, manifest.Generation, asset.PartitionID, vectorPartitionMembershipsForPartitionV1(manifest, asset.PartitionID))
		if err != nil {
			return err
		}
		variant, ok := vectorPartitionLocalProductionGraphVariantV1(want, got)
		if !ok {
			return fmt.Errorf("%w: descriptor membership digest mismatch partition=%d", ErrVectorPartitionSearchUnavailable, asset.PartitionID)
		}
		if asset.Ref.Kind != ColumnAssetKindTCS1HNSWSearchPack || asset.Ref.Length <= 0 || asset.Ref.Length > vectorPartitionSearchAssetMaxBytesV1 {
			return fmt.Errorf("%w: native membership asset ref partition=%d", ErrVectorPartitionSearchUnavailable, asset.PartitionID)
		}
		raw, err := readColumnPhysicalAssetFromManager(c.db.ColumnAssetRootDir(), asset.Ref)
		if err != nil {
			return fmt.Errorf("%w: native membership asset partition=%d: %v", ErrVectorPartitionSearchUnavailable, asset.PartitionID, err)
		}
		pack, _, err := decodeColumnHNSWSearchPackEnvelope(raw, columnHNSWSearchPackDecodeOptions{
			ExpectedBaseIdentity: columnHNSWSearchPackBaseIdentity{
				ManifestGeneration: manifest.SourceGeneration,
				ManifestChecksum:   manifest.SourceChecksum,
				SchemaHash:         manifest.SourceSchemaHash,
			},
			ExpectedMembershipDigest: got,
		})
		packDef, _, definitionErr := vectorPartitionLocalGraphVariantDefinitionV1(def, variant)
		if err != nil || definitionErr != nil || pack.Header.MembershipDigest != got || !pack.Header.HasAuxiliaryNavigation || pack.Header.M != packDef.M || pack.Header.EfConstruction != packDef.EfConstruction || pack.Header.EfSearch != packDef.EfSearch {
			return fmt.Errorf("%w: native membership header partition=%d: %v", ErrVectorPartitionSearchUnavailable, asset.PartitionID, err)
		}
	}
	return nil
}

// MaterializeVectorPartitionLocalSearchAssetsV1 uses the auxiliary-navigation
// V3 production default with the authoritative M1 column-asset definition.
// Callers install the returned descriptors in the generation M1 manifest;
// publication then validates the exact ref, size, CRC and SHA-256.
func (c *Collection) MaterializeVectorPartitionLocalSearchAssetsV1(index string, manifest VectorPartitionManifestV1, fileID uint32, inputs []VectorPartitionSearchAssetV1) ([]VectorPartitionAssetV1, *rootpublication.StableResourceSet, error) {
	return c.materializeVectorPartitionLocalSearchAssetsVariantV1(index, manifest, fileID, inputs, vectorPartitionSearchAssetMaxBytesV1, vectorPartitionLocalDefaultGraphVariantV1, nil)
}

// MaterializeVectorPartitionLocalSearchAssetsVariantV1 constructs explicit
// graph variants. Native and experimental variants remain offline-only; the
// The authoritative-definition and selected M18/eFC256 auxiliary V3 variants
// are production-openable.
func (c *Collection) MaterializeVectorPartitionLocalSearchAssetsVariantV1(index string, manifest VectorPartitionManifestV1, fileID uint32, inputs []VectorPartitionSearchAssetV1, variant VectorPartitionLocalGraphVariantV1) ([]VectorPartitionAssetV1, *rootpublication.StableResourceSet, error) {
	return c.materializeVectorPartitionLocalSearchAssetsVariantV1(index, manifest, fileID, inputs, vectorPartitionSearchAssetMaxBytesV1, variant, nil)
}

// MaterializeVectorPartitionLocalSearchAssetsWithConstructionEvidenceV1 is an
// offline diagnostic sibling of the normal materializer. Pack bytes are
// identical; only the temporary builder receives the nullable trace sink.
func (c *Collection) MaterializeVectorPartitionLocalSearchAssetsWithConstructionEvidenceV1(index string, manifest VectorPartitionManifestV1, fileID uint32, inputs []VectorPartitionSearchAssetV1, variant VectorPartitionLocalGraphVariantV1) ([]VectorPartitionAssetV1, *rootpublication.StableResourceSet, VectorPartitionConstructionEvidenceV1, error) {
	evidence := VectorPartitionConstructionEvidenceV1{Schema: "treedb_vector_partition_construction_evidence_v1", Variant: string(variant), ManifestChecksum: manifest.IntegrityDigest, IndexDefinitionDigest: manifest.IndexDefinitionDigest}
	assets, resources, err := c.materializeVectorPartitionLocalSearchAssetsVariantV1(index, manifest, fileID, inputs, vectorPartitionSearchAssetMaxBytesV1, variant, &evidence)
	return assets, resources, evidence, err
}

// ValidateVectorPartitionLocalConstructionEvidenceV1 fail-closes unless the
// trace binds the supplied immutable assets and its final survivors are their
// exact native packed adjacency.
func (c *Collection) ValidateVectorPartitionLocalConstructionEvidenceV1(ctx context.Context, index string, manifest VectorPartitionManifestV1, assets []VectorPartitionAssetV1, evidence VectorPartitionConstructionEvidenceV1) error {
	if c == nil || evidence.Schema != "treedb_vector_partition_construction_evidence_v1" || evidence.ManifestChecksum != manifest.IntegrityDigest || evidence.IndexDefinitionDigest != manifest.IndexDefinitionDigest || len(assets) != len(evidence.Partitions) {
		return ErrVectorPartitionSearchUnavailable
	}
	variant := VectorPartitionLocalGraphVariantV1(evidence.Variant)
	if _, err := VectorPartitionLocalGraphVariantIdentityV1(variant); err != nil {
		return ErrVectorPartitionSearchUnavailable
	}
	seenPartitions := make(map[uint32]struct{}, len(assets))
	for i, asset := range assets {
		part := evidence.Partitions[i]
		if _, duplicate := seenPartitions[part.PartitionID]; duplicate || (i > 0 && evidence.Partitions[i-1].PartitionID >= part.PartitionID) {
			return ErrVectorPartitionSearchUnavailable
		}
		seenPartitions[part.PartitionID] = struct{}{}
		if asset.PartitionID != part.PartitionID || asset.Checksum != part.AssetChecksum || asset.MembershipDigest != part.MembershipDigest || asset.Bytes != part.AssetBytes {
			return ErrVectorPartitionSearchUnavailable
		}
		searcher, err := c.OpenVectorPartitionLocalSearcherForOfflineAssetVariantWithContextV1(ctx, index, manifest, asset, variant)
		if err != nil {
			return ErrVectorPartitionSearchUnavailable
		}
		searcher.mu.Lock()
		pack := searcher.prepared
		searcher.mu.Unlock()
		if pack == nil {
			searcher.Close()
			return ErrVectorPartitionSearchUnavailable
		}
		if part.PostfillEdges != 0 {
			searcher.Close()
			return ErrVectorPartitionSearchUnavailable
		}
		live := make(map[vectorIndexConstructionEdgeKeyV1]string)
		selectionCounts := make(map[vectorIndexConstructionEdgeKeyV1][2]int)
		for _, selection := range part.Selections {
			if selection.Node < 0 || selection.Node >= pack.Header.Rows || selection.Layer < 0 || selection.Layer >= len(pack.AdjacencyLayers) || selection.Candidates < 0 || selection.Selected < 0 || selection.DiversitySelected < 0 || selection.BackfillSelected < 0 || selection.Selected != selection.DiversitySelected+selection.BackfillSelected || selection.Selected > selection.Candidates {
				searcher.Close()
				return ErrVectorPartitionSearchUnavailable
			}
			key := vectorIndexConstructionEdgeKeyV1{From: selection.Node, Layer: selection.Layer}
			if _, duplicate := selectionCounts[key]; duplicate {
				searcher.Close()
				return ErrVectorPartitionSearchUnavailable
			}
			selectionCounts[key] = [2]int{selection.DiversitySelected, selection.BackfillSelected}
		}
		initialCounts := make(map[vectorIndexConstructionEdgeKeyV1][2]int)
		finals := make(map[vectorIndexConstructionEdgeKeyV1]string)
		seenFinal := false
		for _, event := range part.Events {
			if event.From < 0 || event.To < 0 || event.From == event.To || event.From >= pack.Header.Rows || event.To >= pack.Header.Rows || event.Layer < 0 || event.Layer >= len(pack.AdjacencyLayers) || event.InsertionOrdinal < 0 || event.InsertionOrdinal >= pack.Header.Rows || (event.Origin != "diversity_selected" && event.Origin != "nearest_backfill" && event.Origin != "reciprocal_add") || (event.Action != "initial_add" && event.Action != "reciprocal_add" && event.Action != "reciprocal_prune_keep" && event.Action != "reciprocal_prune_drop" && event.Action != "final_survivor") {
				searcher.Close()
				return ErrVectorPartitionSearchUnavailable
			}
			key := vectorIndexConstructionEdgeKeyV1{From: event.From, To: event.To, Layer: event.Layer}
			switch event.Action {
			case "initial_add":
				if event.Origin != "diversity_selected" && event.Origin != "nearest_backfill" {
					searcher.Close()
					return ErrVectorPartitionSearchUnavailable
				}
				if _, ok := live[key]; ok {
					searcher.Close()
					return ErrVectorPartitionSearchUnavailable
				}
				live[key] = event.Origin
				counts := initialCounts[vectorIndexConstructionEdgeKeyV1{From: event.From, Layer: event.Layer}]
				if event.Origin == "diversity_selected" {
					counts[0]++
				} else {
					counts[1]++
				}
				initialCounts[vectorIndexConstructionEdgeKeyV1{From: event.From, Layer: event.Layer}] = counts
			case "reciprocal_add":
				if event.Origin != "reciprocal_add" {
					searcher.Close()
					return ErrVectorPartitionSearchUnavailable
				}
				if _, ok := live[key]; ok {
					searcher.Close()
					return ErrVectorPartitionSearchUnavailable
				}
				live[key] = event.Origin
			case "reciprocal_prune_keep":
				if live[key] != event.Origin {
					searcher.Close()
					return ErrVectorPartitionSearchUnavailable
				}
			case "reciprocal_prune_drop":
				if live[key] != event.Origin {
					searcher.Close()
					return ErrVectorPartitionSearchUnavailable
				}
				delete(live, key)
			case "final_survivor":
				seenFinal = true
				if origin, ok := finals[key]; ok || live[key] != event.Origin || origin != "" {
					searcher.Close()
					return ErrVectorPartitionSearchUnavailable
				}
				finals[key] = event.Origin
			default:
				searcher.Close()
				return ErrVectorPartitionSearchUnavailable
			}
			if event.Action != "final_survivor" && seenFinal {
				searcher.Close()
				return ErrVectorPartitionSearchUnavailable
			}
		}
		for key, counts := range initialCounts {
			if selectionCounts[key] != counts {
				searcher.Close()
				return ErrVectorPartitionSearchUnavailable
			}
		}
		for key, counts := range selectionCounts {
			if initialCounts[key] != counts {
				searcher.Close()
				return ErrVectorPartitionSearchUnavailable
			}
		}
		want := make(map[vectorIndexConstructionEdgeKeyV1]struct{})
		for layer, adjacency := range pack.AdjacencyLayers {
			for from := 0; from < pack.Header.Rows; from++ {
				for _, to := range adjacency.Neighbors[adjacency.Offsets[from]:adjacency.Offsets[from+1]] {
					want[vectorIndexConstructionEdgeKeyV1{From: from, To: int(to), Layer: layer}] = struct{}{}
				}
			}
		}
		searcher.Close()
		if (len(want) != 0 && !seenFinal) || len(finals) != len(want) || len(live) != len(finals) {
			return ErrVectorPartitionSearchUnavailable
		}
		for key := range want {
			if _, ok := finals[key]; !ok || live[key] != finals[key] {
				return ErrVectorPartitionSearchUnavailable
			}
		}
	}
	return nil
}

func (c *Collection) materializeVectorPartitionLocalSearchAssetsV1(index string, manifest VectorPartitionManifestV1, fileID uint32, inputs []VectorPartitionSearchAssetV1, maxAssetBytes int64) ([]VectorPartitionAssetV1, *rootpublication.StableResourceSet, error) {
	return c.materializeVectorPartitionLocalSearchAssetsVariantV1(index, manifest, fileID, inputs, maxAssetBytes, vectorPartitionLocalDefaultGraphVariantV1, nil)
}

func (c *Collection) materializeVectorPartitionLocalSearchAssetsVariantV1(index string, manifest VectorPartitionManifestV1, fileID uint32, inputs []VectorPartitionSearchAssetV1, maxAssetBytes int64, variant VectorPartitionLocalGraphVariantV1, evidence *VectorPartitionConstructionEvidenceV1) ([]VectorPartitionAssetV1, *rootpublication.StableResourceSet, error) {
	generation := manifest.Generation
	if c == nil || c.db == nil || generation == 0 || len(inputs) == 0 || maxAssetBytes <= 0 || maxAssetBytes > vectorPartitionSearchAssetMaxBytesV1 {
		return nil, nil, ErrVectorPartitionSearchUnavailable
	}
	if _, err := VectorPartitionLocalGraphVariantIdentityV1(variant); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	limits := DefaultVectorPartitionManifestLimits()
	if manifest.SourceRowCount == 0 || manifest.SourceRowCount > uint64(limits.sourceRowLimit()) || len(manifest.Memberships) != int(manifest.SourceRowCount) || len(manifest.OverlapMemberships) > limits.MaxMemberships || len(inputs) > int(manifest.PartitionCount) {
		return nil, nil, fmt.Errorf("%w: manifest count cap", ErrVectorPartitionSearchUnavailable)
	}
	if index == "" || index != manifest.IndexName {
		return nil, nil, fmt.Errorf("%w: index/manifest mismatch", ErrVectorPartitionSearchUnavailable)
	}
	cfg := c.meta.Options.ColumnStore
	if cfg == nil || cfg.AssetManager == nil {
		return nil, nil, fmt.Errorf("%w: column asset manager", ErrVectorPartitionSearchUnavailable)
	}
	def, ok := findVectorIndex(c.meta.VectorIndexes, index)
	if !ok || def.Metric != VectorMetricCosine || def.Encoding != VectorIndexEncodingFloat32 {
		return nil, nil, fmt.Errorf("%w: native FP32 cosine index", ErrVectorPartitionSearchUnavailable)
	}
	if manifest.IndexDefinitionDigest != VectorIndexDefinitionDigestV1(def) {
		return nil, nil, fmt.Errorf("%w: index definition digest", ErrVectorPartitionSearchUnavailable)
	}
	buildDef, hasAuxiliaryNavigation, err := vectorPartitionLocalGraphVariantDefinitionV1(def, variant)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	_, sourceGraph, _, err := c.columnVectorGraphPhysicalRowReaderSnapshotView(index)
	if err != nil || sourceGraph.BaseManifestGeneration != manifest.SourceGeneration || sourceGraph.BaseManifestChecksum != manifest.SourceChecksum || sourceGraph.BaseSchemaHash != manifest.SourceSchemaHash || uint64(sourceGraph.RowCount) != manifest.SourceRowCount {
		return nil, nil, fmt.Errorf("%w: stale authoritative source", ErrVectorPartitionSearchUnavailable)
	}
	reader, err := c.openColumnVectorGraphPhysicalRowReader(index, columnVectorGraphPhysicalRowReaderOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("%w: source reader: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	defer reader.Close()
	members := make(map[uint32][]vectorPartitionMembershipSourceV1)
	for _, x := range manifest.Memberships {
		if x.VectorOrdinal >= manifest.SourceRowCount {
			return nil, nil, fmt.Errorf("%w: source ordinal", ErrVectorPartitionSearchUnavailable)
		}
		members[x.PartitionID] = append(members[x.PartitionID], vectorPartitionMembershipSourceV1{int(x.VectorOrdinal), VectorPartitionMembershipHomeV1})
	}
	for _, x := range manifest.OverlapMemberships {
		if x.VectorOrdinal >= manifest.SourceRowCount {
			return nil, nil, fmt.Errorf("%w: source ordinal", ErrVectorPartitionSearchUnavailable)
		}
		members[x.PartitionID] = append(members[x.PartitionID], vectorPartitionMembershipSourceV1{int(x.VectorOrdinal), VectorPartitionMembershipOverlapV1})
	}
	items := make([]StableColumnPhysicalAssetAppend, len(inputs))
	traces := make([]*vectorIndexConstructionTraceV1, len(inputs))
	seenParts := make(map[uint32]struct{}, len(inputs))
	for i, in := range inputs {
		if in.Generation != generation || in.Dimensions != def.Dimensions || in.Source.Generation != manifest.SourceGeneration || in.Source.Checksum != manifest.SourceChecksum || in.Source.SchemaHash != manifest.SourceSchemaHash || in.Source.RowCount != manifest.SourceRowCount {
			return nil, nil, fmt.Errorf("%w: generation mismatch", ErrVectorPartitionSearchUnavailable)
		}
		if _, duplicate := seenParts[in.PartitionID]; duplicate {
			return nil, nil, fmt.Errorf("%w: duplicate partition", ErrVectorPartitionSearchUnavailable)
		}
		seenParts[in.PartitionID] = struct{}{}
		selected := members[in.PartitionID]
		if len(selected) == 0 {
			return nil, nil, fmt.Errorf("%w: partition has no canonical memberships", ErrVectorPartitionSearchUnavailable)
		}
		membershipDigest, err := vectorPartitionMembershipDigestV1(reader, generation, in.PartitionID, selected)
		if err != nil {
			return nil, nil, err
		}
		if err := preflightVectorPartitionNativePackV1(len(selected), buildDef.Dimensions, buildDef.M); err != nil {
			return nil, nil, err
		}
		type selectedRow struct {
			ordinal int
			id      []byte
		}
		sourceRows := make([]selectedRow, len(selected))
		var documentIDBytes uint64
		for j, x := range selected {
			id, ok := reader.documentIDForOrdinal(x.ordinal)
			if !ok || len(id) == 0 {
				return nil, nil, fmt.Errorf("%w: authoritative stable ID", ErrVectorPartitionSearchUnavailable)
			}
			if uint64(len(id)) > ^uint64(0)-documentIDBytes {
				return nil, nil, fmt.Errorf("%w: stable ID byte overflow", ErrVectorPartitionSearchUnavailable)
			}
			documentIDBytes += uint64(len(id))
			sourceRows[j] = selectedRow{ordinal: x.ordinal, id: id}
		}
		// The caller's cap can be narrower than the public maximum used by the
		// shape-only preflight. Reject the known fixed-width and stable-ID lower
		// bound before copying vectors or constructing the partition-local HNSW.
		if err := preflightVectorPartitionNativePackKnownBytesV1(len(sourceRows), buildDef.Dimensions, documentIDBytes, maxAssetBytes); err != nil {
			return nil, nil, err
		}
		// The partition owns a fresh local HNSW. Source ordinals provide a stable
		// insertion order; the native builder then applies its deterministic
		// entry-first locality order before the pack is encoded.
		sort.Slice(sourceRows, func(a, b int) bool {
			return sourceRows[a].ordinal < sourceRows[b].ordinal
		})
		for j, source := range sourceRows {
			if j > 0 && sourceRows[j-1].ordinal == source.ordinal {
				return nil, nil, fmt.Errorf("%w: duplicate membership ordinal", ErrVectorPartitionSearchUnavailable)
			}
		}
		rows := make([]columnVectorGraphAssetRow, len(sourceRows))
		scratch := &columnPhysicalRowReaderScratch{}
		for j, source := range sourceRows {
			r, fetchErr := reader.FetchRow(source.ordinal, scratch)
			if fetchErr != nil {
				r = columnVectorGraphPhysicalRow{}
			}
			if len(r.Vector) != def.Dimensions {
				r.Vector, _, _, _ = reader.typedVectorForOrdinal(source.ordinal)
			}
			if len(r.Vector) != def.Dimensions {
				return nil, nil, fmt.Errorf("%w: authoritative typed row", ErrVectorPartitionSearchUnavailable)
			}
			ref, refOK := reader.rowRefForOrdinal(source.ordinal)
			if !refOK {
				if fetchErr != nil || r.RowIndex < 0 {
					return nil, nil, fmt.Errorf("%w: authoritative source row ref", ErrVectorPartitionSearchUnavailable)
				}
				ref = DocumentRowRef{Generation: manifest.SourceGeneration, PartID: 1, RowIndex: r.RowIndex, AppliedCommandLSN: 1}
			}
			ref.DocumentID = append([]byte(nil), source.id...)
			if invNorm, _, _, ok := reader.invNormForOrdinal(source.ordinal); ok {
				r.InvNorm = invNorm
			}
			rows[j] = columnVectorGraphAssetRow{ID: append([]byte(nil), source.id...), Vector: append([]float32(nil), r.Vector...), InvNorm: r.InvNorm, BaseRowRef: ref}
		}
		var trace *vectorIndexConstructionTraceV1
		if evidence != nil {
			trace = &vectorIndexConstructionTraceV1{}
			traces[i] = trace
		}
		auxiliary, err := buildVectorPartitionLocalGraphAdjacencyVariantWithConstructionTraceV1(rows, buildDef, variant, trace)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: build partition-local graph: %v", ErrVectorPartitionSearchUnavailable, err)
		}
		maxLayer := 0
		rowMaxLayers := make([]int, len(rows))
		for j := range rows {
			rowMaxLayer, layerErr := columnVectorGraphAdjacencyMaxLayer(rows[j].Adjacency)
			if layerErr != nil {
				return nil, nil, fmt.Errorf("%w: partition-local adjacency: %v", ErrVectorPartitionSearchUnavailable, layerErr)
			}
			rowMaxLayers[j] = rowMaxLayer
			if rowMaxLayer > maxLayer {
				maxLayer = rowMaxLayer
			}
		}
		neighborCounts := make([]uint64, maxLayer+1)
		for j := range rows {
			for layer := 0; layer <= rowMaxLayers[j]; layer++ {
				neighbors, layerErr := columnVectorGraphAdjacencyLayer(rows[j].Adjacency, layer)
				if layerErr != nil {
					return nil, nil, fmt.Errorf("%w: partition-local adjacency: %v", ErrVectorPartitionSearchUnavailable, layerErr)
				}
				if uint64(len(neighbors)) > ^uint64(0)-neighborCounts[layer] {
					return nil, nil, fmt.Errorf("%w: neighbor count overflow", ErrVectorPartitionSearchUnavailable)
				}
				neighborCounts[layer] += uint64(len(neighbors))
			}
		}
		auxiliaryNeighbors := uint64(0)
		if hasAuxiliaryNavigation {
			auxiliaryNeighbors = uint64(len(auxiliary.Neighbors))
		}
		exactPackBytes, err := exactVectorPartitionLocalGraphPackBytesV1(len(rows), buildDef.Dimensions, neighborCounts, documentIDBytes, auxiliaryNeighbors, hasAuxiliaryNavigation, maxAssetBytes)
		if err != nil {
			return nil, nil, err
		}
		graph := columnVectorGraphManifestSnapshot{IndexName: buildDef.Name, Field: buildDef.Field, Metric: buildDef.Metric, Encoding: buildDef.Encoding, Dimensions: buildDef.Dimensions, M: buildDef.M, EfConstruction: buildDef.EfConstruction, EfSearch: buildDef.EfSearch, BaseManifestGeneration: manifest.SourceGeneration, BaseManifestChecksum: manifest.SourceChecksum, BaseSchemaHash: manifest.SourceSchemaHash, GraphSchemaHash: cfg.SchemaHash, RowCount: len(rows)}
		pack, err := buildColumnHNSWSearchPackInput(buildDef, graph, rows)
		if err != nil {
			return nil, nil, err
		}
		// Native A/B packs domain-bind their variant into the existing membership
		// identity. Production publication and serving recompute the canonical
		// membership digest and therefore fail closed on a native offline pack.
		pack.MembershipDigest = vectorPartitionLocalGraphVariantMembershipDigestV1(membershipDigest, variant)
		if hasAuxiliaryNavigation {
			pack.HasAuxiliaryNavigation = true
			pack.AuxiliaryNavigation = columnHNSWSearchPackLayerInput{Offsets: auxiliary.Offsets, Neighbors: auxiliary.Neighbors}
		}
		raw, err := encodeColumnHNSWSearchPack(pack)
		if err != nil {
			return nil, nil, err
		}
		if int64(len(raw)) != exactPackBytes || int64(len(raw)) > maxAssetBytes {
			return nil, nil, fmt.Errorf("%w: encoded native pack bytes=%d exact=%d cap=%d", ErrVectorPartitionSearchUnavailable, len(raw), exactPackBytes, maxAssetBytes)
		}
		// Column assets reserve physical part ID zero; logical partitions are
		// zero-based, so persist their unambiguous +1 representation.
		items[i] = StableColumnPhysicalAssetAppend{Payload: raw, Kind: ColumnAssetKindTCS1HNSWSearchPack, Generation: generation, PartID: uint64(in.PartitionID) + 1}
	}
	lease, err := c.db.AcquireStableResourceCaptureLease()
	if err != nil {
		return nil, nil, err
	}
	defer lease.Release()
	refs, resources, err := AppendColumnPhysicalAssetsWithStableResources(c.db.ColumnAssetRootDir(), *cfg, fileID, items, c.db.StableResourceIdentityPinRegistry(), lease)
	if err != nil {
		return nil, nil, err
	}
	out := make([]VectorPartitionAssetV1, len(inputs))
	for i := range inputs {
		sum := sha256.Sum256(items[i].Payload)
		headerDigest := items[i].Payload[columnHNSWSearchPackHeaderMembershipDigestOffset:columnHNSWSearchPackHeaderSizeV2]
		out[i] = VectorPartitionAssetV1{ID: vectorPartitionLocalAssetIDV1(inputs[i].PartitionID), PartitionID: inputs[i].PartitionID, Checksum: hex.EncodeToString(sum[:]), MembershipDigest: hex.EncodeToString(headerDigest), Bytes: uint64(len(items[i].Payload)), Ref: refs[i]}
		if out[i].Ref.PartID != uint64(inputs[i].PartitionID)+1 {
			return nil, nil, fmt.Errorf("%w: partition ref", ErrVectorPartitionSearchUnavailable)
		}
		if evidence != nil {
			trace := traces[i]
			if trace == nil {
				return nil, nil, fmt.Errorf("%w: construction trace", ErrVectorPartitionSearchUnavailable)
			}
			partition := VectorPartitionConstructionPartitionEvidenceV1{PartitionID: out[i].PartitionID, AssetChecksum: out[i].Checksum, MembershipDigest: out[i].MembershipDigest, AssetBytes: out[i].Bytes}
			for _, selection := range trace.selections {
				partition.Selections = append(partition.Selections, VectorPartitionConstructionSelectionV1{Node: selection.Node, Layer: selection.Layer, Candidates: selection.Candidates, Selected: selection.Selected, DiversitySelected: selection.DiversitySelected, BackfillSelected: selection.BackfillSelected})
			}
			for _, event := range trace.events {
				partition.Events = append(partition.Events, VectorPartitionConstructionEdgeEventV1{From: event.From, To: event.To, Layer: event.Layer, InsertionOrdinal: event.InsertionOrdinal, Origin: event.Origin, Action: event.Action})
			}
			evidence.Partitions = append(evidence.Partitions, partition)
		}
	}
	return out, resources, nil
}

func encodeVectorPartitionSearchAssetV1(a VectorPartitionSearchAssetV1) ([]byte, error) {
	if len(a.Adjacency) == 0 {
		a.Adjacency = make([][]uint32, len(a.IDs))
	}
	if err := validateVectorPartitionSearchAssetV1(a); err != nil {
		return nil, err
	}
	// Bounded preflight covers all count-derived writes before allocating.
	bytesNeeded := int64(4 + 4 + 8 + 4 + 4 + 4 + 4 + sha256.Size + len(a.ManifestChecksum))
	for i, id := range a.IDs {
		bytesNeeded += int64(4 + len(id) + 1 + 4*len(a.Vectors[i]) + 4 + 4*len(a.Adjacency[i]))
		if bytesNeeded > vectorPartitionSearchAssetMaxBytesV1 {
			return nil, errors.New("partition search asset byte cap")
		}
	}
	var b bytes.Buffer
	b.Grow(int(bytesNeeded))
	b.WriteString(vectorPartitionSearchAssetMagicV1)
	var x [8]byte
	binary.BigEndian.PutUint32(x[:4], vectorPartitionSearchAssetVersionV1)
	b.Write(x[:4])
	binary.BigEndian.PutUint64(x[:], a.Generation)
	b.Write(x[:])
	binary.BigEndian.PutUint32(x[:4], a.PartitionID)
	b.Write(x[:4])
	binary.BigEndian.PutUint32(x[:4], uint32(a.Dimensions))
	b.Write(x[:4])
	binary.BigEndian.PutUint32(x[:4], uint32(len(a.IDs)))
	b.Write(x[:4])
	binary.BigEndian.PutUint32(x[:4], uint32(len(a.ManifestChecksum)))
	b.Write(x[:4])
	b.WriteString(a.ManifestChecksum)
	for i, id := range a.IDs {
		binary.BigEndian.PutUint32(x[:4], uint32(len(id)))
		b.Write(x[:4])
		b.WriteString(id)
		if a.Kinds[i] == VectorPartitionMembershipHomeV1 {
			b.WriteByte(1)
		} else {
			b.WriteByte(2)
		}
		for _, v := range a.Vectors[i] {
			binary.BigEndian.PutUint32(x[:4], math.Float32bits(v))
			b.Write(x[:4])
		}
		binary.BigEndian.PutUint32(x[:4], uint32(len(a.Adjacency[i])))
		b.Write(x[:4])
		for _, n := range a.Adjacency[i] {
			binary.BigEndian.PutUint32(x[:4], n)
			b.Write(x[:4])
		}
	}
	sum := sha256.Sum256(b.Bytes())
	b.Write(sum[:])
	return b.Bytes(), nil
}

func decodeVectorPartitionSearchAssetV1(raw []byte) (VectorPartitionSearchAssetV1, error) {
	if len(raw) < 4+4+8+4+4+4+4+sha256.Size || string(raw[:4]) != vectorPartitionSearchAssetMagicV1 || binary.BigEndian.Uint32(raw[4:8]) != vectorPartitionSearchAssetVersionV1 {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset header")
	}
	sum := sha256.Sum256(raw[:len(raw)-sha256.Size])
	if !bytes.Equal(sum[:], raw[len(raw)-sha256.Size:]) {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset checksum")
	}
	off := 8
	take := func(n int) ([]byte, bool) {
		if n < 0 || off > len(raw)-sha256.Size-n {
			return nil, false
		}
		v := raw[off : off+n]
		off += n
		return v, true
	}
	u32 := func() (uint32, bool) {
		v, ok := take(4)
		if !ok {
			return 0, false
		}
		return binary.BigEndian.Uint32(v), true
	}
	u64 := func() (uint64, bool) {
		v, ok := take(8)
		if !ok {
			return 0, false
		}
		return binary.BigEndian.Uint64(v), true
	}
	g, ok := u64()
	if !ok {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset truncated")
	}
	p, ok := u32()
	if !ok {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset truncated")
	}
	dims, ok := u32()
	if !ok || dims == 0 || dims > 4096 {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset dimensions")
	}
	rows, ok := u32()
	if !ok || rows == 0 || rows > 1_000_000 {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset rows")
	}
	n, ok := u32()
	if !ok || n != 64 {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset manifest checksum")
	}
	mb, ok := take(int(n))
	if !ok {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset truncated")
	}
	a := VectorPartitionSearchAssetV1{ManifestChecksum: string(mb), Generation: g, PartitionID: p, Dimensions: int(dims), IDs: make([]string, rows), Vectors: make([][]float32, rows), Kinds: make([]VectorPartitionMembershipKindV1, rows), Adjacency: make([][]uint32, rows)}
	for i := range a.IDs {
		ln, ok := u32()
		if !ok || ln == 0 || ln > 1<<20 {
			return VectorPartitionSearchAssetV1{}, errors.New("partition search asset ID")
		}
		id, ok := take(int(ln))
		if !ok {
			return VectorPartitionSearchAssetV1{}, errors.New("partition search asset truncated")
		}
		a.IDs[i] = string(id)
		k, ok := take(1)
		if !ok {
			return VectorPartitionSearchAssetV1{}, errors.New("partition search asset kind")
		}
		switch k[0] {
		case 1:
			a.Kinds[i] = VectorPartitionMembershipHomeV1
		case 2:
			a.Kinds[i] = VectorPartitionMembershipOverlapV1
		default:
			return VectorPartitionSearchAssetV1{}, errors.New("partition search asset kind")
		}
		a.Vectors[i] = make([]float32, dims)
		for j := range a.Vectors[i] {
			v, ok := u32()
			if !ok {
				return VectorPartitionSearchAssetV1{}, errors.New("partition search asset truncated")
			}
			a.Vectors[i][j] = math.Float32frombits(v)
		}
		count, ok := u32()
		if !ok || count > rows {
			return VectorPartitionSearchAssetV1{}, errors.New("partition search asset adjacency")
		}
		a.Adjacency[i] = make([]uint32, count)
		for j := range a.Adjacency[i] {
			v, ok := u32()
			if !ok {
				return VectorPartitionSearchAssetV1{}, errors.New("partition search asset truncated")
			}
			a.Adjacency[i][j] = v
		}
	}
	if off != len(raw)-sha256.Size {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset trailing bytes")
	}
	if err := validateVectorPartitionSearchAssetV1(a); err != nil {
		return VectorPartitionSearchAssetV1{}, err
	}
	return a, nil
}

func (c *Collection) OpenVectorPartitionLocalSearcherForGenerationV1(index string, generation uint64, partition uint32) (*VectorPartitionLocalSearcherV1, error) {
	return c.OpenVectorPartitionLocalSearcherForGenerationWithContextV1(context.Background(), index, generation, partition)
}

// OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1 opens one
// freshly materialized local pack without publishing its manifest or acquiring
// a production generation pin. It is for bounded offline attribution only.
// The caller keeps the materializer's StableResourceSet alive until Close.
func (c *Collection) OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1(ctx context.Context, index string, manifest VectorPartitionManifestV1, asset VectorPartitionAssetV1) (*VectorPartitionLocalSearcherV1, error) {
	return c.openVectorPartitionLocalSearcherForOfflineAssetWithContextV1(ctx, index, manifest, asset, "")
}

// OpenVectorPartitionLocalSearcherForOfflineAssetVariantWithContextV1 opens
// one offline pack only when its domain-separated membership identity and pack
// header bind it to expectedVariant. Use this boundary when benchmark metadata
// attributes retained assets to a particular construction variant.
func (c *Collection) OpenVectorPartitionLocalSearcherForOfflineAssetVariantWithContextV1(ctx context.Context, index string, manifest VectorPartitionManifestV1, asset VectorPartitionAssetV1, expectedVariant VectorPartitionLocalGraphVariantV1) (*VectorPartitionLocalSearcherV1, error) {
	if _, err := VectorPartitionLocalGraphVariantIdentityV1(expectedVariant); err != nil {
		return nil, fmt.Errorf("%w: offline graph variant", ErrVectorPartitionSearchUnavailable)
	}
	return c.openVectorPartitionLocalSearcherForOfflineAssetWithContextV1(ctx, index, manifest, asset, expectedVariant)
}

func (c *Collection) openVectorPartitionLocalSearcherForOfflineAssetWithContextV1(ctx context.Context, index string, manifest VectorPartitionManifestV1, asset VectorPartitionAssetV1, expectedVariant VectorPartitionLocalGraphVariantV1) (*VectorPartitionLocalSearcherV1, error) {
	if c == nil || c.db == nil || ctx == nil {
		return nil, ErrVectorPartitionSearchUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if manifest.Collection != c.name || manifest.IndexName != index || manifest.Generation == 0 || asset.PartitionID >= manifest.PartitionCount || asset.ID != vectorPartitionLocalAssetIDV1(asset.PartitionID) {
		return nil, fmt.Errorf("%w: offline pack identity", ErrVectorPartitionSearchUnavailable)
	}
	def, ok := findVectorIndex(c.meta.VectorIndexes, index)
	if !ok || manifest.IndexDefinitionDigest != VectorIndexDefinitionDigestV1(def) {
		return nil, fmt.Errorf("%w: offline index identity", ErrVectorPartitionSearchUnavailable)
	}
	source, err := c.VectorPartitionSourceIdentityV1(index)
	if err != nil || source.Generation != manifest.SourceGeneration || source.Checksum != manifest.SourceChecksum || source.SchemaHash != manifest.SourceSchemaHash || source.RowCount != manifest.SourceRowCount {
		return nil, fmt.Errorf("%w: offline source identity", ErrVectorPartitionSearchUnavailable)
	}
	members, err := vectorPartitionMembershipsForPartitionWithContextV1(ctx, manifest, asset.PartitionID)
	if err != nil {
		return nil, err
	}
	home, overlap := 0, 0
	for _, member := range members {
		if member.kind == VectorPartitionMembershipHomeV1 {
			home++
		} else if member.kind == VectorPartitionMembershipOverlapV1 {
			overlap++
		} else {
			return nil, ErrVectorPartitionSearchUnavailable
		}
	}
	return c.openVectorPartitionLocalSearcherForPreparedPartitionWithContextV1(ctx, index, manifest.Generation, asset.PartitionID, manifest.IndexDefinitionDigest, manifest.SourceGeneration, manifest.SourceChecksum, manifest.SourceSchemaHash, &asset, members, home, overlap, true, expectedVariant)
}

// OpenVectorPartitionLocalSearcherForGenerationWithContextV1 is the
// cancellation-aware cold-open boundary used by routed serving. Checks are
// placed around lifecycle reads, membership scans, checksum streaming, and
// mapped-resource preparation; any acquired pin or handle is released on a
// canceled return.
func (c *Collection) OpenVectorPartitionLocalSearcherForGenerationWithContextV1(ctx context.Context, index string, generation uint64, partition uint32) (*VectorPartitionLocalSearcherV1, error) {
	if c == nil || c.db == nil {
		return nil, ErrVectorPartitionSearchUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pin, err := c.AcquireVectorPartitionReaderPinWithContextV1(ctx, index, generation)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	release := true
	defer func() {
		if release {
			pin.Release()
		}
	}()
	store, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
	if err != nil {
		return nil, err
	}
	m, err := store.OpenWithContext(ctx, c.name, index, generation)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	def, ok := findVectorIndex(c.meta.VectorIndexes, index)
	if !ok || m.IndexName != index || m.IndexDefinitionDigest != VectorIndexDefinitionDigestV1(def) || def.Metric != VectorMetricCosine || def.Encoding != VectorIndexEncodingFloat32 {
		return nil, fmt.Errorf("%w: stale index definition", ErrVectorPartitionSearchUnavailable)
	}
	var asset *VectorPartitionAssetV1
	for i := range m.Assets {
		if m.Assets[i].PartitionID == partition && m.Assets[i].ID == vectorPartitionLocalAssetIDV1(partition) {
			asset = &m.Assets[i]
			break
		}
	}
	members, err := vectorPartitionMembershipsForPartitionWithContextV1(ctx, m, partition)
	if err != nil {
		return nil, err
	}
	home, overlap := 0, 0
	for _, member := range members {
		if member.kind == VectorPartitionMembershipHomeV1 {
			home++
		} else if member.kind == VectorPartitionMembershipOverlapV1 {
			overlap++
		}
	}
	searcher, err := c.openVectorPartitionLocalSearcherForPreparedPartitionWithContextV1(
		ctx, index, generation, partition,
		m.IndexDefinitionDigest, m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash,
		asset, members, home, overlap, false, "",
	)
	if err != nil {
		return nil, err
	}
	searcher.partitionPin = pin
	release = false
	return searcher, nil
}

// OpenVectorPartitionLocalSearcherForGenerationSearchPlanWithContextV1 opens a
// persistent local searcher from a generation-wide plan prepared during the
// active-generation load. It clones the supplied validated generation pin for
// the searcher without lifecycle I/O, while avoiding another manifest decode
// and membership scan for every partition in one cold routed request.
func (c *Collection) OpenVectorPartitionLocalSearcherForGenerationSearchPlanWithContextV1(ctx context.Context, index string, generation uint64, partition uint32, plan *VectorPartitionGenerationSearchOpenPlanV1, generationPin *VectorPartitionReaderPinV1) (*VectorPartitionLocalSearcherV1, error) {
	if c == nil || c.db == nil {
		return nil, ErrVectorPartitionSearchUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if plan == nil || plan.collection != c.name || plan.indexName != index || plan.generation != generation {
		return nil, fmt.Errorf("%w: stale generation search-open plan", ErrVectorPartitionSearchUnavailable)
	}
	pinKey := vectorPartitionReaderPinKeyV1(c.db.Dir(), c.name, index, generation)
	pin, err := generationPin.cloneForKey(pinKey)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	release := true
	defer func() {
		if release {
			pin.Release()
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	asset, members, home, overlap, err := plan.partition(partition)
	if err != nil {
		return nil, err
	}
	searcher, err := c.openVectorPartitionLocalSearcherForPreparedPartitionWithContextV1(
		ctx, index, generation, partition,
		plan.indexDefinitionDigest, plan.sourceGeneration, plan.sourceChecksum, plan.sourceSchemaHash,
		asset, members, home, overlap, false, "",
	)
	if err != nil {
		return nil, err
	}
	searcher.partitionPin = pin
	release = false
	return searcher, nil
}

func (c *Collection) openVectorPartitionLocalSearcherForPreparedPartitionWithContextV1(
	ctx context.Context,
	index string,
	generation uint64,
	partition uint32,
	indexDefinitionDigest string,
	sourceGeneration uint64,
	sourceChecksum uint64,
	sourceSchemaHash uint64,
	asset *VectorPartitionAssetV1,
	members []vectorPartitionMembershipSourceV1,
	home int,
	overlap int,
	allowOfflineNative bool,
	expectedGraphVariant VectorPartitionLocalGraphVariantV1,
) (*VectorPartitionLocalSearcherV1, error) {
	def, ok := findVectorIndex(c.meta.VectorIndexes, index)
	if !ok || indexDefinitionDigest != VectorIndexDefinitionDigestV1(def) || def.Metric != VectorMetricCosine || def.Encoding != VectorIndexEncodingFloat32 {
		return nil, fmt.Errorf("%w: stale index definition", ErrVectorPartitionSearchUnavailable)
	}
	if asset == nil || asset.Ref.Kind != ColumnAssetKindTCS1HNSWSearchPack || asset.Ref.Generation != generation || asset.Ref.PartID != uint64(partition)+1 || asset.Ref.Length > vectorPartitionSearchAssetMaxBytesV1 || asset.Ref.Length <= 0 {
		return nil, fmt.Errorf("%w: missing or stale partition asset", ErrVectorPartitionSearchUnavailable)
	}
	expectedMembershipDigest, err := decodeVectorPartitionMembershipDigestV1(asset.MembershipDigest)
	if err != nil {
		return nil, err
	}
	sourceReader, err := c.openColumnVectorGraphPhysicalRowReader(index, columnVectorGraphPhysicalRowReaderOptions{})
	if err != nil {
		return nil, fmt.Errorf("%w: membership source reader: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	recomputedMembershipDigest, digestErr := vectorPartitionMembershipDigestWithContextV1(ctx, sourceReader, generation, partition, members)
	closeErr := sourceReader.Close()
	if digestErr != nil || closeErr != nil {
		if errors.Is(digestErr, context.Canceled) || errors.Is(digestErr, context.DeadlineExceeded) {
			return nil, digestErr
		}
		return nil, fmt.Errorf("%w: membership identity: %v", ErrVectorPartitionSearchUnavailable, errors.Join(digestErr, closeErr))
	}
	packDef := def
	expectAuxiliaryNavigation := false
	offlineV3 := false
	graphVariant := VectorPartitionLocalGraphVariantV1("")
	undomainSeparatedVariant := recomputedMembershipDigest == expectedMembershipDigest
	if recomputedMembershipDigest != expectedMembershipDigest {
		if variant, production := vectorPartitionLocalProductionGraphVariantV1(recomputedMembershipDigest, expectedMembershipDigest); production {
			graphVariant = variant
			var definitionErr error
			packDef, expectAuxiliaryNavigation, definitionErr = vectorPartitionLocalGraphVariantDefinitionV1(def, variant)
			if definitionErr != nil {
				return nil, ErrVectorPartitionSearchUnavailable
			}
			offlineV3 = variant != VectorPartitionLocalGraphVariantAuxiliaryNavigationV1
		} else {
			if !allowOfflineNative {
				return nil, fmt.Errorf("%w: descriptor membership digest mismatch", ErrVectorPartitionSearchUnavailable)
			}
			switch {
			case vectorPartitionLocalGraphVariantMembershipDigestV1(recomputedMembershipDigest, VectorPartitionLocalGraphVariantNativeV1) == expectedMembershipDigest:
				graphVariant = VectorPartitionLocalGraphVariantNativeV1
			case vectorPartitionLocalGraphVariantMembershipDigestV1(recomputedMembershipDigest, VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction256V1) == expectedMembershipDigest:
				graphVariant = VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction256V1
				var definitionErr error
				packDef, expectAuxiliaryNavigation, definitionErr = vectorPartitionLocalGraphVariantDefinitionV1(def, VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction256V1)
				if definitionErr != nil {
					return nil, ErrVectorPartitionSearchUnavailable
				}
				offlineV3 = true
			case vectorPartitionLocalGraphVariantMembershipDigestV1(recomputedMembershipDigest, VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction512V1) == expectedMembershipDigest:
				graphVariant = VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction512V1
				var definitionErr error
				packDef, expectAuxiliaryNavigation, definitionErr = vectorPartitionLocalGraphVariantDefinitionV1(def, VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction512V1)
				if definitionErr != nil {
					return nil, ErrVectorPartitionSearchUnavailable
				}
				offlineV3 = true
			case vectorPartitionLocalGraphVariantMembershipDigestV1(recomputedMembershipDigest, VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1) == expectedMembershipDigest:
				graphVariant = VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1
				var definitionErr error
				packDef, expectAuxiliaryNavigation, definitionErr = vectorPartitionLocalGraphVariantDefinitionV1(def, VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1)
				if definitionErr != nil {
					return nil, ErrVectorPartitionSearchUnavailable
				}
				offlineV3 = true
			case vectorPartitionLocalGraphVariantMembershipDigestV1(recomputedMembershipDigest, VectorPartitionLocalGraphVariantAuxiliaryNavigationM20EfConstruction256V1) == expectedMembershipDigest:
				graphVariant = VectorPartitionLocalGraphVariantAuxiliaryNavigationM20EfConstruction256V1
				var definitionErr error
				packDef, expectAuxiliaryNavigation, definitionErr = vectorPartitionLocalGraphVariantDefinitionV1(def, VectorPartitionLocalGraphVariantAuxiliaryNavigationM20EfConstruction256V1)
				if definitionErr != nil {
					return nil, ErrVectorPartitionSearchUnavailable
				}
				offlineV3 = true
			case vectorPartitionLocalGraphVariantMembershipDigestV1(recomputedMembershipDigest, VectorPartitionLocalGraphVariantAuxiliaryNavigationM22EfConstruction256V1) == expectedMembershipDigest:
				graphVariant = VectorPartitionLocalGraphVariantAuxiliaryNavigationM22EfConstruction256V1
				var definitionErr error
				packDef, expectAuxiliaryNavigation, definitionErr = vectorPartitionLocalGraphVariantDefinitionV1(def, VectorPartitionLocalGraphVariantAuxiliaryNavigationM22EfConstruction256V1)
				if definitionErr != nil {
					return nil, ErrVectorPartitionSearchUnavailable
				}
				offlineV3 = true
			case vectorPartitionLocalGraphVariantMembershipDigestV1(recomputedMembershipDigest, VectorPartitionLocalGraphVariantAuxiliaryNavigationM24EfConstruction256V1) == expectedMembershipDigest:
				graphVariant = VectorPartitionLocalGraphVariantAuxiliaryNavigationM24EfConstruction256V1
				var definitionErr error
				packDef, expectAuxiliaryNavigation, definitionErr = vectorPartitionLocalGraphVariantDefinitionV1(def, VectorPartitionLocalGraphVariantAuxiliaryNavigationM24EfConstruction256V1)
				if definitionErr != nil {
					return nil, ErrVectorPartitionSearchUnavailable
				}
				offlineV3 = true
			case vectorPartitionLocalGraphVariantMembershipDigestV1(recomputedMembershipDigest, VectorPartitionLocalGraphVariantAuxiliaryNavigationM32EfConstruction256V1) == expectedMembershipDigest:
				graphVariant = VectorPartitionLocalGraphVariantAuxiliaryNavigationM32EfConstruction256V1
				var definitionErr error
				packDef, expectAuxiliaryNavigation, definitionErr = vectorPartitionLocalGraphVariantDefinitionV1(def, VectorPartitionLocalGraphVariantAuxiliaryNavigationM32EfConstruction256V1)
				if definitionErr != nil {
					return nil, ErrVectorPartitionSearchUnavailable
				}
				offlineV3 = true
			default:
				return nil, fmt.Errorf("%w: descriptor membership digest mismatch", ErrVectorPartitionSearchUnavailable)
			}
		}
	}
	namespace := c.meta.Options.ColumnStore.AssetManager.Namespace
	if err := verifyVectorPartitionAssetsWithContextV1(ctx, c.db.ColumnAssetRootDir(), namespace, []VectorPartitionAssetV1{*asset}); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	path, err := columnAssetSegmentPath(c.db.ColumnAssetRootDir(), asset.Ref)
	if err != nil {
		return nil, err
	}
	if asset.Ref.Length > vectorPartitionSearchAssetMaxBytesV1 {
		return nil, fmt.Errorf("%w: asset byte cap", ErrVectorPartitionSearchUnavailable)
	}
	manager := mappedresource.NewManager()
	packVersion := columnHNSWSearchPackVersionV3
	if allowOfflineNative && !offlineV3 {
		packVersion = columnHNSWSearchPackVersionV2
	}
	key := mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: asset.Ref.Namespace, Kind: string(asset.Ref.Kind), Generation: asset.Ref.Generation, PartID: asset.Ref.PartID, FileID: asset.Ref.FileID, Offset: asset.Ref.Offset, Length: asset.Ref.Length, Checksum: uint64(asset.Ref.Checksum), Version: packVersion, Encoding: columnVectorIndexStateEncodingHNSWSearchPackV1, Section: mappedresource.Section{Kind: string(columnVectorIndexStateAssetRoleHNSWSearchPack), Category: string(ColumnAssetKindTCS1HNSWSearchPack), Name: asset.ID}}
	openStarted := time.Now()
	h, err := manager.AcquireFileRange(key, mappedresource.Scope{Kind: mappedresource.ScopePreparedSearch, ID: "vector_partition/" + strconv.FormatUint(generation, 10), Collection: c.name, Namespace: asset.Ref.Namespace, Generation: generation, Reason: "vector partition native HNSW"}, path, mappedresource.AcquireOptions{Reason: "vector partition native HNSW", ValidationMode: mappedresource.ValidationVerify, PreferMapped: true, AllowHeapCopy: true, ResourceRoot: c.db.ColumnAssetRootDir(), ResourcePath: path})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		_ = h.Release()
		return nil, err
	}
	view, err := newColumnHNSWSearchPackPreparedViewFromHandle(manager, h, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: columnHNSWSearchPackBaseIdentity{ManifestGeneration: sourceGeneration, ManifestChecksum: sourceChecksum, SchemaHash: sourceSchemaHash}, ExpectedMembershipDigest: expectedMembershipDigest})
	if err != nil {
		_ = h.Release()
		return nil, fmt.Errorf("%w: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	if view.Header.Dimensions != packDef.Dimensions || view.Header.M != packDef.M || view.Header.EfConstruction != packDef.EfConstruction || view.Header.EfSearch != packDef.EfSearch {
		_ = view.Close()
		return nil, ErrVectorPartitionSearchUnavailable
	}
	if undomainSeparatedVariant {
		// The historical overlay and canonical auxiliary-navigation variants
		// intentionally share the authoritative membership digest. Their pack
		// topology is the remaining exact identity boundary.
		if view.Header.HasAuxiliaryNavigation {
			graphVariant = VectorPartitionLocalGraphVariantAuxiliaryNavigationV1
			expectAuxiliaryNavigation = true
		} else {
			graphVariant = VectorPartitionLocalGraphVariantOverlayCurrentV1
		}
	}
	if expectedGraphVariant != "" && graphVariant != expectedGraphVariant {
		_ = view.Close()
		return nil, fmt.Errorf("%w: offline graph variant=%s want=%s", ErrVectorPartitionSearchUnavailable, graphVariant, expectedGraphVariant)
	}
	if !allowOfflineNative && !view.Header.HasAuxiliaryNavigation {
		_ = view.Close()
		return nil, ErrVectorPartitionSearchUnavailable
	}
	if expectAuxiliaryNavigation && !view.Header.HasAuxiliaryNavigation {
		_ = view.Close()
		return nil, ErrVectorPartitionSearchUnavailable
	}
	if err := ctx.Err(); err != nil {
		_ = view.Close()
		return nil, err
	}
	openNanos := time.Since(openStarted).Nanoseconds()
	if openNanos < 1 {
		openNanos = 1
	}
	view.openNanos = uint64(openNanos)
	maxStableIDBytes, err := vectorPartitionPreparedMaxStableIDBytesV1(view)
	if err != nil {
		_ = view.Close()
		return nil, fmt.Errorf("%w: stable ID bounds", ErrVectorPartitionSearchUnavailable)
	}
	s := &VectorPartitionLocalSearcherV1{asset: VectorPartitionSearchAssetV1{Generation: generation, PartitionID: partition, Dimensions: view.Header.Dimensions}, prepared: view, opened: 1, homeMemberships: home, overlapMemberships: overlap, packBytes: uint64(asset.Ref.Length), mappedBytes: view.mappedBytes, heapBytes: view.heapCopyBytes, openNanos: view.openNanos, searchRoute: VectorPartitionSearchRouteHNSWSearchPackV1, maxStableIDBytes: maxStableIDBytes}
	return s, nil
}

func preflightVectorPartitionNativePackV1(rows, dimensions, degree int) error {
	if rows < 1 || rows > 1_000_000 || dimensions < 1 || dimensions > 4096 || degree < 1 {
		return fmt.Errorf("%w: native pack shape cap", ErrVectorPartitionSearchUnavailable)
	}
	stride, err := columnHNSWSearchPackVectorStrideForDimensions(dimensions)
	if err != nil {
		return fmt.Errorf("%w: native pack stride: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	// Reject impossible packs before allocating rows, normalized vectors, or
	// adjacency. This is a conservative minimum: encoding performs the exact
	// final byte cap after source IDs/topology are known.
	perRow := int64(stride)*4 + 2 + 8*6
	if degree <= math.MaxInt/2 {
		perRow += int64(degree*2) * 4
	}
	if int64(rows) > vectorPartitionSearchAssetMaxBytesV1/perRow {
		return fmt.Errorf("%w: native pack byte cap", ErrVectorPartitionSearchUnavailable)
	}
	return nil
}

func preflightVectorPartitionNativePackKnownBytesV1(rows, dimensions int, documentIDBytes uint64, capBytes int64) error {
	// A single layer with no encoded neighbors is the smallest valid topology.
	// The exact pass after graph construction still owns the final layer and
	// adjacency counts; this pass only proves the already-known V3 lower bound.
	_, err := exactVectorPartitionLocalGraphPackBytesV1(rows, dimensions, []uint64{0}, documentIDBytes, 0, true, capBytes)
	return err
}

func exactVectorPartitionNativePackBytesV1(rows, dimensions int, neighborCounts []uint64, documentIDBytes uint64, capBytes int64) (int64, error) {
	return exactVectorPartitionLocalGraphPackBytesV1(rows, dimensions, neighborCounts, documentIDBytes, 0, false, capBytes)
}

func exactVectorPartitionLocalGraphPackBytesV1(rows, dimensions int, neighborCounts []uint64, documentIDBytes, auxiliaryNeighbors uint64, hasAuxiliaryNavigation bool, capBytes int64) (int64, error) {
	if err := preflightVectorPartitionNativePackV1(rows, dimensions, 1); err != nil {
		return 0, err
	}
	if len(neighborCounts) < 1 || len(neighborCounts) > int(columnHNSWSearchPackMaxLayersDefault) || capBytes <= 0 {
		return 0, fmt.Errorf("%w: native pack topology cap", ErrVectorPartitionSearchUnavailable)
	}
	stride, err := columnHNSWSearchPackVectorStrideForDimensions(dimensions)
	if err != nil {
		return 0, fmt.Errorf("%w: native pack stride: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	mul := func(a, b uint64) (uint64, error) {
		if a != 0 && b > ^uint64(0)/a {
			return 0, fmt.Errorf("%w: native pack size overflow", ErrVectorPartitionSearchUnavailable)
		}
		return a * b, nil
	}
	add := func(a, b uint64) (uint64, error) {
		if b > ^uint64(0)-a {
			return 0, fmt.Errorf("%w: native pack size overflow", ErrVectorPartitionSearchUnavailable)
		}
		return a + b, nil
	}
	sectionCount := uint64(8 + 2*len(neighborCounts))
	if hasAuxiliaryNavigation {
		sectionCount += 2
	}
	directoryBytes, err := mul(sectionCount, uint64(columnHNSWSearchPackSectionEntrySize))
	if err != nil {
		return 0, err
	}
	offset, err := add(uint64(columnHNSWSearchPackHeaderSizeV2), directoryBytes)
	if err != nil {
		return 0, err
	}
	offset, ok := alignColumnHNSWSearchPackUint64(offset, uint64(columnHNSWSearchPackAlignment))
	if !ok {
		return 0, fmt.Errorf("%w: native pack size overflow", ErrVectorPartitionSearchUnavailable)
	}
	appendSection := func(length uint64, alignment uint32) error {
		aligned, ok := alignColumnHNSWSearchPackUint64(offset, uint64(alignment))
		if !ok {
			return fmt.Errorf("%w: native pack size overflow", ErrVectorPartitionSearchUnavailable)
		}
		offset, err = add(aligned, length)
		return err
	}
	rowCount := uint64(rows)
	vectorValues, err := mul(rowCount, uint64(stride))
	if err != nil {
		return 0, err
	}
	vectorBytes, err := mul(vectorValues, 4)
	if err != nil {
		return 0, err
	}
	if err := appendSection(vectorBytes, columnHNSWSearchPackVectorSectionAlignment); err != nil {
		return 0, err
	}
	levelBytes, err := mul(rowCount, 2)
	if err != nil {
		return 0, err
	}
	if err := appendSection(levelBytes, columnHNSWSearchPackAlignment); err != nil {
		return 0, err
	}
	offsetRows, err := add(rowCount, 1)
	if err != nil {
		return 0, err
	}
	adjacencyOffsetBytes, err := mul(offsetRows, 8)
	if err != nil {
		return 0, err
	}
	for _, neighbors := range neighborCounts {
		if err := appendSection(adjacencyOffsetBytes, columnHNSWSearchPackAlignment); err != nil {
			return 0, err
		}
		neighborBytes, err := mul(neighbors, 4)
		if err != nil {
			return 0, err
		}
		if err := appendSection(neighborBytes, columnHNSWSearchPackAlignment); err != nil {
			return 0, err
		}
	}
	if hasAuxiliaryNavigation {
		if err := appendSection(adjacencyOffsetBytes, columnHNSWSearchPackAlignment); err != nil {
			return 0, err
		}
		auxiliaryNeighborBytes, err := mul(auxiliaryNeighbors, 4)
		if err != nil {
			return 0, err
		}
		if err := appendSection(auxiliaryNeighborBytes, columnHNSWSearchPackAlignment); err != nil {
			return 0, err
		}
	}
	rowRefBytes, err := mul(rowCount, 8)
	if err != nil {
		return 0, err
	}
	for section := 0; section < 4; section++ {
		if err := appendSection(rowRefBytes, columnHNSWSearchPackAlignment); err != nil {
			return 0, err
		}
	}
	if err := appendSection(adjacencyOffsetBytes, columnHNSWSearchPackAlignment); err != nil {
		return 0, err
	}
	if err := appendSection(documentIDBytes, columnHNSWSearchPackAlignment); err != nil {
		return 0, err
	}
	if offset > uint64(capBytes) || offset > uint64(math.MaxInt64) {
		return 0, fmt.Errorf("%w: native pack byte cap exact=%d cap=%d", ErrVectorPartitionSearchUnavailable, offset, capBytes)
	}
	return int64(offset), nil
}

func vectorPartitionRemappedNeighborCountV1(neighbors []uint32, ordToLocal map[int]int, self int) int {
	seen := make(map[int]struct{}, len(neighbors))
	for _, neighbor := range neighbors {
		if local, ok := ordToLocal[int(neighbor)]; ok && local != self {
			seen[local] = struct{}{}
		}
	}
	return len(seen)
}

func vectorPartitionSourceMaxLayerV1(reader *columnVectorGraphPhysicalRowReader, ordinal int) (int, error) {
	if reader != nil && reader.preparedSearch != nil {
		maxLayer, _, err := reader.preparedSearch.maxAdjacencyLayerForOrdinal(ordinal)
		return maxLayer, err
	}
	if reader != nil {
		if maxLayer, _, _, _, ok := reader.maxDirectAdjacencyLayerForOrdinal(ordinal); ok {
			return maxLayer, nil
		}
		row, err := reader.FetchRow(ordinal, &columnPhysicalRowReaderScratch{})
		if err == nil {
			return columnVectorGraphAdjacencyMaxLayer(row.Adjacency)
		}
	}
	return 0, errors.New("authoritative source adjacency unavailable")
}

func vectorPartitionSourceAdjacencyLayerV1(reader *columnVectorGraphPhysicalRowReader, ordinal, layer int) ([]uint32, error) {
	if reader != nil && reader.preparedSearch != nil {
		neighbors, _, err := reader.preparedSearch.adjacencyLayerForOrdinal(ordinal, layer)
		return neighbors, err
	}
	if reader != nil {
		if neighbors, _, reason, ok := reader.directAdjacencyLayerForOrdinal(ordinal, layer); ok {
			return neighbors, nil
		} else if reason != "" {
			// Fall through to the legacy row only when the direct source does
			// not own this topology shape.
		}
		row, err := reader.FetchRow(ordinal, &columnPhysicalRowReaderScratch{})
		if err == nil {
			return columnVectorGraphAdjacencyLayer(row.Adjacency, layer)
		}
	}
	return nil, errors.New("authoritative source adjacency unavailable")
}

func remapVectorPartitionAdjacencyV1(source []uint32, ordToLocal map[int]int, self int) ([]uint32, error) {
	remapLayer := func(neighbors []uint32) []uint32 {
		out := make([]uint32, 0, len(neighbors))
		for _, neighbor := range neighbors {
			if local, ok := ordToLocal[int(neighbor)]; ok && local != self {
				out = append(out, uint32(local))
			}
		}
		sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
		return compactPartitionAdjacencyV1(out)
	}
	if !columnVectorGraphAdjacencyIsLayered(source) {
		return remapLayer(source), nil
	}
	maxLayer, err := columnVectorGraphAdjacencyMaxLayer(source)
	if err != nil {
		return nil, err
	}
	out := []uint32{columnVectorGraphLayeredAdjacencyMagic, uint32(maxLayer)}
	for layer := 0; layer <= maxLayer; layer++ {
		neighbors, err := columnVectorGraphAdjacencyLayer(source, layer)
		if err != nil {
			return nil, err
		}
		remapped := remapLayer(neighbors)
		out = append(out, uint32(len(remapped)))
		out = append(out, remapped...)
	}
	return out, nil
}

func vectorPartitionSourceAdjacencyV1(reader *columnVectorGraphPhysicalRowReader, ordinal int, legacy []uint32, legacyAvailable bool) ([]uint32, error) {
	encodeLayers := func(maxLayer int, layer func(int) ([]uint32, error)) ([]uint32, error) {
		if maxLayer < 0 {
			return nil, errors.New("negative source adjacency layer")
		}
		out := []uint32{columnVectorGraphLayeredAdjacencyMagic, uint32(maxLayer)}
		for current := 0; current <= maxLayer; current++ {
			neighbors, err := layer(current)
			if err != nil {
				return nil, err
			}
			out = append(out, uint32(len(neighbors)))
			out = append(out, neighbors...)
		}
		return out, nil
	}
	if reader != nil && reader.preparedSearch != nil {
		maxLayer, _, err := reader.preparedSearch.maxAdjacencyLayerForOrdinal(ordinal)
		if err != nil {
			return nil, err
		}
		return encodeLayers(maxLayer, func(layer int) ([]uint32, error) {
			neighbors, _, err := reader.preparedSearch.adjacencyLayerForOrdinal(ordinal, layer)
			return neighbors, err
		})
	}
	if reader != nil {
		if maxLayer, _, _, _, ok := reader.maxDirectAdjacencyLayerForOrdinal(ordinal); ok {
			return encodeLayers(maxLayer, func(layer int) ([]uint32, error) {
				neighbors, _, reason, ok := reader.directAdjacencyLayerForOrdinal(ordinal, layer)
				if !ok {
					return nil, fmt.Errorf("direct source adjacency layer %d unavailable reason=%s", layer, reason)
				}
				return neighbors, nil
			})
		}
	}
	if legacyAvailable {
		return append([]uint32(nil), legacy...), nil
	}
	return nil, errors.New("authoritative source adjacency unavailable")
}
