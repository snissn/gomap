package vectorpartition

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
)

// HomePackingPolicyV1 fixes ascending parent ordinals, induced home graphs,
// unweighted symmetrization, KaHIP 3.25 ECO and zero packing imbalance. Logical
// assignment keeps its original .05 imbalance. No query/truth input is used.
const HomePackingPolicyV1 = "kahip_3_25_eco_induced_home_symmetrized_epsilon0_v1"

// HomePackingReceiptV1 is small provenance, not a second assignment sidecar.
// Physical memberships in the shard-generation record retain the actual homes.
type HomePackingReceiptV1 struct {
	Policy        string `json:"policy"`
	ParentSHA256  string `json:"parent_sha256"`
	RequestSHA256 string `json:"request_sha256"`
	HomesSHA256   string `json:"homes_sha256"`
}

type homePackingRequestV1 struct {
	Kind   string      `json:"kind"`
	Parent Artifact    `json:"parent"`
	Plan   ShardPlanV1 `json:"plan"`
}

type homePackingResponseV1 struct {
	RequestSHA256 string `json:"request_sha256"`
	Policy        string `json:"policy"`
	Homes         []int  `json:"homes"`
}

func homePackingRequestJSONV1(plan ShardPlanV1, parent Artifact) ([]byte, string, error) {
	parentRaw, err := CanonicalJSON(parent)
	if err != nil {
		return nil, "", err
	}
	recomputed, err := PlanByteBoundedShardsV1(plan.request())
	if err != nil || recomputed != plan || plan.Vectors != parent.Source.Vectors || plan.Dimensions != parent.Source.Dimensions || plan.LogicalDomains != parent.Config.Partitions || plan.Imbalance != parent.Config.Imbalance || plan.DomainHomeCapacity != parent.Metrics.Cap {
		return nil, "", errors.New("vectorpartition: home packing plan does not bind parent artifact")
	}
	if parent.Config.Imbalance != .05 || parent.Config.Seed < math.MinInt32 || parent.Config.Seed > math.MaxInt32 || parent.Metrics.GraphEdges > 16_000_000 {
		return nil, "", errors.New("vectorpartition: home packing exceeds selected KaHIP configuration/envelope")
	}
	counts := make([]int, plan.LogicalDomains)
	for _, domain := range parent.Assignment {
		counts[domain]++
	}
	for domain, count := range counts {
		if count < plan.PacksPerDomain {
			return nil, "", fmt.Errorf("vectorpartition: domain %d has %d homes for %d nonempty packs", domain, count, plan.PacksPerDomain)
		}
	}
	parentSHA := fmt.Sprintf("%x", sha256.Sum256(parentRaw))
	parentRaw = nil
	raw, err := json.Marshal(homePackingRequestV1{HomePackingPolicyV1, parent, plan})
	return raw, parentSHA, err
}

// RunExternalHomePackingV1 solves once per parent/geometry under the caller's
// deadline and independent byte caps. The pinned adapter derives every domain
// projection from the unchanged parent; the request hash binds that graph,
// ordinal mapping, seed and policy without inventing projected vector sources.
func RunExternalHomePackingV1(ctx context.Context, command []string, limits ExternalJSONLimits, plan ShardPlanV1, parent Artifact) ([]int, HomePackingReceiptV1, error) {
	input, parentSHA, err := homePackingRequestJSONV1(plan, parent)
	if err != nil {
		return nil, HomePackingReceiptV1{}, err
	}
	raw, err := runExternalJSONBytes(ctx, command, input, limits)
	if err != nil {
		return nil, HomePackingReceiptV1{}, err
	}
	var response homePackingResponseV1
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&response); err != nil {
		return nil, HomePackingReceiptV1{}, err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return nil, HomePackingReceiptV1{}, errors.New("vectorpartition: trailing home packing response")
	}
	requestSHA := fmt.Sprintf("%x", sha256.Sum256(input))
	if response.Policy != HomePackingPolicyV1 || response.RequestSHA256 != requestSHA {
		return nil, HomePackingReceiptV1{}, errors.New("vectorpartition: home packing response does not bind request/projection")
	}
	if err := validateHomePacksV1(plan, parent.Assignment, response.Homes); err != nil {
		return nil, HomePackingReceiptV1{}, err
	}
	receipt := HomePackingReceiptV1{HomePackingPolicyV1, parentSHA, requestSHA, homePacksDigestV1(response.Homes)}
	return response.Homes, receipt, nil
}

// ValidateHomePackingV1 rebinds retained homes to the full parent and selected
// plan without rerunning a solver. In particular, a source/graph/assignment or
// geometry change cannot silently reuse old homes.
func ValidateHomePackingV1(plan ShardPlanV1, parent Artifact, homes []int, receipt HomePackingReceiptV1) error {
	input, parentSHA, err := homePackingRequestJSONV1(plan, parent)
	if err != nil {
		return err
	}
	if receipt.Policy != HomePackingPolicyV1 || receipt.ParentSHA256 != parentSHA || receipt.RequestSHA256 != fmt.Sprintf("%x", sha256.Sum256(input)) || receipt.HomesSHA256 != homePacksDigestV1(homes) {
		return errors.New("vectorpartition: retained home packing provenance mismatch")
	}
	return validateHomePacksV1(plan, parent.Assignment, homes)
}

func homePacksDigestV1(homes []int) string {
	raw, _ := json.Marshal(homes) // []int has no fallible JSON values.
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

func validateHomePacksV1(plan ShardPlanV1, domains, homes []int) error {
	if plan.Vectors < 1 || plan.Vectors > maxVectors || len(domains) != plan.Vectors || len(homes) != plan.Vectors || plan.LogicalDomains < 1 || plan.PacksPerDomain < 1 || plan.Partitions > maxPartitions || plan.LogicalDomains > maxPartitions/plan.PacksPerDomain || plan.Partitions != plan.LogicalDomains*plan.PacksPerDomain {
		return errors.New("vectorpartition: invalid home packing shape")
	}
	counts, loads := make([]int, plan.LogicalDomains), make([]int, plan.Partitions)
	for ordinal, domain := range domains {
		pack := homes[ordinal]
		if domain < 0 || domain >= plan.LogicalDomains || pack < 0 || pack >= plan.Partitions || pack/plan.PacksPerDomain != domain {
			return errors.New("vectorpartition: physical home escapes its logical domain")
		}
		counts[domain]++
		loads[pack]++
	}
	for pack, load := range loads {
		count := counts[pack/plan.PacksPerDomain]
		cap := (count + plan.PacksPerDomain - 1) / plan.PacksPerDomain
		if load < 1 || load > cap || load > plan.HomeCapacity || count > plan.DomainHomeCapacity {
			return fmt.Errorf("vectorpartition: home pack %d load=%d exceeds exact balance/nonempty bounds (cap=%d)", pack, load, cap)
		}
	}
	return nil
}

func validHomePackingDigestV1(value string) bool {
	raw, err := hex.DecodeString(value)
	return err == nil && len(raw) == sha256.Size && hex.EncodeToString(raw) == value
}
