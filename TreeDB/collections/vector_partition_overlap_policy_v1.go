package collections

import (
	"errors"
	"fmt"
	"strings"
)

const vectorPartitionOverlapPolicyPrefixV1 = "m3_bounded_overlap_v1"

// VectorPartitionOverlapPolicyV1 is persisted in the manifest's
// BalancePolicy field. Budget is the requested global extra-membership count,
// Realized is the materialized count, and Unspent is the explicitly rejected
// count. The manifest integrity digest therefore binds target-aware capacity
// and exact-treatment accounting without introducing an optional sidecar.
type VectorPartitionOverlapPolicyV1 struct {
	Capacity            uint64
	Budget              uint64
	Realized            uint64
	Unspent             uint64
	BuildIdentityDigest string
}

func FormatVectorPartitionOverlapPolicyV1(policy VectorPartitionOverlapPolicyV1) (string, error) {
	if policy.Capacity == 0 || policy.Realized > policy.Budget || policy.Unspent != policy.Budget-policy.Realized || policy.BuildIdentityDigest != "" && !isSHA256VPM(policy.BuildIdentityDigest) {
		return "", errors.New("collections: invalid vector partition overlap policy")
	}
	raw := fmt.Sprintf("%s:capacity=%d,budget=%d,realized=%d,unspent=%d", vectorPartitionOverlapPolicyPrefixV1, policy.Capacity, policy.Budget, policy.Realized, policy.Unspent)
	if policy.BuildIdentityDigest != "" {
		raw += ",build_identity=" + policy.BuildIdentityDigest
	}
	return raw, nil
}

func parseVectorPartitionOverlapPolicyV1(raw string) (VectorPartitionOverlapPolicyV1, bool) {
	var policy VectorPartitionOverlapPolicyV1
	base, identity, hasIdentity := strings.Cut(raw, ",build_identity=")
	n, err := fmt.Sscanf(base, vectorPartitionOverlapPolicyPrefixV1+":capacity=%d,budget=%d,realized=%d,unspent=%d", &policy.Capacity, &policy.Budget, &policy.Realized, &policy.Unspent)
	if err != nil || n != 4 || policy.Capacity == 0 || policy.Realized > policy.Budget || policy.Unspent != policy.Budget-policy.Realized {
		return VectorPartitionOverlapPolicyV1{}, false
	}
	if hasIdentity {
		if !isSHA256VPM(identity) || strings.Contains(identity, ",") {
			return VectorPartitionOverlapPolicyV1{}, false
		}
		policy.BuildIdentityDigest = identity
	}
	canonical, err := FormatVectorPartitionOverlapPolicyV1(policy)
	if err != nil || canonical != raw {
		return VectorPartitionOverlapPolicyV1{}, false
	}
	return policy, true
}

// ParseVectorPartitionOverlapPolicyV1 returns the canonical accounting and
// optional build identity covered by a manifest's integrity digest.
func ParseVectorPartitionOverlapPolicyV1(raw string) (VectorPartitionOverlapPolicyV1, bool) {
	return parseVectorPartitionOverlapPolicyV1(raw)
}
