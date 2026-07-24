package collections

import (
	"errors"
	"fmt"
)

const vectorPartitionOverlapPolicyPrefixV1 = "m3_bounded_overlap_v1"

// VectorPartitionOverlapPolicyV1 is persisted in the manifest's
// BalancePolicy field. The manifest integrity digest therefore binds capacity
// and unused-budget accounting without introducing an optional sidecar.
type VectorPartitionOverlapPolicyV1 struct {
	Capacity uint64
	Budget   uint64
	Unspent  uint64
}

func FormatVectorPartitionOverlapPolicyV1(policy VectorPartitionOverlapPolicyV1) (string, error) {
	if policy.Capacity == 0 || policy.Unspent > policy.Budget {
		return "", errors.New("collections: invalid vector partition overlap policy")
	}
	return fmt.Sprintf("%s:capacity=%d,budget=%d,unspent=%d", vectorPartitionOverlapPolicyPrefixV1, policy.Capacity, policy.Budget, policy.Unspent), nil
}

func parseVectorPartitionOverlapPolicyV1(raw string) (VectorPartitionOverlapPolicyV1, bool) {
	var policy VectorPartitionOverlapPolicyV1
	n, err := fmt.Sscanf(raw, vectorPartitionOverlapPolicyPrefixV1+":capacity=%d,budget=%d,unspent=%d", &policy.Capacity, &policy.Budget, &policy.Unspent)
	if err != nil || n != 3 || policy.Capacity == 0 || policy.Unspent > policy.Budget {
		return VectorPartitionOverlapPolicyV1{}, false
	}
	canonical, err := FormatVectorPartitionOverlapPolicyV1(policy)
	if err != nil || canonical != raw {
		return VectorPartitionOverlapPolicyV1{}, false
	}
	return policy, true
}
