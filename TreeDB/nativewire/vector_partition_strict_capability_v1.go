package nativewire

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const vectorPartitionStrictCapabilityVersionV1 uint32 = 1

// vectorPartitionStrictSearchCapabilityV1 is server-owned proof propagation,
// not caller authority. The wire carries it only between topology-owned
// coordinator and shard services.
type vectorPartitionStrictSearchCapabilityV1 struct {
	Version                 uint32
	ServingIdentityDigest   string
	CatalogEpoch            uint64
	CatalogDigest           string
	ProofNodeID             raftcluster.NodeID
	ProofGroupID            raftcluster.GroupID
	ProofLeaderTerm         uint64
	CatalogAppliedIndex     uint64
	CatalogCommitIndex      uint64
	CatalogRaftAppliedIndex uint64
	ValidThroughUnixNano    int64
	TargetGroupID           raftcluster.GroupID
	GroupAppliedIndex       uint64
	MAC                     string
}

func newVectorPartitionStrictSearchCapabilityV1(
	request VectorPartitionShardSearchRequestV1,
	identity VectorPartitionServingSnapshotIdentityV1,
	proof raftcluster.CatalogMetaReadProofV1,
	groupApplied uint64,
	key []byte,
) (*vectorPartitionStrictSearchCapabilityV1, error) {
	if len(key) != sha256.Size || !proof.QuorumVerified || proof.LeaderTerm == 0 || proof.CatalogAppliedIndex == 0 ||
		proof.CommitIndex < proof.CatalogAppliedIndex || proof.RaftAppliedIndex < proof.CatalogAppliedIndex ||
		request.TargetGroupID == "" || groupApplied == 0 || !isVectorPartitionShardSearchDigestV1(identity.ServingIdentityDigest) ||
		identity.CatalogEpoch == 0 || !isVectorPartitionShardSearchDigestV1(identity.CatalogDigest) {
		return nil, ErrVectorPartitionShardSearchGenerationMismatch
	}
	validThrough := proof.ValidThroughUnixNano
	if request.DeadlineUnixNano != 0 && request.DeadlineUnixNano < validThrough {
		validThrough = request.DeadlineUnixNano
	}
	if validThrough <= time.Now().UnixNano() {
		return nil, context.DeadlineExceeded
	}
	capability := &vectorPartitionStrictSearchCapabilityV1{
		Version: vectorPartitionStrictCapabilityVersionV1, ServingIdentityDigest: identity.ServingIdentityDigest,
		CatalogEpoch: identity.CatalogEpoch, CatalogDigest: identity.CatalogDigest,
		ProofNodeID: proof.NodeID, ProofGroupID: proof.GroupID, ProofLeaderTerm: proof.LeaderTerm,
		CatalogAppliedIndex: proof.CatalogAppliedIndex, CatalogCommitIndex: proof.CommitIndex,
		CatalogRaftAppliedIndex: proof.RaftAppliedIndex, ValidThroughUnixNano: validThrough,
		TargetGroupID: request.TargetGroupID, GroupAppliedIndex: groupApplied,
		MAC: string(make([]byte, sha256.Size*2)),
	}
	mac, err := vectorPartitionStrictSearchCapabilityMACV1(request, *capability, key)
	if err != nil {
		return nil, err
	}
	capability.MAC = mac
	return capability, nil
}

func validateVectorPartitionStrictSearchCapabilityV1(request VectorPartitionShardSearchRequestV1, key []byte) error {
	capability := request.StrictCapability
	if len(key) != sha256.Size || capability == nil || capability.Version != vectorPartitionStrictCapabilityVersionV1 ||
		!isVectorPartitionShardSearchDigestV1(capability.ServingIdentityDigest) ||
		capability.CatalogEpoch == 0 || !isVectorPartitionShardSearchDigestV1(capability.CatalogDigest) ||
		capability.ProofNodeID == "" || capability.ProofGroupID == "" || capability.ProofLeaderTerm == 0 ||
		capability.CatalogAppliedIndex == 0 || capability.CatalogCommitIndex < capability.CatalogAppliedIndex ||
		capability.CatalogRaftAppliedIndex < capability.CatalogAppliedIndex || capability.TargetGroupID != request.TargetGroupID ||
		capability.GroupAppliedIndex == 0 || capability.ValidThroughUnixNano <= time.Now().UnixNano() ||
		request.DeadlineUnixNano != 0 && capability.ValidThroughUnixNano > request.DeadlineUnixNano {
		return ErrVectorPartitionShardSearchGenerationMismatch
	}
	want, err := vectorPartitionStrictSearchCapabilityMACV1(request, *capability, key)
	if err != nil {
		return err
	}
	gotBytes, gotErr := hex.DecodeString(capability.MAC)
	wantBytes, wantErr := hex.DecodeString(want)
	if gotErr != nil || wantErr != nil || !hmac.Equal(gotBytes, wantBytes) {
		return ErrVectorPartitionShardSearchGenerationMismatch
	}
	return nil
}

func vectorPartitionStrictSearchCapabilityMACV1(request VectorPartitionShardSearchRequestV1, capability vectorPartitionStrictSearchCapabilityV1, key []byte) (string, error) {
	if len(key) != sha256.Size {
		return "", ErrVectorPartitionShardSearchInvalidRequest
	}
	request.TargetNodeID = ""
	request.StrictCapability = nil
	capability.MAC = ""
	raw, err := json.Marshal(struct {
		Request    VectorPartitionShardSearchRequestV1
		Capability vectorPartitionStrictSearchCapabilityV1
	}{request, capability})
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(raw)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func vectorPartitionStrictSearchCapabilityBytesV1(capability *vectorPartitionStrictSearchCapabilityV1) (uint64, error) {
	if capability == nil {
		return 0, nil
	}
	size := uint64(128)
	for _, value := range []string{
		capability.ServingIdentityDigest, capability.CatalogDigest, string(capability.ProofNodeID),
		string(capability.ProofGroupID), string(capability.TargetGroupID), capability.MAC,
	} {
		var ok bool
		size, ok = addUint64V1(size, uint64(len(value)))
		if !ok {
			return 0, ErrVectorPartitionShardSearchInvalidRequest
		}
	}
	return size, nil
}
