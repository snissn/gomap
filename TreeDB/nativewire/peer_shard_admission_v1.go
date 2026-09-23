package nativewire

import (
	"encoding/binary"
	"io"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func readPeerShardFrameV1(reader io.Reader, maxFrame uint32, admission *peerNodeAdmissionV1, scope string) (vectorPartitionShardSearchTCPFrameV1, peerWorkLeaseV1, error) {
	var work peerWorkLeaseV1
	if admission == nil {
		frame, err := readVectorPartitionShardSearchTCPFrameV1(reader, maxFrame)
		return frame, work, err
	}
	var prefix [4]byte
	if _, err := io.ReadFull(reader, prefix[:]); err != nil {
		return vectorPartitionShardSearchTCPFrameV1{}, work, err
	}
	size := binary.BigEndian.Uint32(prefix[:])
	if size == 0 || size > maxFrame {
		return vectorPartitionShardSearchTCPFrameV1{}, work, raftcluster.ErrAdmissionUnavailable
	}
	// Charge the encoded input and bounded decoder expansion before allocation.
	var err error
	work, err = admission.work(scope, peerRequestsV1, int64(size)*8+(64<<10))
	if err != nil {
		return vectorPartitionShardSearchTCPFrameV1{}, work, err
	}
	raw := make([]byte, size)
	if _, err := io.ReadFull(reader, raw); err != nil {
		work.release()
		return vectorPartitionShardSearchTCPFrameV1{}, work, err
	}
	frame, err := decodeVectorPartitionShardSearchTCPFrameBodyWithResponseBoundsV1(raw, 32, 256)
	if err != nil {
		work.release()
	}
	return frame, work, err
}

// This bound uses the existing public M5 limits and actual fanout/top-k. The
// receiver enforces this same frame ceiling; a peer cannot enlarge it merely
// by declaring a longer string in its response.
func peerShardResponseFrameV1(request VectorPartitionShardSearchRequestV1, configured uint32) (uint32, error) {
	limits := DefaultVectorPartitionShardSearchLimitsV1()
	if len(request.PartitionIDs) == 0 || len(request.PartitionIDs) > limits.MaxPartitions || request.TopK < 1 || request.TopK > limits.MaxTopK {
		return 0, raftcluster.ErrRouteTargetUnsupported
	}
	bound := uint64(64<<10) + uint64(len(request.PartitionIDs))*(uint64(128+limits.MaxIdentityBytes)+uint64(request.TopK)*uint64(16+limits.MaxStableIDBytes))
	return uint32(min(bound, uint64(configured))), nil
}
