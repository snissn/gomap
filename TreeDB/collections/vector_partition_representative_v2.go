package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
)

func vectorPartitionRepresentativeLessV2(a, b VectorPartitionRepresentativeV2) bool {
	if a.PartitionID != b.PartitionID {
		return a.PartitionID < b.PartitionID
	}
	return a.NodeID < b.NodeID
}

func equalVectorPartitionRepresentativesV2(a, b []VectorPartitionRepresentativeV2) bool {
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

func validateRepresentativesWithContextVPM(ctx context.Context, representatives []VectorPartitionRepresentativeV2, domains uint32, rows uint64, capPer int) error {
	count := 0
	for i, r := range representatives {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if r.NodeID == 0 || r.PartitionID >= domains || r.VectorOrdinal >= rows || i > 0 && !vectorPartitionRepresentativeLessV2(representatives[i-1], r) {
			return fmt.Errorf("%w: invalid representative node identity", ErrVectorPartitionManifestInvalid)
		}
		if i == 0 || r.PartitionID != representatives[i-1].PartitionID {
			count = 0
		}
		count++
		if count > capPer {
			return fmt.Errorf("%w: representative domain cap", ErrVectorPartitionManifestInvalid)
		}
	}
	return nil
}

func putRepresentativesWithContextVPM(ctx context.Context, b *bytes.Buffer, representatives []VectorPartitionRepresentativeV2) error {
	putU32VPM(b, uint32(len(representatives)))
	for i, r := range representatives {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		putU64VPM(b, r.VectorOrdinal)
		putU32VPM(b, r.PartitionID)
		putU32VPM(b, r.NodeID)
	}
	return ctx.Err()
}

func (r *vpmReader) representatives() []VectorPartitionRepresentativeV2 {
	n := r.allocationCount(r.l.MaxMemberships, 16, "representative")
	if r.err != nil {
		return nil
	}
	if n > r.l.totalMembershipLimit()-r.membershipTotal {
		r.err = errors.New("total membership cap")
		return nil
	}
	r.membershipTotal += n
	if r.canceled() {
		return nil
	}
	out := make([]VectorPartitionRepresentativeV2, n)
	for i := range out {
		if i&1023 == 0 && r.canceled() {
			return nil
		}
		out[i] = VectorPartitionRepresentativeV2{VectorOrdinal: r.u64(), PartitionID: r.u32(), NodeID: r.u32()}
	}
	return out
}
