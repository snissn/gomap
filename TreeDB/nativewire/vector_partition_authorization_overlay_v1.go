package nativewire

import "sync/atomic"

const maxVectorPartitionAuthorizationOverlayEntriesV1 = 1 << 20

type vectorPartitionAuthorizationOverlayStateV1 struct {
	digest string
	denied map[string]struct{}
}

type vectorPartitionAuthorizationOverlayV1 struct {
	state atomic.Pointer[vectorPartitionAuthorizationOverlayStateV1]
}

func newVectorPartitionAuthorizationOverlayV1(digest string, denied []string, maxIDBytes int) (*vectorPartitionAuthorizationOverlayV1, error) {
	o := &vectorPartitionAuthorizationOverlayV1{}
	if err := o.publishV1(digest, denied, maxIDBytes); err != nil {
		return nil, err
	}
	return o, nil
}

func (o *vectorPartitionAuthorizationOverlayV1) publishV1(digest string, denied []string, maxIDBytes int) error {
	if o == nil {
		return ErrVectorPartitionShardSearchInvalidRequest
	}
	state, err := newVectorPartitionAuthorizationOverlayStateV1(digest, denied, maxIDBytes)
	if err != nil {
		return err
	}
	o.state.Store(state)
	return nil
}

func newVectorPartitionAuthorizationOverlayStateV1(digest string, denied []string, maxIDBytes int) (*vectorPartitionAuthorizationOverlayStateV1, error) {
	if !isVectorPartitionShardSearchDigestV1(digest) || maxIDBytes < 1 || len(denied) > maxVectorPartitionAuthorizationOverlayEntriesV1 {
		return nil, ErrVectorPartitionShardSearchInvalidRequest
	}
	state := &vectorPartitionAuthorizationOverlayStateV1{digest: digest, denied: make(map[string]struct{}, len(denied))}
	for _, id := range denied {
		if id == "" || len(id) > maxIDBytes {
			return nil, ErrVectorPartitionShardSearchInvalidRequest
		}
		if _, exists := state.denied[id]; exists {
			return nil, ErrVectorPartitionShardSearchInvalidRequest
		}
		state.denied[id] = struct{}{}
	}
	return state, nil
}

func (o *vectorPartitionAuthorizationOverlayV1) filterV1(expectedDigest string, response *VectorPartitionCoordinatorResponseV1) error {
	if o == nil || response == nil {
		return ErrVectorPartitionShardSearchAssetsUnavailable
	}
	state := o.state.Load()
	if state == nil || state.digest != expectedDigest {
		return ErrVectorPartitionShardSearchGenerationMismatch
	}
	neighbors := response.Neighbors[:0]
	for _, neighbor := range response.Neighbors {
		if _, denied := state.denied[neighbor.ID]; !denied {
			neighbors = append(neighbors, neighbor)
		}
	}
	response.Neighbors = neighbors
	if o.state.Load() != state {
		return ErrVectorPartitionShardSearchGenerationMismatch
	}
	return nil
}
