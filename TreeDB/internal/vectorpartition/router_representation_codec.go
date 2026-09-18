package vectorpartition

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
)

// Stream the two potentially large model arrays with cancellation and an
// allocation-before-write byte bound. This encoder is specific to the offline
// representation format; it is not another repository serialization framework.
// Each leaf membership is streamed rather than marshaled as one large value.
func encodeRouterRepresentationJSONV1(ctx context.Context, m RouterRepresentationModelV1) ([]byte, string, error) {
	var out bytes.Buffer
	var writeErr error
	write := func(raw []byte) {
		if writeErr != nil {
			return
		}
		if writeErr = routerBuildContextErrV1(ctx); writeErr != nil {
			return
		}
		if uint64(len(raw)) > m.Options.Config.MaxRouterBytes-uint64(out.Len()) {
			writeErr = errors.New("encoded representation byte cap")
			return
		}
		_, writeErr = out.Write(raw)
	}
	value := func(v any) {
		if writeErr != nil {
			return
		}
		if writeErr = routerBuildContextErrV1(ctx); writeErr != nil {
			return
		}
		raw, err := json.Marshal(v)
		if err != nil {
			writeErr = err
			return
		}
		write(raw)
	}
	header := struct {
		Format               string                        `json:"format"`
		Options              RouterRepresentationOptionsV1 `json:"options"`
		Dimensions           int                           `json:"dimensions"`
		SourceSHA256         string                        `json:"source_sha256"`
		MembershipSHA256     string                        `json:"membership_sha256"`
		LeafMembershipSHA256 string                        `json:"leaf_membership_sha256"`
	}{m.Format, m.Options, m.Dimensions, m.SourceSHA256, m.MembershipSHA256, m.LeafMembershipSHA256}
	raw, err := json.Marshal(header)
	if err != nil {
		return nil, "", err
	}
	write(raw[:len(raw)-1])
	write([]byte(`,"nodes":[`))
	var number [32]byte
	for i, n := range m.Nodes {
		if writeErr != nil {
			break
		}
		if i > 0 {
			write([]byte(","))
		}
		ids := n.Members
		n.Members = nil
		raw, err := json.Marshal(n)
		if err != nil {
			return nil, "", err
		}
		if len(ids) == 0 {
			write(raw)
			continue
		}
		write(raw[:len(raw)-1])
		write([]byte(`,"leaf_members":[`))
		for j, id := range ids {
			if j&255 == 0 {
				if err := routerBuildContextErrV1(ctx); err != nil {
					return nil, "", err
				}
			}
			if j > 0 {
				write([]byte(","))
			}
			write(strconv.AppendUint(number[:0], id, 10))
			if writeErr != nil {
				break
			}
		}
		write([]byte("]}"))
	}
	write([]byte(`],"representatives":[`))
	for i, r := range m.Representatives {
		if writeErr != nil {
			break
		}
		if i > 0 {
			write([]byte(","))
		}
		value(r)
	}
	write([]byte(`],"metrics":`))
	value(m.Metrics)
	write([]byte("}"))
	if writeErr != nil {
		return nil, "", writeErr
	}
	if err := routerBuildContextErrV1(ctx); err != nil {
		return nil, "", err
	}
	// Hash in bounded blocks too, not an uninterruptible model-sized Sum256.
	h := sha256.New()
	for start := 0; start < out.Len(); start += 16384 {
		if err := routerBuildContextErrV1(ctx); err != nil {
			return nil, "", err
		}
		end := min(start+16384, out.Len())
		_, _ = h.Write(out.Bytes()[start:end])
	}
	return out.Bytes(), hex.EncodeToString(h.Sum(nil)), routerBuildContextErrV1(ctx)
}
