package vectorpartition

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

const (
	ShardGenerationDescriptorSchemaV1 = 1
	ShardGenerationDescriptorKindV1   = "treedb_vector_partition_shard_generation_v1"
)

// ShardGenerationDescriptorV1 persists selected planner inputs and the
// realized useful-only membership digest. Pre-alpha: unknown versions fail
// closed; there is no migration path.
type ShardGenerationDescriptorV1 struct {
	SchemaVersion    int                  `json:"schema_version"`
	ResultKind       string               `json:"result_kind"`
	Plan             ShardPlanV1          `json:"plan"`
	OverlapConfig    OverlapConfig        `json:"overlap_config"`
	Memberships      []Membership         `json:"memberships"`
	MembershipDigest string               `json:"membership_digest"`
	PackSummaries    []ShardPackSummaryV1 `json:"pack_summaries"`
}

func NewShardGenerationDescriptorV1(plan ShardPlanV1, cfg OverlapConfig, overlap OverlapResult) (ShardGenerationDescriptorV1, error) {
	digest, err := MembershipDigestV1(overlap.Memberships)
	if err != nil {
		return ShardGenerationDescriptorV1{}, err
	}
	summaries, err := AccountShardPacksV1(plan, overlap.Memberships)
	if err != nil {
		return ShardGenerationDescriptorV1{}, err
	}
	// The builder's own realized loads are evidence, not authority: reject a
	// result whose declared loads disagree with the memberships it published.
	if len(overlap.Loads) != len(summaries) {
		return ShardGenerationDescriptorV1{}, errors.New("vectorpartition: overlap loads do not cover every planned pack")
	}
	for partition, load := range overlap.Loads {
		if load != summaries[partition].Rows {
			return ShardGenerationDescriptorV1{}, fmt.Errorf("vectorpartition: pack %d load=%d does not match membership-derived rows=%d", partition, load, summaries[partition].Rows)
		}
	}
	out := ShardGenerationDescriptorV1{
		SchemaVersion:    ShardGenerationDescriptorSchemaV1,
		ResultKind:       ShardGenerationDescriptorKindV1,
		Plan:             plan,
		OverlapConfig:    cfg,
		Memberships:      append([]Membership(nil), overlap.Memberships...),
		MembershipDigest: digest,
		PackSummaries:    summaries,
	}
	if err := ValidateShardGenerationDescriptorV1(out); err != nil {
		return ShardGenerationDescriptorV1{}, err
	}
	return out, nil
}

func CanonicalShardGenerationJSONV1(d ShardGenerationDescriptorV1) ([]byte, error) {
	if err := ValidateShardGenerationDescriptorV1(d); err != nil {
		return nil, err
	}
	return json.Marshal(d)
}

func DecodeShardGenerationDescriptorV1(raw []byte, maxBytes int) (ShardGenerationDescriptorV1, error) {
	if maxBytes < 1 || len(raw) > maxBytes {
		return ShardGenerationDescriptorV1{}, errors.New("vectorpartition: shard generation descriptor exceeds byte cap")
	}
	if !utf8.Valid(raw) {
		return ShardGenerationDescriptorV1{}, errors.New("vectorpartition: shard generation descriptor contains invalid UTF-8")
	}
	var d ShardGenerationDescriptorV1
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		return ShardGenerationDescriptorV1{}, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return ShardGenerationDescriptorV1{}, errors.New("vectorpartition: shard generation descriptor has trailing JSON")
	}
	if err := ValidateShardGenerationDescriptorV1(d); err != nil {
		return ShardGenerationDescriptorV1{}, err
	}
	return d, nil
}

func ValidateShardGenerationDescriptorV1(d ShardGenerationDescriptorV1) error {
	if d.SchemaVersion != ShardGenerationDescriptorSchemaV1 {
		return fmt.Errorf("vectorpartition: unsupported shard generation schema %d", d.SchemaVersion)
	}
	if d.ResultKind != ShardGenerationDescriptorKindV1 {
		return errors.New("vectorpartition: shard generation result kind mismatch")
	}
	if !d.OverlapConfig.UsefulOnly || d.OverlapConfig.RequireExact {
		return errors.New("vectorpartition: shard generation requires useful-only overlap")
	}
	recomputed, err := PlanByteBoundedShardsV1(d.Plan.request())
	if err != nil {
		return err
	}
	if recomputed != d.Plan {
		return errors.New("vectorpartition: shard generation plan does not match selected inputs")
	}
	if d.OverlapConfig.Capacity != d.Plan.OverlapCapacity {
		return errors.New("vectorpartition: shard generation overlap capacity does not match the plan")
	}
	// Pack summaries are re-derived from the memberships, never revalidated
	// against themselves, so an omitted pack or an unrelated declared load
	// cannot survive by recomputing the membership digest.
	summaries, err := AccountShardPacksV1(d.Plan, d.Memberships)
	if err != nil {
		return err
	}
	if len(d.PackSummaries) != len(summaries) {
		return fmt.Errorf("vectorpartition: shard generation declares %d pack summaries, plan requires %d", len(d.PackSummaries), len(summaries))
	}
	for partition, want := range summaries {
		if d.PackSummaries[partition] != want {
			return fmt.Errorf("vectorpartition: shard generation pack %d summary=%+v does not match membership-derived %+v", partition, d.PackSummaries[partition], want)
		}
	}
	digest, err := MembershipDigestV1(d.Memberships)
	if err != nil {
		return err
	}
	if d.MembershipDigest != digest {
		return errors.New("vectorpartition: shard generation membership digest mismatch")
	}
	return nil
}

func MembershipDigestV1(memberships []Membership) (string, error) {
	raw, err := json.Marshal(memberships)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
