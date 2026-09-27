package vectorpartition

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"strings"
)

// ANNSourceRowIdentityV2 names a semantic row in one completed source snapshot.
// No local directory generation, WAL position, or physical row reference is part of it.
type ANNSourceRowIdentityV2 struct {
	SourceOwner      string
	ShardID          string
	SnapshotRevision uint64
	SnapshotDigest   [sha256.Size]byte
	Ordinal          uint64
	DocumentRevision uint64
}

// ANNDomainV2 declares one logical graph pack and its exact intended membership.
// Its physical section chunks are deliberately absent from placement identity.
type ANNDomainV2 struct {
	DomainID        uint32
	LogicalPackID   string
	MembershipCount uint64
}

type ANNMemberV2 struct {
	Source ANNSourceRowIdentityV2
	Kind   string // "home" or "overlap"
}

type ANNOwnerCommitmentV2 struct {
	GroupID          string `json:"group_id"`
	DomainCount      uint64 `json:"domain_count"`
	MembershipCount  uint64 `json:"membership_count"`
	MembershipDigest string `json:"membership_digest"`
}

// ANNOwnerAccumulatorV2 validates and hashes the complete canonical owner
// stream with constant retained state. A caller must independently verify the
// source rows and ownership; a digest alone is not preparation authority.
type ANNOwnerAccumulatorV2 struct {
	owner                           string
	digest                          hash.Hash
	domainDigest                    hash.Hash
	domain                          ANNDomainV2
	domains, members, domainMembers uint64
	previousMember                  string
}

func NewANNOwnerAccumulatorV2(owner string) (*ANNOwnerAccumulatorV2, error) {
	if !annNameV2(owner) {
		return nil, errors.New("vectorpartition: ANN owner")
	}
	h := sha256.New()
	writeSourceSnapshotStringV2(h, "treedb/ann-owner/v2")
	writeSourceSnapshotStringV2(h, owner)
	return &ANNOwnerAccumulatorV2{owner: owner, digest: h}, nil
}

func (a *ANNOwnerAccumulatorV2) BeginDomain(domain ANNDomainV2) error {
	if a == nil || a.digest == nil || !annNameV2(domain.LogicalPackID) || domain.MembershipCount == 0 || domain.MembershipCount > 1<<32 || (a.domains > 0 && (domain.DomainID <= a.domain.DomainID || a.domainMembers != a.domain.MembershipCount)) {
		return errors.New("vectorpartition: noncanonical ANN domain")
	}
	a.domain = domain
	a.domains++
	a.domainMembers = 0
	a.previousMember = ""
	a.domainDigest = sha256.New()
	for _, h := range []hash.Hash{a.digest, a.domainDigest} {
		writeSourceSnapshotStringV2(h, "treedb/ann-domain/v2")
		writeSourceSnapshotStringV2(h, a.owner)
		writeSourceSnapshotUintV2(h, uint64(domain.DomainID))
		writeSourceSnapshotStringV2(h, domain.LogicalPackID)
		writeSourceSnapshotUintV2(h, domain.MembershipCount)
	}
	return nil
}

func ANNSourceRowKeyV2(row ANNSourceRowIdentityV2) (string, error) {
	if !annNameV2(row.SourceOwner) || !annNameV2(row.ShardID) || row.SnapshotRevision == 0 || row.SnapshotDigest == ([sha256.Size]byte{}) || row.DocumentRevision == 0 {
		return "", errors.New("vectorpartition: invalid ANN source row")
	}
	// The key deliberately excludes revision and kind: one source ordinal may
	// occur only once in a domain, even when conflicting identities are supplied.
	return fmt.Sprintf("%s\x00%s\x00%016x", row.SourceOwner, row.ShardID, row.Ordinal), nil
}

func (a *ANNOwnerAccumulatorV2) AddMember(member ANNMemberV2) error {
	key, err := ANNSourceRowKeyV2(member.Source)
	if err != nil {
		return err
	}
	if a == nil || a.domainDigest == nil || a.domainMembers >= a.domain.MembershipCount || key <= a.previousMember || (member.Kind != "home" && member.Kind != "overlap") || a.members == ^uint64(0) {
		return errors.New("vectorpartition: noncanonical ANN membership")
	}
	for _, h := range []hash.Hash{a.digest, a.domainDigest} {
		writeSourceSnapshotStringV2(h, member.Source.SourceOwner)
		writeSourceSnapshotStringV2(h, member.Source.ShardID)
		writeSourceSnapshotUintV2(h, member.Source.SnapshotRevision)
		h.Write(member.Source.SnapshotDigest[:])
		writeSourceSnapshotUintV2(h, member.Source.Ordinal)
		writeSourceSnapshotUintV2(h, member.Source.DocumentRevision)
		writeSourceSnapshotStringV2(h, member.Kind)
	}
	a.previousMember = key
	a.domainMembers++
	a.members++
	return nil
}

func (a *ANNOwnerAccumulatorV2) DomainDigest() ([sha256.Size]byte, error) {
	var out [sha256.Size]byte
	if a == nil || a.domainDigest == nil || a.domainMembers != a.domain.MembershipCount {
		return out, errors.New("vectorpartition: incomplete ANN domain")
	}
	copy(out[:], a.domainDigest.Sum(out[:0]))
	return out, nil
}

func (a *ANNOwnerAccumulatorV2) Commitment() (ANNOwnerCommitmentV2, error) {
	if a == nil || a.domains == 0 || a.domainMembers != a.domain.MembershipCount {
		return ANNOwnerCommitmentV2{}, errors.New("vectorpartition: incomplete ANN owner")
	}
	return ANNOwnerCommitmentV2{GroupID: a.owner, DomainCount: a.domains, MembershipCount: a.members, MembershipDigest: hex.EncodeToString(a.digest.Sum(nil))}, nil
}

func ANNOwnerSetDigestV2(owners []ANNOwnerCommitmentV2) (string, error) {
	if len(owners) == 0 || len(owners) > 128 {
		return "", errors.New("vectorpartition: ANN owner count")
	}
	for i, o := range owners {
		digest, err := hex.DecodeString(o.MembershipDigest)
		if !annNameV2(o.GroupID) || (i > 0 && owners[i-1].GroupID >= o.GroupID) || o.DomainCount == 0 || o.DomainCount > 1<<32 || o.MembershipCount < o.DomainCount || err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != o.MembershipDigest {
			return "", errors.New("vectorpartition: noncanonical ANN owner commitment")
		}
	}
	raw, err := json.Marshal(struct {
		Domain string                 `json:"domain"`
		Owners []ANNOwnerCommitmentV2 `json:"owners"`
	}{"treedb/ann-owner-set/v2", owners})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func annNameV2(s string) bool { return s != "" && len(s) <= 1024 && !strings.ContainsRune(s, 0) }

// ANNRecordV2 is one canonical intended placement record. Exactly one side is
// present; each domain declaration precedes exactly its declared member count.
type ANNRecordV2 struct {
	Domain *ANNDomainV2
	Member *ANNMemberV2
}

func (a *ANNOwnerAccumulatorV2) AddRecord(record ANNRecordV2) error {
	if (record.Domain == nil) == (record.Member == nil) {
		return errors.New("vectorpartition: ANN record shape")
	}
	if record.Domain != nil {
		return a.BeginDomain(*record.Domain)
	}
	return a.AddMember(*record.Member)
}
