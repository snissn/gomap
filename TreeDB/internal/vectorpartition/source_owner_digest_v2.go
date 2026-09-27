package vectorpartition

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"strings"
)

// NewOwnerSnapshotSetHashV2 starts the canonical semantic owner descriptor
// stream used by trusted preparation and pinned local opens. Physical directory
// generations and local completion receipts are deliberately absent.
func NewOwnerSnapshotSetHashV2(owner string) hash.Hash {
	h := sha256.New()
	for _, s := range []string{"treedb/owner-snapshot-set/v2", owner} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(s)))
		h.Write(size[:])
		h.Write([]byte(s))
	}
	return h
}

func WriteOwnerSnapshotIdentityV2(h hash.Hash, s SourceSnapshotV2) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(s.ShardID)))
	h.Write(size[:])
	h.Write([]byte(s.ShardID))
	binary.BigEndian.PutUint64(size[:], s.SnapshotRevision)
	h.Write(size[:])
	h.Write(s.Digest[:])
}

// SourceOwnerCommitmentV2 is the bounded semantic part of a prepared owner
// aggregate. It is input identity, not evidence of local or quorum durability.
type SourceOwnerCommitmentV2 struct {
	GroupID           string `json:"group_id"`
	ShardCount        uint64 `json:"shard_count"`
	SnapshotSetDigest string `json:"snapshot_set_digest"`
}

func SourceOwnerSetDigestV2(owners []SourceOwnerCommitmentV2) (string, error) {
	if len(owners) == 0 || len(owners) > 128 {
		return "", errors.New("vectorpartition: owner commitment count")
	}
	var total uint64
	for i, o := range owners {
		raw, err := hex.DecodeString(o.SnapshotSetDigest)
		if o.GroupID == "" || len(o.GroupID) > 1024 || strings.ContainsRune(o.GroupID, 0) || (i > 0 && owners[i-1].GroupID >= o.GroupID) || o.ShardCount == 0 || o.ShardCount > 65536 || err != nil || len(raw) != sha256.Size || hex.EncodeToString(raw) != o.SnapshotSetDigest {
			return "", errors.New("vectorpartition: noncanonical owner commitment")
		}
		total += o.ShardCount
		if total > 65536 {
			return "", errors.New("vectorpartition: owner shard count")
		}
	}
	raw, err := json.Marshal(struct {
		Domain string                    `json:"domain"`
		Owners []SourceOwnerCommitmentV2 `json:"owners"`
	}{"treedb/source-owner-set/v2", owners})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
