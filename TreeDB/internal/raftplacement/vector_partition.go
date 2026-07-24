package raftplacement

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// VectorPartitionPlacementRecordV1 is deliberately outside token/ring
// placement. Its PartitionID is a derived ANN identity, never a document-ID
// token partition, and several logical partitions may name one Raft group.
type VectorPartitionPlacementRecordV1 struct {
	Collection                                                         CollectionRefV1
	IndexName                                                          string
	IndexDefinitionDigest                                              string
	SourceGeneration, SourceChecksum, SourceSchemaHash, SourceRowCount uint64
	PartitionGeneration                                                uint64
	PartitionCount                                                     uint32
	Partitions                                                         []VectorPartitionGroupV1
}
type VectorPartitionGroupV1 struct {
	PartitionID uint32
	GroupID     raftcluster.GroupID
}

var (
	ErrInvalidVectorPartitionPlacement = errors.New("raftplacement: invalid vector partition placement")
	ErrUnknownVectorPartitionGroup     = errors.New("raftplacement: vector partition references unknown group")
)

// ValidateVectorPartitionPlacementV1 validates the complete logical mapping
// against this catalog's known groups. It does not activate a generation or
// produce a request route.
func (c ResolvedCatalogV1) ValidateVectorPartitionPlacementV1(p VectorPartitionPlacementRecordV1) error {
	if err := validateCollectionRef(p.Collection); err != nil {
		return errors.Join(ErrInvalidVectorPartitionPlacement, err)
	}
	if p.IndexName == "" || len(p.IndexDefinitionDigest) != 64 || !isSHA256HexVectorPartitionV1(p.IndexDefinitionDigest) || p.SourceGeneration == 0 || p.SourceRowCount == 0 || p.PartitionGeneration == 0 || p.PartitionCount == 0 || len(p.Partitions) != int(p.PartitionCount) {
		return ErrInvalidVectorPartitionPlacement
	}
	for i, part := range p.Partitions {
		if part.PartitionID != uint32(i) || part.GroupID == "" {
			return fmt.Errorf("%w: incomplete or duplicate logical partition", ErrInvalidVectorPartitionPlacement)
		}
		if _, ok := c.groups[part.GroupID]; !ok {
			return fmt.Errorf("%w: %s", ErrUnknownVectorPartitionGroup, part.GroupID)
		}
	}
	return nil
}

func isSHA256HexVectorPartitionV1(s string) bool {
	_, err := hex.DecodeString(s)
	return err == nil && len(s) == 64 && s == strings.ToLower(s)
}
