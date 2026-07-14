package dictdb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

var (
	dictionaryIndexPhysicalDigest    = sha256.Sum256([]byte("dictdb-index-v1"))
	dictionaryValueLogPhysicalDigest = sha256.Sum256([]byte("dictdb-value-log-v1"))
)

type dictionaryStablePhysicalRole string

const (
	dictionaryStableIndexRole    dictionaryStablePhysicalRole = "index"
	dictionaryStableValueLogRole dictionaryStablePhysicalRole = "value-log"
)

var addDictionaryStableResourceToken = func(builder *rootpublication.StableResourceSetBuilder, token *rootpublication.StableResourceToken, _ dictionaryStablePhysicalRole) error {
	return builder.Add(token)
}

// CaptureDictionaryResources returns the exact transitive durable resources
// needed to decode dictID. The returned set owns its snapshot and deletion pins
// until Release; callers must merge it into the parent publication before the
// dictionary ID becomes reachable.
func (s *Store) CaptureDictionaryResources(ctx context.Context, dictID uint64) (*rootpublication.StableResourceSet, error) {
	if s == nil || s.backend == nil {
		return nil, errStoreUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if dictID == 0 {
		return nil, fmt.Errorf("dictdb: invalid dictionary id 0")
	}

	snapshot := s.backend.AcquireStableSnapshot()
	if snapshot == nil {
		return nil, errStoreUnavailable
	}
	snapshotOwned := false
	defer func() {
		if !snapshotOwned {
			_ = snapshot.Close()
		}
	}()

	key := bytesKey(dictID)
	dictionary, err := snapshot.Get(key)
	if err != nil {
		return nil, fmt.Errorf("dictdb: read dictionary %d: %w", dictID, err)
	}
	if len(dictionary) == 0 {
		return nil, fmt.Errorf("dictdb: dictionary %d has empty definition", dictID)
	}
	digest := sha256.Sum256(dictionary)
	if binary.BigEndian.Uint64(digest[:8]) != dictID {
		return nil, fmt.Errorf("dictdb: dictionary %d content digest mismatch", dictID)
	}
	entry, err := snapshot.GetEntryExact(key)
	if err != nil {
		return nil, fmt.Errorf("dictdb: read dictionary %d entry: %w", dictID, err)
	}
	logical := rootpublication.StableLogicalObligation{
		Class: "dictionary-generation", Kind: "dictionary", Namespace: "dictdb",
		Generation: dictID, FileID: dictID, Offset: 0, Length: int64(len(dictionary)),
		Reachability: rootpublication.ReachabilityDictionaryGeneration,
		Digest:       digest,
	}

	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityDictionaryGeneration)
	defer builder.Abandon()

	indexToken, err := snapshot.NewStableIndexResourceToken(rootpublication.StableResourceSpec{
		Kind:         rootpublication.ResourceDictionary,
		LogicalLane:  "dictdb/index",
		ResourceID:   "index",
		Digest:       dictionaryIndexPhysicalDigest,
		Reachability: rootpublication.ReachabilityDictionaryGeneration,
		LogicalObligations: []rootpublication.StableLogicalObligation{
			logical,
		},
		ContentSynced: true,
	}, NewStableDictionaryResourceToken)
	if err != nil {
		return nil, fmt.Errorf("dictdb: capture dictionary %d index: %w", dictID, err)
	}
	snapshotOwned = true
	if err := addDictionaryStableResourceToken(builder, indexToken, dictionaryStableIndexRole); err != nil {
		indexToken.Release()
		return nil, fmt.Errorf("dictdb: add dictionary %d index: %w", dictID, err)
	}

	if entry.Flags&node.FlagPointer != 0 {
		state := snapshot.State()
		if state == nil || state.ValueLogSet == nil {
			return nil, fmt.Errorf("dictdb: dictionary %d pointer has no pinned value-log set", dictID)
		}
		segment := state.ValueLogSet.Files[entry.ValuePtr.FileID]
		if segment == nil || segment.File == nil {
			return nil, fmt.Errorf("dictdb: dictionary %d value-log file %d unavailable", dictID, entry.ValuePtr.FileID)
		}
		diagnosticPath, relErr := filepath.Rel(s.dir, segment.Path)
		if relErr != nil || diagnosticPath == "." || filepath.IsAbs(diagnosticPath) || diagnosticPath == ".." {
			return nil, fmt.Errorf("dictdb: resolve dictionary %d value-log path", dictID)
		}
		recordLength := page.ValuePtrRecordLength(entry.ValuePtr)
		if recordLength == 0 || entry.ValuePtr.Offset > ^uint64(0)-uint64(recordLength) {
			return nil, fmt.Errorf("dictdb: dictionary %d has invalid value-log frontier", dictID)
		}
		token, tokenErr := snapshot.NewStableValueLogPhysicalResourceToken(entry.ValuePtr.FileID, rootpublication.StableResourceSpec{
			Kind:           rootpublication.ResourceDictionary,
			LogicalLane:    "dictdb/value-log",
			ResourceID:     "value-log/" + strconv.FormatUint(uint64(entry.ValuePtr.FileID), 10),
			Generation:     uint64(entry.ValuePtr.FileID),
			DiagnosticPath: filepath.ToSlash(diagnosticPath),
			Frontier:       rootpublication.DurableFrontier{Bytes: entry.ValuePtr.Offset + uint64(recordLength)},
			Digest:         dictionaryValueLogPhysicalDigest,
			Reachability:   rootpublication.ReachabilityDictionaryGeneration,
			LogicalObligations: []rootpublication.StableLogicalObligation{
				logical,
			},
			ContentSynced: true,
		}, NewStableDictionaryResourceToken)
		if tokenErr != nil {
			return nil, fmt.Errorf("dictdb: capture dictionary %d value-log: %w", dictID, tokenErr)
		}
		if addErr := addDictionaryStableResourceToken(builder, token, dictionaryStableValueLogRole); addErr != nil {
			token.Release()
			return nil, fmt.Errorf("dictdb: add dictionary %d value-log: %w", dictID, addErr)
		}
	}

	resources, err := builder.Freeze()
	if err != nil {
		return nil, fmt.Errorf("dictdb: freeze dictionary %d resources: %w", dictID, err)
	}
	if err := validateCapturedDictionaryPhysicalClosure(resources, entry.Flags&node.FlagPointer != 0); err != nil {
		resources.Release()
		return nil, fmt.Errorf("dictdb: validate dictionary %d physical closure: %w", dictID, err)
	}
	return resources, nil
}

func validateCapturedDictionaryPhysicalClosure(resources *rootpublication.StableResourceSet, pointer bool) error {
	if resources == nil {
		return fmt.Errorf("%w: dictionary capture returned no physical closure", rootpublication.ErrUnresolvedResource)
	}
	var index, valueLog bool
	for _, descriptor := range resources.Descriptors() {
		if descriptor.Kind() != rootpublication.ResourceDictionary {
			return fmt.Errorf("%w: dictionary closure contains kind %q", rootpublication.ErrResourceConflict, descriptor.Kind())
		}
		switch descriptor.Digest() {
		case dictionaryIndexPhysicalDigest:
			if index {
				return fmt.Errorf("%w: dictionary closure contains duplicate index authority", rootpublication.ErrResourceConflict)
			}
			index = true
		case dictionaryValueLogPhysicalDigest:
			if valueLog {
				return fmt.Errorf("%w: dictionary closure contains duplicate value-log authority", rootpublication.ErrResourceConflict)
			}
			valueLog = true
		default:
			return fmt.Errorf("%w: dictionary closure contains unknown physical role", rootpublication.ErrResourceConflict)
		}
	}
	if !index {
		return fmt.Errorf("%w: dictionary closure omitted index authority", rootpublication.ErrUnresolvedResource)
	}
	if pointer && !valueLog {
		return fmt.Errorf("%w: pointer dictionary closure omitted value-log authority", rootpublication.ErrUnresolvedResource)
	}
	if !pointer && valueLog {
		return fmt.Errorf("%w: inline dictionary closure contains value-log authority", rootpublication.ErrResourceConflict)
	}
	return nil
}
