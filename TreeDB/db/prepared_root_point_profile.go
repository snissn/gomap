package db

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// ErrPreparedRootPointProfileLimit rejects a prepared shape before its command
// WAL append. The ordinary ordered-root publisher does not use this profile.
var ErrPreparedRootPointProfileLimit = errors.New("treedb: prepared root point profile limit")

// PreparedRootPointProfile binds a pure-put page-output allowance to one
// captured root. PointOps and MaxKeyBytes are checked against the actual
// context/system delta after the command WAL append; an excess there is a
// violated preflight invariant and must take the publisher's poison path.
type PreparedRootPointProfile struct {
	BaseRoot    uint64
	PointOps    int
	MaxKeyBytes uint32
	OutputPages uint64
}

// PreparedRootPointCensusLimit limits reads made by the pre-WAL profile. The
// caller must separately reserve its leaf-log decoder and traversal scratch.
type PreparedRootPointCensusLimit struct {
	MaxPages      uint64
	MaxOldEntries uint64
	MaxDepth      uint32
	MaxPointOps   uint64
}

// ProfilePreparedRootPointBatch profiles an already materialized sorted root
// delta without sorting, compacting, or copying its values. It must run inside the serialized ordered
// publication preflight, while the captured root remains stable.
func (db *DB) ProfilePreparedRootPointBatch(input OrderedRootDeltaBatchPublishInput, limit PreparedRootPointCensusLimit) (PreparedRootPointProfile, error) {
	if limit.MaxPages == 0 || input.Delta == nil || input.IncludeDeletedOnColdBuild || input.Delta.HasDeleteRanges() || uint64(input.Delta.Len()) > limit.MaxPointOps {
		return PreparedRootPointProfile{}, ErrPreparedRootPointProfileLimit
	}
	return db.profilePreparedRootPointEntries(input.BaseRoot, input.StoragePolicy, input.Delta.OrderedEntries(), limit)
}

// ProfilePreparedRootPointKeys profiles known keys whose values receive a
// PartID/LSN only after WAL. Keys are borrowed for this call; the temporary
// entry headers are bounded by len(keys) and released on return.
func (db *DB) ProfilePreparedRootPointKeys(rootID uint64, policy OrderedRootStoragePolicy, keys [][]byte, limit PreparedRootPointCensusLimit) (PreparedRootPointProfile, error) {
	if limit.MaxPages == 0 || limit.MaxDepth == 0 || uint64(len(keys)) > limit.MaxPointOps {
		return PreparedRootPointProfile{}, ErrPreparedRootPointProfileLimit
	}
	entries := make([]batch.Entry, len(keys))
	for i := range keys {
		entries[i] = batch.Entry{Type: batch.OpPut, Key: keys[i]}
	}
	return db.profilePreparedRootPointEntries(rootID, policy, entries, limit)
}

func (db *DB) profilePreparedRootPointEntries(rootID uint64, policy OrderedRootStoragePolicy, entries []batch.Entry, limit PreparedRootPointCensusLimit) (PreparedRootPointProfile, error) {
	if db == nil || limit.MaxPages == 0 || limit.MaxDepth == 0 || limit.MaxOldEntries == 0 || limit.MaxPointOps == 0 || len(entries) == 0 || uint64(len(entries)) > limit.MaxPointOps {
		return PreparedRootPointProfile{}, ErrPreparedRootPointProfileLimit
	}
	idx := db.idx.Load()
	if idx == nil {
		return PreparedRootPointProfile{}, ErrClosed
	}
	opts, err := db.orderedRootPublishOptionsForPolicy(policy)
	if err != nil {
		return PreparedRootPointProfile{}, err
	}
	z, err := db.orderedRootZipperForOptions(idx, opts)
	if err != nil {
		return PreparedRootPointProfile{}, err
	}
	var newKeyBytes uint32
	for i := range entries {
		if entries[i].Type != batch.OpPut || len(entries[i].Key) == 0 || len(entries[i].Key) > math.MaxUint32 ||
			(i > 0 && bytes.Compare(entries[i-1].Key, entries[i].Key) >= 0) {
			return PreparedRootPointProfile{}, ErrPreparedRootPointProfileLimit
		}
		if uint32(len(entries[i].Key)) > newKeyBytes {
			newKeyBytes = uint32(len(entries[i].Key))
		}
	}
	result, err := z.PrepareReadOnlyPlan(rootID, entries, nil, zipper.ReadOnlyPrepareOptions{
		CountTouchedOldEntries: true,
		MaxTouchedPages:        limit.MaxPages,
		MaxTouchedDepth:        limit.MaxDepth,
		MaxTouchedOldEntries:   limit.MaxOldEntries,
		OmitKeys:               true,
		DiscardLeafSpans:       true,
	})
	if err != nil {
		return PreparedRootPointProfile{}, fmt.Errorf("%w: %v", ErrPreparedRootPointProfileLimit, err)
	}
	if result.Maintenance || result.DeleteRanges != 0 {
		return PreparedRootPointProfile{}, ErrPreparedRootPointProfileLimit
	}
	maxKeyBytes := newKeyBytes
	if result.MaxTouchedKeyBytes > maxKeyBytes {
		maxKeyBytes = result.MaxTouchedKeyBytes
	}
	if !z.CanFitTwoPurePointInternalChildren(maxKeyBytes) {
		return PreparedRootPointProfile{}, ErrPreparedRootPointProfileLimit
	}
	pages, err := result.PurePointOutputPageUpperBound()
	if err != nil {
		return PreparedRootPointProfile{}, fmt.Errorf("%w: %v", ErrPreparedRootPointProfileLimit, err)
	}
	return PreparedRootPointProfile{BaseRoot: rootID, PointOps: len(entries), MaxKeyBytes: maxKeyBytes, OutputPages: pages}, nil
}

// ProfilePreparedRootWholePointBudget handles a root whose future PUT keys
// cannot be known before WAL, such as a new manifest ownership marker keyed by
// a newly assigned value-log file ID. A bounded whole-root census makes every
// possible touched leaf and internal child part of the allowance.
func (db *DB) ProfilePreparedRootWholePointBudget(rootID uint64, policy OrderedRootStoragePolicy, maxPointOps int, maxNewKeyBytes uint32, limit PreparedRootPointCensusLimit) (PreparedRootPointProfile, error) {
	if db == nil || maxPointOps <= 0 || limit.MaxPages == 0 || limit.MaxOldEntries == 0 || limit.MaxDepth == 0 || uint64(maxPointOps) > limit.MaxPointOps {
		return PreparedRootPointProfile{}, ErrPreparedRootPointProfileLimit
	}
	idx := db.idx.Load()
	if idx == nil {
		return PreparedRootPointProfile{}, ErrClosed
	}
	opts, err := db.orderedRootPublishOptionsForPolicy(policy)
	if err != nil {
		return PreparedRootPointProfile{}, err
	}
	z, err := db.orderedRootZipperForOptions(idx, opts)
	if err != nil {
		return PreparedRootPointProfile{}, err
	}
	census, err := z.CensusRootReadOnly(rootID, zipper.ReadOnlyRootCensusLimits{
		MaxPages: limit.MaxPages, MaxEntries: limit.MaxOldEntries, MaxDepth: limit.MaxDepth,
	})
	if err != nil {
		return PreparedRootPointProfile{}, fmt.Errorf("%w: %v", ErrPreparedRootPointProfileLimit, err)
	}
	maxKeyBytes := maxNewKeyBytes
	if census.MaxKeyBytes > maxKeyBytes {
		maxKeyBytes = census.MaxKeyBytes
	}
	if !z.CanFitTwoPurePointInternalChildren(maxKeyBytes) {
		return PreparedRootPointProfile{}, ErrPreparedRootPointProfileLimit
	}
	pages, err := census.PurePointOutputPageUpperBound(maxPointOps)
	if err != nil {
		return PreparedRootPointProfile{}, fmt.Errorf("%w: %v", ErrPreparedRootPointProfileLimit, err)
	}
	return PreparedRootPointProfile{BaseRoot: rootID, PointOps: maxPointOps, MaxKeyBytes: maxKeyBytes, OutputPages: pages}, nil
}

func checkPreparedRootPointBatch(input OrderedRootDeltaBatchPublishInput, profile PreparedRootPointProfile) error {
	if profile.OutputPages == 0 || input.BaseRoot != profile.BaseRoot || input.Delta == nil || input.Delta.HasDeleteRanges() || input.IncludeDeletedOnColdBuild || input.Delta.Len() > profile.PointOps {
		return ErrPreparedRootPointProfileLimit
	}
	var previousKey []byte
	for i, entry := range input.Delta.OrderedEntries() {
		if entry.Type != batch.OpPut || len(entry.Key) == 0 || len(entry.Key) > int(profile.MaxKeyBytes) ||
			(i > 0 && bytes.Compare(previousKey, entry.Key) >= 0) {
			return ErrPreparedRootPointProfileLimit
		}
		previousKey = entry.Key
	}
	return nil
}
