package collections

import (
	"bytes"
	"fmt"
	"sort"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	// Prepared publication admits a finite existing tree and file frontier.
	// The 16K five-root target currently uses 273 pager pages and two Manager
	// files; these absolute ceilings leave substantial growth room while keeping
	// every later scan and clone finite.
	preparedInsertPublisherMaxBasePages       = uint64(8192)
	preparedInsertPublisherMaxValueLogFiles   = 32
	preparedInsertPublisherMaxVisibleObjects  = 64
	preparedInsertPublisherMaxLeafGenerations = 8192
	preparedInsertPublisherMaxLeafFileIDs     = 8192
	// Sum of the source-derived per-root bounds. The warm 16K five-root target
	// currently admits 74,582 pages; this absolute ceiling also covers cold
	// publication and stays below the fixed byte tranche charged at Prepare.
	preparedInsertPublisherMaxOutputPages = uint64(237942)
	preparedInsertPublisherMaxRecordBytes = int64(64 << 20)
	preparedInsertPublisherMaxCacheBytes  = int64(64 << 20)
	preparedInsertPublisherFixedBytes     = int64(128 << 20)
)

// preparedInsertPublisherReserveBytes is held by the token before Prepare
// returns. Q page images cover pager/zipper output; P resident pages cover the
// admitted base; 512 B/page covers scan maps and root bookkeeping; 256 B/page
// covers COW slices/maps. A no-dictionary read can hold encoded, decoded, and
// selected-record copies together, followed by the bounded grouped-frame
// cache. The final fixed tranche covers Manager/resource sets, descriptors,
// builders, and slice growth. Collection/WAL/asset allocations are charged by
// preparedInsertCommitReserveBytes separately.
func preparedInsertPublisherReserveBytes() int64 {
	p := int64(preparedInsertPublisherMaxBasePages)
	q := int64(preparedInsertPublisherMaxOutputPages)
	reserve := q * 4096
	reserve += p * 4096
	reserve += (p + q) * (512 + 256)
	reserve += 3 * preparedInsertPublisherMaxRecordBytes
	reserve += preparedInsertPublisherMaxCacheBytes
	reserve += preparedInsertPublisherFixedBytes
	return reserve
}

func preparedInsertPublicationLimits() *backenddb.PreparedRootPublicationLimits {
	statePages := preparedInsertPublisherMaxBasePages + preparedInsertPublisherMaxOutputPages
	return &backenddb.PreparedRootPublicationLimits{
		MaxRegisteredValueLogFiles: preparedInsertPublisherMaxValueLogFiles,
		MaxVisibleResources:        preparedInsertPublisherMaxVisibleObjects,
		MaxPendingLeafFileIDs:      preparedInsertPublisherMaxValueLogFiles,
		MaxLeafGenerations:         preparedInsertPublisherMaxLeafGenerations,
		MaxLeafGenerationFileIDs:   preparedInsertPublisherMaxLeafFileIDs,
		MaxDescriptorEntries:       preparedInsertMaxRootDescriptors,
		MaxDescriptorBytes:         preparedInsertMaxRootDescriptorBytes,
		MaxDescriptorRootIDs:       preparedInsertMaxDescriptorRootIDs,
		MaxTotalOutputPages:        preparedInsertPublisherMaxOutputPages,
		MaxVisibleMembers:          preparedInsertPublisherMaxVisibleObjects,
		MaxAllocatorDebt:           preparedInsertPublisherMaxVisibleObjects,
		MaxSeals:                   preparedInsertPublisherMaxVisibleObjects,
		MaxSealPrefixEntries:       int(preparedInsertPublisherMaxBasePages),
		InitialPointCensusLimit:    preparedInsertRootPointCensusLimit,
		FreelistCOW:                backenddb.PreparedFreelistCOWLimitsForPages(statePages),
	}
}

func checkPreparedInsertPublicationBase(profile backenddb.PreparedRootPublicationBaseProfile) error {
	read := profile.ValueLogRead
	cowOver := backenddb.CheckPreparedFreelistCOWProfile(profile.FreelistCOW, backenddb.PreparedFreelistCOWLimitsForPages(preparedInsertPublisherMaxBasePages)) != nil
	if profile.PagerPages > preparedInsertPublisherMaxBasePages ||
		profile.RegisteredValueLogFiles > preparedInsertPublisherMaxValueLogFiles ||
		profile.LeafGenerations > preparedInsertPublisherMaxLeafGenerations ||
		profile.LeafGenerationFileIDs > preparedInsertPublisherMaxLeafFileIDs ||
		profile.PendingLeafFileIDs > preparedInsertPublisherMaxValueLogFiles ||
		profile.VisibleResources > preparedInsertPublisherMaxVisibleObjects ||
		profile.VisibleMembers >= preparedInsertPublisherMaxVisibleObjects ||
		profile.AllocatorDebt > preparedInsertPublisherMaxVisibleObjects-2 ||
		profile.Seals >= preparedInsertPublisherMaxVisibleObjects ||
		profile.SealPrefixEntries > preparedInsertPublisherMaxBasePages ||
		cowOver ||
		read.MaxRecordBytes <= 0 || read.MaxRecordBytes > preparedInsertPublisherMaxRecordBytes ||
		read.RegisteredFiles > preparedInsertPublisherMaxValueLogFiles ||
		read.GroupedFrameCacheMaxBytes < 0 || read.GroupedFrameCacheMaxBytes > preparedInsertPublisherMaxCacheBytes ||
		read.GroupedFrameCacheEntries < 0 || read.GroupedFrameCacheEntries > 2048 ||
		read.GroupedFrameCacheMaxRawBytes < 0 || read.GroupedFrameCacheMaxRawBytes > 1<<20 ||
		read.HasDictionaryLookup || read.HasTemplateLookup {
		return fmt.Errorf("%w: ordered publisher base exceeds prepared admission", ErrPreparedInsertResourceLimit)
	}
	return nil
}

// The target lane admits at most this much old tree traversal while planning
// one root. The page count also needs a separate byte charge before the first
// plan read; these counts alone are not a request-wide memory reservation.
var preparedInsertRootPointCensusLimit = backenddb.PreparedRootPointCensusLimit{
	MaxPages: 8192, MaxOldEntries: 8192, MaxDepth: 32, MaxPointOps: preparedInsertMaxRows,
}

func preparedInsertManifestMaxNewKeyBytes(cfg ColumnStoreConfig) uint32 {
	maxKey := len(columnManifestHeaderRecordKey)
	for _, fixed := range []int{
		len(columnManifestPartRecordPrefix) + 16,
		len(columnManifestSegmentOwnershipRecordPrefix) + 4,
	} {
		if fixed > maxKey {
			maxKey = fixed
		}
	}
	for _, column := range cfg.Columns {
		for _, prefix := range []string{columnManifestDictionaryCodesRecordPrefix, columnManifestInt64ValuesRecordPrefix} {
			if n := len(prefix) + 16 + len(column.Name); n > maxKey {
				maxKey = n
			}
		}
	}
	for _, spec := range cfg.AggregateMetadata {
		if n := len(columnManifestAggregateMetadataRecordPrefix) + 16 + len(spec.Name); n > maxKey {
			maxKey = n
		}
	}
	return uint32(maxKey)
}

// profilePreparedColumnLateRoots runs under the ordered publication write
// lock. The context builder uses assigned identities and runs after WAL, but
// the captured manifest, locator and system root keys can be bounded now.
func (c *Collection) profilePreparedColumnLateRoots(input columnWritePublishInput, rootNames []string, baseRootIDs map[string]uint64) ([]backenddb.PreparedRootPointProfile, backenddb.PreparedRootPointProfile, error) {
	var empty backenddb.PreparedRootPointProfile
	if c == nil || c.db == nil || input.meta.Options.ColumnStore == nil || input.operation != ColumnPublishOperationInsert ||
		len(input.sourceDeleteDocuments) != 0 || len(input.documents) == 0 || len(input.documents) > preparedInsertMaxRows ||
		len(rootNames) != len(input.rootNames)+2 || len(input.rootNames) == 0 || len(input.rootNames) > 2 ||
		input.rootNames[0] != collectionPrimaryRootName(input.meta.Name) {
		return nil, empty, fmt.Errorf("%w: unsupported prepared root group shape", ErrPreparedInsertResourceLimit)
	}
	limit := preparedInsertRootPointCensusLimit
	manifestRootName := collectionColumnManifestRootName(input.meta.Name)
	locatorRootName := collectionColumnRowLocatorRootName(input.meta.Name)
	if rootNames[len(rootNames)-2] != manifestRootName || rootNames[len(rootNames)-1] != locatorRootName {
		return nil, empty, fmt.Errorf("%w: prepared context root order", ErrPreparedInsertResourceLimit)
	}
	manifestRoot, manifestFound := baseRootIDs[manifestRootName]
	locatorRoot, locatorFound := baseRootIDs[locatorRootName]
	if !manifestFound || !locatorFound {
		return nil, empty, fmt.Errorf("%w: missing prepared context root", ErrPreparedInsertResourceLimit)
	}
	manifestPolicy, err := collectionRootStoragePolicyForDB(c.db, input.meta, manifestRootName)
	if err != nil {
		return nil, empty, err
	}
	// Old manifest records are capped and retained by the closure preflight.
	// At most ten new assets, one ownership marker, and the header can change.
	manifest, err := c.db.ProfilePreparedRootWholePointBudget(manifestRoot, manifestPolicy,
		preparedInsertMaxManifestRecords+12, preparedInsertManifestMaxNewKeyBytes(*input.meta.Options.ColumnStore), limit)
	if err != nil {
		return nil, empty, fmt.Errorf("%w: prepared manifest root profile: %v", ErrPreparedInsertResourceLimit, err)
	}
	locatorPolicy, err := collectionRootStoragePolicyForDB(c.db, input.meta, locatorRootName)
	if err != nil {
		return nil, empty, err
	}
	locatorKeys := make([][]byte, len(input.documents))
	for i := range input.documents {
		if n := len(input.documents[i].ID); n == 0 || n > preparedInsertMaxIDBytes {
			return nil, empty, fmt.Errorf("%w: prepared locator key width", ErrPreparedInsertResourceLimit)
		}
		locatorKeys[i] = input.documents[i].ID
	}
	sort.Slice(locatorKeys, func(i, j int) bool { return bytes.Compare(locatorKeys[i], locatorKeys[j]) < 0 })
	locator, err := c.db.ProfilePreparedRootPointKeys(locatorRoot, locatorPolicy, locatorKeys, limit)
	if err != nil {
		return nil, empty, fmt.Errorf("%w: prepared locator root profile: %v", ErrPreparedInsertResourceLimit, err)
	}
	systemKeys := make([][]byte, 0, len(rootNames)+2)
	systemKeys = append(systemKeys, []byte(systemCollectionMetaKey(input.meta.Name)))
	for _, rootName := range rootNames {
		systemKeys = append(systemKeys, []byte(systemCollectionRootKey(rootName)))
	}
	systemKeys = append(systemKeys, []byte(systemCollectionDocumentGenerationKey(input.meta.Name)))
	sort.Slice(systemKeys, func(i, j int) bool { return bytes.Compare(systemKeys[i], systemKeys[j]) < 0 })
	system, err := c.db.ProfilePreparedRootPointKeys(input.baseSystemRoot, backenddb.OrderedRootStorageDefault, systemKeys, limit)
	if err != nil {
		return nil, empty, fmt.Errorf("%w: prepared system root profile: %v", ErrPreparedInsertResourceLimit, err)
	}
	return []backenddb.PreparedRootPointProfile{manifest, locator}, system, nil
}
