package collections

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// ColumnAssetLifecycleRegistryClass identifies the logical registry a
// process-local lifecycle record belongs to.
type ColumnAssetLifecycleRegistryClass string

const (
	ColumnAssetLifecycleRegistryPendingPublish ColumnAssetLifecycleRegistryClass = "pending_publish"
	ColumnAssetLifecycleRegistryPreparedAsset  ColumnAssetLifecycleRegistryClass = "prepared_asset"
	ColumnAssetLifecycleRegistryQuarantine     ColumnAssetLifecycleRegistryClass = "quarantine"
)

// ColumnAssetPendingPublishRegistrationOptions registers refs that are staged
// for a manifest/root publish whose outcome is not yet unambiguous. Slice 2
// records are process-local and report-only.
type ColumnAssetPendingPublishRegistrationOptions struct {
	Owner  string           `json:"owner"`
	Source string           `json:"source"`
	Reason string           `json:"reason,omitempty"`
	Refs   []ColumnAssetRef `json:"refs,omitempty"`
}

// ColumnAssetPreparedAssetRegistrationOptions registers prepared/staged asset
// refs that must be reported as protected roots until the lease is released.
type ColumnAssetPreparedAssetRegistrationOptions struct {
	Owner  string           `json:"owner"`
	Source string           `json:"source"`
	Reason string           `json:"reason,omitempty"`
	Refs   []ColumnAssetRef `json:"refs,omitempty"`
}

// ColumnAssetQuarantineRegistrationOptions registers logical quarantine records
// for refs or whole segments. It does not move, rename, delete, truncate, or
// otherwise mutate the recorded assets.
type ColumnAssetQuarantineRegistrationOptions struct {
	Owner    string                         `json:"owner"`
	Source   string                         `json:"source"`
	Reason   string                         `json:"reason,omitempty"`
	Refs     []ColumnAssetRef               `json:"refs,omitempty"`
	Segments []ColumnAssetQuarantineSegment `json:"segments,omitempty"`
}

// ColumnAssetQuarantineSegment describes a logical segment-level quarantine
// record. Bytes may be zero when the caller has not measured the segment size.
type ColumnAssetQuarantineSegment struct {
	Namespace string `json:"namespace,omitempty"`
	FileID    uint32 `json:"file_id"`
	Bytes     int64  `json:"bytes,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

// ColumnAssetLifecycleRegistryLease is an idempotent close/release handle for a
// process-local lifecycle registry record.
type ColumnAssetLifecycleRegistryLease struct {
	mu       sync.Mutex
	id       uint64
	class    ColumnAssetLifecycleRegistryClass
	owner    string
	source   string
	reason   string
	refs     []ColumnAssetRef
	segments []ColumnAssetQuarantineSegment
	closed   bool
}

type columnAssetLifecycleRegistryRecord struct {
	ID           uint64
	Scope        columnAssetLifecyclePinScope
	Collection   string
	Namespace    string
	Class        ColumnAssetLifecycleRegistryClass
	Owner        string
	Source       string
	Reason       string
	Refs         []ColumnAssetRef
	Segments     []ColumnAssetQuarantineSegment
	RefBytes     int64
	SegmentBytes int64
}

type columnAssetLifecycleRegistrySummaries struct {
	PendingPublish ColumnAssetLifecycleRegistrySummary
	PreparedAssets ColumnAssetLifecycleRegistrySummary
	Quarantine     ColumnAssetLifecycleQuarantineSummary
}

var columnAssetLifecycleProcessRegistries = struct {
	sync.Mutex
	nextID   uint64
	nextDBID uint64
	records  map[uint64]columnAssetLifecycleRegistryRecord
	dbIDs    map[*backenddb.DB]uint64
}{}

// ID returns the process-local registry record identifier.
func (l *ColumnAssetLifecycleRegistryLease) ID() uint64 {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.id
}

// Class returns the lifecycle registry class for this lease.
func (l *ColumnAssetLifecycleRegistryLease) Class() ColumnAssetLifecycleRegistryClass {
	if l == nil {
		return ""
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.class
}

// Source returns the caller-supplied source label for this lease.
func (l *ColumnAssetLifecycleRegistryLease) Source() string {
	if l == nil {
		return ""
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.source
}

// Refs returns a defensive copy of the registered refs.
func (l *ColumnAssetLifecycleRegistryLease) Refs() []ColumnAssetRef {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]ColumnAssetRef(nil), l.refs...)
}

// Segments returns a defensive copy of the registered quarantine segments.
func (l *ColumnAssetLifecycleRegistryLease) Segments() []ColumnAssetQuarantineSegment {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]ColumnAssetQuarantineSegment(nil), l.segments...)
}

// Release releases the process-local registry record. It is idempotent.
func (l *ColumnAssetLifecycleRegistryLease) Release() error {
	return l.Close()
}

// Close releases the process-local registry record. It is idempotent.
func (l *ColumnAssetLifecycleRegistryLease) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	id := l.id
	l.id = 0
	l.mu.Unlock()
	if id == 0 {
		return nil
	}
	columnAssetLifecycleProcessRegistries.Lock()
	if columnAssetLifecycleProcessRegistries.records != nil {
		delete(columnAssetLifecycleProcessRegistries.records, id)
	}
	columnAssetLifecycleProcessRegistries.Unlock()
	return nil
}

// RegisterColumnAssetPendingPublish registers pending-publish refs for
// lifecycle reports. It is process-local/report-only and does not alter publish,
// cleanup, GC, or rewrite behavior.
func (c *Collection) RegisterColumnAssetPendingPublish(opts ColumnAssetPendingPublishRegistrationOptions) (*ColumnAssetLifecycleRegistryLease, error) {
	return c.registerColumnAssetLifecycleRecord(ColumnAssetLifecycleRegistryPendingPublish, opts.Owner, opts.Source, opts.Reason, opts.Refs, nil)
}

// RegisterColumnAssetPreparedAsset registers prepared asset refs for lifecycle
// reports. It is process-local/report-only and does not alter cleanup, GC, or
// rewrite behavior.
func (c *Collection) RegisterColumnAssetPreparedAsset(opts ColumnAssetPreparedAssetRegistrationOptions) (*ColumnAssetLifecycleRegistryLease, error) {
	return c.registerColumnAssetLifecycleRecord(ColumnAssetLifecycleRegistryPreparedAsset, opts.Owner, opts.Source, opts.Reason, opts.Refs, nil)
}

// RegisterColumnAssetPreparedAssets is a convenience alias for callers that
// register a batch of prepared asset refs in one record.
func (c *Collection) RegisterColumnAssetPreparedAssets(opts ColumnAssetPreparedAssetRegistrationOptions) (*ColumnAssetLifecycleRegistryLease, error) {
	return c.RegisterColumnAssetPreparedAsset(opts)
}

// RegisterColumnAssetQuarantine registers logical quarantine refs/segments for
// lifecycle reports. It does not perform physical quarantine or cleanup.
func (c *Collection) RegisterColumnAssetQuarantine(opts ColumnAssetQuarantineRegistrationOptions) (*ColumnAssetLifecycleRegistryLease, error) {
	return c.registerColumnAssetLifecycleRecord(ColumnAssetLifecycleRegistryQuarantine, opts.Owner, opts.Source, opts.Reason, opts.Refs, opts.Segments)
}

func (c *Collection) registerColumnAssetLifecycleRecord(class ColumnAssetLifecycleRegistryClass, owner, source, reason string, refs []ColumnAssetRef, segments []ColumnAssetQuarantineSegment) (*ColumnAssetLifecycleRegistryLease, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if err := validateColumnAssetLifecycleRegistryClass(class); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, errors.New("collections: column asset lifecycle registry owner is required")
	}
	if err := validateColumnAssetLifecycleRegistrySource(source); err != nil {
		return nil, err
	}
	collectionNamespace := columnAssetLifecycleNamespace(c)
	if collectionNamespace == "" {
		return nil, errors.New("collections: column asset lifecycle registry requires collection asset namespace")
	}
	refs = append([]ColumnAssetRef(nil), refs...)
	segments = append([]ColumnAssetQuarantineSegment(nil), segments...)
	if class != ColumnAssetLifecycleRegistryQuarantine && len(refs) == 0 {
		return nil, fmt.Errorf("collections: column asset lifecycle %s registry requires at least one ref", class)
	}
	if class == ColumnAssetLifecycleRegistryQuarantine && len(refs) == 0 && len(segments) == 0 {
		return nil, errors.New("collections: column asset lifecycle quarantine registry requires at least one ref or segment")
	}
	var refBytes int64
	for _, ref := range refs {
		if err := validateColumnAssetRefForPlan(ref); err != nil {
			return nil, fmt.Errorf("collections: column asset lifecycle registry ref: %w", err)
		}
		if ref.Namespace != collectionNamespace {
			return nil, fmt.Errorf("collections: column asset lifecycle registry ref namespace %q does not match collection namespace %q", ref.Namespace, collectionNamespace)
		}
		refBytes = addColumnAssetReachabilityBytes(refBytes, positiveColumnAssetReachabilityLength(ref.Length))
	}
	var segmentBytes int64
	for i := range segments {
		segment, bytes, err := normalizeColumnAssetQuarantineSegment(segments[i], collectionNamespace)
		if err != nil {
			return nil, fmt.Errorf("collections: column asset lifecycle quarantine segment[%d]: %w", i, err)
		}
		segments[i] = segment
		segmentBytes = addColumnAssetReachabilityBytes(segmentBytes, bytes)
	}
	scope := columnAssetLifecyclePinScope{collection: c.meta.Name, namespace: collectionNamespace}
	record := columnAssetLifecycleRegistryRecord{
		Scope:        scope,
		Collection:   scope.collection,
		Namespace:    scope.namespace,
		Class:        class,
		Owner:        owner,
		Source:       source,
		Reason:       reason,
		Refs:         append([]ColumnAssetRef(nil), refs...),
		Segments:     append([]ColumnAssetQuarantineSegment(nil), segments...),
		RefBytes:     refBytes,
		SegmentBytes: segmentBytes,
	}
	id, err := columnAssetLifecycleRegisterProcessRegistryRecord(c.db, record)
	if err != nil {
		return nil, err
	}
	return &ColumnAssetLifecycleRegistryLease{
		id:       id,
		class:    class,
		owner:    owner,
		source:   source,
		reason:   reason,
		refs:     append([]ColumnAssetRef(nil), refs...),
		segments: append([]ColumnAssetQuarantineSegment(nil), segments...),
	}, nil
}

func validateColumnAssetLifecycleRegistryClass(class ColumnAssetLifecycleRegistryClass) error {
	switch class {
	case ColumnAssetLifecycleRegistryPendingPublish,
		ColumnAssetLifecycleRegistryPreparedAsset,
		ColumnAssetLifecycleRegistryQuarantine:
		return nil
	case "":
		return errors.New("collections: column asset lifecycle registry class is required")
	default:
		return fmt.Errorf("collections: unsupported column asset lifecycle registry class %q", class)
	}
}

func validateColumnAssetLifecycleRegistrySource(source string) error {
	if source == "" {
		return errors.New("collections: column asset lifecycle registry source is required")
	}
	if strings.TrimSpace(source) != source || strings.Contains(source, "\x00") {
		return fmt.Errorf("collections: invalid column asset lifecycle registry source %q", source)
	}
	return nil
}

func normalizeColumnAssetQuarantineSegment(segment ColumnAssetQuarantineSegment, collectionNamespace string) (ColumnAssetQuarantineSegment, int64, error) {
	if segment.Namespace == "" {
		segment.Namespace = collectionNamespace
	}
	if _, err := cleanColumnAssetNamespace(segment.Namespace); err != nil {
		return ColumnAssetQuarantineSegment{}, 0, err
	}
	if segment.Namespace != collectionNamespace {
		return ColumnAssetQuarantineSegment{}, 0, fmt.Errorf("namespace %q does not match collection namespace %q", segment.Namespace, collectionNamespace)
	}
	if segment.FileID == 0 {
		return ColumnAssetQuarantineSegment{}, 0, errors.New("file_id is required")
	}
	if segment.Bytes < 0 {
		return ColumnAssetQuarantineSegment{}, 0, fmt.Errorf("bytes=%d cannot be negative", segment.Bytes)
	}
	return segment, segment.Bytes, nil
}

func columnAssetLifecycleRegisterProcessRegistryRecord(db *backenddb.DB, record columnAssetLifecycleRegistryRecord) (uint64, error) {
	if db == nil {
		return 0, errCollectionDBNil
	}
	for {
		columnAssetLifecycleProcessRegistries.Lock()
		if dbID, ok := columnAssetLifecycleProcessRegistries.dbIDs[db]; ok {
			record.Scope.dbID = dbID
			id := columnAssetLifecycleStoreProcessRegistryRecordLocked(record)
			columnAssetLifecycleProcessRegistries.Unlock()
			return id, nil
		}
		columnAssetLifecycleProcessRegistries.Unlock()

		registeredDB := db
		var registeredDBID uint64
		_, ok := registeredDB.RegisterCloseHookIfOpenAfter(func() bool {
			columnAssetLifecycleProcessRegistries.Lock()
			defer columnAssetLifecycleProcessRegistries.Unlock()
			if columnAssetLifecycleProcessRegistries.dbIDs == nil {
				columnAssetLifecycleProcessRegistries.dbIDs = make(map[*backenddb.DB]uint64)
			}
			if existingDBID, ok := columnAssetLifecycleProcessRegistries.dbIDs[registeredDB]; ok {
				registeredDBID = existingDBID
				return false
			}
			columnAssetLifecycleProcessRegistries.nextDBID++
			registeredDBID = columnAssetLifecycleProcessRegistries.nextDBID
			columnAssetLifecycleProcessRegistries.dbIDs[registeredDB] = registeredDBID
			return true
		}, func() error {
			columnAssetLifecycleReleaseProcessRegistryRecordsForDB(registeredDB, registeredDBID)
			return nil
		})
		if !ok {
			return 0, errors.New("collections: column asset lifecycle registry requires an open backend DB")
		}
	}
}

func columnAssetLifecycleStoreProcessRegistryRecordLocked(record columnAssetLifecycleRegistryRecord) uint64 {
	columnAssetLifecycleProcessRegistries.nextID++
	record.ID = columnAssetLifecycleProcessRegistries.nextID
	record.Refs = append([]ColumnAssetRef(nil), record.Refs...)
	record.Segments = append([]ColumnAssetQuarantineSegment(nil), record.Segments...)
	if columnAssetLifecycleProcessRegistries.records == nil {
		columnAssetLifecycleProcessRegistries.records = make(map[uint64]columnAssetLifecycleRegistryRecord)
	}
	columnAssetLifecycleProcessRegistries.records[record.ID] = record
	return record.ID
}

func columnAssetLifecycleRegistryProcessDBID(db *backenddb.DB) uint64 {
	if db == nil {
		return 0
	}
	columnAssetLifecycleProcessRegistries.Lock()
	defer columnAssetLifecycleProcessRegistries.Unlock()
	return columnAssetLifecycleProcessRegistries.dbIDs[db]
}

func columnAssetLifecycleReleaseProcessRegistryRecordsForDB(db *backenddb.DB, dbID uint64) {
	if db == nil || dbID == 0 {
		return
	}
	columnAssetLifecycleProcessRegistries.Lock()
	defer columnAssetLifecycleProcessRegistries.Unlock()
	for id, record := range columnAssetLifecycleProcessRegistries.records {
		if record.Scope.dbID == dbID {
			delete(columnAssetLifecycleProcessRegistries.records, id)
		}
	}
	if columnAssetLifecycleProcessRegistries.dbIDs != nil && columnAssetLifecycleProcessRegistries.dbIDs[db] == dbID {
		delete(columnAssetLifecycleProcessRegistries.dbIDs, db)
	}
}

func (c *Collection) columnAssetLifecycleRegistrySnapshot() []columnAssetLifecycleRegistryRecord {
	if c == nil || c.db == nil {
		return nil
	}
	dbID := columnAssetLifecycleRegistryProcessDBID(c.db)
	if dbID == 0 {
		return nil
	}
	scope := columnAssetLifecyclePinScope{dbID: dbID, collection: c.meta.Name, namespace: columnAssetLifecycleNamespace(c)}
	columnAssetLifecycleProcessRegistries.Lock()
	defer columnAssetLifecycleProcessRegistries.Unlock()
	if len(columnAssetLifecycleProcessRegistries.records) == 0 {
		return nil
	}
	out := make([]columnAssetLifecycleRegistryRecord, 0, len(columnAssetLifecycleProcessRegistries.records))
	for _, record := range columnAssetLifecycleProcessRegistries.records {
		if record.Scope != scope {
			continue
		}
		record.Refs = append([]ColumnAssetRef(nil), record.Refs...)
		record.Segments = append([]ColumnAssetQuarantineSegment(nil), record.Segments...)
		out = append(out, record)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out
}

func summarizeColumnAssetLifecycleRegistries(records []columnAssetLifecycleRegistryRecord) columnAssetLifecycleRegistrySummaries {
	summaries := columnAssetLifecycleRegistrySummaries{
		PendingPublish: columnAssetLifecycleRegistrySummaryBase(),
		PreparedAssets: columnAssetLifecycleRegistrySummaryBase(),
		Quarantine: ColumnAssetLifecycleQuarantineSummary{
			RegistryAvailable: true,
			ProcessLocal:      true,
		},
	}
	pendingSources := make(map[string]int)
	preparedSources := make(map[string]int)
	quarantineSources := make(map[string]int)
	for _, record := range records {
		switch record.Class {
		case ColumnAssetLifecycleRegistryPendingPublish:
			addColumnAssetLifecycleRegistryRecordSummary(&summaries.PendingPublish, pendingSources, record)
		case ColumnAssetLifecycleRegistryPreparedAsset:
			addColumnAssetLifecycleRegistryRecordSummary(&summaries.PreparedAssets, preparedSources, record)
		case ColumnAssetLifecycleRegistryQuarantine:
			addColumnAssetLifecycleQuarantineRecordSummary(&summaries.Quarantine, quarantineSources, record)
		}
	}
	sortColumnAssetLifecycleRegistrySources(summaries.PendingPublish.Sources)
	sortColumnAssetLifecycleRegistrySources(summaries.PreparedAssets.Sources)
	sortColumnAssetLifecycleRegistrySources(summaries.Quarantine.Sources)
	return summaries
}

func columnAssetLifecycleRegistrySummaryBase() ColumnAssetLifecycleRegistrySummary {
	return ColumnAssetLifecycleRegistrySummary{
		RegistryAvailable: true,
		ProcessLocal:      true,
	}
}

func addColumnAssetLifecycleRegistryRecordSummary(summary *ColumnAssetLifecycleRegistrySummary, sourceIndexes map[string]int, record columnAssetLifecycleRegistryRecord) {
	summary.OpenRecords++
	summary.Refs += len(record.Refs)
	summary.Bytes = addColumnAssetReachabilityBytes(summary.Bytes, record.RefBytes)
	idx, ok := sourceIndexes[record.Source]
	if !ok {
		idx = len(summary.Sources)
		sourceIndexes[record.Source] = idx
		summary.Sources = append(summary.Sources, ColumnAssetLifecycleRegistrySourceSummary{Source: record.Source})
	}
	summary.Sources[idx].OpenRecords++
	summary.Sources[idx].Refs += len(record.Refs)
	summary.Sources[idx].Bytes = addColumnAssetReachabilityBytes(summary.Sources[idx].Bytes, record.RefBytes)
}

func addColumnAssetLifecycleQuarantineRecordSummary(summary *ColumnAssetLifecycleQuarantineSummary, sourceIndexes map[string]int, record columnAssetLifecycleRegistryRecord) {
	summary.OpenRecords++
	summary.Refs += len(record.Refs)
	summary.Bytes = addColumnAssetReachabilityBytes(summary.Bytes, record.RefBytes)
	summary.Segments += len(record.Segments)
	summary.SegmentBytes = addColumnAssetReachabilityBytes(summary.SegmentBytes, record.SegmentBytes)
	idx, ok := sourceIndexes[record.Source]
	if !ok {
		idx = len(summary.Sources)
		sourceIndexes[record.Source] = idx
		summary.Sources = append(summary.Sources, ColumnAssetLifecycleRegistrySourceSummary{Source: record.Source})
	}
	summary.Sources[idx].OpenRecords++
	summary.Sources[idx].Refs += len(record.Refs)
	summary.Sources[idx].Bytes = addColumnAssetReachabilityBytes(summary.Sources[idx].Bytes, record.RefBytes)
	summary.Sources[idx].Segments += len(record.Segments)
	summary.Sources[idx].SegmentBytes = addColumnAssetReachabilityBytes(summary.Sources[idx].SegmentBytes, record.SegmentBytes)
}

func sortColumnAssetLifecycleRegistrySources(sources []ColumnAssetLifecycleRegistrySourceSummary) {
	sort.SliceStable(sources, func(i, j int) bool {
		return sources[i].Source < sources[j].Source
	})
}
