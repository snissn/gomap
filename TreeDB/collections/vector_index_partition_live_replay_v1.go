package collections

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

var vectorPartitionLiveReplayAfterAcceptedHookV1 struct {
	sync.RWMutex
	fn func()
}

func runVectorPartitionLiveReplayAfterAcceptedHookV1() {
	vectorPartitionLiveReplayAfterAcceptedHookV1.RLock()
	fn := vectorPartitionLiveReplayAfterAcceptedHookV1.fn
	vectorPartitionLiveReplayAfterAcceptedHookV1.RUnlock()
	if fn != nil {
		fn()
	}
}

// vectorPartitionLiveReplaySpecV1 describes one already-restored carrier that
// must advance in the same root publication as a durable document mutation.
type vectorPartitionLiveReplaySpecV1 struct {
	before       *VectorIndex
	definition   VectorIndexDefinition
	snapshot     vectorIndexPersistSnapshot
	rootName     string
	baseRoot     uint64
	baseCoverage uint64
	beforeSeq    uint64
	vectorColumn int
}

type vectorPartitionLiveReplayEntryV1 struct {
	spec        vectorPartitionLiveReplaySpecV1
	candidate   *VectorIndex
	table       memtable.Table
	publish     memtable.Table
	pointerized bool
	bytesDisk   int64
	snapshotSeq uint64
	iter        iterator.UnsafeIterator
	batch       *batch.Batch
}

type vectorPartitionLiveReplayAttemptV1 struct {
	entries []vectorPartitionLiveReplayEntryV1
}

func (c *Collection) vectorPartitionLiveReplaySpecsV1(input columnWritePublishInput) ([]vectorPartitionLiveReplaySpecV1, error) {
	if input.commandWALIntent == nil {
		return nil, nil
	}
	carriers := c.registeredVectorPartitionLiveCarriersV1()
	if len(carriers) == 0 {
		return nil, nil
	}
	if !columnStoreWriteEnabled(input.meta) || input.catalog == nil || input.meta.Options.ColumnStore == nil {
		return nil, fmt.Errorf("%w: command-WAL partition-live mutation requires a column publication", ErrVectorIndexPartitionLiveUnavailableV1)
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	if snapshotCommitSeq(snap) != input.baseCommitSeq || snapshotSystemRoot(snap) != input.baseSystemRoot {
		_ = snap.Close()
		return nil, ErrConcurrentMutation
	}
	baseCoverage, err := vectorIndexDocumentGenerationForCollection(snap, input.meta.Name)
	_ = snap.Close()
	if err != nil {
		return nil, err
	}
	sort.Slice(carriers, func(i, j int) bool { return carriers[i].name < carriers[j].name })
	specs := make([]vectorPartitionLiveReplaySpecV1, 0, len(carriers))
	for _, carrier := range carriers {
		def, ok := findVectorIndex(input.meta.VectorIndexes, carrier.name)
		if !ok || def.Strategy != VectorIndexStrategyColumnGraph || carrier.validateNativeSnapshotDefinition(def) != "" {
			return nil, fmt.Errorf("%w: replay carrier %q does not match collection metadata", ErrVectorIndexPartitionLiveMismatchV1, carrier.name)
		}
		vectorColumn := -1
		for i, column := range input.meta.Options.ColumnStore.Columns {
			if column.Path == def.Field && column.ValueType == ColumnStoreValueFloat32Vector {
				vectorColumn = i
				break
			}
		}
		if vectorColumn < 0 {
			return nil, fmt.Errorf("%w: replay carrier %q field %q has no float32 vector column", ErrVectorIndexPartitionLiveUnavailableV1, carrier.name, def.Field)
		}
		rootName := collectionVectorIndexRootName(input.meta.Name, def.Name)
		baseRoot := input.catalog.rootID(rootName)
		if reason := carrier.vectorPartitionLiveReplayDurableBaseReasonV1(baseRoot, baseCoverage); baseRoot == 0 || reason != "" {
			return nil, fmt.Errorf("%w: replay carrier %q root is not a clean durable base: %s", ErrVectorIndexPartitionLiveUnavailableV1, carrier.name, reason)
		}
		snapshot, beforeSeq := carrier.persistSnapshot()
		specs = append(specs, vectorPartitionLiveReplaySpecV1{
			before:       carrier,
			definition:   def,
			snapshot:     snapshot,
			rootName:     rootName,
			baseRoot:     baseRoot,
			baseCoverage: baseCoverage,
			beforeSeq:    beforeSeq,
			vectorColumn: vectorColumn,
		})
	}
	return specs, nil
}

func (idx *VectorIndex) vectorPartitionLiveReplayDurableBaseReasonV1(rootID, coverage uint64) string {
	if idx == nil {
		return "nil carrier"
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	// A freshly restored native snapshot gets dirtyMeta set when install refreshes
	// its in-memory state token. That token is not persisted. Permit only that
	// mutation-sequence-zero bookkeeping state; every real post-load live change
	// advances the sequence or dirties persisted rows.
	if idx.persistedEpoch != rootID {
		return fmt.Sprintf("persisted root %d, want %d", idx.persistedEpoch, rootID)
	}
	if idx.persistedSnapshotDirty || len(idx.dirtyNodes) != 0 || len(idx.dirtyDocs) != 0 {
		return "persisted rows are dirty"
	}
	if !idx.sourceDocumentRootsValid || idx.sourceDocumentGeneration != coverage {
		return fmt.Sprintf("coverage %d valid=%t, want %d", idx.sourceDocumentGeneration, idx.sourceDocumentRootsValid, coverage)
	}
	if idx.dirtyMeta && idx.mutationSeq != 0 {
		return fmt.Sprintf("metadata sequence %d is dirty", idx.mutationSeq)
	}
	return ""
}

func (c *Collection) vectorPartitionLiveReplayPreflightV1(specs []vectorPartitionLiveReplaySpecV1) backenddb.OrderedRootGroupPreflight {
	if len(specs) == 0 {
		return nil
	}
	return func() error { return c.validateVectorPartitionLiveReplaySpecsV1(specs) }
}

func (c *Collection) validateVectorPartitionLiveReplaySpecsV1(specs []vectorPartitionLiveReplaySpecV1) error {
	for _, spec := range specs {
		current := c.registeredVectorIndex(spec.definition.Name)
		if current != spec.before || current.nativeMutationSequence() != spec.beforeSeq {
			return fmt.Errorf("%w: replay carrier %q changed before publication", ErrConcurrentMutation, spec.definition.Name)
		}
		if reason := current.vectorPartitionLiveReplayDurableBaseReasonV1(spec.baseRoot, spec.baseCoverage); reason != "" {
			return fmt.Errorf("%w: replay carrier %q durable base changed before publication: %s", ErrConcurrentMutation, spec.definition.Name, reason)
		}
		rootID, err := c.currentNativeVectorIndexRootID(spec.definition.Name)
		if err != nil {
			return err
		}
		if rootID != spec.baseRoot {
			return fmt.Errorf("%w: replay carrier %q current root changed before publication", ErrConcurrentMutation, spec.definition.Name)
		}
	}
	return nil
}

func vectorPartitionLiveReplayMutationsV1(input columnWritePublishInput, vectorColumn int) ([][]byte, [][]float32, error) {
	ids := make([][]byte, 0, len(input.sourceDeleteDocuments)+len(input.documents)+len(input.declaredRows))
	vectors := make([][]float32, 0, cap(ids))
	positions := make(map[string]int, cap(ids))
	set := func(id []byte, vector []float32) {
		key := string(id)
		if pos, ok := positions[key]; ok {
			vectors[pos] = vector
			return
		}
		positions[key] = len(ids)
		ids = append(ids, id)
		vectors = append(vectors, vector)
	}
	for _, doc := range input.sourceDeleteDocuments {
		set(doc.ID, nil)
	}
	if input.operation == ColumnPublishOperationDelete {
		for _, doc := range input.documents {
			set(doc.ID, nil)
		}
	}
	for _, row := range input.declaredRows {
		if row.Deleted {
			set(row.ID, nil)
			continue
		}
		if vectorColumn >= len(row.Values) {
			return nil, nil, fmt.Errorf("collections: replay partition-live row %q missing vector column %d", row.ID, vectorColumn)
		}
		value := row.Values[vectorColumn]
		if !value.Present || value.Null {
			set(row.ID, nil)
			continue
		}
		if value.Type != ColumnStoreValueFloat32Vector {
			return nil, nil, fmt.Errorf("collections: replay partition-live row %q vector column has type %q", row.ID, value.Type)
		}
		set(row.ID, value.Float32Vector)
	}
	return ids, vectors, nil
}

func (c *Collection) buildVectorPartitionLiveReplayAttemptV1(input columnWritePublishInput, specs []vectorPartitionLiveReplaySpecV1) (*vectorPartitionLiveReplayAttemptV1, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	state, ok := c.db.StateToken()
	if !ok {
		return nil, backenddb.ErrClosed
	}
	if state.CommitSeq == ^uint64(0) {
		return nil, errors.New("collections: document generation exhausted")
	}
	targetGeneration := state.CommitSeq + 1
	attempt := &vectorPartitionLiveReplayAttemptV1{entries: make([]vectorPartitionLiveReplayEntryV1, 0, len(specs))}
	for _, spec := range specs {
		candidate, err := newVectorIndex(c, vectorIndexOptionsFromDefinition(spec.definition))
		if err != nil {
			attempt.discard()
			return nil, err
		}
		if reason := candidate.loadPersistSnapshot(spec.snapshot); reason != "" {
			attempt.discard()
			return nil, fmt.Errorf("%w: replay carrier %q clone rejected: %s", ErrVectorIndexPartitionLiveUnavailableV1, spec.definition.Name, reason)
		}
		candidate.recordPersistentDefinition(spec.definition)
		candidate.recordLoadedSnapshot(spec.baseRoot, spec.before.Stats().BytesDisk)
		ids, vectors, err := vectorPartitionLiveReplayMutationsV1(input, spec.vectorColumn)
		if err != nil {
			attempt.discard()
			return nil, err
		}
		candidate.mu.Lock()
		if err = candidate.preflightVectorPartitionMutationBatchLocked(ids, vectors); err == nil {
			for i := range ids {
				if err = candidate.reconcileVectorPartitionMutationLocked(ids[i], vectors[i]); err != nil {
					break
				}
			}
		}
		if err == nil {
			candidate.recordSourceDocumentStateLocked(targetGeneration, backenddb.StateToken{})
		}
		candidate.mu.Unlock()
		if err != nil {
			attempt.discard()
			return nil, err
		}
		table, bytesDisk, snapshotSeq, persistedEpoch, hasWork, err := candidate.persistNativeDeltaTable(true)
		if err != nil {
			attempt.discard()
			return nil, err
		}
		if !hasWork || persistedEpoch != spec.baseRoot {
			resetCollectionRunTable(table)
			attempt.discard()
			return nil, fmt.Errorf("%w: replay carrier %q base root changed", ErrVectorIndexPartitionLiveUnavailableV1, spec.definition.Name)
		}
		table.Freeze()
		publish, pointerized, err := pointerizeCollectionRunTableValuesForRoot(c.db, input.meta, spec.rootName, table)
		if err != nil {
			resetCollectionRunTable(table)
			attempt.discard()
			return nil, err
		}
		attempt.entries = append(attempt.entries, vectorPartitionLiveReplayEntryV1{
			spec: spec, candidate: candidate, table: table, publish: publish,
			pointerized: pointerized, bytesDisk: bytesDisk, snapshotSeq: snapshotSeq,
		})
	}
	if err := c.validateVectorPartitionLiveReplaySpecsV1(specs); err != nil {
		attempt.discard()
		return nil, err
	}
	return attempt, nil
}

func (a *vectorPartitionLiveReplayAttemptV1) closeResources() {
	if a == nil {
		return
	}
	for i := range a.entries {
		if a.entries[i].iter != nil {
			_ = a.entries[i].iter.Close()
			a.entries[i].iter = nil
		}
		if a.entries[i].batch != nil {
			_ = a.entries[i].batch.Close()
			a.entries[i].batch = nil
		}
		if a.entries[i].pointerized {
			resetCollectionRunTable(a.entries[i].publish)
		}
		resetCollectionRunTable(a.entries[i].table)
	}
}

func (a *vectorPartitionLiveReplayAttemptV1) discard() {
	if a == nil {
		return
	}
	a.closeResources()
	a.entries = nil
}

func (c *Collection) installVectorPartitionLiveReplayAttemptV1(attempt *vectorPartitionLiveReplayAttemptV1, rootNames []string, rootIDs []uint64) error {
	if attempt == nil {
		return nil
	}
	for i := range attempt.entries {
		entry := &attempt.entries[i]
		ordinal := -1
		for j, rootName := range rootNames {
			if rootName == entry.spec.rootName {
				ordinal = j
				break
			}
		}
		if ordinal < 0 || ordinal >= len(rootIDs) {
			return fmt.Errorf("collections: replay carrier %q missing published root", entry.spec.definition.Name)
		}
		rootID := rootIDs[ordinal]
		entry.candidate.recordPersistedSnapshot(rootID, entry.bytesDisk, entry.snapshotSeq)
		installed, err := c.installNativeVectorIndexCandidate(entry.candidate, rootID, entry.spec.before, entry.spec.beforeSeq)
		if err != nil {
			return err
		}
		if installed != entry.candidate {
			return fmt.Errorf("%w: replay carrier %q changed during install", ErrConcurrentMutation, entry.spec.definition.Name)
		}
		installed.recordPersistedSnapshot(rootID, entry.bytesDisk, entry.snapshotSeq)
	}
	return nil
}
