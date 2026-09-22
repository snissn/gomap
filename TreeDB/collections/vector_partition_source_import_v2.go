package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/sourcepartition"
	"github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

type VectorPartitionSourceSnapshotV2 = vectorpartition.SourceSnapshotV2
type VectorPartitionSourceOrdinalOriginV2 = vectorpartition.SourceOrdinalOriginV2

// VectorPartitionSourceImportChunkV2 supplies one bounded source-shard range.
// Snapshot revision is immutable input identity; DocumentRevisions are distinct
// per-document revisions. The vector is read from the actual typed projection,
// after its ordinary canonical encoding, rather than from a second carrier.
type VectorPartitionSourceImportChunkV2 struct {
	Snapshot          VectorPartitionSourceSnapshotV2
	IndexName         string
	VectorColumn      string
	GroupID           string
	ChunkIndex        uint64
	DocumentRevisions []uint64
	LegacyOrigins     []*VectorPartitionSourceOrdinalOriginV2
}

type VectorPartitionSourceImportProgressV2 struct {
	ImportedChunks uint64
	Complete       bool
	Snapshot       VectorPartitionSourceSnapshotV2
}

type sourceImportBindingV2 struct {
	Snapshot     vectorpartition.SourceSnapshotV2
	IndexName    string
	VectorColumn string
	GroupID      string
	Start        uint64
	End          uint64
}

type sourceImportCommandMetadataV2 struct {
	Version           uint32
	OrderedIDs        [][]byte
	Binding           sourceImportBindingV2
	ChunkIndex        uint64
	DocumentRevisions []uint64
	LegacyOrigins     []*vectorpartition.SourceOrdinalOriginV2
}

type sourceImportBindingRecordV2 struct {
	Version uint32
	Binding sourceImportBindingV2
}

const maxSourceImportBindingBytesV2 = 3000

type sourceImportPublicationV2 struct {
	bindingKey      string
	binding         []byte
	progressKey     string
	previous        []byte
	previousPresent bool
	next            []byte
	receiptKey      string
	receipt         [sha256.Size]byte
}

// VectorPartitionSourceSchemaDigestV2 binds the normalized typed schema without
// physical manifest generations, which change during a bounded source import.
func VectorPartitionSourceSchemaDigestV2(cfg ColumnStoreConfig) ([sha256.Size]byte, error) {
	raw, err := json.Marshal(struct {
		SchemaHash uint64
		Columns    []ColumnStoreColumn
	}{cfg.SchemaHash, cfg.Columns})
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	h := sha256.New()
	_, _ = h.Write([]byte("gomap/source-import/schema/v2\x00"))
	_, _ = h.Write(raw)
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(digest[:0]))
	return digest, nil
}

// ImportVectorPartitionSourceChunkV2 atomically inserts source rows, immutable
// range receipt and resumable progress using one existing collection/WAL root
// publication. Exact retries do not reinsert rows; changed retries and existing
// IDs refuse. It does not replace/upsert existing rows or publish ANN authority.
//
// This is a local storage primitive. The Raft caller must admit the source map
// and bind GroupID to its local canonical group before invocation. A validated
// map or completed local snapshot is not a catalog/generation certificate.
func (c *Collection) ImportVectorPartitionSourceChunkV2(ownership sourcepartition.ResolvedSourceShardMapV2, input VectorPartitionSourceImportChunkV2, ids, retained [][]byte, columns []TypedColumnBatch) (VectorPartitionSourceImportProgressV2, error) {
	var zero VectorPartitionSourceImportProgressV2
	if c == nil || c.db == nil {
		return zero, errCollectionNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return zero, err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	if err := c.requireTypedBatchVectorAdmission(); err != nil {
		return zero, err
	}
	if !c.commandWALActive(nil) {
		return zero, backenddb.ErrCommandWALRejected
	}
	if len(ids) == 0 || len(ids) > vectorpartition.MaxSourceChunkRowsV2 || len(retained) != len(ids) || len(input.DocumentRevisions) != len(ids) || (len(input.LegacyOrigins) != 0 && len(input.LegacyOrigins) != len(ids)) {
		return zero, fmt.Errorf("collections: source import row bounds")
	}
	ref := ownership.Collection()
	if ref.Collection != c.Meta().Name || input.Snapshot.CollectionScope != ref.Database+"/"+ref.Catalog+"/"+ref.Collection || input.Snapshot.SourceMapEpoch != ownership.Epoch() || hex.EncodeToString(input.Snapshot.SourceMapDigest[:]) != ownership.Digest() {
		return zero, fmt.Errorf("collections: source import ownership identity")
	}
	if err := ownership.ValidateImportIDs(input.Snapshot.ShardID, ids); err != nil {
		return zero, err
	}
	owner, err := ownership.ResolveDocumentID(ids[0])
	if err != nil {
		return zero, err
	}
	if input.GroupID == "" || input.GroupID != owner.GroupID {
		return zero, fmt.Errorf("collections: source import nonowner group")
	}
	if err := sourceImportInputBoundsV2(ids, retained, columns); err != nil {
		return zero, err
	}
	projection, err := newTrustedTypedProjection(c.Meta(), ids, retained, columns)
	if err != nil {
		return zero, err
	}
	if err := c.requireColumnStoreCommandWAL(c.Meta(), nil); err != nil {
		return zero, err
	}
	if err := c.db.CheckCommandWALPublishReady(); err != nil {
		return zero, err
	}
	orderedIDs := make([][]byte, len(ids))
	for i, id := range ids {
		orderedIDs[i] = bytes.Clone(id)
	}
	metadata := sourceImportCommandMetadataV2{Version: 2, OrderedIDs: orderedIDs, Binding: sourceImportBindingV2{Snapshot: input.Snapshot, IndexName: input.IndexName, VectorColumn: input.VectorColumn, GroupID: input.GroupID, Start: owner.Start, End: owner.End}, ChunkIndex: input.ChunkIndex, DocumentRevisions: slices.Clone(input.DocumentRevisions), LegacyOrigins: input.LegacyOrigins}
	return c.importSourceChunkSchemaLockedV2(metadata, ids, retained, projection, nil, nil)
}

func sourceImportChunkFromProjectionV2(meta CollectionMeta, command sourceImportCommandMetadataV2, ids [][]byte, projection *trustedFloat32Projection) (vectorpartition.SourceChunkV2, error) {
	invalid := func(reason string) (vectorpartition.SourceChunkV2, error) {
		return vectorpartition.SourceChunkV2{}, fmt.Errorf("collections: invalid source import: %s", reason)
	}
	s := command.Binding.Snapshot
	if command.Version != 2 || projection == nil || projection.typedRows == nil || meta.Options.ColumnStore == nil || command.Binding.GroupID == "" || command.Binding.Start > command.Binding.End || len(ids) == 0 || len(ids) > vectorpartition.MaxSourceChunkRowsV2 || len(command.OrderedIDs) != len(ids) || len(command.DocumentRevisions) != len(ids) || (len(command.LegacyOrigins) != 0 && len(command.LegacyOrigins) != len(ids)) {
		return invalid("binding or row count")
	}
	schema, err := VectorPartitionSourceSchemaDigestV2(*meta.Options.ColumnStore)
	if err != nil || s.SchemaDigest != schema {
		return invalid("schema identity")
	}
	var def *VectorIndexDefinition
	for i := range meta.VectorIndexes {
		if meta.VectorIndexes[i].Name == command.Binding.IndexName {
			def = &meta.VectorIndexes[i]
			break
		}
	}
	if def == nil || hex.EncodeToString(s.IndexDefinitionDigest[:]) != VectorIndexDefinitionDigestV1(*def) || uint64(def.Dimensions) != uint64(s.Dimensions) {
		return invalid("index identity")
	}
	column := -1
	for i, col := range projection.columns {
		if col.Name == command.Binding.VectorColumn && col.Path == def.Field && col.VectorDims == def.Dimensions {
			column = i
			break
		}
	}
	if column < 0 {
		return invalid("source vector column")
	}
	if s.Digest != ([sha256.Size]byte{}) || s.MerkleRoot != ([sha256.Size]byte{}) {
		sealed, err := vectorpartition.SealSourceSnapshotV2(s, s.MerkleRoot)
		if err != nil || sealed.Digest != s.Digest {
			return invalid("expected immutable snapshot digest")
		}
	}
	if command.ChunkIndex >= s.ChunkCount() {
		return invalid("chunk index")
	}
	chunk := vectorpartition.SourceChunkV2{Index: command.ChunkIndex, Rows: make([]vectorpartition.SourceRowV2, len(ids))}
	start := command.ChunkIndex * uint64(s.RowsPerChunk)
	for i, id := range ids {
		if !bytes.Equal(id, command.OrderedIDs[i]) {
			return invalid("source ordinal order")
		}
		token, err := sourcepartition.DocumentIDTokenV2(id)
		if err != nil || token < command.Binding.Start || token > command.Binding.End {
			return invalid("nonowner document ID")
		}
		values, ok := projection.typedRows[string(id)]
		if !ok || column >= len(values) {
			return invalid("missing typed source row")
		}
		row := vectorpartition.SourceRowV2{LocalOrdinal: start + uint64(i), DocumentID: id, DocumentRevision: command.DocumentRevisions[i], Values: values[column].Float32Vector}
		if len(command.LegacyOrigins) != 0 {
			row.LegacyOrigin = command.LegacyOrigins[i]
		}
		chunk.Rows[i] = row
	}
	if _, err := vectorpartition.SourceChunkDigestV2(s, chunk); err != nil {
		return vectorpartition.SourceChunkV2{}, err
	}
	return chunk, nil
}

func sourceImportKeysV2(collection string, binding sourceImportBindingV2, index uint64) (string, string) {
	h := sha256.New()
	_, _ = h.Write([]byte("gomap/source-import/key/v2\x00"))
	for _, value := range []string{binding.Snapshot.CollectionScope, binding.Snapshot.ShardID, binding.IndexName} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(value))
	}
	var revision [8]byte
	binary.BigEndian.PutUint64(revision[:], binding.Snapshot.SnapshotRevision)
	_, _ = h.Write(revision[:])
	key := "source_import_v2:" + collection + ":" + hex.EncodeToString(h.Sum(nil))
	return key + ":progress", fmt.Sprintf("%s:range:%016x", key, index)
}

func sourceImportReceiptV2(payload []byte) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("gomap/source-import/range/v2\x00"))
	_, _ = h.Write(payload)
	var out [sha256.Size]byte
	copy(out[:], h.Sum(out[:0]))
	return out
}

func decodeSourceImportJSONV2(raw []byte, out any) error {
	if len(raw) == 0 || len(raw) > commitlog.MaxSourceImportMetadataBytesV2 {
		return errors.New("collections: source import metadata bytes cap")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	canonical, err := json.Marshal(out)
	if err != nil {
		return err
	}
	if !bytes.Equal(raw, canonical) {
		return errors.New("collections: noncanonical source import metadata")
	}
	return nil
}

func (c *Collection) importSourceChunkSchemaLockedV2(command sourceImportCommandMetadataV2, ids, retained [][]byte, projection *trustedFloat32Projection, replay *backenddb.CommandWALIntent, hooks *sourcePublicationHooks) (VectorPartitionSourceImportProgressV2, error) {
	var zero VectorPartitionSourceImportProgressV2
	chunk, err := sourceImportChunkFromProjectionV2(c.Meta(), command, ids, projection)
	if err != nil {
		return zero, err
	}
	docs, err := collectionDocumentsFromBatchInput(ids, retained)
	if err != nil {
		return zero, err
	}
	typed, err := typedCommandPayload(c.Meta(), docs, projection)
	if err != nil {
		return zero, err
	}
	metadata, err := json.Marshal(command)
	if err != nil {
		return zero, err
	}
	payload, err := commitlog.EncodeCollectionSourceImportPayloadV2(commitlog.CollectionSourceImportPayloadV2{Metadata: metadata, Inserted: typed})
	if err != nil {
		return zero, err
	}
	binding, err := json.Marshal(sourceImportBindingRecordV2{Version: 2, Binding: command.Binding})
	if err != nil {
		return zero, err
	}
	if len(binding) > maxSourceImportBindingBytesV2 {
		return zero, errors.New("collections: source import binding bytes cap")
	}
	receipt := sourceImportReceiptV2(payload)
	progressKey, receiptKey := sourceImportKeysV2(c.Meta().Name, command.Binding, command.ChunkIndex)
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if err := c.flushBufferedWritesWithVectorAdmissionLocked(); err != nil {
		return zero, err
	}
	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		snap := c.db.AcquireSnapshot()
		if snap == nil {
			return zero, backenddb.ErrClosed
		}
		previous, present, readErr := getSystemValue(snap, progressKey)
		existingReceipt, receiptPresent, receiptErr := getSystemValue(snap, receiptKey)
		storedBinding, bindingPresent, bindingErr := getSystemValue(snap, progressKey+":binding")
		_ = snap.Close()
		if readErr != nil || receiptErr != nil || bindingErr != nil {
			return zero, errors.Join(readErr, receiptErr, bindingErr)
		}
		if bindingPresent != present || (present && !bytes.Equal(storedBinding, binding)) {
			return zero, errors.New("collections: changed immutable source import identity or partial progress")
		}
		acc, err := vectorpartition.NewSourceSnapshotAccumulatorV2(command.Binding.Snapshot)
		if err != nil {
			return zero, err
		}
		if present {
			acc, err = vectorpartition.RestoreSourceSnapshotAccumulatorV2(command.Binding.Snapshot, previous)
			if err != nil {
				return zero, err
			}
		}
		if receiptPresent {
			if !present || !bytes.Equal(existingReceipt, receipt[:]) || command.ChunkIndex >= acc.ImportedChunks() {
				return zero, errors.New("collections: changed or inconsistent source import retry")
			}
			return sourceImportPublicProgressV2(command.Binding.Snapshot, acc)
		}
		if command.ChunkIndex != acc.ImportedChunks() {
			return zero, errors.New("collections: missing, replayed or reordered source import range")
		}
		if err := acc.Append(chunk); err != nil {
			return zero, err
		}
		checkpoint, err := acc.MarshalCheckpoint()
		if err != nil {
			return zero, err
		}
		result, err := sourceImportPublicProgressV2(command.Binding.Snapshot, acc)
		if err != nil {
			return zero, err
		}
		plan, err := c.buildSourceReplacementPlan(nil, ids, retained, nil, replay, hooks, projection, false)
		if err != nil {
			if isRetriableCollectionMutationError(err) {
				lastErr = err
				waitBeforeCollectionMutationRetry(attempt)
				continue
			}
			return zero, err
		}
		plan.sourceImportV2 = &sourceImportPublicationV2{bindingKey: progressKey + ":binding", binding: binding, progressKey: progressKey, previous: previous, previousPresent: present, next: checkpoint, receiptKey: receiptKey, receipt: receipt}
		if replay != nil {
			plan.commandWAL = replay
		} else {
			plan.commandWAL, err = c.db.NewTrustedCommandWALIntent(commitlog.CommandKindCollectionReplaceSourceByID, commitlog.CommandScopeCollection, commitlog.PayloadFormatCollectionSourceImportV2, payload)
		}
		if err != nil {
			plan.close()
			return zero, err
		}
		publishErr := c.publishSourceReplacementPlan(plan, hooks, nil)
		plan.close()
		if isRetriableCollectionMutationError(publishErr) {
			lastErr = publishErr
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		accepted := publishErr == nil || backenddb.CommitPublicationAccepted(publishErr) || errors.Is(publishErr, ErrCommitAmbiguous)
		if !accepted {
			return zero, publishErr
		}
		var notifyErr error
		if _, replaying := replay.ReplayAssignedLSN(); replaying {
			notifyErr = c.reconcileVectorPartitionLiveReplay(ids)
		} else {
			notifyErr = c.reconcileVectorIndexes(ids)
		}
		if notifyErr != nil {
			c.invalidateRegisteredVectorIndexDocumentCoverage()
			notifyErr = commitAmbiguousError("source import vector maintenance", notifyErr)
		}
		if err := c.invalidateVectorIndexCoverageOnAcceptedMutation(errors.Join(publishErr, notifyErr)); err != nil {
			return zero, err
		}
		return result, nil
	}
	return zero, collectionMutationRetryExhausted(lastErr)
}

func sourceImportPublicProgressV2(snapshot vectorpartition.SourceSnapshotV2, acc vectorpartition.SourceSnapshotAccumulatorV2) (VectorPartitionSourceImportProgressV2, error) {
	out := VectorPartitionSourceImportProgressV2{ImportedChunks: acc.ImportedChunks()}
	if acc.ImportedChunks() == snapshot.ChunkCount() {
		sealed, err := acc.Finish()
		if err != nil {
			return VectorPartitionSourceImportProgressV2{}, err
		}
		out.Complete, out.Snapshot = true, sealed
	}
	return out, nil
}

func (c *Collection) buildSourceImportRootSystemDeltaV2(input columnWritePublishInput, rootIDs []uint64) (iterator.UnsafeIterator, error) {
	it, err := c.buildRootDescriptorSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, input.rootNames, input.baseRootIDs, rootIDs)
	if err != nil {
		return nil, err
	}
	return c.appendSourceImportSystemDeltaV2(it, input.sourceImportV2)
}

func (c *Collection) appendSourceImportSystemDeltaV2(it iterator.UnsafeIterator, publication *sourceImportPublicationV2) (iterator.UnsafeIterator, error) {
	if publication == nil {
		return it, nil
	}
	fail := func(err error) (iterator.UnsafeIterator, error) {
		if it != nil {
			_ = it.Close()
		}
		return nil, err
	}
	base, ok := it.(*systemTargetIterator)
	if !ok || base.idx != 0 {
		return fail(errors.New("collections: source import requires untouched bounded system delta"))
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return fail(backenddb.ErrClosed)
	}
	current, present, err := getSystemValue(snap, publication.progressKey)
	_, receiptPresent, receiptErr := getSystemValue(snap, publication.receiptKey)
	currentBinding, bindingPresent, bindingErr := getSystemValue(snap, publication.bindingKey)
	_ = snap.Close()
	if err != nil || receiptErr != nil || bindingErr != nil {
		return fail(errors.Join(err, receiptErr, bindingErr))
	}
	if present != publication.previousPresent || !bytes.Equal(current, publication.previous) || receiptPresent || bindingPresent != present || (bindingPresent && !bytes.Equal(currentBinding, publication.binding)) {
		return fail(errConcurrentRootModification(c.meta.Name, "source_import_v2"))
	}
	base.entries = append(base.entries, systemTargetEntry{key: []byte(publication.progressKey), value: bytes.Clone(publication.next)}, systemTargetEntry{key: []byte(publication.receiptKey), value: bytes.Clone(publication.receipt[:])})
	if !bindingPresent {
		base.entries = append(base.entries, systemTargetEntry{key: []byte(publication.bindingKey), value: bytes.Clone(publication.binding)})
	}
	sort.Slice(base.entries, func(i, j int) bool { return bytes.Compare(base.entries[i].key, base.entries[j].key) < 0 })
	for i := 1; i < len(base.entries); i++ {
		if bytes.Equal(base.entries[i-1].key, base.entries[i].key) {
			return fail(errors.New("collections: source import system key collision"))
		}
	}
	return base, nil
}

func replayCollectionSourceImportV2(db *backenddb.DB, env commitlog.CommandEnvelope) error {
	payload, err := commitlog.DecodeCollectionSourceImportPayloadV2(env.Payload)
	if err != nil {
		return err
	}
	var command sourceImportCommandMetadataV2
	if err := decodeSourceImportJSONV2(payload.Metadata, &command); err != nil {
		return err
	}
	intent, err := db.NewCommandWALReplayIntent(env)
	if err != nil {
		return err
	}
	collection, err := newCommandWALReplayCollectionManager(db).openCollectionWithCommandWALIntent(payload.Inserted.Collection, intent)
	if err != nil {
		return err
	}
	projection, ids, retained, err := typedProjectionFromPayload(collection.meta, payload.Inserted)
	if err != nil {
		return err
	}
	if len(command.OrderedIDs) != len(ids) || len(ids) > vectorpartition.MaxSourceChunkRowsV2 {
		return errors.New("collections: source import replay row order bound")
	}
	positions := make(map[string]int, len(ids))
	for i, id := range ids {
		positions[string(id)] = i
	}
	orderedRetained := make([][]byte, len(ids))
	seen := make([]bool, len(ids))
	for i, id := range command.OrderedIDs {
		position, ok := positions[string(id)]
		if !ok || seen[position] {
			return errors.New("collections: source import replay ID order mismatch")
		}
		seen[position] = true
		orderedRetained[i] = retained[position]
	}
	ids, retained = command.OrderedIDs, orderedRetained
	if projection.retainedJSON != nil {
		projection.retainedJSON = retained
	}
	unlockSchema := collection.lockCollectionSchemaRead()
	defer unlockSchema()
	unlockCoverage := collection.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	_, err = collection.importSourceChunkSchemaLockedV2(command, ids, retained, projection, intent, nil)
	return err
}

func sourceImportInputBoundsV2(ids, retained [][]byte, columns []TypedColumnBatch) error {
	remaining := sourcepartition.MaxSourceImportBytesV2
	take := func(n int) bool {
		if n < 0 || n > remaining {
			return false
		}
		remaining -= n
		return true
	}
	for _, id := range ids {
		if !take(len(id)) {
			return errors.New("collections: source import input bytes cap")
		}
	}
	for _, row := range retained {
		if !take(len(row)) {
			return errors.New("collections: source import input bytes cap")
		}
	}
	for _, column := range columns {
		if !take(len(column.Name)) {
			return errors.New("collections: source import input bytes cap")
		}
		for _, value := range column.Strings {
			if !take(len(value)) {
				return errors.New("collections: source import input bytes cap")
			}
		}
		for _, values := range column.Float32Vectors {
			if len(values) > remaining/4 || !take(len(values)*4) {
				return errors.New("collections: source import input bytes cap")
			}
		}
	}
	return nil
}
