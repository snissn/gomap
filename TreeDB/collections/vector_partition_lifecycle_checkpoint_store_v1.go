package collections

// This file owns the immutable VCP1 checkpoint plus VLC1 delta-tail container
// used by every public vector-partition lifecycle path.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const vectorPartitionLifecycleCheckpointNameWidthV1 = 20

type vectorPartitionLifecycleCheckpointEntryKindV1 uint8

const (
	vectorPartitionLifecycleCheckpointFileV1 vectorPartitionLifecycleCheckpointEntryKindV1 = iota + 1
	vectorPartitionLifecycleDeltaFileV1
)

type vectorPartitionLifecycleCheckpointEntryV1 struct {
	name     string
	kind     vectorPartitionLifecycleCheckpointEntryKindV1
	epoch    uint64
	sequence uint64
	bytes    uint64
	identity rootpublication.StableIdentity
}

type vectorPartitionLifecycleCheckpointStoreStateV1 struct {
	checkpoint    vectorPartitionLifecycleCheckpointV1
	deltas        []vectorPartitionLifecycleRecordV1
	state         vectorPartitionLifecycleStateV1
	entries       []vectorPartitionLifecycleCheckpointEntryV1
	physicalBytes uint64
	tailBytes     uint64
}

func vectorPartitionLifecycleCheckpointNameV1(collection, index string, epoch uint64) (string, error) {
	if collection == "" || index == "" || epoch == 0 {
		return "", fmt.Errorf("%w: lifecycle checkpoint name identity", ErrVectorPartitionManifestInvalid)
	}
	return fmt.Sprintf("%scheckpoint.%020d.vlc", vectorPartitionLifecycleNamePrefixV1(collection, index), epoch), nil
}

func vectorPartitionLifecycleDeltaNameV1(collection, index string, epoch, sequence uint64) (string, error) {
	if collection == "" || index == "" || epoch == 0 || sequence == 0 {
		return "", fmt.Errorf("%w: lifecycle delta name identity", ErrVectorPartitionManifestInvalid)
	}
	return fmt.Sprintf("%sepoch.%020d.delta.%020d.vlc", vectorPartitionLifecycleNamePrefixV1(collection, index), epoch, sequence), nil
}

func parseFixedVectorPartitionLifecycleNumberV1(raw string) (uint64, error) {
	if len(raw) != vectorPartitionLifecycleCheckpointNameWidthV1 {
		return 0, fmt.Errorf("%w: lifecycle checkpoint numeric width", ErrVectorPartitionManifestInvalid)
	}
	for _, digit := range raw {
		if digit < '0' || digit > '9' {
			return 0, fmt.Errorf("%w: lifecycle checkpoint numeric field", ErrVectorPartitionManifestInvalid)
		}
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value == 0 {
		return 0, fmt.Errorf("%w: lifecycle checkpoint numeric value", ErrVectorPartitionManifestInvalid)
	}
	return value, nil
}

func parseVectorPartitionLifecycleCheckpointEntryNameV1(collection, index, name string) (vectorPartitionLifecycleCheckpointEntryV1, error) {
	var zero vectorPartitionLifecycleCheckpointEntryV1
	prefix := vectorPartitionLifecycleNamePrefixV1(collection, index)
	if !strings.HasPrefix(name, prefix) {
		return zero, fmt.Errorf("%w: lifecycle checkpoint name prefix", ErrVectorPartitionManifestInvalid)
	}
	suffix := strings.TrimPrefix(name, prefix)
	if strings.HasPrefix(suffix, "checkpoint.") && strings.HasSuffix(suffix, ".vlc") {
		digits := strings.TrimSuffix(strings.TrimPrefix(suffix, "checkpoint."), ".vlc")
		epoch, err := parseFixedVectorPartitionLifecycleNumberV1(digits)
		if err != nil {
			return zero, err
		}
		canonical, err := vectorPartitionLifecycleCheckpointNameV1(collection, index, epoch)
		if err != nil || canonical != name {
			return zero, fmt.Errorf("%w: noncanonical lifecycle checkpoint name", ErrVectorPartitionManifestInvalid)
		}
		return vectorPartitionLifecycleCheckpointEntryV1{name: name, kind: vectorPartitionLifecycleCheckpointFileV1, epoch: epoch}, nil
	}
	if strings.HasPrefix(suffix, "epoch.") && strings.HasSuffix(suffix, ".vlc") {
		body := strings.TrimSuffix(strings.TrimPrefix(suffix, "epoch."), ".vlc")
		parts := strings.Split(body, ".delta.")
		if len(parts) != 2 {
			return zero, fmt.Errorf("%w: malformed lifecycle delta name", ErrVectorPartitionManifestInvalid)
		}
		epoch, err := parseFixedVectorPartitionLifecycleNumberV1(parts[0])
		if err != nil {
			return zero, err
		}
		sequence, err := parseFixedVectorPartitionLifecycleNumberV1(parts[1])
		if err != nil {
			return zero, err
		}
		canonical, err := vectorPartitionLifecycleDeltaNameV1(collection, index, epoch, sequence)
		if err != nil || canonical != name {
			return zero, fmt.Errorf("%w: noncanonical lifecycle delta name", ErrVectorPartitionManifestInvalid)
		}
		return vectorPartitionLifecycleCheckpointEntryV1{name: name, kind: vectorPartitionLifecycleDeltaFileV1, epoch: epoch, sequence: sequence}, nil
	}
	return zero, fmt.Errorf("%w: malformed lifecycle checkpoint entry %q", ErrVectorPartitionManifestInvalid, name)
}

func inspectVectorPartitionLifecycleCheckpointEntryV1(dir *os.File, name string, flags int) (*os.File, rootpublication.StableIdentity, uint64, error) {
	file, err := rootpublication.OpenStableChildFile(dir, name, flags, 0)
	if err != nil {
		return nil, rootpublication.StableIdentity{}, 0, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 0 {
		_ = file.Close()
		if err != nil {
			return nil, rootpublication.StableIdentity{}, 0, err
		}
		return nil, rootpublication.StableIdentity{}, 0, fmt.Errorf("%w: lifecycle checkpoint entry %q is not a regular file", ErrVectorPartitionManifestInvalid, name)
	}
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		_ = file.Close()
		return nil, rootpublication.StableIdentity{}, 0, err
	}
	if err := rootpublication.ValidateStableChildLink(dir, file, name); err != nil {
		_ = file.Close()
		return nil, rootpublication.StableIdentity{}, 0, err
	}
	return file, identity, uint64(info.Size()), nil
}

func (s *VectorPartitionStoreV1) loadVectorPartitionLifecycleCheckpointStateFromDirV1(dir *os.File, collection, index string) (vectorPartitionLifecycleCheckpointStoreStateV1, error) {
	return s.loadVectorPartitionLifecycleCheckpointStateFromDirWithContextV1(context.Background(), dir, collection, index)
}

func (s *VectorPartitionStoreV1) loadVectorPartitionLifecycleCheckpointStateFromDirWithContextV1(ctx context.Context, dir *os.File, collection, index string) (vectorPartitionLifecycleCheckpointStoreStateV1, error) {
	var loaded vectorPartitionLifecycleCheckpointStoreStateV1
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return loaded, err
	}
	if err := s.verifyBoundDirV1(dir); err != nil {
		return loaded, err
	}
	entries, err := readVectorPartitionDirEntriesBoundedV1(dir)
	if err != nil {
		return loaded, err
	}
	prefix := vectorPartitionLifecycleNamePrefixV1(collection, index)
	var highestEpoch uint64
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return loaded, err
		}
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		parsed, err := parseVectorPartitionLifecycleCheckpointEntryNameV1(collection, index, entry.Name())
		if err != nil {
			return loaded, err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return loaded, fmt.Errorf("%w: lifecycle checkpoint entry %q is a symlink", ErrVectorPartitionManifestInvalid, entry.Name())
		}
		file, identity, exactBytes, err := inspectVectorPartitionLifecycleCheckpointEntryV1(dir, entry.Name(), os.O_RDONLY)
		if err != nil {
			return loaded, err
		}
		closeErr := file.Close()
		if closeErr != nil {
			return loaded, closeErr
		}
		for _, existing := range loaded.entries {
			if rootpublication.SamePhysicalIdentity(existing.identity, identity) {
				return loaded, fmt.Errorf("%w: lifecycle checkpoint entries %q and %q alias one physical file", ErrVectorPartitionManifestInvalid, existing.name, entry.Name())
			}
		}
		if exactBytes > uint64(vectorPartitionStoreMaxBytesV1)-loaded.physicalBytes {
			return loaded, fmt.Errorf("%w: lifecycle checkpoint physical bytes cap", ErrVectorPartitionManifestInvalid)
		}
		parsed.bytes = exactBytes
		parsed.identity = identity
		loaded.physicalBytes += parsed.bytes
		loaded.entries = append(loaded.entries, parsed)
		if parsed.kind == vectorPartitionLifecycleCheckpointFileV1 && parsed.epoch > highestEpoch {
			highestEpoch = parsed.epoch
		}
	}
	if len(loaded.entries) == 0 {
		loaded.state = vectorPartitionLifecycleStateV1{
			Collection:  collection,
			IndexName:   index,
			Generations: make(map[uint64]vectorPartitionLifecycleGenerationStateV1),
		}
		if err := s.verifyBoundDirV1(dir); err != nil {
			return vectorPartitionLifecycleCheckpointStoreStateV1{}, err
		}
		return loaded, nil
	}
	if highestEpoch == 0 {
		return loaded, fmt.Errorf("%w: lifecycle deltas without checkpoint", ErrVectorPartitionManifestInvalid)
	}
	sort.Slice(loaded.entries, func(i, j int) bool {
		if loaded.entries[i].epoch != loaded.entries[j].epoch {
			return loaded.entries[i].epoch < loaded.entries[j].epoch
		}
		if loaded.entries[i].kind != loaded.entries[j].kind {
			return loaded.entries[i].kind < loaded.entries[j].kind
		}
		return loaded.entries[i].sequence < loaded.entries[j].sequence
	})

	var checkpointEntry *vectorPartitionLifecycleCheckpointEntryV1
	currentDeltas := make([]vectorPartitionLifecycleCheckpointEntryV1, 0)
	for i := range loaded.entries {
		entry := loaded.entries[i]
		if entry.epoch > highestEpoch {
			return loaded, fmt.Errorf("%w: lifecycle entry above highest checkpoint", ErrVectorPartitionManifestInvalid)
		}
		if entry.epoch != highestEpoch {
			continue
		}
		switch entry.kind {
		case vectorPartitionLifecycleCheckpointFileV1:
			if checkpointEntry != nil {
				return loaded, fmt.Errorf("%w: duplicate lifecycle checkpoint epoch", ErrVectorPartitionManifestInvalid)
			}
			checkpointEntry = &loaded.entries[i]
		case vectorPartitionLifecycleDeltaFileV1:
			currentDeltas = append(currentDeltas, entry)
		}
	}
	if checkpointEntry == nil || checkpointEntry.bytes == 0 {
		return loaded, fmt.Errorf("%w: highest lifecycle checkpoint is empty", ErrVectorPartitionManifestInvalid)
	}
	checkpointRaw, err := readVectorPartitionLifecycleSlotWithContextV1(ctx, dir, checkpointEntry.name, vectorPartitionLifecycleCheckpointMaxBytesV1)
	if err != nil {
		return loaded, err
	}
	loaded.checkpoint, err = decodeVectorPartitionLifecycleCheckpointCanonicalWithContextV1(ctx, checkpointRaw, collection, index, highestEpoch)
	if err != nil {
		return loaded, err
	}

	sort.Slice(currentDeltas, func(i, j int) bool { return currentDeltas[i].sequence < currentDeltas[j].sequence })
	loaded.deltas = make([]vectorPartitionLifecycleRecordV1, 0, len(currentDeltas))
	nextSequence := loaded.checkpoint.State.LastSequence
	for _, entry := range currentDeltas {
		if err := ctx.Err(); err != nil {
			return loaded, err
		}
		if nextSequence == math.MaxUint64 || entry.sequence != nextSequence+1 || entry.bytes == 0 {
			return loaded, fmt.Errorf("%w: lifecycle checkpoint delta gap or empty slot", ErrVectorPartitionManifestInvalid)
		}
		remaining := vectorPartitionLifecycleCheckpointTailMaxBytesV1 - int(loaded.tailBytes)
		raw, err := readVectorPartitionLifecycleSlotWithContextV1(ctx, dir, entry.name, remaining)
		if err != nil {
			return loaded, err
		}
		if uint64(len(raw)) > uint64(vectorPartitionLifecycleCheckpointTailMaxBytesV1)-loaded.tailBytes {
			return loaded, fmt.Errorf("%w: lifecycle checkpoint tail bytes cap", ErrVectorPartitionManifestInvalid)
		}
		record, err := decodeVectorPartitionLifecycleRecordCanonicalV1(raw)
		if err != nil || record.Sequence != entry.sequence || record.Collection != collection || record.IndexName != index {
			return loaded, fmt.Errorf("%w: lifecycle checkpoint delta identity", ErrVectorPartitionManifestInvalid)
		}
		loaded.tailBytes += uint64(len(raw))
		loaded.deltas = append(loaded.deltas, record)
		nextSequence = entry.sequence
	}
	loaded.state, err = reduceVectorPartitionLifecycleCheckpointTailV1(loaded.checkpoint, loaded.deltas)
	if err != nil {
		return loaded, err
	}
	if err := ctx.Err(); err != nil {
		return loaded, err
	}
	if err := s.verifyBoundDirV1(dir); err != nil {
		return vectorPartitionLifecycleCheckpointStoreStateV1{}, err
	}
	return loaded, nil
}

func (s *VectorPartitionStoreV1) loadVectorPartitionLifecycleCheckpointStateV1(collection, index string) (vectorPartitionLifecycleCheckpointStoreStateV1, error) {
	dir, err := s.openDir()
	if err != nil {
		return vectorPartitionLifecycleCheckpointStoreStateV1{}, err
	}
	defer dir.Close()
	return s.loadVectorPartitionLifecycleCheckpointStateFromDirV1(dir, collection, index)
}

func vectorPartitionLifecycleOperationAlreadyAppliedV1(state vectorPartitionLifecycleStateV1, operation vectorPartitionLifecycleOperationV1, generation uint64, payload []byte) bool {
	entry, present := state.Generations[generation]
	switch operation {
	case vectorPartitionLifecycleBuildV1:
		if !present || entry.Manifest == nil || entry.Manifest.State != "building" || entry.Deleting {
			return false
		}
		manifest, err := DecodeVectorPartitionManifestV1(payload, DefaultVectorPartitionManifestLimits())
		if err != nil {
			return false
		}
		want, err := EncodeVectorPartitionManifestV1(cloneVectorPartitionManifestForCheckpointV1(manifest))
		if err != nil {
			return false
		}
		got, err := EncodeVectorPartitionManifestV1(cloneVectorPartitionManifestForCheckpointV1(*entry.Manifest))
		return err == nil && bytes.Equal(got, want)
	case vectorPartitionLifecycleReadyV1:
		if !present || entry.Manifest == nil || entry.Manifest.State != "ready" || entry.Deleting {
			return false
		}
		promotion, err := decodeVectorPartitionReadyPromotionCanonicalV1(payload)
		if err != nil || promotion.Generation != generation {
			return false
		}
		readyRaw, err := EncodeVectorPartitionManifestV1(cloneVectorPartitionManifestForCheckpointV1(*entry.Manifest))
		if err != nil || promotion.ReadyDigest != sha256SumV1(readyRaw) {
			return false
		}
		building := cloneVectorPartitionManifestForCheckpointV1(*entry.Manifest)
		building.State = "building"
		building.RouterGeneration = 0
		building.RouterAsset = VectorPartitionAssetV1{}
		building.ReadySetDigest = ""
		building.Canonicalize()
		buildingRaw, err := EncodeVectorPartitionManifestV1(building)
		return err == nil && promotion.BuildingDigest == sha256SumV1(buildingRaw)
	case vectorPartitionLifecycleLocalActivateV1:
		return state.ActiveGeneration == generation && present && entry.Manifest != nil && entry.Manifest.State == "ready" && !entry.Deleting
	case vectorPartitionLifecycleDeactivateV1:
		return state.ActiveGeneration == 0 && state.RetiredGeneration == generation
	case vectorPartitionLifecycleDeletePrepareV1:
		if !present || !entry.Deleting || entry.Reclaim == nil {
			return false
		}
		reclaim, err := decodeVectorPartitionReclaimRecordV1(payload)
		if err != nil ||
			reclaim.Collection != state.Collection ||
			reclaim.IndexName != state.IndexName ||
			reclaim.Generation != generation {
			return false
		}
		return len(reclaim.SupersededRefs) == 0 &&
			vectorPartitionLifecycleRefsEqualV1(entry.Reclaim.OriginalRefs, reclaim.OriginalRefs)
	case vectorPartitionLifecycleReclaimProgressV1:
		if !present || !entry.Deleting || entry.Reclaim == nil {
			return false
		}
		reclaim, err := decodeVectorPartitionReclaimRecordV1(payload)
		if err != nil ||
			reclaim.Collection != state.Collection ||
			reclaim.IndexName != state.IndexName ||
			reclaim.Generation != generation {
			return false
		}
		return vectorPartitionLifecycleRefsEqualV1(entry.Reclaim.OriginalRefs, reclaim.OriginalRefs) &&
			vectorPartitionLifecycleRefsSupersetV1(entry.Reclaim.SupersededRefs, reclaim.SupersededRefs)
	case vectorPartitionLifecycleDeleteCompleteV1:
		return !present && generation != 0 && generation <= state.GenerationHighWater
	default:
		return false
	}
}

func sha256SumV1(raw []byte) [32]byte {
	return sha256.Sum256(raw)
}

func (s *VectorPartitionStoreV1) installVectorPartitionLifecycleImmutableV1(dir *os.File, name string, raw []byte, boundary string) error {
	anonymous, err := rootpublication.OpenStableAnonymousFile(dir, 0o600)
	if err != nil {
		return err
	}
	defer anonymous.Close()
	if n, writeErr := anonymous.Write(raw); writeErr != nil || n != len(raw) {
		if writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		return writeErr
	}
	if err := rootpublication.SyncStableFile(anonymous); err != nil {
		return err
	}
	if err := s.verifyBoundDirV1(dir); err != nil {
		return err
	}
	if err := vectorPartitionLifecycleStoreFaultV1("before_" + boundary + "_install"); err != nil {
		return err
	}
	installed, installErr := rootpublication.InstallStableFileHandleNoReplace(anonymous, dir, name)
	if installErr != nil && installed {
		return installErr
	}
	if !installed {
		existing, readErr := readVectorPartitionLifecycleSlotV1(dir, name, len(raw))
		if readErr != nil {
			return errors.Join(installErr, readErr)
		}
		if !bytes.Equal(existing, raw) {
			return fmt.Errorf("%w: lifecycle immutable entry %q contains different bytes", ErrVectorPartitionManifestInvalid, name)
		}
	}
	if err := vectorPartitionLifecycleStoreFaultV1("after_" + boundary + "_install"); err != nil {
		return err
	}
	if err := rootpublication.SyncStableNamespace(dir); err != nil {
		return err
	}
	return s.verifyBoundDirV1(dir)
}

func (s *VectorPartitionStoreV1) truncateVectorPartitionLifecycleAuditEntriesV1(dir *os.File, entries []vectorPartitionLifecycleCheckpointEntryV1, currentEpoch uint64) error {
	current := make([]vectorPartitionLifecycleCheckpointEntryV1, 0)
	for _, entry := range entries {
		if entry.epoch == currentEpoch {
			current = append(current, entry)
		}
	}
	if len(current) == 0 {
		return fmt.Errorf("%w: lifecycle audit missing current authority", ErrVectorPartitionManifestInvalid)
	}
	for _, entry := range entries {
		if entry.epoch >= currentEpoch || entry.bytes == 0 {
			continue
		}
		if err := vectorPartitionLifecycleStoreFaultV1("before_audit_truncate"); err != nil {
			return err
		}
		for _, authoritative := range current {
			file, identity, exactBytes, err := inspectVectorPartitionLifecycleCheckpointEntryV1(dir, authoritative.name, os.O_RDONLY)
			if err != nil {
				return err
			}
			closeErr := file.Close()
			if closeErr != nil {
				return closeErr
			}
			if !rootpublication.SamePhysicalIdentity(identity, authoritative.identity) || exactBytes != authoritative.bytes {
				return fmt.Errorf("%w: current lifecycle authority %q changed before audit", ErrVectorPartitionManifestInvalid, authoritative.name)
			}
		}
		file, identity, exactBytes, err := inspectVectorPartitionLifecycleCheckpointEntryV1(dir, entry.name, os.O_RDWR)
		if err != nil {
			return err
		}
		if !rootpublication.SamePhysicalIdentity(identity, entry.identity) || exactBytes != entry.bytes {
			_ = file.Close()
			return fmt.Errorf("%w: lifecycle audit entry %q changed before truncation", ErrVectorPartitionManifestInvalid, entry.name)
		}
		for _, authoritative := range current {
			if rootpublication.SamePhysicalIdentity(identity, authoritative.identity) {
				_ = file.Close()
				return fmt.Errorf("%w: lifecycle audit entry %q aliases current authority %q", ErrVectorPartitionManifestInvalid, entry.name, authoritative.name)
			}
		}
		truncateErr := file.Truncate(0)
		syncErr := error(nil)
		validateErr := error(nil)
		if truncateErr == nil {
			syncErr = rootpublication.SyncStableFile(file)
		}
		if truncateErr == nil && syncErr == nil {
			validateErr = rootpublication.ValidateStableChildLink(dir, file, entry.name)
		}
		closeErr := file.Close()
		if err := errors.Join(truncateErr, syncErr, validateErr, closeErr); err != nil {
			return err
		}
		if err := vectorPartitionLifecycleStoreFaultV1("after_audit_truncate"); err != nil {
			return err
		}
	}
	return nil
}

func (s *VectorPartitionStoreV1) publishVectorPartitionLifecycleCheckpointV1(dir *os.File, loaded vectorPartitionLifecycleCheckpointStoreStateV1, checkpoint vectorPartitionLifecycleCheckpointV1) error {
	raw, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(checkpoint)
	if err != nil {
		return err
	}
	if len(loaded.entries) >= vectorPartitionStoreMaxEntriesV1 {
		return fmt.Errorf("%w: lifecycle checkpoint proposed entry cap", ErrVectorPartitionManifestInvalid)
	}
	if uint64(len(raw)) > uint64(vectorPartitionStoreMaxBytesV1)-loaded.physicalBytes {
		return fmt.Errorf("%w: lifecycle checkpoint proposed physical bytes cap", ErrVectorPartitionManifestInvalid)
	}
	name, err := vectorPartitionLifecycleCheckpointNameV1(checkpoint.State.Collection, checkpoint.State.IndexName, checkpoint.Epoch)
	if err != nil {
		return err
	}
	if err := s.installVectorPartitionLifecycleImmutableV1(dir, name, raw, "checkpoint"); err != nil {
		return err
	}
	reopened, err := s.loadVectorPartitionLifecycleCheckpointStateFromDirV1(dir, checkpoint.State.Collection, checkpoint.State.IndexName)
	if err != nil {
		return err
	}
	if reopened.checkpoint.Epoch != checkpoint.Epoch ||
		reopened.state.LastSequence != checkpoint.State.LastSequence ||
		reopened.state.LastDigest != checkpoint.State.LastDigest {
		return fmt.Errorf("%w: installed lifecycle checkpoint authority mismatch", ErrVectorPartitionManifestInvalid)
	}
	if err := s.truncateVectorPartitionLifecycleAuditEntriesV1(dir, reopened.entries, checkpoint.Epoch); err != nil {
		return err
	}
	_, err = s.loadVectorPartitionLifecycleCheckpointStateFromDirV1(dir, checkpoint.State.Collection, checkpoint.State.IndexName)
	return err
}

func (s *VectorPartitionStoreV1) persistVectorPartitionLifecycleOperationV1(collection, index string, operation vectorPartitionLifecycleOperationV1, generation uint64, payload []byte) error {
	dir, err := s.openDir()
	if err != nil {
		return err
	}
	defer dir.Close()
	loaded, err := s.loadVectorPartitionLifecycleCheckpointStateFromDirV1(dir, collection, index)
	if err != nil {
		return err
	}
	if err := vectorPartitionLifecyclePayloadV1(vectorPartitionLifecycleRecordV1{
		Collection: collection,
		IndexName:  index,
		Operation:  operation,
		Generation: generation,
		Payload:    payload,
	}); err != nil {
		return err
	}
	if vectorPartitionLifecycleOperationAlreadyAppliedV1(loaded.state, operation, generation, payload) {
		if loaded.checkpoint.Epoch != 0 {
			if err := s.truncateVectorPartitionLifecycleAuditEntriesV1(dir, loaded.entries, loaded.checkpoint.Epoch); err != nil {
				return err
			}
		}
		if err := rootpublication.SyncStableNamespace(dir); err != nil {
			return err
		}
		reopened, err := s.loadVectorPartitionLifecycleCheckpointStateFromDirV1(dir, collection, index)
		if err != nil {
			return err
		}
		if reopened.checkpoint.Epoch != loaded.checkpoint.Epoch ||
			reopened.state.LastSequence != loaded.state.LastSequence ||
			reopened.state.LastDigest != loaded.state.LastDigest {
			return fmt.Errorf("%w: lifecycle authority changed during audit retry", ErrVectorPartitionManifestInvalid)
		}
		return nil
	}
	if loaded.state.LastSequence == math.MaxUint64 {
		return fmt.Errorf("%w: lifecycle sequence exhausted", ErrVectorPartitionManifestInvalid)
	}
	sequence := loaded.state.LastSequence + 1
	recordRaw, err := encodeVectorPartitionLifecycleRecordCanonicalV1(vectorPartitionLifecycleRecordV1{
		Collection:     collection,
		IndexName:      index,
		Sequence:       sequence,
		PreviousDigest: loaded.state.LastDigest,
		Operation:      operation,
		Generation:     generation,
		Payload:        append([]byte(nil), payload...),
	})
	if err != nil {
		return err
	}
	record, err := decodeVectorPartitionLifecycleRecordCanonicalV1(recordRaw)
	if err != nil {
		return err
	}

	if loaded.checkpoint.Epoch == 0 {
		if operation != vectorPartitionLifecycleBuildV1 || sequence != 1 {
			return fmt.Errorf("%w: first lifecycle authority must be BUILD", ErrVectorPartitionManifestInvalid)
		}
		state, err := reduceVectorPartitionLifecycleChainV1([]vectorPartitionLifecycleRecordV1{record})
		if err != nil {
			return err
		}
		return s.publishVectorPartitionLifecycleCheckpointV1(dir, loaded, vectorPartitionLifecycleCheckpointV1{Epoch: 1, State: state})
	}

	if operation == vectorPartitionLifecycleBuildV1 {
		canonical, _, err := canonicalVectorPartitionLifecycleCheckpointV1(vectorPartitionLifecycleCheckpointV1{Epoch: loaded.checkpoint.Epoch, State: loaded.state})
		if err != nil {
			return err
		}
		proposed := canonical.State
		if err := reduceVectorPartitionLifecycleRecordV1(&proposed, record); err != nil {
			return err
		}
		proposed.LastSequence, proposed.LastDigest = record.Sequence, record.Digest
		if loaded.checkpoint.Epoch == math.MaxUint64 {
			return fmt.Errorf("%w: lifecycle checkpoint epoch exhausted", ErrVectorPartitionManifestInvalid)
		}
		return s.publishVectorPartitionLifecycleCheckpointV1(dir, loaded, vectorPartitionLifecycleCheckpointV1{Epoch: loaded.checkpoint.Epoch + 1, State: proposed})
	}

	proposedTailBytes := loaded.tailBytes + uint64(len(recordRaw))
	canAppendDelta := proposedTailBytes <= vectorPartitionLifecycleCheckpointTailMaxBytesV1 &&
		loaded.physicalBytes+uint64(len(recordRaw)) <= vectorPartitionStoreMaxBytesV1
	var proposed vectorPartitionLifecycleStateV1
	if proposedTailBytes <= vectorPartitionLifecycleCheckpointTailMaxBytesV1 {
		proposedDeltas := append(append([]vectorPartitionLifecycleRecordV1(nil), loaded.deltas...), record)
		proposed, err = reduceVectorPartitionLifecycleCheckpointTailV1(loaded.checkpoint, proposedDeltas)
		if err != nil {
			return err
		}
	} else {
		canonical, _, err := canonicalVectorPartitionLifecycleCheckpointV1(vectorPartitionLifecycleCheckpointV1{Epoch: loaded.checkpoint.Epoch, State: loaded.state})
		if err != nil {
			return err
		}
		proposed = canonical.State
		if err := reduceVectorPartitionLifecycleRecordV1(&proposed, record); err != nil {
			return err
		}
		proposed.LastSequence, proposed.LastDigest = record.Sequence, record.Digest
	}
	if canAppendDelta {
		if len(loaded.entries) >= vectorPartitionStoreMaxEntriesV1 {
			return fmt.Errorf("%w: lifecycle delta proposed entry cap", ErrVectorPartitionManifestInvalid)
		}
		name, err := vectorPartitionLifecycleDeltaNameV1(collection, index, loaded.checkpoint.Epoch, record.Sequence)
		if err != nil {
			return err
		}
		if err := s.installVectorPartitionLifecycleImmutableV1(dir, name, recordRaw, "delta"); err != nil {
			return err
		}
		reopened, err := s.loadVectorPartitionLifecycleCheckpointStateFromDirV1(dir, collection, index)
		if err != nil {
			return err
		}
		if reopened.state.LastSequence != record.Sequence || reopened.state.LastDigest != record.Digest {
			return fmt.Errorf("%w: installed lifecycle delta authority mismatch", ErrVectorPartitionManifestInvalid)
		}
		return nil
	}
	if loaded.checkpoint.Epoch == math.MaxUint64 {
		return fmt.Errorf("%w: lifecycle checkpoint epoch exhausted", ErrVectorPartitionManifestInvalid)
	}
	return s.publishVectorPartitionLifecycleCheckpointV1(dir, loaded, vectorPartitionLifecycleCheckpointV1{Epoch: loaded.checkpoint.Epoch + 1, State: proposed})
}
