package collections

// This file owns the append-only VLC1 record and immutable-file primitives
// shared by the checkpoint-backed public lifecycle.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const vectorPartitionLifecycleNameSequenceWidthV1 = 20

var vectorPartitionLifecycleStoreHooksV1 struct {
	sync.RWMutex
	at func(string) error
}

func setVectorPartitionLifecycleStoreHookForTestV1(at func(string) error) func() {
	vectorPartitionLifecycleStoreHooksV1.Lock()
	old := vectorPartitionLifecycleStoreHooksV1.at
	vectorPartitionLifecycleStoreHooksV1.at = at
	vectorPartitionLifecycleStoreHooksV1.Unlock()
	return func() {
		vectorPartitionLifecycleStoreHooksV1.Lock()
		vectorPartitionLifecycleStoreHooksV1.at = old
		vectorPartitionLifecycleStoreHooksV1.Unlock()
	}
}

func vectorPartitionLifecycleStoreFaultV1(boundary string) error {
	vectorPartitionLifecycleStoreHooksV1.RLock()
	at := vectorPartitionLifecycleStoreHooksV1.at
	vectorPartitionLifecycleStoreHooksV1.RUnlock()
	if at == nil {
		return nil
	}
	return at(boundary)
}

func vectorPartitionLifecycleNamePrefixV1(collection, index string) string {
	return safeVPM(collection) + "-" + safeVPM(index) + ".lifecycle."
}

func vectorPartitionLifecycleNameV1(collection, index string, sequence uint64) (string, error) {
	if collection == "" || index == "" || sequence == 0 {
		return "", fmt.Errorf("%w: lifecycle slot identity", ErrVectorPartitionManifestInvalid)
	}
	return fmt.Sprintf("%s%020d.vlc", vectorPartitionLifecycleNamePrefixV1(collection, index), sequence), nil
}

func parseVectorPartitionLifecycleNameV1(collection, index, name string) (uint64, error) {
	prefix := vectorPartitionLifecycleNamePrefixV1(collection, index)
	if !strings.HasPrefix(name, prefix) {
		return 0, fmt.Errorf("%w: lifecycle slot prefix", ErrVectorPartitionManifestInvalid)
	}
	suffix := strings.TrimPrefix(name, prefix)
	if len(suffix) != vectorPartitionLifecycleNameSequenceWidthV1+len(".vlc") || !strings.HasSuffix(suffix, ".vlc") {
		return 0, fmt.Errorf("%w: malformed lifecycle slot %q", ErrVectorPartitionManifestInvalid, name)
	}
	digits := strings.TrimSuffix(suffix, ".vlc")
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return 0, fmt.Errorf("%w: malformed lifecycle sequence", ErrVectorPartitionManifestInvalid)
		}
	}
	sequence, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || sequence == 0 {
		return 0, fmt.Errorf("%w: lifecycle sequence", ErrVectorPartitionManifestInvalid)
	}
	canonical, err := vectorPartitionLifecycleNameV1(collection, index, sequence)
	if err != nil || canonical != name {
		return 0, fmt.Errorf("%w: noncanonical lifecycle slot", ErrVectorPartitionManifestInvalid)
	}
	return sequence, nil
}

func readVectorPartitionLifecycleSlotV1(dir *os.File, name string, max int) ([]byte, error) {
	return readVectorPartitionLifecycleSlotWithContextV1(context.Background(), dir, name, max)
}

func readVectorPartitionLifecycleSlotWithContextV1(ctx context.Context, dir *os.File, name string, max int) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if max < 0 || max > vectorPartitionStoreMaxBytesV1 {
		return nil, fmt.Errorf("%w: lifecycle slot read limit", ErrVectorPartitionManifestInvalid)
	}
	f, err := rootpublication.OpenStableChildFile(dir, name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) > uint64(max) {
		return nil, fmt.Errorf("%w: lifecycle slot %q is not a bounded regular file", ErrVectorPartitionManifestInvalid, name)
	}
	raw := make([]byte, 0, min(int(info.Size()), max))
	buf := make([]byte, 64<<10)
	reader := io.LimitReader(f, int64(max)+1)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, readErr := reader.Read(buf)
		if n > 0 {
			raw = append(raw, buf[:n]...)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	if len(raw) > max {
		return nil, fmt.Errorf("%w: lifecycle slot bytes cap", ErrVectorPartitionManifestInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := vectorPartitionLifecycleStoreFaultV1("after_slot_read"); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := rootpublication.ValidateStableChildLink(dir, f, name); err != nil {
		return nil, err
	}
	return raw, nil
}

// loadVectorPartitionLifecycleChainFromDirV1 reads only the exact hashed
// identity namespace. Any malformed entry there is corruption rather than a
// file to skip, preventing a partial chain from silently becoming authority.
func (s *VectorPartitionStoreV1) loadVectorPartitionLifecycleChainFromDirV1(dir *os.File, collection, index string) ([]vectorPartitionLifecycleRecordV1, vectorPartitionLifecycleStateV1, error) {
	var zero vectorPartitionLifecycleStateV1
	if err := s.verifyBoundDirV1(dir); err != nil {
		return nil, zero, err
	}
	entries, err := readVectorPartitionDirEntriesBoundedV1(dir)
	if err != nil {
		return nil, zero, err
	}
	prefix := vectorPartitionLifecycleNamePrefixV1(collection, index)
	type slot struct {
		name string
		seq  uint64
	}
	slots := make([]slot, 0)
	var metadataTotal uint64
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		sequence, err := parseVectorPartitionLifecycleNameV1(collection, index, entry.Name())
		if err != nil {
			return nil, zero, err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil, zero, fmt.Errorf("%w: lifecycle slot %q is a symlink", ErrVectorPartitionManifestInvalid, entry.Name())
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Size() < 0 {
			return nil, zero, fmt.Errorf("%w: lifecycle slot %q is not a regular file", ErrVectorPartitionManifestInvalid, entry.Name())
		}
		if len(slots) >= vectorPartitionLifecycleMaxRecordsV1 {
			return nil, zero, fmt.Errorf("%w: lifecycle record cap", ErrVectorPartitionManifestInvalid)
		}
		if uint64(info.Size()) > uint64(vectorPartitionStoreMaxBytesV1)-metadataTotal {
			return nil, zero, fmt.Errorf("%w: lifecycle aggregate bytes cap", ErrVectorPartitionManifestInvalid)
		}
		metadataTotal += uint64(info.Size())
		slots = append(slots, slot{name: entry.Name(), seq: sequence})
	}
	if len(slots) == 0 {
		if err := vectorPartitionLifecycleStoreFaultV1("after_scan"); err != nil {
			return nil, zero, err
		}
		if err := s.verifyBoundDirV1(dir); err != nil {
			return nil, zero, err
		}
		return nil, zero, nil
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i].seq < slots[j].seq })
	records := make([]vectorPartitionLifecycleRecordV1, 0, len(slots))
	var total uint64
	for i, slot := range slots {
		if slot.seq != uint64(i+1) {
			return nil, zero, fmt.Errorf("%w: lifecycle slot sequence gap or duplicate", ErrVectorPartitionManifestInvalid)
		}
		remaining := vectorPartitionStoreMaxBytesV1 - int(total)
		raw, err := readVectorPartitionLifecycleSlotV1(dir, slot.name, remaining)
		if err != nil {
			return nil, zero, err
		}
		if uint64(len(raw)) > uint64(vectorPartitionStoreMaxBytesV1)-total {
			return nil, zero, fmt.Errorf("%w: lifecycle aggregate bytes cap", ErrVectorPartitionManifestInvalid)
		}
		total += uint64(len(raw))
		record, err := decodeVectorPartitionLifecycleRecordCanonicalV1(raw)
		if err != nil || record.Sequence != slot.seq || record.Collection != collection || record.IndexName != index {
			return nil, zero, fmt.Errorf("%w: lifecycle slot content identity", ErrVectorPartitionManifestInvalid)
		}
		records = append(records, record)
	}
	state, err := reduceVectorPartitionLifecycleChainV1(records)
	if err != nil {
		return nil, zero, err
	}
	if err := vectorPartitionLifecycleStoreFaultV1("after_scan"); err != nil {
		return nil, zero, err
	}
	if err := s.verifyBoundDirV1(dir); err != nil {
		return nil, zero, err
	}
	return records, state, nil
}

func (s *VectorPartitionStoreV1) loadVectorPartitionLifecycleChainV1(collection, index string) ([]vectorPartitionLifecycleRecordV1, vectorPartitionLifecycleStateV1, error) {
	dir, err := s.openDir()
	if err != nil {
		return nil, vectorPartitionLifecycleStateV1{}, err
	}
	defer dir.Close()
	return s.loadVectorPartitionLifecycleChainFromDirV1(dir, collection, index)
}

// appendVectorPartitionLifecycleRecordV1 derives all chain authority from the
// retained directory. Its no-replace collision handling makes an interrupted
// identical append retry idempotent while rejecting every other occupant.
func (s *VectorPartitionStoreV1) appendVectorPartitionLifecycleRecordV1(collection, index string, operation vectorPartitionLifecycleOperationV1, generation uint64, payload []byte) (vectorPartitionLifecycleRecordV1, error) {
	var zero vectorPartitionLifecycleRecordV1
	dir, err := s.openDir()
	if err != nil {
		return zero, err
	}
	defer dir.Close()
	records, state, err := s.loadVectorPartitionLifecycleChainFromDirV1(dir, collection, index)
	if err != nil {
		return zero, err
	}
	if len(records) >= vectorPartitionLifecycleMaxRecordsV1 {
		return zero, fmt.Errorf("%w: lifecycle record cap", ErrVectorPartitionManifestInvalid)
	}
	record := vectorPartitionLifecycleRecordV1{Collection: collection, IndexName: index, Sequence: uint64(len(records) + 1), PreviousDigest: state.LastDigest, Operation: operation, Generation: generation, Payload: append([]byte(nil), payload...)}
	if len(records) > 0 {
		tail := records[len(records)-1]
		if tail.Operation == operation && tail.Generation == generation && bytes.Equal(tail.Payload, payload) {
			if err := rootpublication.SyncStableNamespace(dir); err != nil {
				return zero, err
			}
			if err := s.verifyBoundDirV1(dir); err != nil {
				return zero, err
			}
			return tail, nil
		}
	}
	raw, err := encodeVectorPartitionLifecycleRecordCanonicalV1(record)
	if err != nil {
		return zero, err
	}
	record, err = decodeVectorPartitionLifecycleRecordCanonicalV1(raw)
	if err != nil {
		return zero, err
	}
	proposed := append(append([]vectorPartitionLifecycleRecordV1(nil), records...), record)
	if _, err := reduceVectorPartitionLifecycleChainV1(proposed); err != nil {
		return zero, err
	}
	name, err := vectorPartitionLifecycleNameV1(collection, index, record.Sequence)
	if err != nil {
		return zero, err
	}
	anonymous, err := rootpublication.OpenStableAnonymousFile(dir, 0o600)
	if err != nil {
		return zero, err
	}
	defer anonymous.Close()
	if n, writeErr := anonymous.Write(raw); writeErr != nil || n != len(raw) {
		if writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		return zero, writeErr
	}
	if err := rootpublication.SyncStableFile(anonymous); err != nil {
		return zero, err
	}
	if err := s.verifyBoundDirV1(dir); err != nil {
		return zero, err
	}
	if err := vectorPartitionLifecycleStoreFaultV1("before_install"); err != nil {
		return zero, err
	}
	installed, installErr := rootpublication.InstallStableFileHandleNoReplace(anonymous, dir, name)
	if installErr != nil && installed {
		return zero, installErr
	}
	if !installed {
		existing, readErr := readVectorPartitionLifecycleSlotV1(dir, name, vectorPartitionStoreMaxBytesV1)
		if readErr != nil {
			return zero, errors.Join(installErr, readErr)
		}
		if !bytes.Equal(existing, raw) {
			return zero, fmt.Errorf("%w: lifecycle slot %q already contains different bytes", ErrVectorPartitionManifestInvalid, name)
		}
		if err := rootpublication.SyncStableNamespace(dir); err != nil {
			return zero, err
		}
		if err := s.verifyBoundDirV1(dir); err != nil {
			return zero, err
		}
		if _, _, err := s.loadVectorPartitionLifecycleChainFromDirV1(dir, collection, index); err != nil {
			return zero, err
		}
		return record, nil
	}
	if err := vectorPartitionLifecycleStoreFaultV1("after_install"); err != nil {
		return zero, err
	}
	if err := rootpublication.SyncStableNamespace(dir); err != nil {
		return zero, err
	}
	if err := s.verifyBoundDirV1(dir); err != nil {
		return zero, err
	}
	if _, _, err := s.loadVectorPartitionLifecycleChainFromDirV1(dir, collection, index); err != nil {
		return zero, err
	}
	if err := anonymous.Close(); err != nil {
		return zero, err
	}
	return record, nil
}
