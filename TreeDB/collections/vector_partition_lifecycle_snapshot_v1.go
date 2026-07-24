package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// VectorPartitionSnapshotEntryV1 binds one selected authority name to the
// exact regular file observed while validating the checkpoint namespace.
type VectorPartitionSnapshotEntryV1 struct {
	Name     string
	Bytes    uint64
	Identity rootpublication.StableIdentity
}

type vectorPartitionSnapshotGroupV1 struct {
	prefix       string
	highestEpoch uint64
}

func vectorPartitionSnapshotGroupAndCheckpointEpochV1(name string) (string, uint64, error) {
	const hashBytes = sha256.Size * 2
	const identityPrefixBytes = hashBytes + 1 + hashBytes + len(".lifecycle.")
	if len(name) <= identityPrefixBytes ||
		name[hashBytes] != '-' ||
		name[hashBytes+1+hashBytes:identityPrefixBytes] != ".lifecycle." {
		return "", 0, fmt.Errorf("%w: malformed lifecycle snapshot name %q", ErrVectorPartitionManifestInvalid, name)
	}
	for _, offset := range []int{0, hashBytes + 1} {
		for _, digit := range name[offset : offset+hashBytes] {
			if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
				return "", 0, fmt.Errorf("%w: lifecycle snapshot identity hash", ErrVectorPartitionManifestInvalid)
			}
		}
	}
	prefix := name[:identityPrefixBytes]
	suffix := name[identityPrefixBytes:]
	if strings.HasPrefix(suffix, "checkpoint.") && strings.HasSuffix(suffix, ".vlc") {
		digits := strings.TrimSuffix(strings.TrimPrefix(suffix, "checkpoint."), ".vlc")
		epoch, err := parseFixedVectorPartitionLifecycleNumberV1(digits)
		if err != nil {
			return "", 0, err
		}
		return prefix, epoch, nil
	}
	if strings.HasPrefix(suffix, "epoch.") && strings.HasSuffix(suffix, ".vlc") {
		body := strings.TrimSuffix(strings.TrimPrefix(suffix, "epoch."), ".vlc")
		parts := strings.Split(body, ".delta.")
		if len(parts) != 2 {
			return "", 0, fmt.Errorf("%w: malformed lifecycle snapshot delta", ErrVectorPartitionManifestInvalid)
		}
		if _, err := parseFixedVectorPartitionLifecycleNumberV1(parts[0]); err != nil {
			return "", 0, err
		}
		if _, err := parseFixedVectorPartitionLifecycleNumberV1(parts[1]); err != nil {
			return "", 0, err
		}
		return prefix, 0, nil
	}
	return "", 0, fmt.Errorf("%w: unexpected lifecycle snapshot entry %q", ErrVectorPartitionManifestInvalid, name)
}

func vectorPartitionCheckpointEnvelopeIdentityV1(raw []byte) (string, string, uint64, error) {
	if len(raw) < vectorPartitionLifecycleCheckpointHeaderBytesV1+vectorPartitionLifecycleCheckpointChecksumBytesV1 ||
		len(raw) > vectorPartitionLifecycleCheckpointMaxBytesV1 ||
		string(raw[:4]) != vectorPartitionLifecycleCheckpointMagicV1 ||
		binary.BigEndian.Uint32(raw[4:8]) != vectorPartitionLifecycleCheckpointVersionV1 {
		return "", "", 0, fmt.Errorf("%w: lifecycle snapshot checkpoint header", ErrVectorPartitionManifestInvalid)
	}
	payloadBytes := uint64(binary.BigEndian.Uint32(raw[8:12]))
	contentBytes := vectorPartitionLifecycleCheckpointHeaderBytesV1 + int(payloadBytes)
	if payloadBytes == 0 ||
		payloadBytes > uint64(vectorPartitionLifecycleCheckpointMaxBytesV1-vectorPartitionLifecycleCheckpointHeaderBytesV1-vectorPartitionLifecycleCheckpointChecksumBytesV1) ||
		contentBytes+vectorPartitionLifecycleCheckpointChecksumBytesV1 != len(raw) {
		return "", "", 0, fmt.Errorf("%w: lifecycle snapshot checkpoint length", ErrVectorPartitionManifestInvalid)
	}
	sum := sha256.Sum256(raw[:contentBytes])
	if !bytes.Equal(sum[:], raw[contentBytes:]) {
		return "", "", 0, fmt.Errorf("%w: lifecycle snapshot checkpoint checksum", ErrVectorPartitionManifestInvalid)
	}
	limits := DefaultVectorPartitionManifestLimits()
	reader := vpmReader{
		b: raw[vectorPartitionLifecycleCheckpointHeaderBytesV1:contentBytes],
		l: VectorPartitionManifestLimits{MaxStringBytes: limits.MaxStringBytes},
	}
	collection := reader.str()
	index := reader.str()
	epoch := reader.u64()
	if reader.err != nil || collection == "" || index == "" || epoch == 0 {
		return "", "", 0, fmt.Errorf("%w: lifecycle snapshot checkpoint identity", ErrVectorPartitionManifestInvalid)
	}
	return collection, index, epoch, nil
}

// VectorPartitionSnapshotEntriesV1 validates every entry in one retained
// vector_partitions directory and returns only the highest checkpoint and its
// contiguous current-epoch tail for each identity. Superseded audit epochs are
// intentionally omitted from snapshots.
func VectorPartitionSnapshotEntriesV1(dir *os.File) ([]VectorPartitionSnapshotEntryV1, error) {
	if dir == nil {
		return nil, fmt.Errorf("%w: nil lifecycle snapshot directory", ErrVectorPartitionManifestInvalid)
	}
	identity, err := rootpublication.StableIdentityFromFile(dir)
	if err != nil {
		return nil, err
	}
	dirPath := filepath.Clean(dir.Name())
	store := &VectorPartitionStoreV1{
		root:        filepath.Dir(dirPath),
		dir:         dirPath,
		dirIdentity: identity,
	}
	if err := store.verifyBoundDirV1(dir); err != nil {
		return nil, err
	}
	if _, err := dir.Seek(0, 0); err != nil {
		return nil, err
	}
	entries, err := dir.ReadDir(-1)
	if err != nil {
		return nil, err
	}
	if len(entries) > vectorPartitionStoreMaxEntriesV1 {
		return nil, fmt.Errorf("%w: lifecycle snapshot entry cap", ErrVectorPartitionManifestInvalid)
	}
	groups := make(map[string]vectorPartitionSnapshotGroupV1)
	seenIdentities := make([]VectorPartitionSnapshotEntryV1, 0, len(entries))
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("%w: lifecycle snapshot symlink %q", ErrVectorPartitionManifestInvalid, entry.Name())
		}
		prefix, checkpointEpoch, err := vectorPartitionSnapshotGroupAndCheckpointEpochV1(entry.Name())
		if err != nil {
			return nil, err
		}
		file, stable, exactBytes, err := inspectVectorPartitionLifecycleCheckpointEntryV1(dir, entry.Name(), os.O_RDONLY)
		if err != nil {
			return nil, err
		}
		closeErr := file.Close()
		if closeErr != nil {
			return nil, closeErr
		}
		for _, seen := range seenIdentities {
			if rootpublication.SamePhysicalIdentity(stable, seen.Identity) {
				return nil, fmt.Errorf("%w: lifecycle snapshot entries alias one physical file", ErrVectorPartitionManifestInvalid)
			}
		}
		seenIdentities = append(seenIdentities, VectorPartitionSnapshotEntryV1{Name: entry.Name(), Bytes: exactBytes, Identity: stable})
		group := groups[prefix]
		group.prefix = prefix
		if checkpointEpoch > group.highestEpoch {
			group.highestEpoch = checkpointEpoch
		}
		groups[prefix] = group
	}

	selected := make([]VectorPartitionSnapshotEntryV1, 0, len(entries))
	for _, group := range groups {
		if group.highestEpoch == 0 {
			return nil, fmt.Errorf("%w: lifecycle snapshot deltas without checkpoint", ErrVectorPartitionManifestInvalid)
		}
		checkpointSuffix := fmt.Sprintf("checkpoint.%020d.vlc", group.highestEpoch)
		checkpointName := group.prefix + checkpointSuffix
		raw, err := readVectorPartitionLifecycleSlotV1(dir, checkpointName, vectorPartitionLifecycleCheckpointMaxBytesV1)
		if err != nil {
			return nil, err
		}
		collection, index, epoch, err := vectorPartitionCheckpointEnvelopeIdentityV1(raw)
		if err != nil {
			return nil, err
		}
		if epoch != group.highestEpoch || vectorPartitionLifecycleNamePrefixV1(collection, index) != group.prefix {
			return nil, fmt.Errorf("%w: lifecycle snapshot filename identity", ErrVectorPartitionManifestInvalid)
		}
		loaded, err := store.loadVectorPartitionLifecycleCheckpointStateFromDirV1(dir, collection, index)
		if err != nil {
			return nil, err
		}
		for _, entry := range loaded.entries {
			if entry.epoch != group.highestEpoch {
				continue
			}
			if entry.bytes == 0 {
				return nil, fmt.Errorf("%w: empty current lifecycle snapshot entry", ErrVectorPartitionManifestInvalid)
			}
			selected = append(selected, VectorPartitionSnapshotEntryV1{Name: entry.name, Bytes: entry.bytes, Identity: entry.identity})
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Name < selected[j].Name })
	if err := store.verifyBoundDirV1(dir); err != nil {
		return nil, err
	}
	return selected, nil
}

// ValidateVectorPartitionSnapshotNamespaceV1 validates an extracted snapshot
// namespace. Unlike a live store, an archive must contain exactly the selected
// highest checkpoint and its current tail; superseded audit epochs are never
// valid snapshot payload.
func ValidateVectorPartitionSnapshotNamespaceV1(root string) error {
	store, err := OpenExistingVectorPartitionStoreV1(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	dir, err := store.openDir()
	if err != nil {
		return err
	}
	selected, err := VectorPartitionSnapshotEntriesV1(dir)
	if err != nil {
		_ = dir.Close()
		return err
	}
	if _, err := dir.Seek(0, io.SeekStart); err != nil {
		_ = dir.Close()
		return err
	}
	all, err := dir.ReadDir(-1)
	closeErr := dir.Close()
	if err != nil || closeErr != nil {
		return errors.Join(err, closeErr)
	}
	if len(all) != len(selected) {
		return fmt.Errorf("%w: snapshot namespace contains superseded lifecycle audit entries", ErrVectorPartitionManifestInvalid)
	}
	return nil
}
