package nativewire

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const replacementSeedOwnerV1 = "owner-v1.json"
const replacementSeedOwnerTempV1 = "owner-v1.tmp"

func replacementSeedNameV1(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// The caller owns the single group worker and has freshly verified this BEGIN.
// A strictly newer committed epoch proves completion of the globally serialized
// prior operation. Keep its owner until deletion and parent sync succeed, so a
// crash or failed unlink can only retry that same bounded cleanup target.
func prepareReplacementSeedDirectoryV1(path string, command raftplacement.ReplicaReplacementBeginV1) (string, error) {
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return "", err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", raftcluster.ErrInvalidConfig
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return "", err
	}
	defer root.Close()
	parent, err := root.Open(".")
	if err != nil {
		return "", err
	}
	defer parent.Close()
	opened, err := parent.Stat()
	if err != nil {
		return "", err
	}
	if !os.SameFile(info, opened) {
		return "", raftcluster.ErrInvalidConfig
	}
	entries, err := parent.ReadDir(4)
	if err != nil && err != io.EOF {
		return "", err
	}
	if len(entries) > 3 {
		return "", raftcluster.ErrAdmissionUnavailable
	}
	var owner []byte
	var oldName string
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 {
			return "", raftcluster.ErrInvalidConfig
		}
		switch entry.Name() {
		case replacementSeedOwnerV1:
			if !entry.Type().IsRegular() {
				return "", raftcluster.ErrInvalidConfig
			}
			file, err := root.Open(entry.Name())
			if err != nil {
				return "", err
			}
			owner, err = io.ReadAll(io.LimitReader(file, replacementReceiverRecordMaxBytesV1+1))
			if err = errors.Join(err, file.Close()); err != nil {
				return "", err
			}
		case replacementSeedOwnerTempV1:
			if !entry.Type().IsRegular() {
				return "", raftcluster.ErrInvalidConfig
			}
		default:
			if !entry.IsDir() || oldName != "" {
				return "", raftcluster.ErrInvalidConfig
			}
			oldName = entry.Name()
		}
	}
	if len(owner) == 0 && oldName != "" {
		return "", raftplacement.ErrCatalogMetaConflict
	}
	if len(owner) != 0 {
		previous, err := raftplacement.DecodeReplicaReplacementBeginV1(owner)
		if err != nil {
			return "", err
		}
		if previous.ConfigDigest != command.ConfigDigest || previous.GroupID != command.GroupID || oldName != "" && oldName != replacementSeedNameV1(owner) {
			return "", raftplacement.ErrCatalogMetaConflict
		}
		if !bytes.Equal(owner, raw) {
			if command.ExpectedEpoch <= previous.ExpectedEpoch {
				return "", raftplacement.ErrCatalogMetaConflict
			}
			if oldName != "" {
				// Root.RemoveAll confines traversal to this captured namespace and
				// does not follow symlinks in an interrupted native snapshot tree.
				if err := root.RemoveAll(oldName); err != nil {
					return "", err
				}
			}
			if err := rootpublication.SyncStableNamespace(parent); err != nil {
				return "", err
			}
		}
	}
	if !bytes.Equal(owner, raw) {
		if err := root.Remove(replacementSeedOwnerTempV1); err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		file, err := root.OpenFile(replacementSeedOwnerTempV1, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return "", err
		}
		_, writeErr := file.Write(raw)
		if err := errors.Join(writeErr, rootpublication.SyncStableFile(file), file.Close()); err != nil {
			return "", err
		}
		if err := root.Rename(replacementSeedOwnerTempV1, replacementSeedOwnerV1); err != nil {
			return "", err
		}
	}
	// Replay this barrier even on an exact retry after an ambiguous rename.
	if err := rootpublication.SyncStableNamespace(parent); err != nil {
		return "", err
	}
	current, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !os.SameFile(opened, current) {
		return "", raftcluster.ErrInvalidConfig
	}
	selected := filepath.Join(path, replacementSeedNameV1(raw))
	if _, err := os.Lstat(selected); err == nil {
		if _, err := replacementSeedDirectoryEntriesV1(selected); err != nil {
			return "", err
		}
		if _, err := replacementSeedDirectoryEntriesV1(filepath.Join(selected, "snapshots")); err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	// The directory's own fsync does not make its new GroupDir entry durable.
	if err := syncFixedPeerDirectoryV1(filepath.Dir(path)); err != nil {
		return "", err
	}
	return selected, nil
}
