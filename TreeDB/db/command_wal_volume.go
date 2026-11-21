package db

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func commandWALSeparateIndexVolume(idx *indexGen, wal rootpublication.StableIdentity) (rootpublication.StableIdentity, bool) {
	if idx == nil || idx.pager == nil || wal == (rootpublication.StableIdentity{}) {
		return rootpublication.StableIdentity{}, false
	}
	var indexIdentity rootpublication.StableIdentity
	err := idx.pager.WithStableResourceFile(func(file *os.File) error {
		var err error
		indexIdentity, err = rootpublication.StableIdentityFromFile(file)
		return err
	})
	return indexIdentity, err == nil && indexIdentity.Platform == wal.Platform && indexIdentity.VolumeID != wal.VolumeID
}

// commandWALHasDedicatedVolume compares a known ext4 WAL descriptor with the
// live pager and resolved storage roles. Invoke it from pinned maintenance.
func (db *DB) commandWALHasDedicatedVolume(wal rootpublication.StableIdentity) (bool, error) {
	idx := db.idx.Load()
	indexIdentity, separate := commandWALSeparateIndexVolume(idx, wal)
	if !separate {
		return false, nil
	}
	if idx.primary != nil {
		var primaryIdentity rootpublication.StableIdentity
		err := idx.primary.Pager().WithStableResourceFile(func(file *os.File) error {
			var err error
			primaryIdentity, err = rootpublication.StableIdentityFromFile(file)
			return err
		})
		if err != nil || primaryIdentity.Platform != indexIdentity.Platform || primaryIdentity.VolumeID != indexIdentity.VolumeID {
			return false, nil
		}
	}
	layout := resolveStorageLayout(db.dir)
	for _, path := range [...]string{layout.rootDir, layout.valueVLogDir, layout.leafVLogDir, layout.columnAssetDir} {
		dir, err := openCommandWALRoleDirectory(path)
		if err != nil {
			return false, nil
		}
		identity, identityErr := rootpublication.StableIdentityFromFile(dir)
		eligible := identityErr == nil && identity.Platform == indexIdentity.Platform && identity.VolumeID == indexIdentity.VolumeID
		if eligible && path == layout.columnAssetDir {
			// A nested column namespace can place assets on another volume.
			names, err := dir.Readdirnames(1)
			eligible = len(names) == 0 && errors.Is(err, io.EOF)
		}
		if err := dir.Close(); err != nil {
			return false, errors.Join(identityErr, err)
		}
		if !eligible {
			return false, nil
		}
	}
	return true, nil
}

// Role directories may be configured through a final symlink. Open and
// identify the resolved directory using the same stable directory primitive.
func openCommandWALRoleDirectory(path string) (*os.File, error) {
	dir, err := rootpublication.OpenStableParent(path)
	if err == nil || commandWALCleanupDescriptorExhausted(err) {
		return dir, err
	}
	info, statErr := os.Lstat(path)
	if statErr != nil || info.Mode()&os.ModeSymlink == 0 {
		return nil, err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	return rootpublication.OpenStableParent(resolved)
}
