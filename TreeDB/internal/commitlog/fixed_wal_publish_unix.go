//go:build darwin || linux || freebsd || netbsd || openbsd

package commitlog

import (
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const fixedWALCreationSupported = true

func publishInitializedFixedWAL(parent *os.File, private, canonical string) error {
	if err := rootpublication.RenameStableChildFile(parent, private, canonical); err != nil {
		return err
	}
	root := parent.Name()
	if err := durabilitycut.EmitStableNamespace(durabilitycut.NamespaceRename, durabilitycut.ResourceCommandWAL, root, filepath.Join(root, private), filepath.Join(root, canonical), parent, nil, private, canonical); err != nil {
		return err
	}
	if err := durabilitycut.EmitStablePath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceCommandWAL, root, root, parent); err != nil {
		return err
	}
	if err := rootpublication.SyncStableNamespace(parent); err != nil {
		return err
	}
	return durabilitycut.EmitStablePath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceCommandWAL, root, root, parent)
}

func removePrivateFixedWAL(parent *os.File, name string) {
	_ = rootpublication.RemoveStableChildFile(parent, name)
}
