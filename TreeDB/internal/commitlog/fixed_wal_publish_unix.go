//go:build darwin || linux || freebsd || netbsd || openbsd

package commitlog

import (
	"os"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const fixedWALCreationSupported = true

func publishInitializedFixedWAL(parent *os.File, private, canonical string) error {
	if err := rootpublication.RenameStableChildFile(parent, private, canonical); err != nil {
		return err
	}
	return rootpublication.SyncStableNamespace(parent)
}

func removePrivateFixedWAL(parent *os.File, name string) {
	_ = rootpublication.RemoveStableChildFile(parent, name)
}
