//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd

package commitlog

import (
	"os"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const fixedWALCreationSupported = false

func publishInitializedFixedWAL(*os.File, string, string) error {
	return rootpublication.ErrNamespacePersistenceUnsupported
}

func removePrivateFixedWAL(parent *os.File, name string) {
	_ = rootpublication.RemoveStableChildFile(parent, name)
}
