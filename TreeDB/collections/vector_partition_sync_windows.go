//go:build windows

package collections

import (
	"fmt"
	"golang.org/x/sys/windows"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Windows cannot provide a generic durable proof for the link/remove namespace
// transitions used by VPM. A write-through replacement alone does not prove
// link creation or removal. M1 therefore rejects every mutation before it
// changes the namespace; read-only restore/open remains supported.
func syncDirVPM(path string) error {
	return fmt.Errorf("%w: vector partition namespace %q requires link/remove durability", rootpublication.ErrNamespacePersistenceUnsupported, path)
}

func renameVPM(old, new string) error {
	oldp, err := windows.UTF16PtrFromString(old)
	if err != nil {
		return err
	}
	newp, err := windows.UTF16PtrFromString(new)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(oldp, newp, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
}

func vpmNamespacePersistenceSupported() bool { return false }
