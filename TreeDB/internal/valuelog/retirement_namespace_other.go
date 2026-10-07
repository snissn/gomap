//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd && !windows

package valuelog

import (
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
)

func retirementRenameChild(*os.File, string, *os.File, string) error {
	return rootpublication.ErrNamespacePersistenceUnsupported
}
func retirementLinkChild(*os.File, string, *os.File, string) error {
	return rootpublication.ErrNamespacePersistenceUnsupported
}
func retirementRemoveDirectory(*os.File, *os.File, string) error {
	return rootpublication.ErrNamespacePersistenceUnsupported
}
