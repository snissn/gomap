//go:build windows

package valuelog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Windows may refuse a directory rename while its descendant segment handles
// remain open despite delete sharing. These quiescent test setups temporarily
// close raw child handles, rename, then restore the SAME physical children from
// the original parent. No File lifecycle, namespace proof or reader state changes.
func retirementTestRenameParent(dir, saved string, files []*File) error {
	identities := make([]rootpublication.StableIdentity, len(files))
	for i, file := range files {
		if file == nil || file.File == nil || file.RefCount.Load() != 0 || file.closed.Load() {
			return fmt.Errorf("fixture parent rename requires unborrowed open File")
		}
		if mapping, _ := file.mmapData.Load().([]byte); len(mapping) != 0 || len(file.deadMappings) != 0 {
			return fmt.Errorf("fixture parent rename refuses mapped File")
		}
		identity, err := rootpublication.StableIdentityFromFile(file.File)
		if err != nil {
			return err
		}
		identities[i] = identity
	}
	var closeErr error
	for _, file := range files {
		closeErr = errors.Join(closeErr, file.File.Close())
	}
	// Even a failed rename/close restores handles through the original parent.
	original := dir
	var renameErr error
	if closeErr == nil {
		renameErr = os.Rename(dir, saved)
		if renameErr == nil {
			original = saved
		}
	}
	parent, parentErr := rootpublication.OpenStableParent(original)
	if parentErr != nil {
		return errors.Join(closeErr, renameErr, parentErr)
	}
	defer parent.Close()
	parentIdentity, parentIdentityErr := rootpublication.StableIdentityFromFile(parent)
	if parentIdentityErr != nil {
		return errors.Join(closeErr, renameErr, parentIdentityErr)
	}
	var reopenErr error
	for i, file := range files {
		if !rootpublication.SamePhysicalIdentity(file.registeredParentIdentity, parentIdentity) {
			reopenErr = errors.Join(reopenErr, fmt.Errorf("fixture reopen changed original parent"))
			continue
		}
		child, err := rootpublication.OpenStableChildFile(parent, filepath.Base(file.Path), os.O_RDONLY, 0)
		if err != nil {
			reopenErr = errors.Join(reopenErr, err)
			continue
		}
		identity, err := rootpublication.StableIdentityFromFile(child)
		if err != nil || !rootpublication.SamePhysicalIdentity(identities[i], identity) {
			reopenErr = errors.Join(reopenErr, err, fmt.Errorf("fixture reopen changed original child"), child.Close())
			continue
		}
		file.File = child
	}
	return errors.Join(closeErr, renameErr, reopenErr)
}
