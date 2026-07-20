//go:build windows

package collections

import (
	"golang.org/x/sys/windows"
)

// Windows has no sound generic directory-handle fsync: opening a directory
// through os.File and calling Sync returns ERROR_ACCESS_DENIED. TreeDB's
// rootpublication contract instead makes a created child durable through its
// FlushFileBuffers proof. Every VPM temporary/linked child is file-synced, and
// replacement transitions use MOVEFILE_WRITE_THROUGH below. Therefore this is
// not a skipped Unix directory durability claim; it is the platform-specific
// namespace primitive used by the rest of TreeDB's stable-resource layer.
func syncDirVPM(string) error { return nil }

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
