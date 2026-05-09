package collectionwal

import (
	"fmt"
	"path"
	"strings"
)

// ValidateRelativePath validates advisory side-ref path metadata. A valid path
// is still not authority to open or delete a file; collection WAL cleanup must
// resolve the file through the trusted RefClass/FileID registry.
func ValidateRelativePath(p string) error {
	if p == "" {
		return fmt.Errorf("%w: empty relative path", ErrCollectionWALUnsafePath)
	}
	if len(p) > MaxRelativePathBytes {
		return fmt.Errorf("%w: relative path length %d exceeds %d", ErrCollectionWALUnsafePath, len(p), MaxRelativePathBytes)
	}
	if strings.ContainsRune(p, '\x00') {
		return fmt.Errorf("%w: relative path contains NUL", ErrCollectionWALUnsafePath)
	}
	if strings.Contains(p, "\\") {
		return fmt.Errorf("%w: relative path contains backslash", ErrCollectionWALUnsafePath)
	}
	if path.IsAbs(p) || strings.HasPrefix(p, "//") {
		return fmt.Errorf("%w: relative path is absolute", ErrCollectionWALUnsafePath)
	}
	if hasWindowsDrivePrefix(p) {
		return fmt.Errorf("%w: relative path has Windows drive prefix", ErrCollectionWALUnsafePath)
	}
	if strings.Contains(p, "//") {
		return fmt.Errorf("%w: relative path contains empty component", ErrCollectionWALUnsafePath)
	}
	parts := strings.Split(p, "/")
	if len(parts) > MaxRelativePathComponents {
		return fmt.Errorf("%w: relative path component count %d exceeds %d", ErrCollectionWALUnsafePath, len(parts), MaxRelativePathComponents)
	}
	for _, part := range parts {
		switch part {
		case "", ".", "..":
			return fmt.Errorf("%w: relative path contains invalid component", ErrCollectionWALUnsafePath)
		}
		if len(part) > MaxRelativePathComponentBytes {
			return fmt.Errorf("%w: relative path component length %d exceeds %d", ErrCollectionWALUnsafePath, len(part), MaxRelativePathComponentBytes)
		}
	}
	return nil
}

func hasWindowsDrivePrefix(p string) bool {
	return len(p) >= 2 && p[1] == ':' && ((p[0] >= 'A' && p[0] <= 'Z') || (p[0] >= 'a' && p[0] <= 'z'))
}
