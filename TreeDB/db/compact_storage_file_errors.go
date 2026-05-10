//go:build !windows

package db

func compactStorageIsBusyRemoveError(error) bool {
	return false
}
