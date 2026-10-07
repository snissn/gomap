//go:build !windows

package valuelog

import "os"

// Non-Windows retirement absence requires the ordinary exact-child lookup.
func retirementParentDeleted(*os.File, error) (bool, error) {
	return false, nil
}
