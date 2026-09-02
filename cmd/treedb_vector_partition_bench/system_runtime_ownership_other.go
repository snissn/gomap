//go:build !linux

package main

import "errors"

func applyVectorPartitionSystemRuntimeOwnershipPlatformV1([]int) error {
	return errors.New("runtime ownership is unsupported on this platform")
}
