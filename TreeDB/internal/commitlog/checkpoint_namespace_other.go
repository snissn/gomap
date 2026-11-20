//go:build !linux

package commitlog

import "os"

func checkpointChildLink(_, _ *os.File) error { return os.ErrInvalid }
