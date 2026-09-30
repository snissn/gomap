//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd

package nativewire

import (
	"errors"
	"os"
)

func liveLifecycleRetainedFileLinkCountV1(os.FileInfo) (uint64, error) {
	return 0, errors.New("private retained copies require supported durable namespaces")
}
