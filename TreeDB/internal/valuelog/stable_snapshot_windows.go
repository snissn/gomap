//go:build windows

package valuelog

import (
	"os"

	"golang.org/x/sys/windows"
)

func duplicateStableFile(f *os.File) (*os.File, error) {
	process := windows.CurrentProcess()
	var duplicate windows.Handle
	if err := windows.DuplicateHandle(
		process,
		windows.Handle(f.Fd()),
		process,
		&duplicate,
		0,
		false,
		windows.DUPLICATE_SAME_ACCESS,
	); err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(duplicate), f.Name()+" (stable)"), nil
}
