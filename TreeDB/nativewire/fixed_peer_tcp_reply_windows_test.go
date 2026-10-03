package nativewire

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

// Rename makes the reply name visible before its DELETE-access handle closes.
// Sharing delete lets the parent read that complete file during this window.
func fixedPeerReadWindowsReplyV1(path string) ([]byte, error) {
	pathp, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	handle, err := windows.CreateFile(pathp, windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, &os.PathError{Op: "open", Path: path, Err: errors.New("invalid Windows file handle")}
	}
	defer file.Close()
	return io.ReadAll(file)
}

func TestFixedPeerWindowsReplyReadSharesRenameHandleV1(t *testing.T) {
	stage := &fixedPeerWindowsStageV1{path: filepath.Join(t.TempDir(), "config.json")}
	// Both allocation and activation use this same reader. Hold the rename's
	// DELETE-access condition deterministically, rather than racing MoveFileEx.
	for repeat := 0; repeat < 20; repeat++ {
		for sequence := 1; sequence <= 2; sequence++ {
			func() {
				want := fixedPeerWindowsStageReplyV1{}
				if sequence == 1 {
					want.Address = "127.0.0.1:12345"
				}
				if err := fixedPeerWindowsWriteReplyV1(stage.path, sequence, want); err != nil {
					t.Fatal(err)
				}
				path := fmt.Sprintf("%s.reply-%d", stage.path, sequence)
				pathp, err := windows.UTF16PtrFromString(path)
				if err != nil {
					t.Fatal(err)
				}
				handle, err := windows.CreateFile(pathp, windows.DELETE,
					windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
					nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer windows.CloseHandle(handle)
				_, oldErr := os.ReadFile(path)
				if !errors.Is(oldErr, windows.ERROR_SHARING_VIOLATION) {
					t.Fatalf("old reader did not reject held DELETE handle: %v", oldErr)
				}
				got, err := fixedPeerWindowsWaitReplyV1(stage, sequence)
				if err != nil || got != want {
					t.Fatalf("reply with held DELETE handle: got=%+v want=%+v err=%v", got, want, err)
				}
				if repeat == 0 {
					t.Logf("sequence=%d held_DELETE_handle=%d old_reader_error=%v actual_reply=%+v", sequence, handle, oldErr, got)
				}
			}()
		}
	}
}
