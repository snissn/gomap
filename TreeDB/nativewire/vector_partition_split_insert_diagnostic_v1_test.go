package nativewire

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// Temporary diagnostic only: root keeps this evidence outside product acceptance.
func init() {
	dir := os.Getenv("GOMAP_P4_WAL_TRACE_DIR")
	if dir == "" || os.Getenv("GOMAP_FIXED_PEER_TEST_CONFIG_FILE") == "" {
		return
	}
	durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point != durabilitycut.AfterAppliedLSNAdvance && !(e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyAppend) {
			return nil
		}
		raw, err := json.Marshal(struct {
			Point durabilitycut.Point
			LSN   uint64
			Root  string
			Stack string
		}{e.Point, e.LSN, e.Root, string(debug.Stack())})
		if err != nil {
			return err
		}
		f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("wal-trace-%d.jsonl", os.Getpid())), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		_, err = f.Write(append(raw, '\n'))
		return errors.Join(err, f.Close())
	})
}
