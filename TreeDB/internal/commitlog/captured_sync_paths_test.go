package commitlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// Witness strings remain lexical diagnostics across rotation and reopen. The
// captured file, rather than either string, remains the physical sync authority.
func TestCapturedSyncWitnessPathsAcrossRotationAndReopen(t *testing.T) {
	for _, spelling := range []string{"clean", "trailing_separator", "dot_component"} {
		t.Run(spelling, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "wal")
			walDir := dir
			switch spelling {
			case "trailing_separator":
				walDir += string(os.PathSeparator)
			case "dot_component":
				walDir += string(os.PathSeparator) + "."
			}
			wantRoot := filepath.Dir(walDir)
			journal, err := OpenCommandJournal(walDir, CommandJournalOptions{CaptureStableResources: true, SegmentTargetBytes: 4096})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = journal.Close() })
			var lastLSN uint64
			for stage := 0; stage < 3; stage++ {
				if stage == 1 {
					rotation, err := journal.RotateActiveSegmentWithStableResources(true)
					if err != nil {
						t.Fatal(err)
					}
					rotation.Release()
				} else if stage == 2 {
					if err := journal.Close(); err != nil {
						t.Fatal(err)
					}
					journal, err = OpenCommandJournal(walDir, CommandJournalOptions{CaptureStableResources: true, SegmentTargetBytes: 4096})
					if err != nil {
						t.Fatal(err)
					}
				}
				lastLSN, err = journal.AppendCommand(fixedWALTestBarrier())
				if err != nil {
					t.Fatal(err)
				}
				target, err := journal.CaptureSync(nil)
				if err != nil {
					t.Fatal(err)
				}
				func() {
					defer target.Release()
					wantPath := filepath.Join(walDir, CommandSegmentName(0, journal.segmentSeq))
					var seen []durabilitycut.Point
					restore := durabilitycut.Install(func(event durabilitycut.Event) error {
						if event.Resource != durabilitycut.ResourceCommandWAL ||
							(event.Point != durabilitycut.BeforeDependencyFileSync && event.Point != durabilitycut.AfterDependencyFileSync) {
							return nil
						}
						if event.Root != wantRoot || event.Path != wantPath || event.LSN != lastLSN || event.StablePath == nil {
							t.Errorf("stage %d changed witness path/LSN: %+v; root=%q path=%q LSN=%d", stage, event, wantRoot, wantPath, lastLSN)
						}
						if err := target.active.token.WithPinnedFile(func(file *os.File) error {
							if event.StablePath != file {
								t.Errorf("stage %d changed exact captured handle", stage)
							}
							return nil
						}); err != nil {
							t.Error(err)
						}
						seen = append(seen, event.Point)
						return nil
					})
					defer restore()
					if err := target.Sync(); err != nil {
						t.Fatal(err)
					}
					if len(seen) != 2 || seen[0] != durabilitycut.BeforeDependencyFileSync || seen[1] != durabilitycut.AfterDependencyFileSync {
						t.Fatalf("stage %d physical sync witnesses=%v", stage, seen)
					}
				}()
			}
		})
	}
}
