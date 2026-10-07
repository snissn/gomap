package valuelog

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// The admission barrier exists only on the repaired source, so this companion
// is not part of the additive pre-repair causal RED overlay.
func TestManagerDirectRetirementQueuedReplacement(t *testing.T) {
	for _, mode := range []string{"RemoveSegment", "RemoveSegmentExpectedIdentity", "RemoveSegmentIfUnpinned", "RemoveSegmentForce"} {
		t.Run(mode, func(t *testing.T) {
			manager, file := directRetirementManager(t, false)
			identity := retirementIdentityAtPath(t, file.Path)
			preparedDir := t.TempDir()
			writeTestSegment(t, preparedDir, 0, 1, 2, []byte("queued successor must survive"))
			preparedPath := filepath.Join(preparedDir, filepath.Base(file.Path))
			if rootpublication.SamePhysicalIdentity(identity, retirementIdentityAtPath(t, preparedPath)) {
				t.Fatal("queued successor reused the still-live original identity")
			}
			wantBytes, err := os.ReadFile(preparedPath)
			if err != nil {
				t.Fatal(err)
			}
			firstAtUnlink, secondAdmitted := make(chan struct{}), make(chan struct{})
			resumeFirst := make(chan struct{})
			var resumeOnce sync.Once
			resume := func() { resumeOnce.Do(func() { close(resumeFirst) }) }
			var admissions, unlinks atomic.Int32
			manager.directDeleteAdmissionHook = func() {
				if admissions.Add(1) == 2 {
					close(secondAdmitted)
				}
			}
			originalRemove := removeSegmentPath
			removeSegmentPath = func(path string) error {
				if unlinks.Add(1) != 1 {
					return os.ErrExist
				}
				close(firstAtUnlink)
				<-resumeFirst
				if err := originalRemove(path); err != nil {
					return err
				}
				// First removal has physically succeeded. Its delete mutex is
				// still held until this callback returns; publish a different
				// canonical inode before the queued second operation may enter.
				// The successor was created while the original inode still existed,
				// so the filesystem cannot recycle the original identity here.
				return os.Rename(preparedPath, file.Path)
			}
			firstDone, secondDone := make(chan error, 1), make(chan error, 1)
			firstJoined, secondStarted, secondJoined := false, false, false
			t.Cleanup(func() {
				resume()
				for _, pending := range []struct {
					needed bool
					done   <-chan error
				}{{!firstJoined, firstDone}, {secondStarted && !secondJoined, secondDone}} {
					if pending.needed {
						select {
						case <-pending.done:
						case <-time.After(5 * time.Second):
							t.Error("queued direct deletion cleanup did not join")
						}
					}
				}
				removeSegmentPath = originalRemove
			})
			go func() { firstDone <- retirementIdentityRemove(manager, file, identity, mode) }()
			select {
			case <-firstAtUnlink:
			case <-time.After(5 * time.Second):
				t.Fatal("first real direct call never reached physical unlink")
			}
			secondStarted = true
			go func() { secondDone <- retirementIdentityRemove(manager, file, identity, mode) }()
			select {
			case <-secondAdmitted:
			case err := <-secondDone:
				secondJoined = true
				t.Fatalf("second real direct call did not retain admission: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("second real direct call was not admitted")
			}
			resume()
			select {
			case err := <-firstDone:
				firstJoined = true
				if err != nil {
					t.Fatalf("first direct physical success failed: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("first direct call did not join")
			}
			select {
			case err := <-secondDone:
				secondJoined = true
				if err != nil {
					t.Fatalf("queued direct call did not observe completed original deletion: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("queued direct call did not join")
			}
			directRetirementAssertState(t, manager, file, false)
			gotBytes, err := os.ReadFile(file.Path)
			if err != nil || !bytes.Equal(gotBytes, wantBytes) || unlinks.Load() != 1 {
				t.Fatalf("queued completion changed canonical successor: %v", err)
			}
			replacementIdentity := retirementIdentityAtPath(t, file.Path)
			if rootpublication.SamePhysicalIdentity(identity, replacementIdentity) {
				t.Fatal("queued fixture did not create a new physical identity")
			}
			if err := manager.Refresh(); err != nil {
				t.Fatalf("successful original deletion did not permit new registration: %v", err)
			}
			if !manager.HasSegment(file.ID) || !rootpublication.SamePhysicalIdentity(replacementIdentity, retirementIdentityAtPath(t, file.Path)) {
				t.Fatal("new registration changed preserved successor")
			}
		})
	}
}
