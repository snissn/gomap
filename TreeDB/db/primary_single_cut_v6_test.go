package db

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func primarySingleCutOptionsV6(dir string) Options {
	return Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
}

// Capture ordinary physical bytes while the attempted publication is stopped;
// neither a Close nor a second fence is allowed to manufacture this image.
func primarySingleCutFilesV6(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	files := make(map[string][]byte)
	if err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		name, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		files[name], err = os.ReadFile(path)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return files
}

func primarySingleCutOldPagesV6(t *testing.T, d *DB) []bool {
	t.Helper()
	r := d.durableRoot.primary
	s := d.durableRoot.slot
	a := d.idx.Load().primary
	pages, err := a.CapturePhysicalPagesOrdinary(a.Pager().PageCount(), []primaryarena.Ref{r.records[s], r.proofs[s], r.closures[s], r.proofClosures[s]})
	if err != nil {
		t.Fatal(err)
	}
	return pages
}

func primarySingleCutAssertOldBytesV6(t *testing.T, before, after []byte, pages []bool) {
	t.Helper()
	for id, owned := range pages {
		if !owned {
			continue
		}
		lo, hi := id*page.PageSize, (id+1)*page.PageSize
		if hi > len(before) || hi > len(after) || !bytes.Equal(before[lo:hi], after[lo:hi]) {
			t.Fatalf("acknowledged physical closure page %d was overwritten", id)
		}
	}
}

func TestPrimarySingleCutV6PersistedImages(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(primarySingleCutOptionsV6(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err = d.SetSync([]byte("key"), []byte("A")); err != nil {
		t.Fatal(err)
	}
	old := d.AcquireSnapshot()
	defer old.Close()
	aSeq, aSlot := d.durableRoot.record.CommitSeq, d.durableRoot.slot
	oldPages := primarySingleCutOldPagesV6(t, d)
	aFiles := primarySingleCutFilesV6(t, dir)
	stop := errors.New("captured staged image before final fence")
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeMetaSync && filepath.Base(e.Path) == primaryIndexFileName {
			return stop
		}
		return nil
	})
	err = d.SetSync([]byte("key"), []byte("B"))
	restore()
	if !errors.Is(err, stop) || !errors.Is(err, ErrRecoveryRequired) || !d.publicationPoisoned.Load() {
		t.Fatalf("post-stage failure: %v", err)
	}
	bFiles := primarySingleCutFilesV6(t, dir)
	aImage, bImage := aFiles[primaryIndexFileName], bFiles[primaryIndexFileName]
	primarySingleCutAssertOldBytesV6(t, aImage, bImage, oldPages)
	target := aSlot ^ 1
	start, end := int(2+3*target)*page.PageSize, int(5+3*target)*page.PageSize
	oldStart := int(2+3*aSlot) * page.PageSize
	if !bytes.Equal(aImage[oldStart:oldStart+3*page.PageSize], bImage[oldStart:oldStart+3*page.PageSize]) {
		t.Fatal("staging touched the acknowledged actual capsule slot")
	}
	// Identify actual newly materialized bank operands, not assumed page IDs.
	var changed []int
	for off := 8 * page.PageSize; off+page.PageSize <= len(bImage); off += page.PageSize {
		if off+page.PageSize > len(aImage) || !bytes.Equal(aImage[off:off+page.PageSize], bImage[off:off+page.PageSize]) {
			changed = append(changed, off)
		}
	}
	if len(changed) == 0 {
		t.Fatal("B did not materialize any physical bank")
	}
	for _, tc := range []struct {
		name   string
		mutate func([]byte)
		newest bool
	}{
		{"authority-first-no-new-banks", func(image []byte) {
			for _, off := range changed {
				clear(image[off : off+page.PageSize])
			}
		}, false},
		{"authority-first-partial-bank", func(image []byte) { clear(image[changed[0] : changed[0]+page.PageSize/2]) }, false},
		{"banks-first-no-capsule", func(image []byte) { copy(image[start:end], aImage[start:end]) }, false},
		{"capsule-front-torn", func(image []byte) { copy(image[start+page.PageSize:end], aImage[start+page.PageSize:end]) }, false},
		{"capsule-back-torn", func(image []byte) { copy(image[start:start+page.PageSize], aImage[start:start+page.PageSize]) }, false},
		{"complete-unacked-closure", func([]byte) {}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			image := bytes.Clone(bImage)
			tc.mutate(image)
			if !tc.newest && bytes.Equal(image, bImage) {
				t.Fatal("torn-image fixture did not change the actual B operands")
			}
			primarySingleCutAssertOldBytesV6(t, aImage, image, oldPages)
			restored := t.TempDir()
			for name, contents := range bFiles {
				if name == primaryIndexFileName {
					contents = image
				}
				path := filepath.Join(restored, name)
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, contents, 0600); err != nil {
					t.Fatal(err)
				}
			}
			reopened, err := Open(primarySingleCutOptionsV6(restored))
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			want := "A"
			if tc.newest {
				want = "B"
			} else if reopened.durableRoot.record.CommitSeq != aSeq {
				t.Fatalf("invalid B selected commit %d want actual A %d", reopened.durableRoot.record.CommitSeq, aSeq)
			}
			if value, err := reopened.Get([]byte("key")); err != nil || string(value) != want {
				t.Fatalf("recovered value=%q want%s err=%v", value, want, err)
			}
		})
	}
	if value, err := old.Get([]byte("key")); err != nil || string(value) != "A" {
		t.Fatalf("interrupted publication lost retained reader: %q %v", value, err)
	}
}

func TestPrimarySingleCutV6HeldFenceExactPrefixAndSlotReuse(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(primarySingleCutOptionsV6(dir))
	if err != nil {
		t.Fatal(err)
	}
	allJoined := true
	defer func() {
		// Do not wait in Close behind a worker that failed its bounded join.
		if d != nil && allJoined {
			d.Close()
		}
	}()
	if err = d.SetSync([]byte("key"), []byte("A")); err != nil {
		t.Fatal(err)
	}
	old := d.AcquireSnapshot()
	defer old.Close()
	oldPages := primarySingleCutOldPagesV6(t, d)
	before := primarySingleCutFilesV6(t, dir)[primaryIndexFileName]
	aSeq := d.durableRoot.record.CommitSeq
	held, release := make(chan uint64, 1), make(chan struct{})
	staged := false
	restore := sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
		if filepath.Base(e.Path) != primaryIndexFileName {
			return nil
		}
		if e.Point == durabilitycut.AfterMetaWrite {
			staged = true
		}
		if e.Point == durabilitycut.BeforeIndexDataSync && staged {
			held <- d.rootPublication.activeSeal.record.CommitSeq
			<-release
		}
		return nil
	}))
	done := make(chan error, 1)
	joined := make(chan struct{})
	allJoined = false
	go func() { defer close(joined); done <- d.SetSync([]byte("key"), []byte("B")) }()
	releaseOnce := sync.OnceFunc(func() { close(release) })
	var successorJoined <-chan struct{}
	defer func() {
		releaseOnce()
		waitJoined := func(ch <-chan struct{}, name string) bool {
			if ch == nil {
				return true
			}
			select {
			case <-ch:
				return true
			case <-time.After(10 * time.Second):
				t.Errorf("%s did not join after releasing the final fence", name)
				return false
			}
		}
		publisherJoined := waitJoined(joined, "publisher")
		successorStopped := waitJoined(successorJoined, "successor")
		allJoined = publisherJoined && successorStopped
		restore()
	}()
	var bSeq uint64
	select {
	case bSeq = <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("publication did not reach post-stage PRIMARY fence")
	}
	if d.durableRoot.record.CommitSeq != aSeq {
		t.Fatal("held final fence advanced durable authority")
	}
	select {
	case err := <-done:
		t.Fatalf("ACK before final fence: %v", err)
	default:
	}
	// A point Set would queue behind B in the single point combiner and wait
	// for the very fence held by this test. The public batch route exercises
	// independent producer preparation without that unrelated serialization.
	successorDone := make(chan error, 1)
	successorExited := make(chan struct{})
	successorJoined = successorExited
	go func() {
		defer close(successorExited)
		b := d.NewBatch()
		err := b.Set([]byte("future"), []byte("C"))
		if err == nil {
			err = b.Write()
		}
		successorDone <- errors.Join(err, b.Close())
	}()
	select {
	case err := <-successorDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("disjoint public batch did not prepare while final fence held")
	}
	if d.State().CommitSeq <= bSeq {
		t.Fatal("disjoint successor was not prepared while sync held")
	}
	primarySingleCutAssertOldBytesV6(t, before, primarySingleCutFilesV6(t, dir)[primaryIndexFileName], oldPages)
	releaseOnce()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("publisher did not complete after final fence release")
	}
	restore()
	if d.durableRoot.record.CommitSeq != bSeq {
		t.Fatalf("final fence certified another prefix: got%d want%d", d.durableRoot.record.CommitSeq, bSeq)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	primarySingleCutAssertOldBytesV6(t, before, primarySingleCutFilesV6(t, dir)[primaryIndexFileName], oldPages)
	if value, err := old.Get([]byte("key")); err != nil || string(value) != "A" {
		t.Fatalf("third-slot reuse lost retained A: %q %v", value, err)
	}
	old.Close()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(primarySingleCutOptionsV6(dir))
	if err != nil {
		t.Fatal(err)
	}
	if value, err := d.Get([]byte("future")); err != nil || string(value) != "C" {
		t.Fatalf("ACKed complete image lost successor: %q %v", value, err)
	}
}

func TestPrimarySingleCutV6PostStageFailures(t *testing.T) {
	for _, point := range []durabilitycut.Point{durabilitycut.AfterMetaWrite, durabilitycut.BeforeIndexDataSync, durabilitycut.AfterIndexDataSync} {
		t.Run(string(point), func(t *testing.T) {
			d, err := Open(primarySingleCutOptionsV6(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if err := d.SetSync([]byte("key"), []byte("A")); err != nil {
				t.Fatal(err)
			}
			before := d.durableRoot.record
			injected := errors.New("post-stage publication failure")
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Point == point && filepath.Base(e.Path) == primaryIndexFileName {
					return injected
				}
				return nil
			})
			err = d.SetSync([]byte("key"), []byte("B"))
			restore()
			if !errors.Is(err, injected) || !errors.Is(err, ErrRecoveryRequired) || !d.publicationPoisoned.Load() || d.durableRoot.record != before {
				t.Fatalf("post-stage error lost uncertain ownership: %v", err)
			}
			image := primarySingleCutFilesV6(t, d.dir)[primaryIndexFileName]
			if err := d.SetSync([]byte("after"), []byte("refused")); !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("uncertain publication admitted successor: %v", err)
			}
			if !bytes.Equal(image, primarySingleCutFilesV6(t, d.dir)[primaryIndexFileName]) {
				t.Fatal("poisoned successor changed physical authority")
			}
		})
	}
}

func TestPrimarySingleCutV6FinalFenceUsesCapturedFile(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact relative namespace replacement control unsupported")
	}
	dir := t.TempDir()
	d, err := Open(primarySingleCutOptionsV6(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.SetSync([]byte("key"), []byte("A")); err != nil {
		t.Fatal(err)
	}
	path, moved := filepath.Join(dir, primaryIndexFileName), filepath.Join(dir, "primary-retained-inode")
	original, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	foreign := []byte("foreign pathname must not receive the PRIMARY fence")
	rebound, completed := false, 0
	putBack := func() error {
		if !rebound {
			return nil
		}
		if err := os.Remove(path); err != nil {
			return err
		}
		if err := os.Rename(moved, path); err != nil {
			return err
		}
		rebound = false
		return nil
	}
	defer func() {
		if err := putBack(); err != nil {
			t.Error(err)
		}
	}()
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if filepath.Base(e.Path) != primaryIndexFileName {
			return nil
		}
		if e.Point == durabilitycut.AfterMetaWrite {
			if err := os.Rename(path, moved); err != nil {
				return err
			}
			rebound = true
			return os.WriteFile(path, foreign, 0600)
		}
		if e.Point == durabilitycut.AfterIndexDataSync {
			if !rebound || e.StablePath == nil {
				return errors.New("final fence did not follow capsule stage on a retained handle")
			}
			actual, err := e.StablePath.Stat()
			if err != nil || !os.SameFile(original, actual) {
				return errors.New("final fence used rebound pathname")
			}
			bytes, err := os.ReadFile(path)
			if err != nil || string(bytes) != string(foreign) {
				return errors.New("fence mutated foreign pathname")
			}
			completed++
			// Restore the unchanged original namespace before its required
			// stabilization; this test isolates content-handle selection.
			return putBack()
		}
		return nil
	})
	err = d.SetSync([]byte("key"), []byte("B"))
	restore()
	if err != nil || completed != 1 {
		t.Fatalf("captured final fence: completed%d err%v", completed, err)
	}
}
