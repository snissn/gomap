package valuelog

import (
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func originalTerminalFixture(t *testing.T) (*Manager, *File, *rootpublication.IdentityPinRegistry) {
	t.Helper()
	dir := t.TempDir()
	id := page.ValueLogFileID(1)
	w, err := NewWriter(SegmentPath(dir, id), id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Append(0, nil, 1, []byte("original physical file")); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	registry := rootpublication.NewIdentityPinRegistry()
	m, err := NewManagerWithStableResourcePinRegistry(dir, registry)
	if err != nil {
		t.Fatal(err)
	}
	f := m.files[id]
	if f == nil {
		t.Fatal("missing original production file")
	}
	return m, f, registry
}

func TestOriginalManagerHeldSetDefersPhysicalCloseAndRefund(t *testing.T) {
	m, f, registry := originalTerminalFixture(t)
	set := m.CurrentSetNoRefresh()
	charge := registry.MetadataOwner().Bytes()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if f.closed.Load() || m.files[f.ID] != f || m.producerCharge == 0 || f.producerCharge == 0 {
		t.Fatal("terminal request consumed original held physical payload")
	}
	if registry.MetadataOwner().Bytes() != charge {
		t.Fatal("terminal request refunded held payload")
	}
	if _, err := f.File.Stat(); err != nil {
		t.Fatal(err)
	}
	if err := m.Release(set); err != nil {
		t.Fatal(err)
	}
	if !f.closed.Load() || len(m.files) != 0 || m.producerCharge != 0 || f.producerCharge != 0 {
		t.Fatal("successful actual last set release did not consume original payload")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOriginalManagerActualLastSetCloseFailureIsSticky(t *testing.T) {
	m, f, registry := originalTerminalFixture(t)
	set := m.CurrentSetNoRefresh()
	charge := registry.MetadataOwner().Bytes()
	managerCharge, fileCharge := m.producerCharge, f.producerCharge
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.File.Close(); err != nil {
		t.Fatal(err)
	}
	err := m.Release(set)
	var receipt *ManagerCleanupFailure
	if !errors.Is(err, os.ErrClosed) || !errors.As(err, &receipt) || receipt.manager != m || m.files[f.ID] != f {
		t.Fatalf("actual late refusal lost original Manager/File: %v", err)
	}
	if registry.MetadataOwner().Bytes() < managerCharge+fileCharge || m.producerCharge != managerCharge || f.producerCharge != fileCharge {
		t.Fatal("actual failed disposal refunded original payload")
	}
	charge = registry.MetadataOwner().Bytes() // independently consumed registry observation may refund
	fileErr := f.Close()
	var repeated *ManagerCleanupFailure
	if !errors.Is(fileErr, os.ErrClosed) || f.Close() != fileErr || !errors.As(m.Close(), &repeated) || repeated != receipt {
		t.Fatal("repeated terminal observation changed/replayed original physical outcome")
	}
	if registry.MetadataOwner().Bytes() != charge {
		t.Fatal("repeated failure refunded payload")
	}
}

func TestOriginalManagerTerminalRequestRacesLastSetRelease(t *testing.T) {
	for i := 0; i < 16; i++ {
		m, f, _ := originalTerminalFixture(t)
		set := m.CurrentSetNoRefresh()
		start := make(chan struct{})
		results := make(chan error, 2)
		go func() { <-start; results <- m.Close() }()
		go func() { <-start; results <- m.Release(set) }()
		close(start)
		if err := <-results; err != nil {
			t.Fatal(err)
		}
		if err := <-results; err != nil {
			t.Fatal(err)
		}
		if !f.closed.Load() || len(m.files) != 0 || f.producerCharge != 0 || m.producerCharge != 0 {
			t.Fatal("racing original terminal consumers missed actual disposal/refund")
		}
	}
}

func TestOriginalManagerPartialFileParentRefusalRetainsProducer(t *testing.T) {
	dir := t.TempDir()
	id := page.ValueLogFileID(1)
	w, err := NewWriter(SegmentPath(dir, id), id)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = parent.Close(); err != nil {
		t.Fatal(err)
	}
	captureCause := errors.New("original manager parent capture refusal")
	oldOpen := openStableValueLogParent
	captures := 0
	openStableValueLogParent = func(string) (*os.File, error) { captures++; return parent, captureCause }
	defer func() { openStableValueLogParent = oldOpen }()
	registry := rootpublication.NewIdentityPinRegistry()
	m, err := NewManagerWithStableResourcePinRegistry(dir, registry)
	if m != nil {
		t.Fatal("published Manager after original cleanup refusal")
	}
	var receipt *ManagerCleanupFailure
	if !errors.As(err, &receipt) || !errors.Is(err, os.ErrClosed) || !errors.Is(err, captureCause) {
		t.Fatalf("original partial constructor receipt=%v", err)
	}
	original := receipt.manager
	f := original.failedFiles
	if f == nil || f.constructionParents[0] != parent || !f.constructionParentAttempted[0] ||
		f.producerCharge == 0 || original.producerCharge == 0 || len(original.files) != 0 {
		t.Fatal("partial original File/parent/producer custody or charge lost")
	}
	before := registry.MetadataOwner().Bytes()
	if original.Close() != receipt || original.Close() != receipt || captures != 1 {
		t.Fatal("partial original terminal cleanup replayed")
	}
	if registry.MetadataOwner().Bytes() != before {
		t.Fatal("failed partial producer charge refunded")
	}
}

// The caller supplies arbitrary construction code. The temporary borrow is
// the SAME registered File; terminal exclusion must not require m.mu here.
func TestOriginalManagerPhysicalCaptureConstructorReentersClose(t *testing.T) {
	m, f, registry := originalTerminalFixture(t)
	cause := errors.New("original constructor refusal after terminal request")
	called := 0
	token, err := m.StableExistingPhysicalResourceToken(f.ID, rootpublication.StableResourceSpec{
		MetadataOwner: registry.MetadataOwner(), DiagnosticPath: "maindb/value_vlog/" + f.File.Name(),
	}, func(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
		called++
		if e := m.Close(); e != nil {
			t.Fatal(e)
		}
		if spec.File != f.File || f.RefCount.Load() != 1 {
			t.Fatal("constructor did not retain original File")
		}
		if f.closed.Load() || m.files[f.ID] != f {
			t.Fatal("terminal request consumed borrowed original File")
		}
		return nil, cause
	})
	if token != nil || !errors.Is(err, cause) || called != 1 {
		t.Fatalf("capture result token=%v err=%v calls=%d", token, err, called)
	}
	if f.RefCount.Load() != 0 || !f.closed.Load() || len(m.files) != 0 || f.producerCharge != 0 || m.producerCharge != 0 {
		t.Fatal("last original temporary borrow did not finish physical cleanup")
	}
	if e := m.Close(); e != nil {
		t.Fatal(e)
	}
}
