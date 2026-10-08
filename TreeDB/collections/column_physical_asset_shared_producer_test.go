package collections

import (
	"bytes"
	"errors"
	"os"
	"runtime"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func sharedColumnProducerTestDB(t *testing.T) (*backenddb.DB, string, ColumnStoreConfig) {
	t.Helper()
	if !rootpublication.StableNamespaceCreationSupported() {
		t.Skip("exact stable namespace creation unsupported")
	}
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d, d.ColumnAssetRootDir(), stableColumnAppendTestConfig("shared-row-producer")
}
func sharedColumnProducerTestAppend(t *testing.T, d *backenddb.DB, root string, cfg ColumnStoreConfig, payload []byte, validate func(*rootpublication.StableResourceSet) error) (ColumnAssetRef, *rootpublication.StableResourceSet, *columnPhysicalAssetSegmentAppender, error) {
	t.Helper()
	s := newColumnPhysicalAssetAppendSessionWithStableResources(root, cfg, d.StableResourceIdentityPinRegistry())
	s.producerDB = d
	refs, err := s.appendKinds(columnAssetM12ASegmentFileID, []columnPhysicalAssetAppendItem{{payload: payload, kind: ColumnAssetKindTCS1PartImage, generation: 7, partID: columnPhysicalRowAssetPartID}})
	if err != nil {
		_ = s.abort()
		return ColumnAssetRef{}, nil, nil, err
	}
	a := s.active
	_, set, err := s.closeWithStableResourcesValidated(validate)
	return refs[0], set, a, err
}

func TestColumnSharedProducerReusesInstalledHandleAndCapturedReadSurvivesClose(t *testing.T) {
	d, root, cfg := sharedColumnProducerTestDB(t)
	first := []byte("first independently captured physical row")
	ref1, set1, a1, err := sharedColumnProducerTestAppend(t, d, root, cfg, first, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer set1.Release()
	if !a1.producerInstalled || a1.pendingProducer != nil {
		t.Fatal("successful constructor did not transfer actual producer edge")
	}
	path, err := columnAssetSegmentPath(root, ref1)
	if err != nil {
		t.Fatal(err)
	}
	stripe := rootpublication.SegmentWriteStripeIndex(path)
	incarnation, err := d.ColumnSegmentProducerIdentityV1(ref1.Namespace, ref1.FileID, stripe)
	if err != nil {
		t.Fatal(err)
	}
	second := []byte("second independently captured physical row")
	ref2, set2, a2, err := sharedColumnProducerTestAppend(t, d, root, cfg, second, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer set2.Release()
	if !a2.producerReused || a2.file != nil || a2.stableParent != nil || a2.pendingProducer != nil || a2.producerIncarnation != incarnation {
		t.Fatal("second append constructed another file/parent owner")
	}
	again, err := d.ColumnSegmentProducerIdentityV1(ref1.Namespace, ref1.FileID, stripe)
	if err != nil || again != incarnation || ref2.Offset != ref1.Offset+ref1.Length {
		t.Fatal("shared physical identity/frontier changed", again, err, ref1, ref2)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	for i, item := range []struct {
		set     *rootpublication.StableResourceSet
		ref     ColumnAssetRef
		payload []byte
	}{{set1, ref1, first}, {set2, ref2, second}} {
		tokens := item.set.Tokens()
		if len(tokens) != 1 {
			t.Fatal("logical capture lost its exact token", i)
		}
		got := make([]byte, len(item.payload))
		n, err := tokens[0].ReadAt(got, item.ref.Offset)
		if err != nil || n != len(got) || !bytes.Equal(got, item.payload) {
			t.Fatal("DB shutdown retired an independently captured reader", i, n, err, got)
		}
	}
}

func TestColumnSharedProducerValidationRefusalNeverAttachesConstructor(t *testing.T) {
	d, root, cfg := sharedColumnProducerTestDB(t)
	refused := errors.New("complete publication closure rejected")
	ref, set, a, err := sharedColumnProducerTestAppend(t, d, root, cfg, []byte("unpublished"), func(*rootpublication.StableResourceSet) error { return refused })
	if set != nil {
		defer set.Release()
	}
	if !errors.Is(err, refused) || set != nil || a.producerInstalled || a.pendingProducer != nil {
		t.Fatal("failed constructor became installed authority", err)
	}
	path, e := columnAssetSegmentPath(root, ref)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := d.ColumnSegmentProducerIdentityV1(ref.Namespace, ref.FileID, rootpublication.SegmentWriteStripeIndex(path)); e == nil {
		t.Fatal("failed constructor left writable DB slot")
	}
	if _, e := os.Stat(path); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("failed fresh constructor did not unlink exact unpublished child", e)
	}
	_, good, _, e := sharedColumnProducerTestAppend(t, d, root, cfg, []byte("later accepted"), nil)
	if e != nil {
		t.Fatal("cancelled constructor blocked a later real construction", e)
	}
	defer good.Release()
}

func TestColumnSharedProducerValidationRefusalRestoresSuffixAndPreservesOldCapture(t *testing.T) {
	d, root, cfg := sharedColumnProducerTestDB(t)
	original := []byte("durable old image")
	ref, old, _, err := sharedColumnProducerTestAppend(t, d, root, cfg, original, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Release()
	refused := errors.New("new exact logical closure rejected")
	_, rejected, a, err := sharedColumnProducerTestAppend(t, d, root, cfg, []byte("rejected new image"), func(*rootpublication.StableResourceSet) error { return refused })
	if rejected != nil {
		defer rejected.Release()
	}
	if !errors.Is(err, refused) || errors.Is(err, ErrRecoveryRequired) || rejected != nil || !a.producerReused {
		t.Fatal("ordinary exact rollback did not refuse the rejected output", err)
	}
	path, err := columnAssetSegmentPath(root, ref)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, original) {
		t.Fatal("rollback retained rejected suffix or changed older image", err, got)
	}
	later, accepted, _, err := sharedColumnProducerTestAppend(t, d, root, cfg, []byte("accepted replacement suffix"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Release()
	if later.Offset != int64(len(original)) {
		t.Fatal("rollback advanced the certified frontier", later)
	}
	got = make([]byte, len(original))
	n, err := old.Tokens()[0].ReadAt(got, ref.Offset)
	if err != nil || n != len(got) || !bytes.Equal(got, original) {
		t.Fatal("rollback invalidated older independent capture", n, err)
	}
}

// The actual append constructor has already opened/written its child when
// shutdown begins. Its reservation must remain live until checked cancellation;
// table-slot existence alone must not let Close race the retained handle.
func TestColumnSharedProducerCloseJoinsActualConstructorCancellation(t *testing.T) {
	d, root, cfg := sharedColumnProducerTestDB(t)
	session := newColumnPhysicalAssetAppendSessionWithStableResources(root, cfg, d.StableResourceIdentityPinRegistry())
	session.producerDB = d
	_, err := session.appendKinds(columnAssetM12ASegmentFileID, []columnPhysicalAssetAppendItem{{payload: []byte("constructor in flight"), kind: ColumnAssetKindTCS1PartImage, generation: 7, partID: columnPhysicalRowAssetPartID}})
	if err != nil {
		t.Fatal(err)
	}
	if session.active == nil || session.active.file == nil || session.active.producerIncarnation == 0 || session.active.producerInstalled {
		t.Fatal("fixture did not reach actual pending file constructor")
	}
	done := make(chan error, 1)
	go func() { done <- d.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for !d.IsClosing() {
		if time.Now().After(deadline) {
			t.Fatal("Close did not enter shutdown")
		}
		runtime.Gosched()
	}
	select {
	case err := <-done:
		t.Fatal("Close detached an in-flight actual constructor", err)
	default:
	}
	refused := errors.New("cancel pending constructor during shutdown")
	_, resources, closeErr := session.closeWithStableResourcesValidated(func(*rootpublication.StableResourceSet) error { return refused })
	if resources != nil {
		defer resources.Release()
	}
	if !errors.Is(closeErr, refused) {
		t.Fatal("actual constructor did not take checked cancellation", closeErr)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("Close did not join cancelled constructor", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close remained joined after exact cancellation")
	}
}
