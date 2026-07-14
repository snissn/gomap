package db

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type leafGenerationPackStablePrepareResult struct {
	closure *LeafGenerationPackStablePreparedClosure
	err     error
}

func buildLeafGenerationPackStablePage(t testing.TB, tag byte) []byte {
	t.Helper()
	buf := make([]byte, page.PageSize)
	builder := node.NewBuilderWithOptions(buf, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	for i := 0; i < 4; i++ {
		key := []byte{'p', 'a', 'c', 'k', '-', tag, '-', byte('a' + i)}
		value := []byte{'v', 'a', 'l', 'u', 'e', '-', tag, '-', byte('a' + i)}
		if err := builder.AddLeafEntry(key, value, node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry(%d): %v", i, err)
		}
	}
	builder.FinishNoNode()
	return buf
}

func TestPrepareLeafGenerationPackStableClosureCloseAdmission(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported() {
		t.Skip("stable packed promotion requires exact relative cross-parent namespace support")
	}
	leafPages := [][]byte{
		buildLeafGenerationPackStablePage(t, 'a'),
		buildLeafGenerationPackStablePage(t, 'b'),
	}

	t.Run("close-wins-before-admission", func(t *testing.T) {
		database, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
		if err != nil {
			t.Fatal(err)
		}
		registry := database.StableResourceIdentityPinRegistry()
		if got := registry.Stats(); got != (rootpublication.IdentityPinRegistryStats{}) {
			_ = database.Close()
			t.Fatalf("initial stable registry stats=%+v want zero", got)
		}
		if err := database.RunCloseHooks(); err != nil {
			_ = database.Close()
			t.Fatalf("RunCloseHooks: %v", err)
		}

		paused := make(chan struct{})
		resume := make(chan struct{})
		var pauseOnce sync.Once
		restore := setLeafGenerationPackStableBeforeCaptureAdmissionTestHook(func() {
			pauseOnce.Do(func() { close(paused) })
			<-resume
		})
		defer restore()

		prepared := make(chan leafGenerationPackStablePrepareResult, 1)
		go func() {
			closure, err := database.PrepareLeafGenerationPackStableClosure(context.Background(), leafPages)
			prepared <- leafGenerationPackStablePrepareResult{closure: closure, err: err}
		}()
		awaitLeafGenerationPackStablePrepareSignal(t, paused, "pre-admission pause")

		closed := make(chan error, 1)
		go func() { closed <- database.Close() }()
		select {
		case err := <-closed:
			if err != nil {
				close(resume)
				t.Fatalf("Close: %v", err)
			}
		case <-time.After(10 * time.Second):
			close(resume)
			t.Fatal("Close did not complete while packed preparation was paused before admission")
		}
		close(resume)

		result := awaitLeafGenerationPackStablePrepareResult(t, prepared)
		if result.closure != nil {
			_ = result.closure.Release()
			t.Fatal("pre-admission Close race returned a closure")
		}
		if !errors.Is(result.err, ErrClosed) {
			t.Fatalf("pre-admission Close race error=%v want ErrClosed", result.err)
		}
		if got := registry.Stats(); got != (rootpublication.IdentityPinRegistryStats{}) {
			t.Fatalf("close-wins stable registry stats=%+v want zero", got)
		}
	})

	t.Run("admitted-prepare-drains", func(t *testing.T) {
		database, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
		if err != nil {
			t.Fatal(err)
		}
		registry := database.StableResourceIdentityPinRegistry()
		if err := database.RunCloseHooks(); err != nil {
			_ = database.Close()
			t.Fatalf("RunCloseHooks: %v", err)
		}

		preparedAuthority := make(chan struct{})
		resume := make(chan struct{})
		var pauseOnce sync.Once
		restore := setLeafGenerationPackStablePreparedClosureTestHook(func() {
			pauseOnce.Do(func() { close(preparedAuthority) })
			<-resume
		})
		defer restore()

		prepared := make(chan leafGenerationPackStablePrepareResult, 1)
		go func() {
			closure, err := database.PrepareLeafGenerationPackStableClosure(context.Background(), leafPages)
			prepared <- leafGenerationPackStablePrepareResult{closure: closure, err: err}
		}()
		awaitLeafGenerationPackStablePrepareSignal(t, preparedAuthority, "stable packed authority capture")
		stats := registry.Stats()
		if stats.ActivePins == 0 || stats.ActiveIdentities == 0 || stats.ActiveStableNamespaceLinks == 0 {
			close(resume)
			t.Fatalf("admitted packed capture stats=%+v want live pins, identities, and namespace proofs", stats)
		}

		closed := make(chan error, 1)
		go func() { closed <- database.Close() }()
		deadline := time.Now().Add(10 * time.Second)
		for !database.IsClosing() && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if !database.IsClosing() {
			close(resume)
			t.Fatal("Close did not reach teardown admission while packed capture lease was held")
		}
		select {
		case err := <-closed:
			close(resume)
			t.Fatalf("Close returned before admitted packed capture drained: %v", err)
		default:
		}
		close(resume)

		result := awaitLeafGenerationPackStablePrepareResult(t, prepared)
		if result.err != nil || result.closure == nil {
			t.Fatalf("admitted packed preparation closure=%v err=%v", result.closure, result.err)
		}
		select {
		case err := <-closed:
			if err != nil {
				_ = result.closure.Release()
				t.Fatalf("Close after packed capture drain: %v", err)
			}
		case <-time.After(10 * time.Second):
			_ = result.closure.Release()
			t.Fatal("Close did not complete after admitted packed capture drained")
		}
		postClose := registry.Stats()
		if postClose.ActivePins == 0 || postClose.ActiveIdentities == 0 || postClose.ActiveStableNamespaceLinks != 0 {
			_ = result.closure.Release()
			t.Fatalf("post-Close retained packed closure stats=%+v want retained identities but cleared DB-lifetime namespace proofs", postClose)
		}
		if len(result.closure.Segments()) == 0 || len(result.closure.Pointers()) != len(leafPages) {
			_ = result.closure.Release()
			t.Fatalf("post-Close packed closure segments=%d pointers=%d want retained physical authority", len(result.closure.Segments()), len(result.closure.Pointers()))
		}
		observed := result.closure.Observations()
		if observed.Segments == 0 || observed.ContentSyncs == 0 || observed.NamespaceSyncs == 0 || observed.NamespaceObligations == 0 {
			_ = result.closure.Release()
			t.Fatalf("post-Close packed closure observations=%+v want complete producer evidence", observed)
		}
		if err := result.closure.Release(); err != nil {
			t.Fatalf("release retained packed closure after DB close: %v", err)
		}
		if got := registry.Stats(); got != (rootpublication.IdentityPinRegistryStats{}) {
			t.Fatalf("packed registry after retained closure release=%+v want zero", got)
		}

		lateClosure, err := database.PrepareLeafGenerationPackStableClosure(context.Background(), leafPages)
		if lateClosure != nil {
			_ = lateClosure.Release()
			t.Fatal("post-Close packed preparation returned a closure")
		}
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("post-Close packed preparation error=%v want ErrClosed", err)
		}
		if got := registry.Stats(); got != (rootpublication.IdentityPinRegistryStats{}) {
			t.Fatalf("post-Close rejected packed capture registry stats=%+v want zero", got)
		}
	})
}

func awaitLeafGenerationPackStablePrepareSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func awaitLeafGenerationPackStablePrepareResult(t *testing.T, result <-chan leafGenerationPackStablePrepareResult) leafGenerationPackStablePrepareResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for stable packed preparation result")
		return leafGenerationPackStablePrepareResult{}
	}
}

func TestLeafGenerationPackStablePreparedClosureTransfersExactAuthorityOnce(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported() {
		t.Skip("stable packed promotion requires exact relative cross-parent namespace support")
	}
	database, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	registry := database.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	baselineIdentities := registry.ActiveIdentities()
	before := *database.State()

	closure, err := database.PrepareLeafGenerationPackStableClosure(context.Background(), [][]byte{
		buildLeafGenerationPackStablePage(t, 'c'),
		buildLeafGenerationPackStablePage(t, 'd'),
	})
	if err != nil {
		t.Fatalf("PrepareLeafGenerationPackStableClosure: %v", err)
	}
	after := *database.State()
	if after.CommitSeq != before.CommitSeq || after.SystemRootPageID != before.SystemRootPageID || after.AppliedCommandLSN != before.AppliedCommandLSN {
		closure.Release()
		t.Fatalf("packed preparation published visibility before=%+v after=%+v", before, after)
	}
	segments := closure.Segments()
	pointers := closure.Pointers()
	observed := closure.Observations()
	if len(segments) == 0 || len(pointers) != 2 || observed.Segments != uint64(len(segments)) {
		closure.Release()
		t.Fatalf("segments=%d pointers=%d observations=%+v", len(segments), len(pointers), observed)
	}
	if observed.ContentSyncs == 0 || observed.NamespaceSyncs != 2 || observed.NamespaceObligations != observed.Segments {
		closure.Release()
		t.Fatalf("producer observations=%+v want content>0 two physical parent syncs and one obligation per segment", observed)
	}
	if got := registry.ActivePins(); got != baselinePins+observed.Segments {
		closure.Release()
		t.Fatalf("active pins after packed prepare=%d want %d", got, baselinePins+observed.Segments)
	}
	for _, segment := range segments {
		if _, registered := database.valueLogManager.StableSegmentIdentity(segment.FileID); registered {
			closure.Release()
			t.Fatalf("prepared segment %d became manager-visible", segment.FileID)
		}
	}

	resources, err := closure.TakeStableResources()
	if err != nil {
		closure.Release()
		t.Fatalf("TakeStableResources: %v", err)
	}
	descriptors := make(map[uint64]rootpublication.StableResourceDescriptor, len(segments))
	for _, descriptor := range resources.Descriptors() {
		if descriptor.Kind() == rootpublication.ResourceOuterLeafPack {
			descriptors[descriptor.Generation()] = descriptor
		}
	}
	if len(descriptors) != len(segments) {
		resources.Release()
		t.Fatalf("packed descriptors=%d want promoted segments=%d", len(descriptors), len(segments))
	}
	for _, segment := range segments {
		descriptor, ok := descriptors[uint64(segment.FileID)]
		if !ok || descriptor.Identity().Generation != uint64(segment.FileID) {
			resources.Release()
			t.Fatalf("packed segment %d has no generation-bound descriptor", segment.FileID)
		}
		promoted, openErr := os.Open(segment.Path)
		if openErr != nil {
			resources.Release()
			t.Fatalf("open promoted segment %q: %v", segment.Path, openErr)
		}
		identity, identityErr := rootpublication.StableIdentityFromFile(promoted)
		closeErr := promoted.Close()
		if identityErr != nil || closeErr != nil || descriptor.Identity() == identity || !rootpublication.SamePhysicalIdentity(descriptor.Identity(), identity) {
			resources.Release()
			t.Fatalf("packed segment %d descriptor identity must be generation-bound and physically match exact promoted child: identity_err=%v close_err=%v", segment.FileID, identityErr, closeErr)
		}
	}
	if _, err := closure.TakeStableResources(); !errors.Is(err, ErrLeafGenerationPackStablePreparedClosureConsumed) {
		resources.Release()
		t.Fatalf("second TakeStableResources error=%v want consumed", err)
	}
	closure.Release()
	closure.Abandon()
	resources.Release()
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("active pins after release=%d want %d", got, baselinePins)
	}
	if got := registry.ActiveIdentities(); got != baselineIdentities {
		t.Fatalf("active identities after release=%d want %d", got, baselineIdentities)
	}
	for _, segment := range segments {
		if _, err := os.Stat(segment.Path); err != nil {
			t.Fatalf("transferred segment %q no longer exists: %v", segment.Path, err)
		}
	}
}

func TestLeafGenerationPackStablePreparedClosureAbandonRemovesPromotedChildren(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported() {
		t.Skip("stable packed promotion requires exact relative cross-parent namespace support")
	}
	database, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	registry := database.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	baselineIdentities := registry.ActiveIdentities()
	closure, err := database.PrepareLeafGenerationPackStableClosure(context.Background(), [][]byte{
		buildLeafGenerationPackStablePage(t, 'e'),
		buildLeafGenerationPackStablePage(t, 'f'),
	})
	if err != nil {
		t.Fatal(err)
	}
	segments := closure.Segments()
	if len(segments) == 0 {
		t.Fatal("packed preparation returned no promoted children")
	}
	if err := closure.Abandon(); err != nil {
		t.Fatalf("Abandon: %v", err)
	}
	if err := closure.Release(); err != nil {
		t.Fatalf("idempotent Release: %v", err)
	}
	if _, err := closure.TakeStableResources(); !errors.Is(err, ErrLeafGenerationPackStablePreparedClosureConsumed) {
		t.Fatalf("TakeStableResources after abandon error=%v want consumed", err)
	}
	for _, segment := range segments {
		if _, err := os.Stat(segment.Path); !os.IsNotExist(err) {
			t.Fatalf("abandoned promoted segment %q still exists: %v", segment.Path, err)
		}
	}
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("active pins after abandon=%d want %d", got, baselinePins)
	}
	if got := registry.ActiveIdentities(); got != baselineIdentities {
		t.Fatalf("active identities after abandon=%d want %d", got, baselineIdentities)
	}
}

func TestLeafGenerationPackStablePrepareLateCleanupErrorReturnsNoAuthority(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported() {
		t.Skip("stable packed promotion requires exact relative cross-parent namespace support")
	}
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	registry := database.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	baselineIdentities := registry.ActiveIdentities()
	sentinel := errors.New("injected late staging cleanup failure")
	originalRemove := removeLeafGenerationPackStagingDirFn
	removeLeafGenerationPackStagingDirFn = func(string) error { return sentinel }
	t.Cleanup(func() { removeLeafGenerationPackStagingDirFn = originalRemove })
	closure, err := database.PrepareLeafGenerationPackStableClosure(context.Background(), [][]byte{
		buildLeafGenerationPackStablePage(t, 'g'),
	})
	if closure != nil {
		_ = closure.Release()
		t.Fatal("late cleanup failure returned non-nil authority")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("PrepareLeafGenerationPackStableClosure error=%v want injected cleanup failure", err)
	}
	entries, err := os.ReadDir(resolveStorageLayout(dir).leafVLogDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".vlog" {
			t.Fatalf("late cleanup failure leaked promoted child %q", entry.Name())
		}
	}
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("active pins after late cleanup failure=%d want %d", got, baselinePins)
	}
	if got := registry.ActiveIdentities(); got != baselineIdentities {
		t.Fatalf("active identities after late cleanup failure=%d want %d", got, baselineIdentities)
	}
}

func TestLeafGenerationPackStablePrepareUnsupportedFailsBeforeVisibility(t *testing.T) {
	if rootpublication.StableRelativeNamespaceSupported() && rootpublication.StableCrossParentMoveNoReplaceSupported() {
		t.Skip("platform supports packed promotion; positive lifecycle test covers this path")
	}
	database, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	before := *database.State()
	closure, err := database.PrepareLeafGenerationPackStableClosure(context.Background(), [][]byte{[]byte("packed")})
	if closure != nil {
		closure.Release()
		t.Fatal("unsupported packed prepare returned a closure")
	}
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("PrepareLeafGenerationPackStableClosure error=%v want typed namespace unsupported", err)
	}
	after := *database.State()
	if after.CommitSeq != before.CommitSeq || after.SystemRootPageID != before.SystemRootPageID || after.AppliedCommandLSN != before.AppliedCommandLSN {
		t.Fatalf("unsupported packed prepare published visibility before=%+v after=%+v", before, after)
	}
}
