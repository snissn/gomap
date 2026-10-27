package db

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

type packHandoffCachedLog struct{ LeafPageLog }

func (*packHandoffCachedLog) CompactStorageCachedWrapperOwner() bool { return true }

type packHandoffObservedLog struct {
	packHandoffCachedLog
	handoff func(context.Context, *transactionowner.Owner, []LeafPageLogPackSource) error
}

func (l *packHandoffObservedLog) HandoffPrivateCOWLeafPackSources(ctx context.Context, owner *transactionowner.Owner, sources []LeafPageLogPackSource) error {
	return l.handoff(ctx, owner, sources)
}

func TestPrivateCOWLeafPackSourceHandoffAdmissionAndExactWitness(t *testing.T) {
	d, leafLog, _ := openLeafGenerationPackTestDB(t)
	prepareLeafGenerationPackTestCandidate(t, d, leafLog, 1024)
	owner := transactionowner.New()
	if err := d.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	d.nativeSemanticActive.Store(true)
	originalLog := d.leafPageLog
	originalStaging := makeLeafGenerationPackStagingDirFn
	stagingCalls := 0
	makeLeafGenerationPackStagingDirFn = func(string, string) (string, error) {
		stagingCalls++
		return "", errors.New("copy must not start")
	}
	t.Cleanup(func() { makeLeafGenerationPackStagingDirFn = originalStaging })
	calls := 0
	failure := errors.New("stop at exact original append owner")
	observed := &packHandoffObservedLog{packHandoffCachedLog: packHandoffCachedLog{LeafPageLog: originalLog}}
	observed.handoff = func(ctx context.Context, gotOwner *transactionowner.Owner, sources []LeafPageLogPackSource) error {
		calls++
		if gotOwner != owner || len(sources) == 0 {
			t.Fatalf("owner=%p sources=%v", gotOwner, sources)
		}
		set := d.valueLogManager.CurrentSetNoRefresh()
		defer d.valueLogManager.Release(set)
		for _, source := range sources {
			file := set.Files[source.FileID]
			if file == nil {
				t.Fatalf("source %d is not a retained file ID", source.FileID)
			}
			identity, err := rootpublication.StableIdentityFromFile(file.File)
			if err != nil || identity != source.Identity {
				t.Fatalf("wrong exact handle witness: %+v err=%v", source, err)
			}
		}
		return failure
	}
	// The normal record-length wrapper must preserve the originally installed
	// owner's capability, rather than silently treating it as a backend writer.
	d.leafPageLog = &leafPageLogWithRecordLengthHints{db: d, inner: observed}
	t.Cleanup(func() { d.leafPageLog = originalLog })
	before, _ := d.StateToken()
	opts := LeafGenerationPackFromPlanOptions{MaxGenerations: 1, Force: true, Sync: true}

	if _, err := d.leafGenerationPackLocked(context.Background(), LeafGenerationPackOptions{privateCOWOwner: owner}, LeafGenerationPlan{}, LeafGenerationPackStats{}, nil); err != nil || calls != 0 {
		t.Fatalf("zero-source handoff calls=%d err=%v", calls, err)
	}
	declined := opts
	declined.Force = false
	declined.MinCandidateGenerations = 1000
	if stats, err := d.LeafGenerationPackRunOnceForPrivateCOW(owner, context.Background(), declined); err != nil || stats.Ran || calls != 0 {
		t.Fatalf("declined plan invoked handoff: calls=%d stats=%+v err=%v", calls, stats, err)
	}
	originalProbe := leafGenerationPackPromotionTargetProbe
	probeFailure := errors.New("target preflight rejected")
	leafGenerationPackPromotionTargetProbe = func(*leafGenerationPackPromotionAuthority) error { return probeFailure }
	_, err := d.LeafGenerationPackRunOnceForPrivateCOW(owner, context.Background(), opts)
	leafGenerationPackPromotionTargetProbe = originalProbe
	if !errors.Is(err, probeFailure) || calls != 0 {
		t.Fatalf("preflight handoff calls=%d err=%v", calls, err)
	}
	if _, err := d.LeafGenerationPackRunOnceForPrivateCOW(transactionowner.New(), context.Background(), opts); !errors.Is(err, ErrNativeSemanticUnsupported) || calls != 0 {
		t.Fatalf("wrong owner handoff calls=%d err=%v", calls, err)
	}
	stats, err := d.LeafGenerationPackRunOnceForPrivateCOW(owner, context.Background(), opts)
	if !errors.Is(err, failure) || calls != 1 || stats.Ran || stats.Pack.CopyAttempts != 0 {
		t.Fatalf("handoff refusal outcome: calls=%d stats=%+v err=%v", calls, stats, err)
	}
	after, _ := d.StateToken()
	if after != before {
		t.Fatalf("refused handoff published state: before=%+v after=%+v", before, after)
	}
	expectLeafGenerationValue(t, d, leafGenerationKey("pack-concurrent", 768), 'a')

	// A cached owner must explicitly support the handoff; it cannot be bypassed
	// merely because its ordinary append methods remain available.
	d.leafPageLog = &packHandoffCachedLog{LeafPageLog: originalLog}
	stats, err = d.LeafGenerationPackRunOnceForPrivateCOW(owner, context.Background(), opts)
	if err == nil || stats.Ran || stats.Pack.CopyAttempts != 0 {
		t.Fatalf("missing owner capability admitted copy: %+v err=%v", stats, err)
	}
	after, _ = d.StateToken()
	if after != before {
		t.Fatal("missing owner capability published state")
	}
	if stagingCalls != 0 {
		t.Fatalf("pre-handoff failure started %d private copies", stagingCalls)
	}
}
