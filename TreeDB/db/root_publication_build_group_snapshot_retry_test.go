package db

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestRootPublicationSnapshotRetryRequiresExactCapturedCoverage(t *testing.T) {
	c, err := rootpublication.New(rootpublication.Options{
		StartupSynchronous: true,
		Publisher: rootpublication.PublisherFunc(func(context.Context, *rootpublication.PreparedRootCandidate) rootpublication.PublishResult {
			return rootpublication.PublishResult{Outcome: rootpublication.PublishSucceeded}
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Stop(context.Background()); err != nil {
			t.Error(err)
		}
		if err := c.CloseAccepted(); err != nil {
			t.Error(err)
		}
	})
	opening := rootpublication.SealedPrefix{LogicalSequence: 1, CanonicalLSN: 7}
	if err := c.ConfigureAcceptedPrefix(2, rootpublication.LogicalFrontiers{V: 1, C: 7, W: 7, S: opening}); err != nil {
		t.Fatal(err)
	}
	ticket, err := c.ReserveAccepted(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := ticket.RecordAssigned(8, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := ticket.InstallAccepted(rootpublication.AcceptedDescriptor{Sequence: 2}); err != nil {
		t.Fatal(err)
	}
	cut, err := c.CaptureAcceptedCut(rootpublication.SealedPrefix{LogicalSequence: 2, CanonicalLSN: 8}, rootpublication.LogicalRootSet{})
	if err != nil {
		t.Fatal(err)
	}
	group := &RootPublicationBuildGroup{coordinator: c, acceptedPrefixCut: cut, capturedBasis: &Snapshot{state: &DBState{AppliedCommandLSN: 7}}}
	intent := commandWALBatchIntent{lsn: 8, coveredRange: [1]CommandWALLSNRange{{First: 8, Last: 8}}}
	if !group.hasExactCapturedCoverage(&intent) {
		t.Fatal("exact pre-assigned frozen coverage refused")
	}
	for name, bad := range map[string]*commandWALBatchIntent{
		"missing":     nil,
		"fresh":       {},
		"replay":      {lsn: 8, fromReplay: true, coveredRange: intent.coveredRange},
		"other":       {lsn: 9, coveredRange: [1]CommandWALLSNRange{{First: 8, Last: 9}}},
		"wrong start": {lsn: 8, coveredRange: [1]CommandWALLSNRange{{First: 7, Last: 8}}},
		"wrong end":   {lsn: 8, coveredRange: [1]CommandWALLSNRange{{First: 8, Last: 9}}},
	} {
		if group.hasExactCapturedCoverage(bad) {
			t.Errorf("%s intent acquired the retry exception", name)
		}
	}
	if err := cut.Abandon(); err != nil {
		t.Fatal(err)
	}
	if group.hasExactCapturedCoverage(&intent) {
		t.Fatal("abandoned captured coverage acquired the retry exception")
	}
	if IsOnlyRootPublicationSnapshotAdmissionRefusal(errors.Join(errRootPublicationSnapshotRegistryRefused, ErrRecoveryRequired)) {
		t.Fatal("receipt marker masked an unrelated error")
	}
}
