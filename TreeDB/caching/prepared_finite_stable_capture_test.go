package caching

import (
	"errors"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestPreparedFiniteLeafStableMethodsRefuseBeforeRIDsAndPreparation(t *testing.T) {
	owner, db := finiteLeafTestWorkspace(t, func(uint64) error { return nil })
	defer owner.Close()
	input := finiteLeafTestPage(t)
	initialRID := db.nextRID.Load()
	if _, resources, err := owner.AppendLeafPageWithStableResources(input); !errors.Is(err, valuelog.ErrFiniteStableMetadataHooksUnavailable) || resources != nil {
		t.Fatalf("scalar closed route: resources=%v err=%v", resources, err)
	}
	if _, resources, err := owner.AppendLeafPagesWithStableResources([][]byte{input, input}); !errors.Is(err, valuelog.ErrFiniteStableMetadataHooksUnavailable) || resources != nil {
		t.Fatalf("batch closed route: resources=%v err=%v", resources, err)
	}
	if db.nextRID.Load() != initialRID || owner.used != 0 || owner.active ||
		cap(owner.records) != 0 || cap(owner.compact) != 0 || cap(owner.ptrs) != 0 ||
		owner.lane.finiteWorkspace != nil || owner.lane.vlog != nil {
		t.Fatal("missing stable hook performed preparation, RID assignment or writer construction")
	}
}

func TestFiniteStableCaptureChargesExactMembershipAndClearsTails(t *testing.T) {
	var debit uint64
	owner, db := finiteLeafTestWorkspace(t, func(n uint64) error { debit += n; return nil })
	defer owner.Close()
	capture, err := newFiniteStableOuterLeafCapture(db, &db.leafLog, owner, 2)
	if err != nil {
		t.Fatal(err)
	}
	if capture.builder != nil {
		t.Fatal("staged capture constructed uncharged builder")
	}
	if err := capture.requireFiniteHooks(owner); !errors.Is(err, valuelog.ErrFiniteStableMetadataHooksUnavailable) {
		t.Fatalf("hook refusal: %v", err)
	}
	countBefore := debit
	if err := capture.sortedRequired([]page.ValuePtr{{FileID: 3}, {FileID: 1}}); err != nil {
		t.Fatal(err)
	}
	if cap(capture.required) != 2 || !slices.Equal(capture.required, []uint64{1, 3}) || debit-countBefore != 2*8 {
		t.Fatalf("exact sorted membership=%v cap=%d debit=%d", capture.required, cap(capture.required), debit-countBefore)
	}
	amount := debit
	if err := capture.sortedRequired([]page.ValuePtr{{FileID: 9}}); err != nil {
		t.Fatal(err)
	}
	if debit != amount || capture.required[:cap(capture.required)][1] != 0 {
		t.Fatal("warm membership retains stale generation or grows backing")
	}
	if err := capture.sortedRequired([]page.ValuePtr{{FileID: 1}, {FileID: 2}, {FileID: 3}}); err == nil {
		t.Fatal("exceeded captured pointer bound")
	}
	requiredBacking := capture.required[:cap(capture.required)]
	capture.abandon()
	for _, v := range requiredBacking {
		if v != 0 {
			t.Fatal("abandon retained membership backing")
		}
	}
}

func TestFiniteStableCaptureCreditPrecedesLocalBacking(t *testing.T) {
	reject := false
	denied := errors.New("deny local stable backing")
	owner, db := finiteLeafTestWorkspace(t, func(uint64) error {
		if reject {
			return denied
		}
		return nil
	})
	defer owner.Close()
	capture, err := newFiniteStableOuterLeafCapture(db, &db.leafLog, owner, 2)
	if err != nil {
		t.Fatal(err)
	}
	reject = true
	if err := capture.sortedRequired([]page.ValuePtr{{FileID: 1}}); !errors.Is(err, denied) {
		t.Fatalf("membership credit: %v", err)
	}
	// A zero token has no resource authority; only local capture backing is
	// exercised here. Stable construction remains unconditionally refused.
	token := &rootpublication.StableResourceToken{}
	if err := capture.addToken(token); !errors.Is(err, denied) {
		t.Fatalf("token backing credit: %v", err)
	}
	if cap(capture.required) != 0 || cap(capture.tokens) != 0 {
		t.Fatal("allocated backing before denied credit")
	}
	capture.abandon()
}

func TestFiniteStableCaptureGlobalCountAndClosedOwner(t *testing.T) {
	owner, db := finiteLeafTestWorkspace(t, func(uint64) error { return nil })
	// Fixture output max=3 therefore ONE union budget allows21 token births,
	// even though each two-page capture has a local bound of14.
	first, err := newFiniteStableOuterLeafCapture(db, &db.leafLog, owner, 2)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newFiniteStableOuterLeafCapture(db, &db.leafLog, owner, 2)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 14; i++ {
		if err := first.metadata.AdmitCapturedToken(); err != nil {
			t.Fatal(err)
		}
		if err := first.addToken(&rootpublication.StableResourceToken{}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 7; i++ {
		if err := second.metadata.AdmitCapturedToken(); err != nil {
			t.Fatal(err)
		}
		if err := second.addToken(&rootpublication.StableResourceToken{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := second.metadata.AdmitCapturedToken(); !errors.Is(err, valuelog.ErrFiniteWriterLoan) {
		t.Fatalf("capture reset global count: %v", err)
	}
	firstBacking, secondBacking := first.tokens[:cap(first.tokens)], second.tokens[:cap(second.tokens)]
	first.abandon()
	second.abandon()
	for _, backing := range [][]*rootpublication.StableResourceToken{firstBacking, secondBacking} {
		for _, token := range backing {
			if token != nil {
				t.Fatal("retained token tail")
			}
		}
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.sortedRequired(nil); err == nil {
		t.Fatal("closed owner reused membership backing")
	}
}

func TestPreparedFiniteSuppliedCollectorRefusesBeforeAppendEffects(t *testing.T) {
	old, db := finiteLeafTestWorkspace(t, func(uint64) error { return nil })
	installed := old.installed
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	metadata, err := valuelog.NewFiniteStableMetadata(21, 9, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	resident, err := valuelog.NewFiniteStableMetadata(21, 9, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewPreparedFiniteLeafWorkspaceWithStableMetadata(installed, 3, 2, func(uint64) error { return nil }, metadata, resident)
	if err != nil {
		t.Fatal(err)
	}
	if err = owner.PrepareStableResources(2, 32); err != nil {
		owner.Close()
		t.Fatal(err)
	}
	before := db.nextRID.Load()
	if _, err = owner.AppendLeafPage(finiteLeafTestPage(t)); !errors.Is(err, valuelog.ErrFiniteStableMetadataHooksUnavailable) {
		t.Fatal("collector bypassed closed hook", err)
	}
	if db.nextRID.Load() != before || owner.used != 0 || owner.active || owner.lane.vlog != nil || cap(owner.records) != 0 {
		t.Fatal("closed collector changed RID/prepare/writer state")
	}
	if err = metadata.Close(); !errors.Is(err, valuelog.ErrFiniteWriterLoan) {
		t.Fatal("facade discarded actual workspace/collector holds", err)
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err = metadata.Close(); err != nil {
		t.Fatal("workspace retained caller facade", err)
	}
	if err = resident.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedFiniteScopedProducerDischargesInstalledDBEdge(t *testing.T) {
	old, db := finiteLeafTestWorkspace(t, func(uint64) error { return nil })
	installed := old.installed
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	metadata, err := valuelog.NewFiniteStableMetadata(21, 9, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	resident, err := valuelog.NewFiniteStableMetadata(21, 9, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewScopedPreparedFiniteLeafWorkspace(installed, 3, 2, func(uint64) error { return nil }, metadata, resident)
	if err != nil {
		t.Fatal(err)
	}
	if owner.installed != nil || owner.db != nil || owner.lane != nil {
		t.Fatal("retained scratch reaches installed DB")
	}
	input := finiteLeafTestPage(t)
	if _, err = owner.preparePages([][]byte{input}); err == nil {
		t.Fatal("unbound scratch used as producer authority")
	}
	if err = owner.BeginOwnedApply(installed); err != nil {
		t.Fatal(err)
	}
	if err = owner.BeginOwnedApply(installed); err == nil {
		t.Fatal("nested producer binding")
	}
	records, err := owner.preparePages([][]byte{input})
	if err != nil || len(records) != 1 {
		t.Fatal(err)
	}
	owner.EndOwnedApply()
	if owner.installed != nil || owner.db != nil || owner.lane != nil || owner.active || records[0].Value != nil {
		t.Fatal("Apply retained DB or borrowed record payload")
	}
	if db.leafLog.finiteWorkspace != nil {
		t.Fatal("ended Apply kept lane attachment")
	}
	close(db.closeCh)
	if err = owner.BeginOwnedApply(installed); err == nil || owner.db != nil || owner.installed != nil || owner.lane != nil {
		t.Fatal("closed producer rebound or escaped", err)
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err = metadata.Close(); err != nil {
		t.Fatal(err)
	}
	if err = resident.Close(); err != nil {
		t.Fatal(err)
	}
}
