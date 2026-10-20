package db

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

func TestPrivateCOWEquivalentRewriteRejectsWrongRoleAndFrontierChange(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if closed {
			return
		}
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	owner, foreign := transactionowner.New(), transactionowner.New()
	if err := d.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	before := d.state.Load()
	for _, invalid := range []*transactionowner.Owner{nil, foreign} {
		stats, err := d.ValueLogRewriteOnlineForPrivateCOW(invalid, context.Background(), ValueLogRewriteOnlineOptions{})
		if !errors.Is(err, ErrNativeSemanticUnsupported) || stats.RecordsCopied != 0 || d.state.Load() != before {
			t.Fatalf("wrong role changed original state: stats=%+v err=%v", stats, err)
		}
	}
	for _, opts := range []finalizeCommitOptions{
		{privateCOWEquivalentRewriteOwner: foreign},
		{privateCOWEquivalentRewriteOwner: owner, commandWALPublish: true},
		{privateCOWEquivalentRewriteOwner: owner, appliedCommandLSN: 1},
		{privateCOWEquivalentRewriteOwner: owner, maxEntryRevision: 1},
		{privateCOWEquivalentRewriteOwner: owner, appliedRanges: []CommandWALLSNRange{{First: 1, Last: 1}}},
	} {
		_, err := d.finalizeCommitLockedWithOptions(0, 0, nil, false, adaptive.Metrics{}, nil, false, nil, nil, nil, opts)
		if !errors.Is(err, ErrNativeSemanticUnsupported) || d.state.Load() != before {
			t.Fatalf("incompatible rewrite role advanced publication: %v", err)
		}
	}
	before = nil
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	if _, err := d.ValueLogRewriteOnlineForPrivateCOW(owner, context.Background(), ValueLogRewriteOnlineOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed original owner: %v", err)
	}
}
