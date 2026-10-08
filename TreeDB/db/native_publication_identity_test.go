package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"testing"
)

func TestNativePublicationTentativeIdentityUsesHeldJournalWithoutBurning(t *testing.T) {
	dir := t.TempDir()
	if err := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureCommandWALV1}, DurabilityProfile: ProfileCommandWALDurable}); err != nil {
		t.Fatal(err)
	}
	d, err := Open(Options{Dir: dir, CommandWAL: true, Durability: DurabilityDurable, ResolvedProfile: ProfileCommandWALDurable, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{ReadIntegrity: IntegrityVerify}})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	makeIntent := func(key string) *CommandWALIntent {
		payload, e := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte(key), Value: []byte("value")}})
		if e != nil {
			t.Fatal(e)
		}
		intent, e := d.NewCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
		if e != nil {
			t.Fatal(e)
		}
		return intent
	}
	first, second := makeIntent("first"), makeIntent("second")
	group, err := d.NewCommandWALDurablePrefixGroupIntent([]*CommandWALIntent{first, second})
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := d.LockCommandWALPublishWithBarriers()
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	before := d.CommandWALNextLSN()
	malformed := &CommandWALIntent{inner: commandWALBatchIntent{durablePrefixGroup: []*CommandWALIntent{first, first}}}
	if _, err = d.tentativePublicCommandWALIdentityLocked(malformed); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatal(err)
	}
	for range 2 {
		predicted, e := d.tentativePublicCommandWALIdentityLocked(group)
		if e != nil || predicted != before+2 {
			t.Fatalf("predicted=%d next=%d err=%v", predicted, before, e)
		}
	}
	if d.CommandWALNextLSN() != before || first.AssignedLSN() != 0 || second.AssignedLSN() != 0 || group.AssignedLSN() != 0 {
		t.Fatal("late-key refusal/planning consumed journal identity")
	}
	predicted, err := d.tentativePublicCommandWALIdentityLocked(group)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := d.appendPublicCommandWALIntent(group, true)
	if err != nil {
		t.Fatal(err)
	}
	if actual != predicted || first.AssignedLSN() != before || second.AssignedLSN() != before+1 || d.CommandWALNextLSN() != predicted+1 {
		t.Fatal("captured late identity differs from actual command prefix/barrier")
	}
	next := d.CommandWALNextLSN()
	if _, err := d.tentativePublicCommandWALIdentityLocked(group); !errors.Is(err, ErrCommandWALRejected) || d.CommandWALNextLSN() != next {
		t.Fatal("assigned identity admitted as fresh packet", err)
	}
}
