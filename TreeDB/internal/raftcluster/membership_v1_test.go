package raftcluster

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	hraft "github.com/hashicorp/raft"
)

type replacementConfigurationSnapshotStoreV1 struct {
	hraft.SnapshotStore
	metas []*hraft.SnapshotMeta
}

func (s replacementConfigurationSnapshotStoreV1) List() ([]*hraft.SnapshotMeta, error) {
	return s.metas, nil
}

func TestReplacementPersistedConfigurationV1(t *testing.T) {
	configuration := hraft.Configuration{Servers: []hraft.Server{{ID: "a", Address: "a:1", Suffrage: hraft.Voter}}}
	encoded := hraft.EncodeConfiguration(configuration)
	store := hraft.NewInmemStore()
	for _, entry := range []hraft.Log{
		{Index: 1, Type: hraft.LogConfiguration, Data: encoded},
		{Index: 2, Type: hraft.LogNoop},
		// Identical bytes do not make this the old configuration at index 1.
		{Index: 3, Type: hraft.LogConfiguration, Data: encoded},
		{Index: 4, Type: hraft.LogCommand, Data: []byte("command")},
	} {
		if err := store.StoreLog(&entry); err != nil {
			t.Fatal(err)
		}
	}
	provider := &HashicorpRaftProvider{logStore: store, snapshotStore: hraft.NewInmemSnapshotStore()}
	assertConfiguration := func(wantIndex, wantLast uint64) {
		t.Helper()
		index, actual, last, err := provider.persistedConfigurationV1(context.Background())
		if err != nil || index != wantIndex || last != wantLast || !bytes.Equal(actual, encoded) {
			t.Fatalf("configuration=%d/%d/%x err=%v; want %d/%d/%x", index, last, actual, err, wantIndex, wantLast, encoded)
		}
	}
	assertConfiguration(3, 4)
	// A restarted provider derives the same authority without an in-memory
	// configuration cache. Native snapshot metadata covers compacted prefixes.
	provider = &HashicorpRaftProvider{logStore: store, snapshotStore: replacementConfigurationSnapshotStoreV1{metas: []*hraft.SnapshotMeta{{Index: 4, ConfigurationIndex: 3, Configuration: configuration}}}}
	if err := store.DeleteRange(1, 4); err != nil {
		t.Fatal(err)
	}
	assertConfiguration(3, 0)
	if err := store.StoreLog(&hraft.Log{Index: 5, Type: hraft.LogNoop}); err != nil {
		t.Fatal(err)
	}
	assertConfiguration(3, 5)
	if err := store.StoreLog(&hraft.Log{Index: 6, Type: hraft.LogConfiguration, Data: encoded}); err != nil {
		t.Fatal(err)
	}
	assertConfiguration(6, 6)
	// A hole above the snapshot cannot be silently covered by the older
	// snapshot. The caller retries a fresh native snapshot/log view.
	if err := store.DeleteRange(6, 6); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreLog(&hraft.Log{Index: 7, Type: hraft.LogNoop}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := provider.persistedConfigurationV1(context.Background()); !errors.Is(err, hraft.ErrLogNotFound) {
		t.Fatalf("missing log error=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err := provider.persistedConfigurationV1(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
}

type replacementConfigurationLongTailV1 struct {
	hraft.LogStore
	reads int
}

func (*replacementConfigurationLongTailV1) LastIndex() (uint64, error) {
	return replacementConfigurationScanLimitV1 + 1, nil
}
func (s *replacementConfigurationLongTailV1) GetLog(index uint64, entry *hraft.Log) error {
	s.reads++
	*entry = hraft.Log{Index: index, Type: hraft.LogNoop}
	return nil
}

func TestReplacementPersistedConfigurationScanBoundV1(t *testing.T) {
	logs := &replacementConfigurationLongTailV1{}
	provider := &HashicorpRaftProvider{logStore: logs, snapshotStore: hraft.NewInmemSnapshotStore()}
	_, _, _, err := provider.persistedConfigurationV1(context.Background())
	if !errors.Is(err, ErrHashicorpRaftUnavailable) || !strings.Contains(err.Error(), "native snapshot required") || logs.reads != replacementConfigurationScanLimitV1 {
		t.Fatalf("reads=%d err=%v", logs.reads, err)
	}
}
