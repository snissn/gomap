package raftcluster

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestValidateRejectsInvalidConfig(t *testing.T) {
	base := validConfig(t)
	tests := []struct {
		name string
		mut  func(*Config)
		want error
	}{
		{
			name: "missing node id",
			mut:  func(cfg *Config) { cfg.NodeID = "" },
			want: ErrMissingNodeID,
		},
		{
			name: "missing group id",
			mut:  func(cfg *Config) { cfg.GroupID = "" },
			want: ErrMissingGroupID,
		},
		{
			name: "missing peers",
			mut:  func(cfg *Config) { cfg.Peers = nil },
			want: ErrMissingPeer,
		},
		{
			name: "duplicate peers",
			mut: func(cfg *Config) {
				cfg.Peers = append(cfg.Peers, Peer{ID: "node-b", Address: "127.0.0.1:9203"})
			},
			want: ErrDuplicatePeer,
		},
		{
			name: "missing local member",
			mut: func(cfg *Config) {
				cfg.Peers = []Peer{
					{ID: "node-b", Address: "127.0.0.1:9202"},
					{ID: "node-c", Address: "127.0.0.1:9203"},
				}
			},
			want: ErrLocalMemberMissing,
		},
		{
			name: "invalid peer id",
			mut: func(cfg *Config) {
				cfg.Peers[0].ID = "../node-a"
			},
			want: ErrMissingPeer,
		},
		{
			name: "empty peer address",
			mut: func(cfg *Config) {
				cfg.Peers[0].Address = " "
			},
			want: ErrMissingPeer,
		},
		{
			name: "cluster dir inside command wal",
			mut: func(cfg *Config) {
				cfg.ClusterDir = CommandWALDir(cfg.Dir) + "/raft"
			},
			want: ErrInvalidStoragePath,
		},
		{
			name: "cluster dir inside value vlog",
			mut: func(cfg *Config) {
				cfg.ClusterDir = ValueLogDir(cfg.Dir) + "/raft"
			},
			want: ErrInvalidStoragePath,
		},
		{
			name: "cluster dir inside command wal with mixed path forms",
			mut: func(cfg *Config) {
				cwd := t.TempDir()
				t.Chdir(cwd)
				cfg.Dir = "./db"
				cfg.ClusterDir = filepath.Join(cwd, "db", "maindb", "wal", "raft")
			},
			want: ErrInvalidStoragePath,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			cfg.Peers = append([]Peer(nil), base.Peers...)
			tc.mut(&cfg)
			_, err := Validate(cfg)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Validate err=%v want errors.Is(%v)", err, tc.want)
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate err=%v want ErrInvalidConfig", err)
			}
		})
	}
}

func TestValidateFeatureFloorFailsClosed(t *testing.T) {
	base := validConfig(t)
	tests := []struct {
		name string
		mut  func(*Config)
		want error
	}{
		{
			name: "unsupported config major",
			mut: func(cfg *Config) {
				cfg.Features = DefaultFeatureSet()
				cfg.Features.ConfigVersion = Version{Major: 2, Minor: 0}
			},
			want: ErrUnsupportedVersion,
		},
		{
			name: "unsupported config minor",
			mut: func(cfg *Config) {
				cfg.Features = DefaultFeatureSet()
				cfg.Features.ConfigVersion = Version{Major: 1, Minor: 1}
			},
			want: ErrUnsupportedVersion,
		},
		{
			name: "unknown required feature",
			mut: func(cfg *Config) {
				cfg.Features = FeatureSet{
					ConfigVersion: SupportedConfigVersion,
					Required: []RequiredFeature{
						{Name: "treedb.raftcluster.future_multi_group", Version: Version{Major: 1, Minor: 0}},
					},
				}
			},
			want: ErrUnsupportedFeature,
		},
		{
			name: "unsupported feature major",
			mut: func(cfg *Config) {
				cfg.Features = FeatureSet{
					ConfigVersion: SupportedConfigVersion,
					Required: []RequiredFeature{
						{Name: FeatureSingleGroupProvider, Version: Version{Major: 2, Minor: 0}},
					},
				}
			},
			want: ErrUnsupportedVersion,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			cfg.Peers = append([]Peer(nil), base.Peers...)
			tc.mut(&cfg)
			_, err := Validate(cfg)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Validate err=%v want errors.Is(%v)", err, tc.want)
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate err=%v want ErrInvalidConfig", err)
			}
		})
	}
}

func TestStorageLayoutDeterministicAndSeparated(t *testing.T) {
	cfg := validConfig(t)
	a, err := Validate(cfg)
	if err != nil {
		t.Fatalf("Validate a: %v", err)
	}
	b, err := Validate(cfg)
	if err != nil {
		t.Fatalf("Validate b: %v", err)
	}
	if !reflect.DeepEqual(a.Layout, b.Layout) {
		t.Fatalf("layout is not deterministic:\na=%+v\nb=%+v", a.Layout, b.Layout)
	}
	if a.Layout.RootDir != DefaultClusterDir(cfg.Dir) {
		t.Fatalf("root dir=%q want %q", a.Layout.RootDir, DefaultClusterDir(cfg.Dir))
	}
	if a.Layout.LogDir == a.Layout.StableDir || a.Layout.LogDir == a.Layout.SnapshotDir || a.Layout.StableDir == a.Layout.SnapshotDir {
		t.Fatalf("log/stable/snapshot dirs must be distinct: %+v", a.Layout)
	}
	for _, path := range []string{a.Layout.RootDir, a.Layout.NodeDir, a.Layout.GroupDir, a.Layout.LogDir, a.Layout.StableDir, a.Layout.SnapshotDir} {
		if sameOrUnder(path, CommandWALDir(cfg.Dir)) {
			t.Fatalf("layout path %q overlaps command WAL dir %q", path, CommandWALDir(cfg.Dir))
		}
		if sameOrUnder(path, ValueLogDir(cfg.Dir)) {
			t.Fatalf("layout path %q overlaps value_vlog dir %q", path, ValueLogDir(cfg.Dir))
		}
	}
	for _, id := range []NodeID{"node-a", "node-b", "node-c"} {
		if dir, ok := a.Layout.PeerDir(id); !ok || dir == "" {
			t.Fatalf("PeerDir(%q)=%q,%v want non-empty", id, dir, ok)
		}
	}
}

func TestValidateSortsPeersAndPeerDirs(t *testing.T) {
	cfg := validConfig(t)
	cfg.Peers = []Peer{
		{ID: "node-c", Address: "127.0.0.1:9203"},
		{ID: "node-a", Address: "127.0.0.1:9201"},
		{ID: "node-b", Address: "127.0.0.1:9202"},
	}
	resolved, err := Validate(cfg)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	for i, want := range []NodeID{"node-a", "node-b", "node-c"} {
		if got := resolved.Peers[i].ID; got != want {
			t.Fatalf("peer[%d]=%q want %q", i, got, want)
		}
		if got := resolved.Layout.PeerDirs[i].ID; got != want {
			t.Fatalf("peer dir[%d]=%q want %q", i, got, want)
		}
	}
}

func validConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		Dir:     t.TempDir(),
		NodeID:  "node-a",
		GroupID: "default",
		Peers: []Peer{
			{ID: "node-a", Address: "127.0.0.1:9201"},
			{ID: "node-b", Address: "127.0.0.1:9202"},
			{ID: "node-c", Address: "127.0.0.1:9203"},
		},
	}
}
