package raftcluster

import (
	"errors"
	"os"
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
			name: "cluster dir inside leaf vlog",
			mut: func(cfg *Config) {
				cfg.ClusterDir = LeafLogDir(cfg.Dir) + "/raft"
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
		{
			name: "cluster dir inside command wal with maindb options dir",
			mut: func(cfg *Config) {
				root := t.TempDir()
				mainDir := filepath.Join(root, "maindb")
				if err := os.MkdirAll(filepath.Join(mainDir, "wal"), 0o755); err != nil {
					t.Fatalf("mkdir wal: %v", err)
				}
				if err := os.WriteFile(filepath.Join(mainDir, "index.db"), nil, 0o600); err != nil {
					t.Fatalf("write index.db: %v", err)
				}
				if err := os.MkdirAll(filepath.Join(root, "dictdb"), 0o755); err != nil {
					t.Fatalf("mkdir dictdb: %v", err)
				}
				cfg.Dir = mainDir
				cfg.ClusterDir = filepath.Join(mainDir, "wal", "raft")
			},
			want: ErrInvalidStoragePath,
		},
		{
			name: "cluster dir inside dict side store with root options dir",
			mut: func(cfg *Config) {
				root := t.TempDir()
				cfg.Dir = root
				cfg.ClusterDir = filepath.Join(root, "dictdb", "raft")
			},
			want: ErrInvalidStoragePath,
		},
		{
			name: "cluster dir inside template side store with maindb options dir",
			mut: func(cfg *Config) {
				root := t.TempDir()
				mainDir := filepath.Join(root, "maindb")
				if err := os.MkdirAll(mainDir, 0o755); err != nil {
					t.Fatalf("mkdir maindb: %v", err)
				}
				if err := os.WriteFile(filepath.Join(mainDir, "index.db"), nil, 0o600); err != nil {
					t.Fatalf("write index.db: %v", err)
				}
				if err := os.MkdirAll(filepath.Join(root, "dictdb"), 0o755); err != nil {
					t.Fatalf("mkdir dictdb: %v", err)
				}
				cfg.Dir = mainDir
				cfg.ClusterDir = filepath.Join(root, "templatedb", "raft")
			},
			want: ErrInvalidStoragePath,
		},
		{
			name: "cluster dir inside command wal with flat options dir",
			mut: func(cfg *Config) {
				root := t.TempDir()
				if err := os.MkdirAll(filepath.Join(root, "wal"), 0o755); err != nil {
					t.Fatalf("mkdir wal: %v", err)
				}
				if err := os.WriteFile(filepath.Join(root, "index.db"), nil, 0o600); err != nil {
					t.Fatalf("write index.db: %v", err)
				}
				cfg.Dir = root
				cfg.ClusterDir = filepath.Join(root, "wal", "raft")
			},
			want: ErrInvalidStoragePath,
		},
		{
			name: "cluster dir inside column assets with flat options dir",
			mut: func(cfg *Config) {
				root := t.TempDir()
				if err := os.MkdirAll(filepath.Join(root, "column_assets"), 0o755); err != nil {
					t.Fatalf("mkdir column_assets: %v", err)
				}
				if err := os.WriteFile(filepath.Join(root, "index.db"), nil, 0o600); err != nil {
					t.Fatalf("write index.db: %v", err)
				}
				cfg.Dir = root
				cfg.ClusterDir = filepath.Join(root, "column_assets", "raft")
			},
			want: ErrInvalidStoragePath,
		},
		{
			name: "cluster dir inside command wal with disable side stores",
			mut: func(cfg *Config) {
				root := t.TempDir()
				cfg.Dir = root
				cfg.DisableSideStores = true
				cfg.ClusterDir = filepath.Join(root, "wal", "raft")
			},
			want: ErrInvalidStoragePath,
		},
		{
			name: "cluster dir inside column assets with disable side stores",
			mut: func(cfg *Config) {
				root := t.TempDir()
				cfg.Dir = root
				cfg.DisableSideStores = true
				cfg.ClusterDir = filepath.Join(root, "column_assets", "raft")
			},
			want: ErrInvalidStoragePath,
		},
		{
			name: "disable side stores rejects root layout",
			mut: func(cfg *Config) {
				root := t.TempDir()
				if err := os.MkdirAll(filepath.Join(root, "maindb"), 0o755); err != nil {
					t.Fatalf("mkdir maindb: %v", err)
				}
				cfg.Dir = root
				cfg.DisableSideStores = true
			},
			want: ErrInvalidStoragePath,
		},
		{
			name: "symlinked cluster dir into command wal",
			mut: func(cfg *Config) {
				root := t.TempDir()
				walDir := filepath.Join(root, "maindb", "wal")
				if err := os.MkdirAll(walDir, 0o755); err != nil {
					t.Fatalf("mkdir wal: %v", err)
				}
				link := filepath.Join(root, "raft-link")
				if err := os.Symlink(walDir, link); err != nil {
					t.Skipf("symlink unsupported: %v", err)
				}
				cfg.Dir = root
				cfg.ClusterDir = filepath.Join(link, "raft")
			},
			want: ErrInvalidStoragePath,
		},
		{
			name: "dangling symlinked cluster dir into command wal",
			mut: func(cfg *Config) {
				root := t.TempDir()
				link := filepath.Join(root, "raft-link")
				if err := os.Symlink(filepath.Join(root, "maindb", "wal"), link); err != nil {
					t.Skipf("symlink unsupported: %v", err)
				}
				cfg.Dir = root
				cfg.ClusterDir = filepath.Join(link, "raft")
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
	if a.Layout.LogDir == a.Layout.StableDir ||
		a.Layout.LogDir == a.Layout.ApplyDir ||
		a.Layout.LogDir == a.Layout.SnapshotDir ||
		a.Layout.StableDir == a.Layout.ApplyDir ||
		a.Layout.StableDir == a.Layout.SnapshotDir ||
		a.Layout.ApplyDir == a.Layout.SnapshotDir {
		t.Fatalf("log/stable/apply/snapshot dirs must be distinct: %+v", a.Layout)
	}
	for _, path := range []string{a.Layout.RootDir, a.Layout.NodeDir, a.Layout.GroupDir, a.Layout.LogDir, a.Layout.StableDir, a.Layout.ApplyDir, a.Layout.SnapshotDir} {
		if sameOrUnder(path, CommandWALDir(cfg.Dir)) {
			t.Fatalf("layout path %q overlaps command WAL dir %q", path, CommandWALDir(cfg.Dir))
		}
		if sameOrUnder(path, ValueLogDir(cfg.Dir)) {
			t.Fatalf("layout path %q overlaps value_vlog dir %q", path, ValueLogDir(cfg.Dir))
		}
		if sameOrUnder(path, LeafLogDir(cfg.Dir)) {
			t.Fatalf("layout path %q overlaps leaf_vlog dir %q", path, LeafLogDir(cfg.Dir))
		}
		if sameOrUnder(path, columnAssetDir(cfg.Dir)) {
			t.Fatalf("layout path %q overlaps column_assets dir %q", path, columnAssetDir(cfg.Dir))
		}
	}
	for _, id := range []NodeID{"node-a", "node-b", "node-c"} {
		if dir, ok := a.Layout.PeerDir(id); !ok || dir == "" {
			t.Fatalf("PeerDir(%q)=%q,%v want non-empty", id, dir, ok)
		}
	}
}

func TestValidateAllowsDefaultClusterDirForExistingFlatDB(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "index.db"), nil, 0o600); err != nil {
		t.Fatalf("write index.db: %v", err)
	}
	cfg := validConfig(t)
	cfg.Dir = root
	cfg.ClusterDir = ""

	resolved, err := Validate(cfg)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got, want := resolved.ClusterDir, filepath.Join(root, "raftcluster"); got != want {
		t.Fatalf("ClusterDir=%q want %q", got, want)
	}
	if got, want := resolved.Layout.RootDir, filepath.Join(root, "raftcluster"); got != want {
		t.Fatalf("Layout.RootDir=%q want %q", got, want)
	}
}

func TestValidateAllowsDefaultClusterDirForExistingMaindbNamedFlatDB(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "maindb")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir maindb: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "index.db"), nil, 0o600); err != nil {
		t.Fatalf("write index.db: %v", err)
	}
	cfg := validConfig(t)
	cfg.Dir = root
	cfg.ClusterDir = ""

	resolved, err := Validate(cfg)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got, want := resolved.ClusterDir, filepath.Join(root, "raftcluster"); got != want {
		t.Fatalf("ClusterDir=%q want %q", got, want)
	}
	if got, want := resolved.Layout.RootDir, filepath.Join(root, "raftcluster"); got != want {
		t.Fatalf("Layout.RootDir=%q want %q", got, want)
	}
}

func TestValidateDisableSideStoresAllowsDefaultClusterDir(t *testing.T) {
	root := t.TempDir()
	cfg := validConfig(t)
	cfg.Dir = root
	cfg.DisableSideStores = true
	cfg.ClusterDir = ""

	resolved, err := Validate(cfg)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got, want := resolved.ClusterDir, filepath.Join(root, "raftcluster"); got != want {
		t.Fatalf("ClusterDir=%q want %q", got, want)
	}
	if got, want := resolved.Layout.RootDir, filepath.Join(root, "raftcluster"); got != want {
		t.Fatalf("Layout.RootDir=%q want %q", got, want)
	}
}

func TestStoragePathsResolveMainDirOptionsLayout(t *testing.T) {
	root := t.TempDir()
	mainDir := filepath.Join(root, "maindb")
	if err := os.MkdirAll(mainDir, 0o755); err != nil {
		t.Fatalf("mkdir maindb: %v", err)
	}
	if err := os.WriteFile(filepath.Join(mainDir, "index.db"), nil, 0o600); err != nil {
		t.Fatalf("write index.db: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "dictdb"), 0o755); err != nil {
		t.Fatalf("mkdir dictdb: %v", err)
	}
	if got, want := DefaultClusterDir(mainDir), filepath.Join(root, "raftcluster"); got != want {
		t.Fatalf("DefaultClusterDir(%q)=%q want %q", mainDir, got, want)
	}
	if got, want := CommandWALDir(mainDir), filepath.Join(mainDir, "wal"); got != want {
		t.Fatalf("CommandWALDir(%q)=%q want %q", mainDir, got, want)
	}
}

func TestStoragePathsResolveMaindbNamedFlatOptionsLayout(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "maindb")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir maindb: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "index.db"), nil, 0o600); err != nil {
		t.Fatalf("write index.db: %v", err)
	}
	if got, want := DefaultClusterDir(root), filepath.Join(root, "raftcluster"); got != want {
		t.Fatalf("DefaultClusterDir(%q)=%q want %q", root, got, want)
	}
	if got, want := CommandWALDir(root), filepath.Join(root, "wal"); got != want {
		t.Fatalf("CommandWALDir(%q)=%q want %q", root, got, want)
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
