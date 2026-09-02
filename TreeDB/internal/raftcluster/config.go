package raftcluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	// FeatureSingleGroupProvider is the feature gate for the #3044-0 provider
	// and storage boundary. It reserves only the single-group contract; it does
	// not imply a selected Raft library or production HA behavior.
	FeatureSingleGroupProvider  FeatureName = "treedb.raftcluster.single_group_provider"
	FeatureCatalogMetaAuthority FeatureName = "treedb.raftcluster.catalog_meta_authority"
	// FeatureVectorPartitionLifecycle gates the M7 catalog/meta lifecycle and
	// invalidation-before-mutation contract. It is opt-in during pre-alpha;
	// legacy clusters do not acquire the new admission requirement implicitly.
	FeatureVectorPartitionLifecycle FeatureName = "treedb.raftcluster.vector_partition_lifecycle"
)

var (
	SupportedConfigVersion = Version{Major: 1, Minor: 0}
	SupportedFeatureFloors = map[FeatureName]Version{
		FeatureSingleGroupProvider:      {Major: 1, Minor: 0},
		FeatureCatalogMetaAuthority:     {Major: 1, Minor: 0},
		FeatureVectorPartitionLifecycle: {Major: 1, Minor: 0},
	}

	ErrInvalidConfig       = errors.New("raftcluster: invalid config")
	ErrMissingNodeID       = errors.New("raftcluster: missing node id")
	ErrMissingGroupID      = errors.New("raftcluster: missing group id")
	ErrMissingPeer         = errors.New("raftcluster: missing peer")
	ErrDuplicatePeer       = errors.New("raftcluster: duplicate peer")
	ErrLocalMemberMissing  = errors.New("raftcluster: local member missing")
	ErrInvalidStoragePath  = errors.New("raftcluster: invalid storage path")
	ErrUnsupportedFeature  = errors.New("raftcluster: unsupported feature")
	ErrUnsupportedVersion  = errors.New("raftcluster: unsupported version")
	ErrUnsupportedProvider = errors.New("raftcluster: unsupported provider")
)

type NodeID string

type GroupID string

type FeatureName string

type Version struct {
	Major uint16
	Minor uint16
}

func (v Version) isZero() bool {
	return v.Major == 0 && v.Minor == 0
}

// RequiredFeature declares a cluster feature that must be understood before a
// node may open the provider boundary.
type RequiredFeature struct {
	Name    FeatureName
	Version Version
}

// FeatureSet carries the config format and feature floor required by this
// package. A zero value is normalized to the current single-group provider v1
// floor.
type FeatureSet struct {
	ConfigVersion Version
	Required      []RequiredFeature
}

func DefaultFeatureSet() FeatureSet {
	return FeatureSet{
		ConfigVersion: SupportedConfigVersion,
		Required: []RequiredFeature{
			{Name: FeatureSingleGroupProvider, Version: SupportedFeatureFloors[FeatureSingleGroupProvider]},
		},
	}
}

type Peer struct {
	ID      NodeID
	Address string
	// Capabilities is the fixed voter's declared supported feature set. A zero
	// value retains legacy config compatibility and normalizes to DefaultFeatureSet.
	Capabilities FeatureSet
}

// Config is the user-provided single-group cluster configuration.
//
// Dir is the TreeDB Options.Dir value. ClusterDir optionally overrides the
// default raftcluster directory. Even with an explicit ClusterDir, Dir is
// required so validation can reject layouts that overlap the resolved TreeDB
// main DB, WAL, value_vlog, or leaf_vlog paths.
type Config struct {
	Dir               string
	ClusterDir        string
	DisableSideStores bool
	NodeID            NodeID
	GroupID           GroupID
	Peers             []Peer
	Features          FeatureSet
}

type ResolvedConfig struct {
	Dir               string
	ClusterDir        string
	DisableSideStores bool
	NodeID            NodeID
	GroupID           GroupID
	Peers             []Peer
	Features          FeatureSet
	Layout            StorageLayout
}

type PeerStorageDir struct {
	ID  NodeID
	Dir string
}

// StorageLayout is the deterministic local storage identity for one node in
// one Raft group.
type StorageLayout struct {
	RootDir     string
	NodeDir     string
	GroupDir    string
	LogDir      string
	StableDir   string
	ApplyDir    string
	SnapshotDir string
	PeerDirs    []PeerStorageDir
}

func (l StorageLayout) PeerDir(id NodeID) (string, bool) {
	for _, p := range l.PeerDirs {
		if p.ID == id {
			return p.Dir, true
		}
	}
	return "", false
}

// Provider is the shape future Raft library adapters expose to the rest of
// TreeDB. It intentionally exposes only identity and storage layout in this
// slice.
type Provider interface {
	Config() ResolvedConfig
}

// FeatureRequirementProviderV1 exposes feature requirements for composite
// command submitters that do not have one single ResolvedConfig.
type FeatureRequirementProviderV1 interface {
	RequiresFeatureV1(FeatureName) (bool, error)
}

// FeatureSetRequiresV1 reports whether a validated feature set requires at
// least the supported floor for name.
func FeatureSetRequiresV1(features FeatureSet, name FeatureName) bool {
	floor, known := SupportedFeatureFloors[name]
	if !known {
		return false
	}
	for _, required := range features.Required {
		if required.Name == name && required.Version.Major == floor.Major && required.Version.Minor >= floor.Minor {
			return true
		}
	}
	return false
}

// ProviderFactory creates a provider from a validated single-group config. It
// is a factory boundary, not an admission or consensus API.
type ProviderFactory interface {
	OpenProvider(context.Context, ResolvedConfig) (Provider, error)
}

func Validate(cfg Config) (ResolvedConfig, error) {
	dir, err := cleanRequiredPath("dir", cfg.Dir)
	if err != nil {
		return ResolvedConfig{}, err
	}
	if err := validateID("node id", string(cfg.NodeID)); err != nil {
		return ResolvedConfig{}, errors.Join(ErrInvalidConfig, ErrMissingNodeID, err)
	}
	if err := validateID("group id", string(cfg.GroupID)); err != nil {
		return ResolvedConfig{}, errors.Join(ErrInvalidConfig, ErrMissingGroupID, err)
	}
	features, err := validateFeatures(cfg.Features)
	if err != nil {
		return ResolvedConfig{}, err
	}
	peers, err := validatePeers(cfg.NodeID, cfg.Peers)
	if err != nil {
		return ResolvedConfig{}, err
	}
	treeLayout, err := resolveConfigTreeDBStorageLayout(dir, cfg.DisableSideStores)
	if err != nil {
		return ResolvedConfig{}, err
	}
	clusterDir := cfg.ClusterDir
	if strings.TrimSpace(clusterDir) == "" {
		clusterDir = defaultClusterDir(treeLayout)
	}
	clusterDir, err = cleanStorageRoot(treeLayout, clusterDir)
	if err != nil {
		return ResolvedConfig{}, err
	}
	layout := storageLayout(clusterDir, cfg.NodeID, cfg.GroupID, peers)
	return ResolvedConfig{
		Dir:               dir,
		ClusterDir:        clusterDir,
		DisableSideStores: cfg.DisableSideStores,
		NodeID:            cfg.NodeID,
		GroupID:           cfg.GroupID,
		Peers:             peers,
		Features:          features,
		Layout:            layout,
	}, nil
}

func DefaultClusterDir(dir string) string {
	if strings.TrimSpace(dir) == "" {
		return ""
	}
	return defaultClusterDir(resolveTreeDBStorageLayout(dir))
}

func MainDBDir(dir string) string {
	if strings.TrimSpace(dir) == "" {
		return ""
	}
	return resolveTreeDBStorageLayout(dir).mainDir
}

func CommandWALDir(dir string) string {
	if strings.TrimSpace(dir) == "" {
		return ""
	}
	return filepath.Join(MainDBDir(dir), "wal")
}

func ValueLogDir(dir string) string {
	if strings.TrimSpace(dir) == "" {
		return ""
	}
	return filepath.Join(MainDBDir(dir), "value_vlog")
}

func LeafLogDir(dir string) string {
	if strings.TrimSpace(dir) == "" {
		return ""
	}
	return filepath.Join(MainDBDir(dir), "leaf_vlog")
}

func columnAssetDir(dir string) string {
	if strings.TrimSpace(dir) == "" {
		return ""
	}
	return filepath.Join(MainDBDir(dir), "column_assets")
}

func validateFeatures(features FeatureSet) (FeatureSet, error) {
	if features.ConfigVersion.isZero() && len(features.Required) == 0 {
		return DefaultFeatureSet(), nil
	}
	if features.ConfigVersion.isZero() {
		features.ConfigVersion = SupportedConfigVersion
	}
	if features.ConfigVersion.Major != SupportedConfigVersion.Major || features.ConfigVersion.Minor > SupportedConfigVersion.Minor {
		return FeatureSet{}, errors.Join(ErrInvalidConfig, ErrUnsupportedVersion, fmt.Errorf("config version %d.%d exceeds supported %d.%d", features.ConfigVersion.Major, features.ConfigVersion.Minor, SupportedConfigVersion.Major, SupportedConfigVersion.Minor))
	}
	if len(features.Required) == 0 {
		features.Required = DefaultFeatureSet().Required
	}
	out := FeatureSet{ConfigVersion: features.ConfigVersion, Required: make([]RequiredFeature, 0, len(features.Required))}
	seen := make(map[FeatureName]struct{}, len(features.Required))
	for _, required := range features.Required {
		required.Name = FeatureName(strings.TrimSpace(string(required.Name)))
		floor, ok := SupportedFeatureFloors[required.Name]
		if !ok {
			return FeatureSet{}, errors.Join(ErrInvalidConfig, ErrUnsupportedFeature, fmt.Errorf("required feature %q", required.Name))
		}
		if required.Version.Major != floor.Major || required.Version.Minor > floor.Minor {
			return FeatureSet{}, errors.Join(ErrInvalidConfig, ErrUnsupportedVersion, fmt.Errorf("required feature %q version %d.%d exceeds supported floor %d.%d", required.Name, required.Version.Major, required.Version.Minor, floor.Major, floor.Minor))
		}
		if _, exists := seen[required.Name]; exists {
			return FeatureSet{}, errors.Join(ErrInvalidConfig, ErrUnsupportedFeature, fmt.Errorf("duplicate required feature %q", required.Name))
		}
		seen[required.Name] = struct{}{}
		out.Required = append(out.Required, required)
	}
	sort.Slice(out.Required, func(i, j int) bool {
		return out.Required[i].Name < out.Required[j].Name
	})
	return out, nil
}

func validatePeers(local NodeID, peers []Peer) ([]Peer, error) {
	if len(peers) == 0 {
		return nil, errors.Join(ErrInvalidConfig, ErrMissingPeer, fmt.Errorf("at least one peer is required"))
	}
	out := make([]Peer, 0, len(peers))
	seen := make(map[NodeID]struct{}, len(peers))
	localFound := false
	for i, peer := range peers {
		if err := validateID("peer id", string(peer.ID)); err != nil {
			return nil, errors.Join(ErrInvalidConfig, ErrMissingPeer, fmt.Errorf("peer[%d]: %w", i, err))
		}
		if _, exists := seen[peer.ID]; exists {
			return nil, errors.Join(ErrInvalidConfig, ErrDuplicatePeer, fmt.Errorf("peer %q appears more than once", peer.ID))
		}
		seen[peer.ID] = struct{}{}
		address := strings.TrimSpace(peer.Address)
		if address == "" {
			return nil, errors.Join(ErrInvalidConfig, ErrMissingPeer, fmt.Errorf("peer %q address is empty", peer.ID))
		}
		if strings.ContainsAny(address, "\x00\r\n") {
			return nil, errors.Join(ErrInvalidConfig, ErrMissingPeer, fmt.Errorf("peer %q address contains control characters", peer.ID))
		}
		if peer.ID == local {
			localFound = true
		}
		capabilities, err := validateFeatures(peer.Capabilities)
		if err != nil {
			return nil, errors.Join(ErrInvalidConfig, fmt.Errorf("peer %q capabilities: %w", peer.ID, err))
		}
		out = append(out, Peer{ID: peer.ID, Address: address, Capabilities: capabilities})
	}
	if !localFound {
		return nil, errors.Join(ErrInvalidConfig, ErrLocalMemberMissing, fmt.Errorf("local node %q is not in peer set", local))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func validateID(label, id string) error {
	if id == "" {
		return fmt.Errorf("%s is empty", label)
	}
	if strings.TrimSpace(id) != id {
		return fmt.Errorf("%s has leading or trailing whitespace", label)
	}
	if id == "." || id == ".." {
		return fmt.Errorf("%s %q is not a valid path segment", label, id)
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.':
		default:
			return fmt.Errorf("%s %q contains unsupported character %q", label, id, r)
		}
	}
	return nil
}

func cleanRequiredPath(label, p string) (string, error) {
	if strings.TrimSpace(p) == "" {
		return "", errors.Join(ErrInvalidConfig, ErrInvalidStoragePath, fmt.Errorf("%s is empty", label))
	}
	if strings.ContainsRune(p, '\x00') {
		return "", errors.Join(ErrInvalidConfig, ErrInvalidStoragePath, fmt.Errorf("%s contains NUL byte", label))
	}
	return filepath.Clean(p), nil
}

func cleanStorageRoot(layout treeDBStorageLayout, clusterDir string) (string, error) {
	clusterDir, err := cleanRequiredPath("cluster dir", clusterDir)
	if err != nil {
		return "", err
	}
	clusterDirAbs, err := realCleanStoragePath("cluster dir", clusterDir)
	if err != nil {
		return "", err
	}
	reserved := []string{
		filepath.Join(layout.mainDir, "wal"),
		filepath.Join(layout.mainDir, "value_vlog"),
		filepath.Join(layout.mainDir, "leaf_vlog"),
		filepath.Join(layout.mainDir, "column_assets"),
	}
	if layout.flat {
		reserved = append(reserved, filepath.Join(layout.mainDir, "index.db"))
	} else {
		reserved = append(
			reserved,
			layout.mainDir,
			filepath.Join(layout.rootDir, "dictdb"),
			filepath.Join(layout.rootDir, "templatedb"),
		)
	}
	for _, path := range reserved {
		reservedAbs, err := realCleanStoragePath("reserved TreeDB path", path)
		if err != nil {
			return "", err
		}
		if sameOrUnder(clusterDirAbs, reservedAbs) || sameOrUnder(reservedAbs, clusterDirAbs) {
			return "", errors.Join(ErrInvalidConfig, ErrInvalidStoragePath, fmt.Errorf("cluster dir %q overlaps reserved TreeDB path %q", clusterDir, path))
		}
	}
	return clusterDir, nil
}

func defaultClusterDir(layout treeDBStorageLayout) string {
	return filepath.Join(layout.rootDir, "raftcluster")
}

type treeDBStorageLayout struct {
	rootDir string
	mainDir string
	flat    bool
}

func resolveTreeDBStorageLayout(dir string) treeDBStorageLayout {
	clean := filepath.Clean(dir)
	if info, err := os.Stat(filepath.Join(clean, "maindb")); err == nil && info.IsDir() {
		return treeDBStorageLayout{
			rootDir: clean,
			mainDir: filepath.Join(clean, "maindb"),
		}
	}
	if info, err := os.Stat(filepath.Join(clean, "index.db")); err == nil && !info.IsDir() {
		if filepath.Base(clean) == "maindb" {
			parent := filepath.Dir(clean)
			if treeDBSideStoreDirExists(parent) {
				return treeDBStorageLayout{rootDir: parent, mainDir: clean}
			}
		}
		return treeDBStorageLayout{rootDir: clean, mainDir: clean, flat: true}
	}
	if filepath.Base(clean) == "maindb" {
		return treeDBStorageLayout{
			rootDir: filepath.Dir(clean),
			mainDir: clean,
		}
	}
	return treeDBStorageLayout{
		rootDir: clean,
		mainDir: filepath.Join(clean, "maindb"),
	}
}

func treeDBSideStoreDirExists(parent string) bool {
	for _, name := range []string{"dictdb", "templatedb"} {
		if info, err := os.Stat(filepath.Join(parent, name)); err == nil && info.IsDir() {
			return true
		}
	}
	return false
}

func resolveConfigTreeDBStorageLayout(dir string, disableSideStores bool) (treeDBStorageLayout, error) {
	clean := filepath.Clean(dir)
	if !disableSideStores {
		return resolveTreeDBStorageLayout(clean), nil
	}
	if info, err := os.Stat(filepath.Join(clean, "maindb")); err == nil && info.IsDir() {
		return treeDBStorageLayout{}, errors.Join(ErrInvalidConfig, ErrInvalidStoragePath, fmt.Errorf("DisableSideStores=true but dir looks like a TreeDB root containing maindb/: %s", clean))
	}
	return treeDBStorageLayout{
		rootDir: clean,
		mainDir: clean,
		flat:    true,
	}, nil
}

func realCleanStoragePath(label, p string) (string, error) {
	abs, err := filepath.Abs(filepath.Clean(p))
	if err != nil {
		return "", errors.Join(ErrInvalidConfig, ErrInvalidStoragePath, fmt.Errorf("%s %q cannot be made absolute: %w", label, p, err))
	}
	real, err := evalExistingStoragePath(abs)
	if err != nil {
		return "", errors.Join(ErrInvalidConfig, ErrInvalidStoragePath, fmt.Errorf("%s %q cannot resolve symlinks: %w", label, p, err))
	}
	return real, nil
}

func evalExistingStoragePath(abs string) (string, error) {
	if real, err := filepath.EvalSymlinks(abs); err == nil {
		return filepath.Clean(real), nil
	}
	var suffix []string
	for cur := abs; ; cur = filepath.Dir(cur) {
		if real, err := filepath.EvalSymlinks(cur); err == nil {
			for i := len(suffix) - 1; i >= 0; i-- {
				real = filepath.Join(real, suffix[i])
			}
			return filepath.Clean(real), nil
		}
		if info, err := os.Lstat(cur); err == nil {
			if info.Mode()&os.ModeSymlink != 0 {
				return "", fmt.Errorf("path component %q is an unresolved symlink", cur)
			}
		} else if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return abs, nil
		}
		suffix = append(suffix, filepath.Base(cur))
	}
}

func sameOrUnder(path, root string) bool {
	path = filepath.Clean(path)
	root = filepath.Clean(root)
	if path == root {
		return true
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel != "." && rel != "" && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != ".." && !filepath.IsAbs(rel)
}

func storageLayout(clusterDir string, nodeID NodeID, groupID GroupID, peers []Peer) StorageLayout {
	root := filepath.Clean(clusterDir)
	nodeDir := filepath.Join(root, "nodes", string(nodeID))
	groupDir := filepath.Join(nodeDir, "groups", string(groupID))
	peerDirs := make([]PeerStorageDir, 0, len(peers))
	for _, peer := range peers {
		peerDirs = append(peerDirs, PeerStorageDir{
			ID:  peer.ID,
			Dir: filepath.Join(groupDir, "peers", string(peer.ID)),
		})
	}
	return StorageLayout{
		RootDir:     root,
		NodeDir:     nodeDir,
		GroupDir:    groupDir,
		LogDir:      filepath.Join(groupDir, "log"),
		StableDir:   filepath.Join(groupDir, "stable"),
		ApplyDir:    filepath.Join(groupDir, "apply"),
		SnapshotDir: filepath.Join(groupDir, "snapshots"),
		PeerDirs:    peerDirs,
	}
}
