package kvstoreadapter

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

const (
	// EnvOpenProfile selects the TreeDB profile for downstream wrappers.
	EnvOpenProfile = "TREEDB_OPEN_PROFILE"
	// EnvKeepRecent overrides the wrapper's KeepRecent default.
	EnvKeepRecent = "TREEDB_KEEP_RECENT"
	// EnvMemtableMode overrides the wrapper's memtable mode.
	EnvMemtableMode = "TREEDB_MEMTABLE_MODE"
)

// ErrUnsupportedAdapterFeature is returned when a downstream wrapper requests
// a Badger-style feature that TreeDB does not implement natively.
var ErrUnsupportedAdapterFeature = errors.New("treedb adapter: unsupported feature")

// OpenConfig standardizes the common "TreeDB behind a kvstore-style wrapper"
// open path used by downstream integrations such as cosmos-db/cometbft-db.
//
// The intended usage is:
//  1. Pick a TreeDB profile.
//  2. Optionally pin a small KeepRecent window.
//  3. Optionally allow an integration-specific memtable default/override.
//  4. Open TreeDB and wrap it with the canonical kvstore adapter.
type OpenConfig struct {
	ParentDir string
	Name      string

	// AdapterName controls kvstore-facing Name() output.
	AdapterName string
	// DBFileSuffix defaults to ".db".
	DBFileSuffix string
	// DefaultProfile defaults to treedb.ProfileCommandWALRelaxed.
	DefaultProfile treedb.Profile
	// DefaultKeepRecent is applied when the env override is unset.
	DefaultKeepRecent uint64

	// DefaultMemtableMode applies when the env override is unset.
	DefaultMemtableMode string
	// DefaultAdaptiveMemtableBase maps an empty/adaptive profile default to
	// adaptive:<base> when DefaultMemtableMode is unset.
	DefaultAdaptiveMemtableBase string

	// Environment keys default to the standard TREEDB_* names when empty.
	ProfileEnvKey      string
	KeepRecentEnvKey   string
	MemtableModeEnvKey string

	// ReadWorkers overrides adapter read worker count when > 0.
	ReadWorkers int

	// RequireEncryption fails closed when a downstream Badger-compatible wrapper
	// requires per-DB encryption. TreeDB does not expose native encryption here.
	RequireEncryption bool
	// RequireInMemory fails closed when a downstream wrapper requests an
	// in-memory-only TreeDB backend. This adapter opens persistent TreeDB dirs.
	RequireInMemory bool
}

// Opened is the standardized result returned to downstream wrappers.
type Opened struct {
	Path    string
	Options treedb.Options
	DB      *treedb.DB
	KV      *treedbadapter.DB
}

// ParseProfile parses the common downstream profile vocabulary and falls back
// to fallback for empty or unknown values.
//
// Deprecated: use ParsePublicProfile at env/CLI boundaries that must reject
// deprecated TreeDB profiles.
func ParseProfile(raw string, fallback treedb.Profile) treedb.Profile {
	if fallback == "" {
		fallback = treedb.ProfileCommandWALRelaxed
	}
	if profile, ok := treedb.ParseProfile(raw, fallback); ok {
		return profile
	}
	return fallback
}

// ParsePublicProfile parses the public downstream profile vocabulary. Empty
// input falls back to fallback, but the fallback must also be a public profile.
func ParsePublicProfile(raw string, fallback treedb.Profile) (treedb.Profile, error) {
	if fallback == "" {
		fallback = treedb.ProfileCommandWALRelaxed
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		profile, ok := treedb.ParsePublicProfile("", fallback)
		if ok {
			return profile, nil
		}
		return "", fmt.Errorf("unsupported TreeDB fallback profile %q; allowed: %s", fallback, treedb.ProfileFlagHelp)
	}
	if profile, ok := treedb.ParsePublicProfile(trimmed, fallback); ok {
		return profile, nil
	}
	return "", fmt.Errorf("unsupported TreeDB profile %q; allowed: %s", raw, treedb.ProfileFlagHelp)
}

// ResolveOptions converts downstream wrapper defaults and standard TREEDB_*
// environment overrides into TreeDB Options.
func ResolveOptions(cfg OpenConfig) (treedb.Options, string, error) {
	if cfg.RequireEncryption {
		return treedb.Options{}, "", unsupportedAdapterFeature("encryption")
	}
	if cfg.RequireInMemory {
		return treedb.Options{}, "", unsupportedAdapterFeature("in-memory mode")
	}

	parentDir := strings.TrimSpace(cfg.ParentDir)
	if parentDir == "" {
		return treedb.Options{}, "", fmt.Errorf("parent directory must be non-empty")
	}
	name := strings.TrimSpace(cfg.Name)
	if name == "" {
		return treedb.Options{}, "", fmt.Errorf("database name must be non-empty")
	}
	if name == "." || name == ".." || strings.Contains(name, "/") || strings.Contains(name, "\\") {
		return treedb.Options{}, "", fmt.Errorf("database name must not contain path separators")
	}

	suffix := strings.TrimSpace(cfg.DBFileSuffix)
	if suffix == "" {
		suffix = ".db"
	}
	if suffix == "." || suffix == ".." || strings.Contains(suffix, "/") || strings.Contains(suffix, "\\") || strings.Contains(suffix, "..") {
		return treedb.Options{}, "", fmt.Errorf("database file suffix must be a simple suffix without path separators or traversal")
	}
	dbPath := filepath.Join(parentDir, name+suffix)

	profileEnvKey := cfg.ProfileEnvKey
	if profileEnvKey == "" {
		profileEnvKey = EnvOpenProfile
	}
	rawProfile := os.Getenv(profileEnvKey)
	profile, err := ParsePublicProfile(rawProfile, cfg.DefaultProfile)
	if err != nil {
		return treedb.Options{}, "", fmt.Errorf("invalid %s=%q: %w", profileEnvKey, rawProfile, err)
	}
	opts := treedb.OptionsFor(profile, dbPath)

	keepRecentEnvKey := cfg.KeepRecentEnvKey
	if keepRecentEnvKey == "" {
		keepRecentEnvKey = EnvKeepRecent
	}
	if raw := strings.TrimSpace(os.Getenv(keepRecentEnvKey)); raw != "" {
		keepRecent, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return treedb.Options{}, "", fmt.Errorf("invalid %s=%q: %w", keepRecentEnvKey, raw, err)
		}
		opts.KeepRecent = keepRecent
	} else if cfg.DefaultKeepRecent != 0 {
		opts.KeepRecent = cfg.DefaultKeepRecent
	}

	memtableModeEnvKey := cfg.MemtableModeEnvKey
	if memtableModeEnvKey == "" {
		memtableModeEnvKey = EnvMemtableMode
	}
	defaultMemtableMode := strings.ToLower(strings.TrimSpace(cfg.DefaultMemtableMode))
	if defaultMemtableMode == "" {
		profileMemtableMode := strings.TrimSpace(opts.MemtableMode)
		if cfg.DefaultAdaptiveMemtableBase != "" &&
			(profileMemtableMode == "" || strings.EqualFold(profileMemtableMode, "adaptive")) {
			defaultMemtableMode = "adaptive:" + strings.ToLower(strings.TrimSpace(cfg.DefaultAdaptiveMemtableBase))
		}
	}
	if raw := strings.TrimSpace(os.Getenv(memtableModeEnvKey)); raw != "" {
		opts.MemtableMode = strings.ToLower(raw)
	} else if defaultMemtableMode != "" {
		opts.MemtableMode = defaultMemtableMode
	}

	return opts, dbPath, nil
}

func unsupportedAdapterFeature(feature string) error {
	return fmt.Errorf("%w: %s", ErrUnsupportedAdapterFeature, feature)
}

// Open resolves standardized wrapper options, opens TreeDB, and returns the
// canonical kvstore adapter.
func Open(cfg OpenConfig) (*Opened, error) {
	opts, dbPath, err := ResolveOptions(cfg)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		return nil, fmt.Errorf("error creating treedb directory: %w", err)
	}
	tdb, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	adapterName := cfg.AdapterName
	if adapterName == "" {
		adapterName = "TreeDB"
	}
	kv := treedbadapter.WrapNamed(tdb, adapterName)
	if cfg.ReadWorkers > 0 {
		kv.SetReadWorkers(cfg.ReadWorkers)
	}
	return &Opened{
		Path:    dbPath,
		Options: opts,
		DB:      tdb,
		KV:      kv,
	}, nil
}
