package redisserver

import (
	"errors"
	"fmt"
	"strings"

	hashdb "github.com/snissn/gomap/HashDB"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/kvstore"
	hashdbadapter "github.com/snissn/gomap/kvstore/adapters/hashdb"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

// OpenEngine opens the configured backend and returns a kvstore wrapper.
func OpenEngine(cfg Config) (kvstore.DB, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Engine)) {
	case "hashdb", "gomap":
		return openHashDB(cfg)
	case "treedb":
		return openTreeDB(cfg)
	default:
		return nil, fmt.Errorf("unknown engine: %s", cfg.Engine)
	}
}

func openHashDB(cfg Config) (kvstore.DB, error) {
	if cfg.Dir == "" {
		return nil, errors.New("db dir required")
	}
	store := &hashdb.HashDB{}
	var err error
	if cfg.HashDBShards > 0 {
		err = store.NewWithShards(cfg.Dir, cfg.HashDBShards)
	} else {
		err = store.New(cfg.Dir)
	}
	if err != nil {
		return nil, err
	}
	store.SetCompression(cfg.HashDBCompression)
	return hashdbadapter.Wrap(store), nil
}

func openTreeDB(cfg Config) (kvstore.DB, error) {
	if cfg.Dir == "" {
		return nil, errors.New("db dir required")
	}
	profile, ok := treedb.ParsePublicProfile(cfg.TreeDBProfile, treedb.ProfileCommandWALDurable)
	if !ok {
		return nil, fmt.Errorf("unsupported TreeDB profile %q; allowed: %s", cfg.TreeDBProfile, treedb.ProfileFlagHelp)
	}
	opts := treedb.OptionsFor(profile, cfg.Dir)
	opts.FlushThreshold = cfg.TreeDBFlushThreshold
	opts.ValueLog.PointerThreshold = cfg.TreeDBValueLogThreshold
	opts.JournalLanes = cfg.TreeDBWriteLanes
	opts.MemtableShards = cfg.TreeDBMemtableShards
	db, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	return treedbadapter.WrapNamed(db, "TreeDB"), nil
}

// Optional interfaces used by admin commands.
type checkpointer interface{ Checkpoint() error }
type compactor interface{ Compact() error }
type clearer interface{ Clear() error }

// treeDBCompactor adapts TreeDB's compaction API to the compactor interface.
type treeDBCompactor struct {
	db *treedb.DB
}

func (c *treeDBCompactor) Compact() error {
	if c == nil || c.db == nil {
		return errors.New("treedb compactor unavailable")
	}
	return c.db.CompactIndex()
}

func treeDBCommandWALEnabled(db *treedb.DB) bool {
	if db == nil {
		return false
	}
	stats := db.Stats()
	return stats["treedb.command_wal.enabled"] == "true"
}
