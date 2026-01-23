package redisserver

import (
	"errors"
	"fmt"
	"strings"

	hashdb "github.com/snissn/gomap/HashDB"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/compaction"
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
	opts := treedb.Options{
		Dir:                      cfg.Dir,
		FlushThreshold:           cfg.TreeDBFlushThreshold,
		ValueLogPointerThreshold: cfg.TreeDBValueLogThreshold,
		DisableValueLog:          cfg.TreeDBDisableValueLog,
		DisableWAL:               cfg.TreeDBDisableWAL,
		DisableJournal:           cfg.TreeDBDisableJournal,
		RelaxedSync:              cfg.TreeDBRelaxedSync,
	}
	switch strings.ToLower(strings.TrimSpace(cfg.TreeDBMode)) {
	case "", "cached":
		opts.Mode = treedb.ModeCached
	case "backend":
		opts.Mode = treedb.ModeBackend
	default:
		return nil, fmt.Errorf("unknown treedb mode: %s", cfg.TreeDBMode)
	}
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
	db  *treedb.DB
	cfg Config
}

func (c *treeDBCompactor) Compact() error {
	opts := compaction.Options{
		DeadRatioThreshold: c.cfg.CompactDeadRatio,
		MinTotalBytes:      c.cfg.CompactMinBytes,
		MaxSlabs:           c.cfg.CompactMaxSlabs,
		MicroBatchSize:     c.cfg.CompactMicroBatch,
		RotateBeforeWrite:  c.cfg.CompactRotateBeforeWrite,
		CopyBytesPerSec:    c.cfg.CompactCopyBytesPerSec,
		CopyBurstBytes:     c.cfg.CompactCopyBurstBytes,
	}
	return c.db.CompactCandidates(opts)
}
