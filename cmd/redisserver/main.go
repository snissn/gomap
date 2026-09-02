package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/internal/redisserver"
)

func main() {
	var cfg redisserver.Config

	registerConfigFlags(flag.CommandLine, &cfg)

	idleClose := flag.Duration("idle-close", 0, "close idle connections after this duration")
	cpuprofile := flag.String("cpuprofile", "", "write CPU profile to file (optional)")
	cpuprofileSeconds := flag.Int("cpuprofile-seconds", 0, "seconds to capture CPU profile (0=until process exit)")
	cpuprofileDelay := flag.Int("cpuprofile-delay", 2, "seconds to wait before starting CPU profile")

	flag.Parse()

	if cfg.Dir == "" {
		fmt.Fprintln(os.Stderr, "missing -dir")
		os.Exit(2)
	}

	cfg.IdleClose = *idleClose

	engine := strings.ToLower(strings.TrimSpace(cfg.Engine))
	log.Printf("redisserver: engine=%s addr=%s dir=%s", engine, cfg.Addr, cfg.Dir)

	server, err := redisserver.New(cfg)
	if err != nil {
		log.Fatalf("failed to start server: %v", err)
	}

	if *cpuprofile != "" {
		if *cpuprofileSeconds <= 0 {
			f, err := os.Create(*cpuprofile)
			if err != nil {
				log.Fatalf("cpuprofile create: %v", err)
			}
			if err := pprof.StartCPUProfile(f); err != nil {
				_ = f.Close()
				log.Fatalf("cpuprofile start: %v", err)
			}
			defer func() {
				pprof.StopCPUProfile()
				_ = f.Close()
			}()
		} else {
			path := *cpuprofile
			seconds := *cpuprofileSeconds
			delay := *cpuprofileDelay
			go func() {
				if delay > 0 {
					time.Sleep(time.Duration(delay) * time.Second)
				}
				f, err := os.Create(path)
				if err != nil {
					log.Printf("cpuprofile create: %v", err)
					return
				}
				if err := pprof.StartCPUProfile(f); err != nil {
					_ = f.Close()
					log.Printf("cpuprofile start: %v", err)
					return
				}
				time.Sleep(time.Duration(seconds) * time.Second)
				pprof.StopCPUProfile()
				_ = f.Close()
				log.Printf("cpuprofile written: %s", path)
			}()
		}
	}
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func registerConfigFlags(fs *flag.FlagSet, cfg *redisserver.Config) {
	if fs == nil || cfg == nil {
		return
	}

	fs.StringVar(&cfg.Addr, "addr", ":6380", "listen address")
	fs.StringVar(&cfg.Dir, "dir", "", "database directory (required)")
	fs.StringVar(&cfg.Engine, "engine", "hashdb", "engine: hashdb|treedb")
	fs.StringVar(&cfg.Auth, "auth", "", "require AUTH with the provided password")

	cfg.BatchFlushOnNonset = true
	fs.BoolVar(&cfg.BatchSets, "batch-sets", false, "enable per-connection SET batching")
	fs.IntVar(&cfg.BatchSize, "batch-size", 16, "batch size for batched SETs")
	fs.Var(&boolFlag{v: &cfg.BatchFlushOnNonset, set: &cfg.BatchFlushOnNonsetSet}, "batch-flush-on-nonset", "flush pending SETs before any non-SET command")

	fs.IntVar(&cfg.HashDBShards, "hashdb-shards", 0, "HashDB shard count (0=default)")
	fs.BoolVar(&cfg.HashDBCompression, "hashdb-compression", false, "HashDB value compression")

	fs.StringVar(&cfg.TreeDBProfile, "treedb-profile", string(treedb.ProfileCommandWALDurable), "TreeDB profile: "+treedb.ProfileFlagHelp)
	fs.Int64Var(&cfg.TreeDBFlushThreshold, "treedb-flush-threshold", 64*1024*1024, "TreeDB flush threshold in bytes")
	fs.IntVar(&cfg.TreeDBValueLogThreshold, "treedb-value-log-threshold", 0, "TreeDB value-log pointer threshold")
	fs.IntVar(&cfg.TreeDBWriteLanes, "treedb-write-lanes", 0, "TreeDB: command/value log write lanes (0=default)")
	fs.IntVar(&cfg.TreeDBMemtableShards, "treedb-memtable-shards", 0, "TreeDB: memtable shard count (0=default)")

	fs.Float64Var(&cfg.CompactDeadRatio, "compact-dead-ratio", 0.50, "compaction dead ratio threshold")
	fs.Uint64Var(&cfg.CompactMinBytes, "compact-min-bytes", 1*1024*1024, "compaction minimum slab bytes")
	fs.IntVar(&cfg.CompactMaxSlabs, "compact-max-slabs", 1, "compaction max slabs per run")
	fs.IntVar(&cfg.CompactMicroBatch, "compact-microbatch", 256, "compaction micro-batch size")
	fs.BoolVar(&cfg.CompactRotateBeforeWrite, "compact-rotate-before-write", false, "compaction rotate before copy")
	fs.Int64Var(&cfg.CompactCopyBytesPerSec, "compact-copy-bps", 0, "compaction copy rate limit (bytes/sec)")
	fs.Int64Var(&cfg.CompactCopyBurstBytes, "compact-copy-burst", 0, "compaction copy burst (bytes)")
}

type boolFlag struct {
	v   *bool
	set *bool
}

func (b *boolFlag) String() string {
	if b == nil || b.v == nil {
		return ""
	}
	return strconv.FormatBool(*b.v)
}

func (b *boolFlag) Set(s string) error {
	val, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	if b.v != nil {
		*b.v = val
	}
	if b.set != nil {
		*b.set = true
	}
	return nil
}

func (b *boolFlag) IsBoolFlag() bool { return true }
