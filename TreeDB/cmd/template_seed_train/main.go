package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/template"
)

type templateBackendKV struct{ db *treedbdb.DB }

func (kv templateBackendKV) Get(key []byte) ([]byte, error) {
	if kv.db == nil {
		return nil, nil
	}
	return kv.db.Get(key)
}

func (kv templateBackendKV) SetSync(key, value []byte) error {
	if kv.db == nil {
		return nil
	}
	return kv.db.SetSync(key, value)
}

func (kv templateBackendKV) DeleteSync(key []byte) error {
	if kv.db == nil {
		return nil
	}
	return kv.db.DeleteSync(key)
}

func (kv templateBackendKV) NewBatch() templatedb.Batch {
	if kv.db == nil {
		return nil
	}
	return kv.db.NewBatch()
}

func prefixEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func statU64(m map[string]string, k string) uint64 {
	v, ok := m[k]
	if !ok {
		return 0
	}
	u, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0
	}
	return u
}

func applyPersistedFormatConfig(dir string, opts *treedbdb.Options) {
	if opts == nil || opts.IgnoreFormatConfig {
		return
	}
	cfg, ok, err := treedbdb.LoadFormatConfig(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: format config for %s: %v\n", dir, err)
		return
	}
	if ok {
		cfg.ApplyToOptions(opts)
	}
}

func countPrefix(db *treedbdb.DB, prefix []byte) (int, error) {
	it, err := db.Iterator(prefix, prefixEnd(prefix))
	if err != nil {
		return 0, err
	}
	defer it.Close()
	n := 0
	for ; it.Valid(); it.Next() {
		n++
	}
	if err := it.Error(); err != nil {
		return 0, err
	}
	return n, nil
}

type trainStats struct {
	scanned   int
	processed int
	kept      int
}

type readOnlyTemplateStore struct {
	inner template.Store
}

func (s readOnlyTemplateStore) GetCandidates(ctx context.Context, fp uint64, max int) ([]template.Candidate, error) {
	if s.inner == nil {
		return nil, nil
	}
	return s.inner.GetCandidates(ctx, fp, max)
}

func (s readOnlyTemplateStore) GetTemplateDef(ctx context.Context, templateID uint64) ([]byte, error) {
	if s.inner == nil {
		return nil, template.ErrMissingTemplate
	}
	return s.inner.GetTemplateDef(ctx, templateID)
}

func (s readOnlyTemplateStore) PutTemplateDef(context.Context, []byte, []uint64) (uint64, error) {
	return 0, errors.New("template-seed: probe store is read-only")
}

var errStopScan = errors.New("template-seed: stop scan")

func shouldSample(idx, stride int) bool {
	if stride <= 1 {
		return true
	}
	return idx%stride == 0
}

func trainPrefix(engine *template.Engine, store *templatedb.Store, it iterator.UnsafeIterator, limit, stride, printEvery int) (trainStats, error) {
	defer it.Close()
	stats := trainStats{}
	if stride <= 0 {
		stride = 1
	}

	for ; it.Valid(); it.Next() {
		if limit > 0 && stats.processed >= limit {
			break
		}
		if shouldSample(stats.scanned, stride) {
			val := it.ValueCopy(nil)
			if len(val) > 0 {
				stats.processed++
				if _, ok := engine.Encode(context.Background(), val, store); ok {
					stats.kept++
				}
			}
			if printEvery > 0 && stats.processed > 0 && stats.processed%printEvery == 0 {
				s := engine.StatsSnapshot()
				fmt.Printf("seed-progress source=prefix processed=%d kept=%d templates_published=%d publish_defs=%d train_enqueued=%d train_processed=%d\n",
					stats.processed,
					stats.kept,
					statU64(s, "templates_published_total"),
					statU64(s, "publish_defs_total"),
					statU64(s, "train_enqueued_total"),
					statU64(s, "train_processed_total"),
				)
			}
		}
		stats.scanned++
	}
	if err := it.Error(); err != nil {
		return stats, err
	}
	return stats, nil
}

func trainPointerValues(engine *template.Engine, store *templatedb.Store, it iterator.UnsafeIterator, limit, stride, printEvery int) (trainStats, error) {
	defer it.Close()
	stats := trainStats{}
	if stride <= 0 {
		stride = 1
	}

	for ; it.Valid(); it.Next() {
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer == 0 || !page.IsValueLogFileID(ptr.FileID) {
			continue
		}
		if limit > 0 && stats.processed >= limit {
			break
		}

		if shouldSample(stats.scanned, stride) {
			val := it.ValueCopy(nil)
			if len(val) > 0 {
				stats.processed++
				if _, ok := engine.Encode(context.Background(), val, store); ok {
					stats.kept++
				}
				if printEvery > 0 && stats.processed%printEvery == 0 {
					s := engine.StatsSnapshot()
					fmt.Printf("seed-progress source=pointer processed=%d kept=%d templates_published=%d publish_defs=%d train_enqueued=%d train_processed=%d\n",
						stats.processed,
						stats.kept,
						statU64(s, "templates_published_total"),
						statU64(s, "publish_defs_total"),
						statU64(s, "train_enqueued_total"),
						statU64(s, "train_processed_total"),
					)
				}
			}
		}
		stats.scanned++
	}
	if err := it.Error(); err != nil {
		return stats, err
	}
	return stats, nil
}

type outerLeafKey struct {
	fileID uint32
	offset uint64
}

func trainOuterLeafValues(engine *template.Engine, store *templatedb.Store, backend *treedbdb.DB, limit, stride, printEvery int) (trainStats, error) {
	stats := trainStats{}
	if stride <= 0 {
		stride = 1
	}
	snap := backend.AcquireSnapshot()
	if snap == nil {
		return stats, errors.New("acquire snapshot: nil snapshot")
	}
	defer snap.Close()

	state := snap.State()
	if state == nil {
		return stats, errors.New("snapshot state unavailable")
	}
	pgr := snap.Pager()
	if pgr == nil {
		return stats, errors.New("snapshot pager unavailable")
	}
	reader := treedbdb.ValueReaderForState(state)
	if reader == nil {
		return stats, errors.New("value-log reader unavailable")
	}

	seenCap := 0
	if limit > 0 {
		seenCap = limit
		if stride > 1 {
			seenCap = (limit + stride - 1) / stride
		}
		if seenCap < 1 {
			seenCap = 1
		}
		if seenCap > 1<<20 {
			seenCap = 1 << 20
		}
	}
	seen := make(map[outerLeafKey]struct{}, seenCap)
	visit := func(ptr page.LeafLogPtr) error {
		key := outerLeafKey{fileID: ptr.ValueLogFileID(), offset: ptr.Offset}
		if _, ok := seen[key]; ok {
			return nil
		}
		seen[key] = struct{}{}

		if limit > 0 && stats.processed >= limit {
			return errStopScan
		}
		if shouldSample(stats.scanned, stride) {
			payload, err := reader.ReadUnsafe(ptr.ValuePtr())
			if err != nil {
				return err
			}
			if len(payload) > 0 {
				stats.processed++
				if _, ok := engine.Encode(context.Background(), payload, store); ok {
					stats.kept++
				}
				if printEvery > 0 && stats.processed%printEvery == 0 {
					s := engine.StatsSnapshot()
					fmt.Printf("seed-progress source=outerleaf processed=%d kept=%d templates_published=%d publish_defs=%d train_enqueued=%d train_processed=%d\n",
						stats.processed,
						stats.kept,
						statU64(s, "templates_published_total"),
						statU64(s, "publish_defs_total"),
						statU64(s, "train_enqueued_total"),
						statU64(s, "train_processed_total"),
					)
				}
			}
		}
		stats.scanned++
		return nil
	}

	ctx := context.Background()
	roots := []uint64{state.RootPageID, state.SystemRootPageID}
	for _, root := range roots {
		if root == 0 {
			continue
		}
		if err := leafrefscan.Walk(ctx, root, pgr.Get, nil, visit); err != nil {
			if errors.Is(err, errStopScan) {
				break
			}
			return stats, err
		}
	}
	return stats, nil
}

func probePrefix(engine *template.Engine, store template.Store, it iterator.UnsafeIterator, limit int) (attempted, kept int, err error) {
	defer it.Close()
	for ; it.Valid(); it.Next() {
		if limit > 0 && attempted >= limit {
			break
		}
		val := it.ValueCopy(nil)
		if len(val) == 0 {
			continue
		}
		attempted++
		if _, ok := engine.Encode(context.Background(), val, store); ok {
			kept++
		}
	}
	if err := it.Error(); err != nil {
		return attempted, kept, err
	}
	return attempted, kept, nil
}

func probePointer(engine *template.Engine, store template.Store, it iterator.UnsafeIterator, limit int) (attempted, kept int, err error) {
	defer it.Close()
	for ; it.Valid(); it.Next() {
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer == 0 || !page.IsValueLogFileID(ptr.FileID) {
			continue
		}
		if limit > 0 && attempted >= limit {
			break
		}
		val := it.ValueCopy(nil)
		if len(val) == 0 {
			continue
		}
		attempted++
		if _, ok := engine.Encode(context.Background(), val, store); ok {
			kept++
		}
	}
	if err := it.Error(); err != nil {
		return attempted, kept, err
	}
	return attempted, kept, nil
}

func probeOuterLeaf(engine *template.Engine, store template.Store, backend *treedbdb.DB, limit int) (attempted, kept int, err error) {
	snap := backend.AcquireSnapshot()
	if snap == nil {
		return 0, 0, errors.New("acquire snapshot: nil snapshot")
	}
	defer snap.Close()
	state := snap.State()
	if state == nil {
		return 0, 0, errors.New("snapshot state unavailable")
	}
	pgr := snap.Pager()
	if pgr == nil {
		return 0, 0, errors.New("snapshot pager unavailable")
	}
	reader := treedbdb.ValueReaderForState(state)
	if reader == nil {
		return 0, 0, errors.New("value-log reader unavailable")
	}
	seenCap := 0
	if limit > 0 {
		seenCap = limit
		if seenCap > 1<<20 {
			seenCap = 1 << 20
		}
	}
	seen := make(map[outerLeafKey]struct{}, seenCap)
	visit := func(ptr page.LeafLogPtr) error {
		key := outerLeafKey{fileID: ptr.ValueLogFileID(), offset: ptr.Offset}
		if _, ok := seen[key]; ok {
			return nil
		}
		seen[key] = struct{}{}
		if limit > 0 && attempted >= limit {
			return errStopScan
		}
		payload, err := reader.ReadUnsafe(ptr.ValuePtr())
		if err != nil {
			return err
		}
		if len(payload) == 0 {
			return nil
		}
		attempted++
		if _, ok := engine.Encode(context.Background(), payload, store); ok {
			kept++
		}
		return nil
	}
	roots := []uint64{state.RootPageID, state.SystemRootPageID}
	for _, root := range roots {
		if root == 0 {
			continue
		}
		if err := leafrefscan.Walk(context.Background(), root, pgr.Get, nil, visit); err != nil {
			if errors.Is(err, errStopScan) {
				break
			}
			return attempted, kept, err
		}
	}
	return attempted, kept, nil
}

func waitForTrainer(engine *template.Engine, maxWait time.Duration) map[string]string {
	deadline := time.Now().Add(maxWait)
	stable := 0
	var prevDefs uint64
	var last map[string]string
	for {
		last = engine.StatsSnapshot()
		enq := statU64(last, "train_enqueued_total")
		proc := statU64(last, "train_processed_total")
		defs := statU64(last, "publish_defs_total")
		if enq == proc && defs == prevDefs {
			stable++
		} else {
			stable = 0
		}
		prevDefs = defs
		if stable >= 4 || time.Now().After(deadline) {
			return last
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func main() {
	appDir := flag.String("app-dir", "", "TreeDB application dir (contains maindb/dictdb/templatedb)")
	source := flag.String("source", "mixed", "training source: prefix|pointer|outerleaf|mixed")
	prefix := flag.String("prefix", "s/", "key prefix to sample for training")
	limit := flag.Int("limit", 250000, "max records to scan (0=all)")
	stride := flag.Int("stride", 1, "sample stride")
	probe := flag.Int("probe", 50000, "probe records after training (0=skip)")
	pointerLimit := flag.Int("pointer-limit", 0, "override pointer source limit (0 uses -limit)")
	outerLeafLimit := flag.Int("outer-leaf-limit", 0, "override outer-leaf source limit (0 uses -limit)")
	pointerProbe := flag.Int("pointer-probe", 0, "override pointer probe count (0 uses -probe)")
	outerLeafProbe := flag.Int("outer-leaf-probe", 0, "override outer-leaf probe count (0 uses -probe)")
	printEvery := flag.Int("print-every", 20000, "progress print frequency in processed records")
	waitSec := flag.Int("wait-seconds", 90, "max wait for async trainer/publisher")

	minSavings := flag.Int("min-savings-bytes", 1, "template min savings bytes")
	minPresence := flag.Float64("min-presence-ratio", 0.85, "min presence ratio")
	minAnchorFreq := flag.Int("min-anchor-freq", 8, "min anchor freq")
	synthEvery := flag.Int("synthesize-every", 32, "samples per synthesis")
	minPublishBytes := flag.Int("min-publish-savings-bytes", 8, "min publish savings bytes")
	minPublishRatio := flag.Float64("min-publish-ratio", 0.995, "min publish ratio")
	flag.Parse()

	if *appDir == "" {
		log.Fatal("-app-dir is required")
	}

	rootDir := filepath.Clean(*appDir)
	mainDir := rootDir
	if info, statErr := os.Stat(filepath.Join(rootDir, "maindb", "index.db")); statErr == nil && !info.IsDir() {
		mainDir = filepath.Join(rootDir, "maindb")
	} else if filepath.Base(rootDir) == "maindb" {
		rootDir = filepath.Dir(rootDir)
	}

	templateDir := filepath.Join(rootDir, "templatedb")
	templateOpts := treedbdb.Options{
		Dir:                    templateDir,
		DisableBackgroundPrune: true,
	}
	applyPersistedFormatConfig(templateDir, &templateOpts)
	templateOpts.IndexOuterLeavesInValueLog = false
	templateOpts.ValueLog.Compression = treedbdb.ValueLogCompressionOff
	templateOpts.ValueLog.DictLookup = nil
	templateOpts.ValueLog.TemplateMode = template.TemplateOff
	templateOpts.ValueLog.TemplateLookup = nil
	templateOpts.ValueLog.TemplateDecodeOptions = template.DecodeOptions{}
	tmplDB, err := treedbdb.Open(templateOpts)
	if err != nil {
		log.Fatalf("open templatedb: %v", err)
	}
	defer tmplDB.Close()
	store := templatedb.New(templateBackendKV{db: tmplDB}, templatedb.Config{})

	backendOpts := treedbdb.Options{
		Dir:                    mainDir,
		ReadOnly:               true,
		DisableBackgroundPrune: true,
	}
	applyPersistedFormatConfig(mainDir, &backendOpts)
	backendOpts.ValueLog.TemplateLookup = func(templateID uint64) ([]byte, error) {
		return store.GetTemplateDef(context.Background(), templateID)
	}
	if backendOpts.ValueLog.TemplateDecodeOptions == (template.DecodeOptions{}) {
		tcfg := template.NormalizeConfig(backendOpts.ValueLog.TemplateConfig)
		backendOpts.ValueLog.TemplateDecodeOptions = template.DecodeOptions{
			MaxDecodedBytes: tcfg.MaxDecodedBytes,
			MaxGaps:         tcfg.MaxGaps,
			DefCacheSize:    tcfg.DefCacheSize,
		}
	}

	dictDir := filepath.Join(rootDir, "dictdb")
	dictIndex := filepath.Join(dictDir, "index.db")
	if info, statErr := os.Stat(dictIndex); statErr == nil && !info.IsDir() {
		dictOpts := treedbdb.Options{
			Dir:                        dictDir,
			ReadOnly:                   true,
			DisableBackgroundPrune:     true,
			IndexOuterLeavesInValueLog: false,
		}
		applyPersistedFormatConfig(dictDir, &dictOpts)
		dictOpts.IndexOuterLeavesInValueLog = false
		dictBackend, err := treedbdb.Open(dictOpts)
		if err != nil {
			log.Fatalf("open dictdb: %v", err)
		}
		defer dictBackend.Close()
		dictStore := dictdb.New(dictBackend)
		backendOpts.ValueLog.DictLookup = func(dictID uint64) ([]byte, error) {
			return dictStore.GetDictBytes(context.Background(), dictID)
		}
	}

	backend, err := treedbdb.Open(backendOpts)
	if err != nil {
		log.Fatalf("open backend DB: %v", err)
	}
	defer backend.Close()

	defsBefore, err := countPrefix(tmplDB, []byte{1, 't'})
	if err != nil {
		log.Fatalf("count template defs before: %v", err)
	}
	fpsBefore, err := countPrefix(tmplDB, []byte{1, 'f'})
	if err != nil {
		log.Fatalf("count fp lists before: %v", err)
	}

	cfg := template.Config{
		MinSavingsBytes:        *minSavings,
		TrainSampleStride:      1,
		SynthesizeEverySamples: *synthEvery,
		MinAnchorFreq:          *minAnchorFreq,
		MinPresenceRatio:       *minPresence,
		MinPublishSavingsBytes: *minPublishBytes,
		MinPublishRatio:        *minPublishRatio,
		MaxValuesPerBucket:     512,
		MaxBytesPerBucket:      16 << 20,
		ColdSearchAfter:        1 << 30,
		ColdSearchProbeEvery:   1 << 30,
	}
	engine := template.NewEngine(cfg)
	defer engine.Close()

	mode := strings.ToLower(strings.TrimSpace(*source))
	switch mode {
	case "prefix", "pointer", "outerleaf", "mixed":
	default:
		log.Fatalf("invalid -source %q (want prefix|pointer|outerleaf|mixed)", mode)
	}

	pLimit := *limit
	oLimit := *limit
	pProbe := *probe
	oProbe := *probe
	if *pointerLimit > 0 {
		pLimit = *pointerLimit
	}
	if *outerLeafLimit > 0 {
		oLimit = *outerLeafLimit
	}
	if *pointerProbe > 0 {
		pProbe = *pointerProbe
	}
	if *outerLeafProbe > 0 {
		oProbe = *outerLeafProbe
	}

	fmt.Printf("seeding-start app=%s source=%s prefix=%q limit=%d pointer_limit=%d outer_leaf_limit=%d stride=%d probe=%d pointer_probe=%d outer_leaf_probe=%d\n",
		*appDir, mode, *prefix, *limit, pLimit, oLimit, *stride, *probe, pProbe, oProbe)

	var prefixStats trainStats
	var pointerStats trainStats
	var outerLeafStats trainStats

	seedStart := time.Now()
	if mode == "prefix" || mode == "mixed" {
		start := []byte(*prefix)
		end := prefixEnd(start)
		it, err := backend.Iterator(start, end)
		if err != nil {
			log.Fatalf("iterator train prefix: %v", err)
		}
		l := *limit
		prefixStats, err = trainPrefix(engine, store, it, l, *stride, *printEvery)
		if err != nil {
			log.Fatalf("train prefix: %v", err)
		}
	}
	if mode == "pointer" || mode == "mixed" {
		it, err := backend.Iterator(nil, nil)
		if err != nil {
			log.Fatalf("iterator train pointer: %v", err)
		}
		pointerStats, err = trainPointerValues(engine, store, it, pLimit, *stride, *printEvery)
		if err != nil {
			log.Fatalf("train pointer: %v", err)
		}
	}
	if mode == "outerleaf" || mode == "mixed" {
		outerLeafStats, err = trainOuterLeafValues(engine, store, backend, oLimit, *stride, *printEvery)
		if err != nil {
			log.Fatalf("train outerleaf: %v", err)
		}
	}
	seedElapsed := time.Since(seedStart)

	statsAfterWait := waitForTrainer(engine, time.Duration(*waitSec)*time.Second)
	defsAfter, err := countPrefix(tmplDB, []byte{1, 't'})
	if err != nil {
		log.Fatalf("count template defs after: %v", err)
	}
	fpsAfter, err := countPrefix(tmplDB, []byte{1, 'f'})
	if err != nil {
		log.Fatalf("count fp lists after: %v", err)
	}

	probeAttempted := 0
	probeKept := 0
	probePointerAttempted := 0
	probePointerKept := 0
	probeOuterAttempted := 0
	probeOuterKept := 0
	if *probe > 0 || pProbe > 0 || oProbe > 0 {
		probeEngine := template.NewEngine(cfg)
		probeStore := readOnlyTemplateStore{inner: store}

		if (mode == "prefix" || mode == "mixed") && *probe > 0 {
			start := []byte(*prefix)
			end := prefixEnd(start)
			pit, err := backend.Iterator(start, end)
			if err != nil {
				log.Fatalf("iterator probe prefix: %v", err)
			}
			a, k, err := probePrefix(probeEngine, probeStore, pit, *probe)
			if err != nil {
				log.Fatalf("probe prefix: %v", err)
			}
			probeAttempted += a
			probeKept += k
		}
		if (mode == "pointer" || mode == "mixed") && pProbe > 0 {
			pit, err := backend.Iterator(nil, nil)
			if err != nil {
				log.Fatalf("iterator probe pointer: %v", err)
			}
			a, k, err := probePointer(probeEngine, probeStore, pit, pProbe)
			if err != nil {
				log.Fatalf("probe pointer: %v", err)
			}
			probePointerAttempted = a
			probePointerKept = k
			probeAttempted += a
			probeKept += k
		}
		if (mode == "outerleaf" || mode == "mixed") && oProbe > 0 {
			a, k, err := probeOuterLeaf(probeEngine, probeStore, backend, oProbe)
			if err != nil {
				log.Fatalf("probe outerleaf: %v", err)
			}
			probeOuterAttempted = a
			probeOuterKept = k
			probeAttempted += a
			probeKept += k
		}
		probeEngine.Close()
	}

	fmt.Printf("seed-summary source=%s scanned=%d processed=%d kept_during_seed=%d elapsed=%s prefix_scanned=%d prefix_processed=%d prefix_kept=%d pointer_scanned=%d pointer_processed=%d pointer_kept=%d outerleaf_scanned=%d outerleaf_processed=%d outerleaf_kept=%d defs_before=%d defs_after=%d fp_before=%d fp_after=%d templates_published=%d publish_defs=%d train_enqueued=%d train_processed=%d probe_attempted=%d probe_kept=%d pointer_probe_attempted=%d pointer_probe_kept=%d outerleaf_probe_attempted=%d outerleaf_probe_kept=%d\n",
		mode,
		prefixStats.scanned+pointerStats.scanned+outerLeafStats.scanned,
		prefixStats.processed+pointerStats.processed+outerLeafStats.processed,
		prefixStats.kept+pointerStats.kept+outerLeafStats.kept,
		seedElapsed.Round(time.Millisecond),
		prefixStats.scanned,
		prefixStats.processed,
		prefixStats.kept,
		pointerStats.scanned,
		pointerStats.processed,
		pointerStats.kept,
		outerLeafStats.scanned,
		outerLeafStats.processed,
		outerLeafStats.kept,
		defsBefore,
		defsAfter,
		fpsBefore,
		fpsAfter,
		statU64(statsAfterWait, "templates_published_total"),
		statU64(statsAfterWait, "publish_defs_total"),
		statU64(statsAfterWait, "train_enqueued_total"),
		statU64(statsAfterWait, "train_processed_total"),
		probeAttempted,
		probeKept,
		probePointerAttempted,
		probePointerKept,
		probeOuterAttempted,
		probeOuterKept,
	)
}
