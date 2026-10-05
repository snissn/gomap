#!/usr/bin/env python3
"""Generate a diagnostic-only suite overlay; no Go invocation or checkout edits."""
import argparse, hashlib, json, pathlib, subprocess

BASE = 'a6b383b6c0d6269a598390032ecce9fd619a4eac'
CAND = '90219d64ced3539accff5d0e50d9b5fa6424ed20'
REL = 'cmd/unified_bench/suite_quicksilver.go'
OUT = pathlib.Path(__file__).resolve().parent
sha = lambda b: hashlib.sha256(b).hexdigest()

def once(s, old, new):
    assert s.count(old) == 1, ('patch anchor count', old[:100], s.count(old))
    return s.replace(old, new)

def patch(s):
    s = once(s, '\tc = c.resolved()\n\tif err = c.validate();', '''\tc = c.resolved()
\tdiagnosticMode := os.Getenv("GOMAP_QS_REPLAY_MODE")
\tif (diagnosticMode != "create" && diagnosticMode != "read") || engine != "treedb" || cfg.Profile != "durable" || !cfg.KeepDir || c.Case != "realistic" || c.Keys != 3000000 || c.Reads != 6000000 || c.Workers != 4 || c.ReadBatch != 64 || c.Seed != 24 || c.Mixture != "primary" || c.WorkingSet != "uniform" || c.MissPercent != 90 || c.Updates != 40000 || c.Duration != 8*time.Second || c.CommitMode != "ordinary" {
\t\treturn res, fmt.Errorf("DIAGNOSTIC replay: invalid mode/configuration; use frozen durable-primary caller flags and -keep")
\t}
\tif err = c.validate();''')
    s = once(s, '\tt := time.Now()\n\tfor i := 0; i < c.Keys; i += 1000 {', '''\tt := time.Now()
\tvar elapsed time.Duration
\tvar e error
\tif diagnosticMode == "create" {
\tfor i := 0; i < c.Keys; i += 1000 {''')
    s = once(s, '\telapsed, e := quicksilverCheckpoint(db, cfg, engine, "quicksilver_initial")', '\telapsed, e = quicksilverCheckpoint(db, cfg, engine, "quicksilver_initial")')
    s = once(s, '\tres.InitialCheckpointMS = float64(elapsed) / float64(time.Millisecond)\n\tres.InitialStats', '\tres.InitialCheckpointMS = float64(elapsed) / float64(time.Millisecond)\n\t}\n\tres.InitialStats')
    s = once(s, '\t// Identical deterministic untimed warmup; no cold-device claim.', '''\tres.Correctness = "DIAGNOSTIC initial checkpoint replay: full initial oracle per process; existing timed owned identity/length/generation checks; no concurrent mutation or final checkpoint"
\tdiagnosticCapture := func(phase string) error {
\t\tfiles, e := quicksilverFiles(dir)
\t\tif e != nil { return e }
\t\tpath := os.Getenv("GOMAP_QS_REPLAY_JSONL")
\t\tif !filepath.IsAbs(path) || path == dir || strings.HasPrefix(path, dir+string(filepath.Separator)) { return fmt.Errorf("DIAGNOSTIC capture path must be absolute and outside DB") }
\t\tf, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
\t\tif os.IsExist(e) { f, e = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600) }
\t\tif e != nil { return e }
\t\te = json.NewEncoder(f).Encode(map[string]any{"phase": phase, "mode": diagnosticMode, "db": dir, "pid": os.Getpid(), "files": files, "stats": quicksilverStats(db)})
\t\treturn errors.Join(e, f.Close())
\t}
\tif err = diagnosticCapture("after_reopen_and_full_initial_oracle_before_warmup"); err != nil { return }
\tif diagnosticMode == "create" {
\t\treturn res, nil
\t}
\t// Identical deterministic untimed warmup; no cold-device claim.''')
    s = once(s, '\twriter := func(ctx context.Context) error {', '\tif err = diagnosticCapture("after_modes_0_1_2_before_close"); err != nil { return }\n\treturn res, nil // Diagnostic modes 0/1/2 only; deferred Close still runs.\n\twriter := func(ctx context.Context) error {')
    s = once(s, '''\t\tdir, e := os.MkdirTemp("", "bench-quicksilver-"+name+"-")
\t\tif e != nil {
\t\t\treturn "", e
\t\t}''', '''\t\tdir := os.Getenv("GOMAP_QS_REPLAY_DB")
\t\tif len(names) != 1 || !cfg.KeepDir || !filepath.IsAbs(dir) || filepath.Clean(dir) == string(filepath.Separator) {
\t\t\treturn "", fmt.Errorf("DIAGNOSTIC replay requires one engine, -keep, and an explicit absolute owned-copy GOMAP_QS_REPLAY_DB")
\t\t}
\t\tentries, e := os.ReadDir(dir)
\t\tif e != nil {
\t\t\treturn "", e
\t\t}
\t\tif (os.Getenv("GOMAP_QS_REPLAY_MODE") == "create") != (len(entries) == 0) {
\t\t\treturn "", fmt.Errorf("DIAGNOSTIC replay: create requires empty directory; read requires a sealed/rebound initial-checkpoint clone")
\t\t}''')
    return s

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--repo', default='/private/tmp/gomap-qs-5004-offsets')
    p.add_argument('--build-root', help='Write portable overlay manifest against this extracted source; verify original suite hash.')
    p.add_argument('--label', choices=['baseline', 'candidate'])
    a = p.parse_args()
    if a.build_root:
        receipt = json.loads((OUT/'receipt.json').read_text())
        assert a.label
        root = pathlib.Path(a.build_root).resolve()
        assert sha((root/REL).read_bytes()) == receipt['original_suite_sha256']
        assert sha((OUT/'suite_quicksilver.go').read_bytes()) == receipt['overlay_suite_sha256']
        (OUT/(a.label+'-overlay.json')).write_text(json.dumps({'Replace': {str(root/REL): str(OUT/'suite_quicksilver.go')}}, indent=2)+'\n')
        return
    sources = [subprocess.check_output(['git', '-C', a.repo, 'show', head+':'+REL]).decode() for head in [BASE, CAND]]
    assert sources[0] == sources[1], 'baseline/candidate suite drift'
    original = sources[0]
    overlaid = patch(original)
    try:
        patch(overlaid)
        raise AssertionError('duplicate patch accepted')
    except AssertionError as e:
        assert e.args[0] != 'duplicate patch accepted'
    # Prove the actual deterministic warmup, profile/read-phase loops and full oracle call remain literal source.
    start = '\t// Identical deterministic untimed warmup; no cold-device claim.'
    end = '\twriter := func(ctx context.Context) error {'
    original_reads = original[original.index(start):original.index(end)]
    assert original_reads in overlaid
    oracle = 'res.InitialVerifiedKeys, res.InitialVerifiedMisses, err = quicksilverRealisticVerify(db, c, res.UpdateStride, guard, false)'
    assert original.count(oracle) == overlaid.count(oracle) == 1
    create = overlaid.split('\tif diagnosticMode == "create" {', 1)[1].split('\n\t}\n\tres.InitialStats', 1)[0]
    for call in ['quicksilverWrite(db', 'quicksilverPrepareDeleted(db', 'quicksilverCheckpoint(db, cfg, engine, "quicksilver_initial")']:
        assert call in create
    assert overlaid.index('\treturn res, nil // Diagnostic modes') < overlaid.index(end)
    (OUT/'suite_quicksilver.go').write_text(overlaid)
    receipt = dict(baseline=BASE, candidate=CAND, original_suite_sha256=sha(original.encode()), overlay_suite_sha256=sha(overlaid.encode()), unchanged_read_region_sha256=sha(original_reads.encode()), validation='Python exact-once anchors, identical frozen source, unchanged warmup/read-phase/oracle, create-only initial write/checkpoint branch; NOT Go-built or syntax-checked')
    (OUT/'receipt.json').write_text(json.dumps(receipt, indent=2)+'\n')
    print(json.dumps(receipt, indent=2))

if __name__ == '__main__': main()
