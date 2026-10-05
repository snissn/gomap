#!/usr/bin/env python3
"""Generate diagnostic-only Go overlays; no repository writes or builds."""
import argparse
import hashlib
import json
import subprocess
from pathlib import Path

REFS = {
    'baseline': 'a6b383b6c0d6269a598390032ecce9fd619a4eac',
    'candidate': '90219d64ced3539accff5d0e50d9b5fa6424ed20',
}
FROZEN = 'fb42d5ba229d2cbfba8727272af02f9544a33389'
CACHE = 'TreeDB/internal/valuelog/grouped_frame_cache.go'
MAIN = 'cmd/unified_bench/main.go'
CACHE_ENV = 'GOMAP_QS_DIAG_CACHE_JSONL'
SNAP_ENV = 'GOMAP_QS_DIAG_SNAPSHOT_DIR'
IMPORTS = '\t"encoding/json"\n\t"fmt"\n\t"os"\n\t"reflect"\n\t"time"\n'
DECL = '''// Diagnostic overlay only: serialize JSONL writes from concurrent stats calls.
var groupedFrameCacheDiagnosticMu sync.Mutex

'''
SETUP = '''	diagnosticPath := os.Getenv("GOMAP_QS_DIAG_CACHE_JSONL")
	var diagnostic struct {
		Schema int
		PID int
		UnixNano int64
		Cache string
		OwnerPath string
		Stats GroupedFrameCacheStats
		Inline bool
		SlotSizeBytes uint64
		SlotStructBytes uint64
		InlineOffsetBytes uint64
		OffsetBackingBytes uint64
		StructuralMetadataBytes uint64
		LiveK map[int]uint64
		AllSlotOffsetCapacity map[int]uint64
		EmptySlotOffsetCapacity map[int]uint64
	}
	if diagnosticPath != "" {
		diagnostic.Schema, diagnostic.PID = 1, os.Getpid()
		diagnostic.UnixNano, diagnostic.Cache = time.Now().UnixNano(), fmt.Sprintf("%p", c)
		if c.owner != nil { diagnostic.OwnerPath = c.owner.Path }
		t := reflect.TypeOf(groupedFrameCacheSlot{})
		field, _ := t.FieldByName("offsets")
		diagnostic.Inline, diagnostic.SlotSizeBytes = field.Type.Kind() == reflect.Array, uint64(t.Size())
		diagnostic.LiveK = make(map[int]uint64)
		diagnostic.AllSlotOffsetCapacity = make(map[int]uint64)
		diagnostic.EmptySlotOffsetCapacity = make(map[int]uint64)
	}
'''
COLLECT = '''			if diagnosticPath != "" {
				n := cap(slot.offsets)
				diagnostic.AllSlotOffsetCapacity[n]++
				if valid { diagnostic.LiveK[slot.k]++ } else { diagnostic.EmptySlotOffsetCapacity[n]++ }
				if diagnostic.Inline { diagnostic.InlineOffsetBytes += uint64(n)*4 } else { diagnostic.OffsetBackingBytes += uint64(n)*4 }
			}
'''
EMIT = '''	if diagnosticPath != "" {
		diagnostic.Stats = st
		diagnostic.SlotStructBytes = uint64(st.AllocatedSlots)*diagnostic.SlotSizeBytes
		diagnostic.StructuralMetadataBytes = diagnostic.SlotStructBytes + diagnostic.OffsetBackingBytes
		groupedFrameCacheDiagnosticMu.Lock()
		defer groupedFrameCacheDiagnosticMu.Unlock()
		f, err := os.OpenFile(diagnosticPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil { panic(err) }
		err = json.NewEncoder(f).Encode(&diagnostic)
		closeErr := f.Close()
		if err != nil { panic(err) }
		if closeErr != nil { panic(closeErr) }
	}
'''
LINK = '''	if diagnosticDir := os.Getenv("GOMAP_QS_DIAG_SNAPSHOT_DIR"); diagnosticDir != "" {
		if err := os.Link(path, filepath.Join(diagnosticDir, filepath.Base(path))); err != nil {
			return fmt.Errorf("preserve diagnostic allocs snapshot: %w", err)
		}
	}
'''


def once(source, old, new):
    assert source.count(old) == 1, ('expected exactly one patch anchor', old, source.count(old))
    return source.replace(old, new, 1)


def patch_cache(source):
    source = once(source, 'import (\n', 'import (\n' + IMPORTS)
    source = once(source, 'func (c *groupedFrameCache) stats() GroupedFrameCacheStats {', DECL + 'func (c *groupedFrameCache) stats() GroupedFrameCacheStats {')
    source = once(source, '\tst := GroupedFrameCacheStats{\n', SETUP + '\tst := GroupedFrameCacheStats{\n')
    source = once(source, '\t\t\tvalid := slot.valid\n\t\t\tslot.mu.RUnlock()', '\t\t\tvalid := slot.valid\n' + COLLECT + '\t\t\tslot.mu.RUnlock()')
    return once(source, '\treturn st\n}\n\nfunc (st *GroupedFrameCacheStats) add', EMIT + '\treturn st\n}\n\nfunc (st *GroupedFrameCacheStats) add')


def patch_main(source):
    start = source.index('func writeAllocsSnapshot(path string) error {')
    end = source.index('\nfunc writeAllocsSnapshotTemp(', start)
    section = source[start:end]
    anchor = '\tif err := prof.WriteTo(f, 0); err != nil {\n\t\treturn err\n\t}\n\treturn nil'
    section = once(section, anchor, anchor[:-len('\treturn nil')] + LINK + '\treturn nil')
    return source[:start] + section + source[end:]


def git_source(repo, ref, path):
    return subprocess.check_output(['git', '-C', str(repo), 'show', ref + ':' + path]).decode()


def digest(data):
    return hashlib.sha256(data.encode() if isinstance(data, str) else data).hexdigest()


def validate():
    # Mechanical arithmetic fixture: live small K, never-used, empty retained backing,
    # maximum K. Baseline owns inline tables for every slot including empty slots.
    fixture = [(True, 1, 2), (True, 4, 5), (False, 0, 0), (False, 0, 33), (True, 255, 256)]
    expected_live = {1: 1, 4: 1, 255: 1}
    for inline, slot_size in [(True, 1136), (False, 136)]:
        live, all_caps, empty_caps = {}, {}, {}
        for valid, k, cap in fixture:
            cap = 256 if inline else cap
            all_caps[cap] = all_caps.get(cap, 0) + 1
            if valid: live[k] = live.get(k, 0) + 1
            else: empty_caps[cap] = empty_caps.get(cap, 0) + 1
        offset_bytes = sum(cap*4*n for cap, n in all_caps.items())
        backing_bytes = 0 if inline else offset_bytes
        assert live == expected_live and sum(all_caps.values()) == 5 and sum(empty_caps.values()) == 2
        assert offset_bytes == (5120 if inline else 1184)
        assert 5*slot_size + backing_bytes == (5680 if inline else 1864)
        assert empty_caps == ({256: 2} if inline else {0: 1, 33: 1})
    # os.Link preserves the inode after removing the original temporary name.
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        original, retained = Path(tmp)/'before.pprof', Path(tmp)/'retained.pprof'
        original.write_bytes(b'full-snapshot-fixture')
        retained.hardlink_to(original)
        assert original.stat().st_ino == retained.stat().st_ino
        original.unlink()
        assert retained.read_bytes() == b'full-snapshot-fixture'


def manifests(out, build_root):
    for name in REFS:
        replace = {str(build_root/path): str(out/name/Path(path).name) for path in (CACHE, MAIN)}
        (out/(name + '-overlay.json')).write_text(json.dumps({'Replace': replace}, indent=2)+'\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    parser.add_argument('--out', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--build-root', type=Path)
    parser.add_argument('--manifest-only', action='store_true', help='After copying packet to runner, rebase absolute overlay paths; no git needed.')
    args = parser.parse_args()
    out, build_root = args.out.resolve(), (args.build_root or args.repo).resolve()
    if args.manifest_only:
        manifests(out, build_root)
        print('PASS rebased manifests', out)
        return
    out.mkdir(parents=True, exist_ok=True)
    validate()
    assert subprocess.check_output(['git', '-C', str(args.repo), 'rev-parse', FROZEN+'^{tree}']) == subprocess.check_output(['git', '-C', str(args.repo), 'rev-parse', REFS['baseline']+'^{tree}'])
    line_count = sum(len(fragment.splitlines()) for fragment in (IMPORTS, DECL, SETUP, COLLECT, EMIT, LINK))
    assert line_count <= 80, line_count
    receipt = {'refs': REFS, 'frozen_baseline_tree': FROZEN, 'instrumentation_lines': line_count, 'source_hashes': {}, 'self_check': 'PASS exact-once anchors; duplicate patch rejected; baseline/candidate histogram arithmetic; hard-link retention'}
    for name, ref in REFS.items():
        (out/name).mkdir(exist_ok=True)
        for path, patcher in [(CACHE, patch_cache), (MAIN, patch_main)]:
            original = git_source(args.repo, ref, path)
            patched = patcher(original)
            try:
                patcher(patched)
            except AssertionError:
                pass
            else:
                raise AssertionError('duplicate instrumentation accepted')
            if path == CACHE:
                assert patched.count('diagnostic.LiveK[slot.k]++') == 1
                assert patched.count('json.NewEncoder(f).Encode(&diagnostic)') == 1
                assert patched.index('diagnostic.LiveK[slot.k]++') < patched.index('slot.mu.RUnlock()', patched.index('valid := slot.valid'))
                assert patched.count(CACHE_ENV) == 1
            else:
                assert patched.count(SNAP_ENV) == 1
                assert patched.count('runtime.GC()') == original.count('runtime.GC()')
                assert patched.index('os.Link(path,') > patched.index('if err := prof.WriteTo(f, 0);', patched.index('func writeAllocsSnapshot('))
            target = out/name/Path(path).name
            target.write_text(patched)
            receipt['source_hashes'][name+'/'+Path(path).name] = {'original_sha256': digest(original), 'overlay_sha256': digest(patched)}
    manifests(out, build_root)
    receipt['generator_sha256'] = digest(Path(__file__).read_bytes())
    (out/'receipt.json').write_text(json.dumps(receipt, indent=2)+'\n')
    print(json.dumps(receipt, indent=2))


if __name__ == '__main__':
    main()
