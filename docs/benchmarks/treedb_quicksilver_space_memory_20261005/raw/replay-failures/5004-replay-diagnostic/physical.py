#!/usr/bin/env python3
"""Hash regular files or report exact rebind page deltas; never opens a DB."""
import argparse, hashlib, json, pathlib, struct

def digest(p):
    h = hashlib.sha256()
    with p.open('rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''): h.update(b)
    return h.hexdigest()

def census(root):
    result = {}
    for p in sorted(root.rglob('*')):
        assert not p.is_symlink(), 'symlink not permitted in checkpoint copy'
        if p.is_file():
            s = p.stat()
            result[str(p.relative_to(root))] = dict(size=s.st_size, blocks=s.st_blocks, sha256=digest(p))
    return result

def main():
    p = argparse.ArgumentParser()
    p.add_argument('mode', choices=['census', 'exact', 'rebind-diff'])
    p.add_argument('root', type=pathlib.Path)
    p.add_argument('--master', type=pathlib.Path)
    p.add_argument('--sealed', type=pathlib.Path)
    p.add_argument('--output', type=pathlib.Path, required=True)
    a = p.parse_args()
    root = a.root.resolve()
    assert root.is_dir() and not a.output.exists()
    actual = census(root)
    result = dict(root=str(root), files=actual)
    if a.mode == 'exact':
        sealed = json.loads(a.sealed.read_text())['files']
        assert actual.keys() == sealed.keys()
        assert all((v['size'], v['sha256']) == (sealed[k]['size'], sealed[k]['sha256']) for k, v in actual.items())
        result['exact_pre_rebind_clone'] = True
    if a.mode == 'rebind-diff':
        master = a.master.resolve()
        assert master != root
        before = census(master)
        assert actual.keys() == before.keys(), 'rebind changed file membership'
        result['changed_index_pages'] = {}
        for rel, v in actual.items():
            assert v['size'] == before[rel]['size'], ('extent changed', rel)
            if v['sha256'] == before[rel]['sha256']: continue
            assert pathlib.Path(rel).name == 'index.db', ('non-index persistent bytes changed', rel)
            deltas = []
            with (master/rel).open('rb') as left, (root/rel).open('rb') as right:
                page_id = 0
                while True:
                    x, y = left.read(4096), right.read(4096)
                    if not x and not y: break
                    assert len(x) == len(y) == 4096
                    if x != y:
                        deltas.append(dict(page_id=page_id, before_sha256=hashlib.sha256(x).hexdigest(), after_sha256=hashlib.sha256(y).hexdigest()))
                    page_id += 1
            result['changed_index_pages'][rel] = deltas
        result['qualification'] = 'All non-index bytes and index extents equal; listed page differences require attribution to the frozen R2 explicit-restore metadata helper. No blanket index-page exemption.'
    a.output.write_text(json.dumps(result, indent=2)+'\n')

if __name__ == '__main__': main()
