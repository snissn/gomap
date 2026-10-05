#!/usr/bin/env python3
import importlib.util, json, os, pathlib, shutil, tempfile
from contextlib import ExitStack

packet = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('inplace', packet/'inplace.py')
h = importlib.util.module_from_spec(spec); spec.loader.exec_module(h)

def rejected(fn):
    try: fn()
    except AssertionError: return
    raise AssertionError('invalid resource change accepted')

with tempfile.TemporaryDirectory(prefix='selfcheck-', dir=packet) as temp:
    t = pathlib.Path(temp); root = t/'live'; (root/'maindb').mkdir(parents=True)
    (root/'maindb/index.db').write_bytes(b'index initial')
    (root/'maindb/format.json').write_bytes(b'format initial')
    original = h.census(root); backup = t/'backup'; shutil.copytree(root, backup, copy_function=shutil.copyfile)
    manifest = dict(original=original, backup=h.census(backup), format_identity_exceptions=['maindb/format.json'])
    formats = set(manifest['format_identity_exceptions'])
    with ExitStack() as stack:
        pins = h.pin(root, original, formats, stack)
        (root/'maindb/index.db').write_bytes(b'index modified in place plus tail')
        tempformat = root/'maindb/new-format'; tempformat.write_bytes(b'format changed'); os.replace(tempformat, root/'maindb/format.json')
        (root/'new-owned-file').write_bytes(b'new file')
        h.preserve(root, original, h.census(root), t/'post')
        restored = h.reset(root, backup, manifest, pins)
        h.exact_guard(original, restored, formats)
        assert (root/'maindb/index.db').stat().st_ino == original['maindb/index.db']['ino']
        assert (root/'maindb/format.json').stat().st_ino != original['maindb/format.json']['ino']
        replacement = root/'replace-index'; replacement.write_bytes(b'index initial'); os.replace(replacement, root/'maindb/index.db')
        rejected(lambda: h.identity_guard(original, h.census(root), formats, pins))
        before_failed = h.census(root)
        rejected(lambda: h.reset(root, backup, manifest, pins))
        assert before_failed == h.census(root), 'failed preflight mutated live DB'
        (root/'maindb/index.db').unlink()
        rejected(lambda: h.identity_guard(original, h.census(root), formats, pins))
        shutil.rmtree(root/'maindb'); (root/'maindb').mkdir()
        (root/'maindb/index.db').write_bytes(b'index initial'); (root/'maindb/format.json').write_bytes(b'format initial')
        rejected(lambda: h.identity_guard(original, h.census(root), formats, pins))
        h.backup_guard(manifest, backup)

receipt = dict(result='PASS', checks=['in-place byte/size reset', 'preserved new-file deletion', 'exact format replacement exception', 'identical-byte index replacement rejected', 'rejected reset does not mutate', 'resource deletion rejected', 'directory replacement rejected', 'sealed backup unchanged'], go_native_execution='NONE')
(packet/'self-check.json').write_text(json.dumps(receipt, indent=2)+'\n')
print(json.dumps(receipt, indent=2))
