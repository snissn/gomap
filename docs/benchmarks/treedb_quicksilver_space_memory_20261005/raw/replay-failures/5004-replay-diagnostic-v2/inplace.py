#!/usr/bin/env python3
"""Owned diagnostic DB seal/run/reset. No DB parser, rebind, or Go calls."""
import argparse, hashlib, json, os, pathlib, shutil, stat, subprocess, sys
from contextlib import ExitStack

FORMATS = {'maindb/format.json', 'dictdb/format.json', 'templatedb/format.json'}

def digest(p):
    h = hashlib.sha256()
    with p.open('rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''): h.update(b)
    return h.hexdigest()

def census(root):
    result = {}
    for p in [root, *sorted(root.rglob('*'))]:
        s = p.lstat()
        assert stat.S_ISREG(s.st_mode) or stat.S_ISDIR(s.st_mode), ('nonregular/symlink', p)
        isfile = stat.S_ISREG(s.st_mode)
        if isfile: assert s.st_nlink == 1, ('hardlinked file', p)
        result[str(p.relative_to(root))] = dict(kind='file' if isfile else 'dir', dev=s.st_dev, ino=s.st_ino, nlink=s.st_nlink, size=s.st_size, blocks=s.st_blocks, sha256=digest(p) if isfile else None)
    return result

def identity_guard(original, current, formats, pins=None):
    for rel, before in original.items():
        assert rel in current, ('original removed', rel)
        after = current[rel]
        assert after['kind'] == before['kind'], ('type changed', rel)
        if rel not in formats:
            assert (before['dev'], before['ino']) == (after['dev'], after['ino']), ('resource identity changed', rel)
            if pins is not None:
                s = os.fstat(pins[rel])
                assert s.st_nlink > 0 and (s.st_dev, s.st_ino) == (after['dev'], after['ino']), ('pinned original unlinked/replaced', rel)
    assert all(v['kind'] == 'file' for k, v in current.items() if k not in original), 'new directory: fail closed'

def exact_guard(original, current, formats):
    identity_guard(original, current, formats)
    assert original.keys() == current.keys(), 'membership differs'
    for rel, before in original.items():
        if before['kind'] == 'file':
            assert (before['size'], before['sha256']) == (current[rel]['size'], current[rel]['sha256']), ('bytes differ', rel)

def backup_guard(manifest, backup):
    actual = census(backup)
    assert actual.keys() == manifest['backup'].keys()
    for rel, before in manifest['backup'].items():
        after = actual[rel]
        assert (before['kind'], before['dev'], before['ino']) == (after['kind'], after['dev'], after['ino']), ('backup identity changed', rel)
        if before['kind'] == 'file': assert (before['size'], before['sha256']) == (after['size'], after['sha256']), ('backup modified', rel)

def pin(root, original, formats, stack):
    pins = {}
    for rel, before in original.items():
        if rel in formats: continue
        flags = os.O_RDONLY | os.O_NOFOLLOW
        if before['kind'] == 'dir': flags |= os.O_DIRECTORY
        fd = os.open(root/rel, flags)
        stack.callback(os.close, fd)
        s = os.fstat(fd)
        assert (s.st_dev, s.st_ino) == (before['dev'], before['ino']), ('pin identity changed', rel)
        pins[rel] = fd
    return pins

def preserve(root, original, current, out):
    out.mkdir()
    (out/'census.json').write_text(json.dumps(current, indent=2)+'\n')
    for rel, after in current.items():
        before = original.get(rel)
        if after['kind'] == 'file' and (before is None or before['sha256'] != after['sha256'] or before['ino'] != after['ino']):
            p = out/'changed-files'/rel
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root/rel, p)

def reset(root, backup, manifest, pins):
    original, formats = manifest['original'], set(manifest['format_identity_exceptions'])
    backup_guard(manifest, backup)
    current = census(root)
    identity_guard(original, current, formats, pins) # Complete preflight before any mutation.
    for rel, after in current.items():
        if rel not in original:
            os.unlink(root/rel) # Only newly-created owned files, already preserved by caller.
    for rel, before in original.items():
        if before['kind'] != 'file' or before['sha256'] == current[rel]['sha256']: continue
        fd = os.open(root/rel, os.O_RDWR | os.O_NOFOLLOW) # Never create, rename or replace.
        with os.fdopen(fd, 'r+b') as dst, (backup/rel).open('rb') as src:
            s = os.fstat(dst.fileno())
            assert (s.st_dev, s.st_ino) == (current[rel]['dev'], current[rel]['ino'])
            offset = 0
            for wanted in iter(lambda: src.read(1 << 20), b''):
                dst.seek(offset)
                if dst.read(len(wanted)) != wanted:
                    dst.seek(offset); dst.write(wanted)
                offset += len(wanted)
            dst.truncate(before['size']); dst.flush(); os.fsync(dst.fileno())
    for rel, before in original.items():
        if before['kind'] == 'dir':
            fd = os.open(root/rel, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try: os.fsync(fd)
            finally: os.close(fd)
    after = census(root)
    exact_guard(original, after, formats)
    backup_guard(manifest, backup)
    return after

def owned(p, owner):
    p = p.absolute()
    assert p == p.resolve(), ('symlink/unclean path', p)
    assert p != owner and p.is_relative_to(owner), ('outside owned graph', p)
    return p

def main():
    a = argparse.ArgumentParser()
    a.add_argument('mode', choices=['seal', 'check', 'run'])
    a.add_argument('--owned-root', type=pathlib.Path, required=True)
    a.add_argument('--manifest', type=pathlib.Path, required=True)
    a.add_argument('--db', type=pathlib.Path)
    a.add_argument('--backup', type=pathlib.Path)
    a.add_argument('--out', type=pathlib.Path)
    raw, command = sys.argv[1:], []
    if '--' in raw:
        cut = raw.index('--'); command = raw[cut+1:]; raw = raw[:cut]
    args = a.parse_args(raw)
    owner = args.owned_root.resolve()
    mf = owned(args.manifest, owner)
    if args.mode == 'seal':
        root, backup = owned(args.db, owner), owned(args.backup, owner)
        assert root.is_dir() and not backup.exists() and not mf.exists()
        assert not backup.is_relative_to(root) and not root.is_relative_to(backup)
        assert not mf.is_relative_to(root) and not mf.is_relative_to(backup)
        original = census(root)
        assert 'maindb/format.json' in original
        assert all(original[rel]['kind'] == 'file' for rel in FORMATS & original.keys())
        shutil.copytree(root, backup, copy_function=shutil.copyfile) # No hardlinks.
        sealed = census(backup)
        for rel, before in original.items():
            assert before['kind'] == sealed[rel]['kind']
            if before['kind'] == 'file':
                assert before['sha256'] == sealed[rel]['sha256'] and (before['dev'], before['ino']) != (sealed[rel]['dev'], sealed[rel]['ino'])
        mf.write_text(json.dumps(dict(db=str(root), backup_dir=str(backup), original=original, backup=sealed, format_identity_exceptions=sorted(FORMATS & original.keys())), indent=2)+'\n')
        return
    manifest = json.loads(mf.read_text())
    root, backup = owned(pathlib.Path(manifest['db']), owner), owned(pathlib.Path(manifest['backup_dir']), owner)
    formats = set(manifest['format_identity_exceptions'])
    assert formats == FORMATS & manifest['original'].keys()
    assert all(manifest['original'][rel]['kind'] == 'file' for rel in formats)
    backup_guard(manifest, backup)
    exact_guard(manifest['original'], census(root), formats)
    if args.mode == 'check': return
    out = owned(args.out, owner)
    assert not out.exists() and not out.is_relative_to(root) and not out.is_relative_to(backup)
    out.mkdir()
    assert command and pathlib.Path(command[0]).is_absolute()
    owned(pathlib.Path(command[0]), owner)
    assert os.environ.get('GOMAP_QS_REPLAY_MODE') == 'read' and os.environ.get('GOMAP_QS_REPLAY_DB') == str(root)
    with ExitStack() as stack:
        pins = pin(root, manifest['original'], formats, stack) # Hold across child; prevent inode recycling from hiding replacement.
        (out/'before.json').write_text(json.dumps(census(root), indent=2)+'\n')
        with (out/'stdout.json').open('wb') as stdout, (out/'stderr.txt').open('wb') as stderr:
            rc = subprocess.run(command, stdout=stdout, stderr=stderr, check=False).returncode
        current = census(root)
        preserve(root, manifest['original'], current, out/'post')
        (out/'run.json').write_text(json.dumps(dict(command=command, rc=rc, binary_sha256=digest(pathlib.Path(command[0])), manifest_sha256=digest(mf)), indent=2)+'\n')
        identity_guard(manifest['original'], current, formats, pins)
        assert rc == 0, 'failed child retained; no reset'
        after = reset(root, backup, manifest, pins)
        (out/'after-reset.json').write_text(json.dumps(after, indent=2)+'\n')

if __name__ == '__main__': main()
