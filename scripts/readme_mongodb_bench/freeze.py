#!/usr/bin/env python3
"""Freeze a clean product, external client, reviewed recipe and prebuilt binaries."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--harness-repo', type=Path, required=True)
    parser.add_argument('--go', default='go')
    parser.add_argument('--mongo-image-id', required=True)
    args = parser.parse_args()
    root, repo, harness = args.root.resolve(), args.repo.resolve(), args.harness_repo.resolve()
    env = os.environ.copy()
    env.pop('GOROOT', None)
    env.update(GOTOOLCHAIN='go1.26.10', GOWORK='off')

    def run(argv):
        return subprocess.check_output([str(a) for a in argv], cwd=repo, env=env, text=True).strip()

    def digest(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    sources = []
    for path, names in ((repo, ('TreeDB', 'internal', 'cmd', 'scripts', 'third_party', 'go.mod', 'go.sum')),
                        (root/'go-ycsb-source', ('cmd', 'db', 'pkg', 'go.mod', 'go.sum'))):
        if run(['git', '-C', path, 'status', '--porcelain']):
            raise ValueError('source checkout is dirty: '+str(path))
        existing = run(['git', '-C', path, 'ls-tree', '--name-only', 'HEAD']).splitlines()
        sources.append(dict(path=str(path), commit=run(['git', '-C', path, 'rev-parse', 'HEAD']),
                            root_tree=run(['git', '-C', path, 'rev-parse', 'HEAD^{tree}']),
                            blobs={n: run(['git', '-C', path, 'rev-parse', 'HEAD:'+n]) for n in names if n in existing}))
    recipe_path = 'scripts/readme_mongodb_bench'
    if run(['git', '-C', harness, 'status', '--porcelain']):
        raise ValueError('harness checkout is dirty')
    recipe_names = ('run.py', 'census.go', 'freeze.py', 'aggregate.py', 'archive.py', 'verify_archive.py', 'ycsb_log.py')
    for name in recipe_names:
        # Compare bytes through git hash-object, avoiding newline normalization.
        actual_blob = run(['git', '-C', harness, 'hash-object', root/'recipe'/name])
        if actual_blob != run(['git', '-C', harness, 'rev-parse', 'HEAD:'+recipe_path+'/'+name]):
            raise ValueError('copied recipe differs from committed harness: '+name)
    image = json.loads(run(['docker', 'image', 'inspect', args.mongo_image_id]))[0]
    info = json.loads(run(['docker', 'info', '--format', '{{json .}}']))
    host = dict(os=platform.platform(), kernel=run(['uname', '-a']),
                mongo_container_user=str(os.getuid())+':'+str(os.getgid()) if platform.system() == 'Linux' else '',
                disk=run(['df', '-k', root]), background_processes=run(['ps', '-axo', 'pid,comm,%cpu,%mem']),
                gomaxprocs='unset for all benchmark processes; runtime value not independently observed',
                infrastructure_unavailable=['INFRASTRUCTURE_UNAVAILABLE: runner: shared user-selected server185; unrelated services remain active'],
                cache='persistent shared Go build and module caches', storage='durable task directory')
    if platform.system() == 'Linux':
        host.update(cpu=Path('/proc/cpuinfo').read_text(), memory=Path('/proc/meminfo').read_text(),
                    loadavg=Path('/proc/loadavg').read_text(), logical_cpus=os.cpu_count(),
                    filesystem=run(['findmnt', '-T', root, '-o', 'TARGET,SOURCE,FSTYPE,OPTIONS']),
                    block_devices=run(['lsblk', '-o', 'NAME,MODEL,SIZE,ROTA,TYPE,MOUNTPOINTS']))
    elif platform.system() == 'Darwin':
        host.update(model=run(['sysctl', '-n', 'hw.model']), logical_cpus=int(run(['sysctl', '-n', 'hw.logicalcpu'])),
                    memory_bytes=int(run(['sysctl', '-n', 'hw.memsize'])), power=run(['pmset', '-g', 'batt']))
    else:
        raise ValueError('unsupported host OS')
    value = dict(schema_version=1, created_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                 stage='pre-smoke freeze; identities unchanged for retained collection',
                 product_commit=sources[0]['commit'], ycsb_commit=sources[1]['commit'], sources=sources,
                 harness=dict(commit=run(['git', '-C', harness, 'rev-parse', 'HEAD']), path=recipe_path,
                              tree=run(['git', '-C', harness, 'rev-parse', 'HEAD:'+recipe_path])),
                 binaries={p.name: digest(p) for p in sorted((root/'bin').iterdir()) if p.is_file()},
                 recipe_sources={n: digest(root/'recipe'/n) for n in recipe_names},
                 mongodb_image_id=image['Id'], mongodb_repo_digests=image['RepoDigests'],
                 mongodb_image_platform=dict(os=image['Os'], architecture=image['Architecture']),
                 build_environment=dict(go_version=run([args.go, 'version']),
                     go_env=json.loads(run([args.go, 'env', '-json', 'GOVERSION', 'GOROOT', 'GOTOOLDIR', 'GOOS', 'GOARCH', 'GOCACHE', 'GOMODCACHE', 'CGO_ENABLED'])),
                     ycsb_tags='libsqlite3,sqlite_omit_load_extension',
                     binaries_build_info={p.name: run([args.go, 'version', '-m', p]) for p in sorted((root/'bin').iterdir()) if p.is_file()}),
                 host=host, docker={k: info[k] for k in ('ServerVersion', 'OperatingSystem', 'KernelVersion', 'Architecture', 'NCPU', 'MemTotal', 'ContainersRunning')})
    with (root/'freeze.json').open('x') as f:
        json.dump(value, f, indent=2, allow_nan=False)
        f.write('\n')
    print(json.dumps({k: value[k] for k in ('product_commit', 'ycsb_commit', 'harness', 'mongodb_image_id')}, indent=2))


if __name__ == '__main__':
    main()
