#!/usr/bin/env python3
"""Root-private FUTURE derive-only action; never launches observers or captures."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

sys.dont_write_bytecode = True
LOCK = '/home/mikers/gomap-r1-evidence-20261005/timed-capture.lock'
GO_ROOT = '/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def freeze(path, value):
    with path.open('x', encoding='utf-8') as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write('\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root_inputs', type=Path)
    parser.add_argument('--config-dir', type=Path, required=True, help='NEW directory outside checkout/captures/receipts')
    args = parser.parse_args()
    original = args.root_inputs.resolve()
    input_bytes = original.read_bytes()
    config = json.loads(input_bytes)
    input_sha256 = hashlib.sha256(input_bytes).hexdigest()
    required = ('selected_merge_sha', 'review_url', 'landing_observation_file', 'repo', 'gocache', 'captures', 'receipts')
    if not all(config.get(key) for key in required) or config.get('landing_verified_by_root') is not True:
        raise ValueError('root must independently verify landing and complete actual inputs first')
    selected = config['selected_merge_sha']
    if not re.fullmatch('[0-9a-f]{40}', selected) or not config['review_url'].startswith('https://github.com/'):
        raise ValueError('actual merge SHA and GitHub review URL required')
    observation = Path(config['landing_observation_file']).resolve()
    if observation.is_symlink() or not observation.is_file():
        raise ValueError('landing observation must be independently frozen regular text')
    text = observation.read_text(encoding='utf-8')
    if not text.strip() or '\0' in text:
        raise ValueError('landing observation empty or nontext')
    repo, output = Path(config['repo']).resolve(), args.config_dir.resolve()
    if sys.platform != 'linux' or os.environ.get('R1_RECEIPT_LOCKED') != 'yes':
        raise ValueError('future derivation runs on matching Linux under the actual canonical111 flock')
    if repo == output or repo in output.parents:
        raise ValueError('config directory must be outside source checkout')
    paths = [output]
    for key in ('captures', 'receipts'):
        if set(config[key]) != {'A','C','D'}:
            raise ValueError('three distinct A/C/D paths required')
        paths.extend(Path(config[key][lane]).resolve() for lane in ('A','C','D'))
    for i, path in enumerate(paths):
        if path.exists() or path.is_symlink() or path == repo or repo in path.parents:
            raise ValueError('config/output/receipt leaf paths must be NEW and outside checkout')
        for previous in paths[:i]:
            if path == previous or path in previous.parents or previous in path.parents:
                raise ValueError('config/output/receipt paths must be disjoint')
    if not all(Path(config['captures'][lane]).resolve().parent.is_dir() for lane in ('A','C','D')):
        raise ValueError('capture parents must already exist for actual filesystem observations')
    real_go = GO_ROOT + '/bin/go'
    environment = {k: os.environ[k] for k in ('HOME','PATH') if k in os.environ}
    environment.update(GOROOT=GO_ROOT, GOCACHE=str(Path(config['gocache']).resolve()),
                       GOMODCACHE='/home/mikers/go/pkg/mod', GOTOOLCHAIN='local', GOWORK='off',
                       GOENV='off', GOFLAGS='', GODEBUG='', GOMAXPROCS='16', GOGC='100',
                       GOMEMLIMIT='off', PYTHONDONTWRITEBYTECODE='1')
    os.environ.clear()
    os.environ.update(environment)
    os.chdir(repo)
    records = []

    def command(argv):
        result = subprocess.run(argv, text=True, capture_output=True)
        records.append({'argv':argv, 'exit_code':result.returncode, 'stdout':result.stdout, 'stderr':result.stderr})
        if result.returncode:
            raise ValueError('derivation command failed: '+repr(argv))
        return result.stdout.strip()

    def exact_clean():
        if command(['git','rev-parse','HEAD']) != selected or command(['git','status','--porcelain','--untracked-files=normal']):
            raise ValueError('actual selected clean merge checkout absent')

    exact_clean()
    if command([real_go,'version']) != 'go version go1.26.4 linux/amd64':
        raise ValueError('matching pinned toolchain absent')
    go_environment = json.loads(command([real_go,'env','-json']))
    sys.path.insert(0, str(repo/'scripts'))
    collection = read_module('root_observed_collection', repo/'scripts/r1_collection_source.py')
    ac = collection.source_identity()
    lifecycle = read_module('root_observed_lifecycle', repo/'scripts/r1_lifecycle_capture.py')
    d = lifecycle.source(real_go)  # Actual matching Linux go list; NOT any saved packet/prelanding manifest.
    if collection.source_identity() != ac or lifecycle.source(real_go) != d:
        raise ValueError('source changed during actual derivation')
    exact_clean()
    if not all(source['commit'] == selected and source['clean'] is True for source in (ac,d)) or ac['runtime_sha256'] != d['runtime_sha256']:
        raise ValueError('actual source/runtime identity mismatch')
    if sha(original) != input_sha256:
        raise ValueError('root inputs changed')
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    freeze(output/'root-inputs.json', config)
    freeze(output/'derived-A-C-source.json', ac)
    freeze(output/'derived-D-source.json', d)
    for lane in ('A','C','D'):
        source = d if lane == 'D' else ac
        frozen = {'repo':str(repo), 'out':str(Path(config['captures'][lane]).resolve()),
                  'receipt_dir':str(Path(config['receipts'][lane]).resolve()),
                  'real_go':real_go, 'goroot':GO_ROOT, 'gocache':environment['GOCACHE'],
                  'gomodcache':environment['GOMODCACHE'], 'lock':LOCK,
                  'source_commit':selected, 'landed_tooling_commit':selected,
                  'runtime_sha256':source['runtime_sha256'], 'harness_sha256':source['harness_sha256'],
                  'review_url':config['review_url'], 'landing_observation':text}
        freeze(output/(lane+'-config.json'), frozen)
    freeze(output/'source-derivation-receipt.json',
           {'status':'ACTUAL_SOURCE_DERIVATION_ONLY_NOT_BUILD_RUN_OR_ACCEPTANCE',
            'utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()), 'selected_merge_sha':selected,
            'root_input_sha256':input_sha256, 'landing_observation_sha256':sha(observation),
            'helper_sha256':sha(__file__), 'environment':environment,'go_environment':go_environment,
            'go_executable_sha256':sha(real_go), 'commands':records,
            'source_helpers':{str(p.relative_to(repo)):sha(p) for p in
                (repo/'scripts/r1_collection_source.py',repo/'scripts/r1_lifecycle_capture.py',repo/'scripts/r1_lifecycle_validate.py')},
            'source_config_hashes':{p.name:sha(p) for p in sorted(output.iterdir()) if p.is_file()},
            'authority_limit':'Root supplies verified landing/review authority; this script does not query GitHub or run capture. Actual flock is held by caller, not proven by marker.'})
    print(str(output))


if __name__ == '__main__':
    try:
        main()
    except ValueError as error:
        sys.exit(str(error))
