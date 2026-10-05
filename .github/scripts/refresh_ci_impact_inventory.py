#!/usr/bin/env python3
"""Refresh source bindings in the reviewed advisory manifest; never accept policy.

Run: uv run --with pyyaml python .github/scripts/refresh_ci_impact_inventory.py
Review the resulting manifest and ownership rules before committing it.
PyYAML is only needed by this maintenance command, not the workflow planner.
"""
import hashlib
import itertools
import json
from pathlib import Path

import yaml
import ci_impact


def mapping(node):
    return {key.value: value for key, value in node.value}


def refresh(root):
    manifest = root / ci_impact.POLICY
    policy = json.loads(manifest.read_text())
    previous = {m['id']: m for m in policy['members']}
    workflows = {}
    members = []
    for path in sorted(p for p in (root / '.github/workflows').iterdir() if p.suffix in ('.yml', '.yaml')):
        raw = path.read_bytes()
        source = yaml.safe_load(raw)
        nodes = mapping(mapping(yaml.compose(raw))['jobs'])
        workflows[path.name] = {'sha256': ci_impact.digest(raw),
                                'git_blob': hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest(),
                                'events': source.get('on', source.get(True)),
                                'coverage': 'existing execution only; triggers/path filters are unchanged', 'jobs': {}}
        for name, job in source['jobs'].items():
            if name == 'impact-shadow':
                continue  # Control job is not part of the original execution universe.
            node_steps = mapping(nodes[name]).get('steps')
            commands = []
            for step, node in zip(job.get('steps', []), node_steps.value if node_steps else []):
                if 'run' not in step:
                    continue
                record = {key: step[key] for key in ('name', 'working-directory', 'if', 'env') if key in step}
                record['entrypoint'] = f'.github/workflows/{path.name}:{mapping(node)["run"].start_mark.line + 1}'
                record['command_sha256'] = ci_impact.digest(step['run'].encode())
                record['invocation'] = step['run'] if '\n' not in step['run'] else 'shell block at source entrypoint; source workflow owns exact commands'
                commands.append(record)
            workflows[path.name]['jobs'][name] = {'commands': commands, 'runs_on': job['runs-on'],
                                                  'checkout': [s.get('with', {}) for s in job.get('steps', []) if s.get('uses', '').startswith('actions/checkout@')],
                                                  'environment': job.get('env', {}),
                                                  'module': 'root go.mod (GOWORK per entry command; no nested module qualification)'}
            matrix = job.get('strategy', {}).get('matrix', {})
            if 'include' in matrix:
                if set(matrix) != {'include'}:
                    raise ci_impact.ContractError('unsupported mixed matrix; review discovery')
                variants = matrix['include']
            elif matrix:
                if any(not isinstance(v, list) for v in matrix.values()):
                    raise ci_impact.ContractError('dynamic matrix requires review')
                variants = [dict(zip(matrix, values)) for values in itertools.product(*matrix.values())]
            else:
                variants = [{}]
            for variant in variants:
                label = str(variant.get('name') or variant.get('shard') or variant.get('os') or variant.get('profile') or 'single')
                member_id = f'{path.stem}/{name}/{label}'
                members.append({'id': member_id, 'workflow': path.name, 'job': name,
                                'variant': {'matrix': variant, 'runs_on': job['runs-on'],
                                            'race': name == 'race' or any(' -race ' in s.get('run', '') for s in job.get('steps', [])),
                                            'cgo': 'setup-go platform default; explicit command overrides retained',
                                            'tags': 'entry command defaults/flags retained'},
                                'owner': previous.get(member_id, {}).get('owner', 'UNREVIEWED: assign affected owner')})
    policy['workflows'] = workflows
    policy['members'] = members
    # Include pending source edits/additions/deletions so this describes the
    # actual candidate to be committed, rather than refreshing from old HEAD.
    names = sorted(set(ci_impact.path_text(p) for p in ci_impact.nul_fields(
        ci_impact.git(root, 'ls-files', '-z', '--cached', '--others', '--exclude-standard'))))
    inputs = {}
    for name in names:
        if ci_impact.is_discovery_source(name) and (root / name).is_file():
            data = (root / name).read_bytes()
            inputs[name] = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
    policy['discovery_source_sha256'] = ci_impact.discovery_digest(inputs)
    # Newly discovered members remain visibly unresolved in the output. Runtime
    # qualification rejects this marker until a reviewer assigns an owner.
    ci_impact.check_policy(policy, allow_unreviewed=True)
    manifest.write_text(json.dumps(policy, indent=2, sort_keys=True) + '\n')
    print(f'Refreshed {len(workflows)} workflows / {len(members)} original members; ownership review still required.')


if __name__ == '__main__':
    refresh(Path(__file__).resolve().parents[2])
