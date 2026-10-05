#!/usr/bin/env python3
"""Refresh source bindings in the reviewed advisory manifest; never accept policy.

Stage intended source/workflow edits first, then run:
  uv run --with pyyaml python .github/scripts/refresh_ci_impact_inventory.py
Review the resulting manifest and ownership rules before committing it.
PyYAML is only needed by this maintenance command, not the workflow planner.
"""
import itertools
import json
from pathlib import Path

import yaml
import ci_impact


def mapping(node):
    return {key.value: value for key, value in node.value}


def refresh(root):
    # One immutable intended commit snapshot, including staged additions and
    # deletions. Untracked/unstaged bytes never define executable inputs. An
    # unmerged or unsupported index fails before any manifest write.
    snapshot = ci_impact.git(root, 'write-tree').decode().strip()
    inputs, _ = ci_impact.tree_inventory(root, snapshot)
    manifest = root / ci_impact.POLICY
    # Reviewed owners/rules are the explicit editable policy input; refreshing
    # executable bindings does not stage files or accept these policy choices.
    policy = json.loads(manifest.read_text())
    previous = {m['id']: m for m in policy['members']}
    workflows = {}
    members = []
    for name in sorted(inputs):
        path = Path(name)
        if path.parent != Path('.github/workflows') or path.suffix not in ('.yml', '.yaml'):
            continue
        raw = ci_impact.git(root, 'cat-file', 'blob', inputs[name])
        source = yaml.safe_load(raw)
        nodes = mapping(mapping(yaml.compose(raw))['jobs'])
        workflows[path.name] = {'sha256': ci_impact.digest(raw),
                                'git_blob': inputs[name],
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
    policy['discovery_source_sha256'] = ci_impact.discovery_digest(inputs)
    # Newly discovered members remain visibly unresolved in the output. Runtime
    # qualification rejects this marker until a reviewer assigns an owner.
    ci_impact.check_policy(policy, allow_unreviewed=True)
    manifest.write_text(json.dumps(policy, indent=2, sort_keys=True) + '\n')
    print(f'Refreshed {len(workflows)} workflows / {len(members)} original members; ownership review still required.')


if __name__ == '__main__':
    refresh(Path(__file__).resolve().parents[2])
