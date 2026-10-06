#!/usr/bin/env python3
"""Refresh source bindings in the reviewed advisory manifest; never accept policy.

Stage intended source/workflow edits first, then run:
  uv run --with pyyaml python .github/scripts/refresh_ci_impact_inventory.py
Review the resulting manifest and ownership rules before committing it.
PyYAML is only needed by this maintenance command, not the workflow planner.
"""
import argparse
import json
from pathlib import Path
import sys

import yaml
import ci_impact


def mapping(node):
    return {key.value: value for key, value in node.value}


def source_workflows(root, inputs):
    """Extract contracts from immutable blobs, using PyYAML only in maintenance."""
    workflows = {}
    for filename, full_path in sorted(ci_impact.workflow_paths(inputs).items()):
        path = Path(filename)
        raw = ci_impact.git(root, 'cat-file', 'blob', inputs[full_path])
        source = yaml.safe_load(raw)
        nodes = mapping(mapping(yaml.compose(raw))['jobs'])
        workflows[path.name] = {'sha256': ci_impact.digest(raw),
                                'git_blob': inputs[full_path],
                                'events': source.get('on', source.get(True)),
                                'coverage': 'existing execution only; triggers/path filters are unchanged', 'jobs': {}}
        for name, job in source['jobs'].items():
            if (filename, name) == ci_impact.CONTROL_JOB:
                continue  # Control job is not part of the original execution universe.
            strategy = job.get('strategy', {})
            if not isinstance(strategy, dict) or 'matrix' in strategy and strategy['matrix'] is None:
                raise ci_impact.ContractError('malformed-source-matrix')
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
                                                  'matrix': strategy.get('matrix'),
                                                  'race': name == 'race' or any(' -race ' in s.get('run', '') for s in job.get('steps', [])),
                                                  'checkout': [s.get('with', {}) for s in job.get('steps', []) if s.get('uses', '').startswith('actions/checkout@')],
                                                  'environment': job.get('env', {}),
                                                  'module': 'root go.mod (GOWORK per entry command; no nested module qualification)'}
    return workflows


def snapshot_inputs(root):
    snapshot = ci_impact.git(root, 'write-tree').decode().strip()
    return ci_impact.tree_inventory(root, snapshot)[0]


def check_source_contracts(root, policy, inputs, *, allow_unreviewed=False):
    """Corroborate policy metadata against staged YAML, not against itself."""
    ci_impact.check_policy(policy, allow_unreviewed=allow_unreviewed)
    if ci_impact.typed_json(policy['workflows']) != ci_impact.typed_json(source_workflows(root, inputs)):
        raise ci_impact.ContractError('workflow-source-contract-mismatch')
    if policy['discovery_source_sha256'] != ci_impact.discovery_digest(inputs):
        raise ci_impact.ContractError('consumer-discovery-drift')
    if any(inputs.get(path) != oid for path, oid in policy['harness_inputs'].items()):
        raise ci_impact.ContractError('harness-contract-drift')


def refresh(root):
    # One immutable intended commit snapshot for sources, YAML and harnesses.
    # Untracked/unstaged bytes are excluded; errors precede any manifest write.
    inputs = snapshot_inputs(root)
    manifest = root / ci_impact.POLICY
    # Reviewed owners/rules and harness paths remain explicit policy inputs.
    policy = json.loads(manifest.read_text())
    previous = {m['id']: m for m in policy['members']}
    policy['workflows'] = source_workflows(root, inputs)
    policy['members'] = [dict(m, owner=previous.get(m['id'], {}).get('owner', 'UNREVIEWED: assign affected owner'))
                         for m in ci_impact.expected_members(policy['workflows'])]
    paths = policy['harness_inputs']
    # The old reviewed path list can be migrated, but runtime policy requires a
    # valid path=>blob map. Removed nonplanner paths stay a policy review choice.
    if not isinstance(paths, (list, dict)) or len(paths) != len(set(paths)) or ci_impact.PLANNER not in paths:
        raise ci_impact.ContractError('malformed-harness-binding')
    if any(path not in inputs for path in paths):
        raise ci_impact.ContractError('missing-harness-input')
    policy['harness_inputs'] = {path: inputs[path] for path in paths}
    policy['discovery_source_sha256'] = ci_impact.discovery_digest(inputs)
    # Newly discovered members remain visibly unresolved in the output. Runtime
    # qualification rejects this marker until a reviewer assigns an owner.
    check_source_contracts(root, policy, inputs, allow_unreviewed=True)
    manifest.write_text(json.dumps(policy, indent=2, sort_keys=True) + '\n')
    print(f"Refreshed {len(policy['workflows'])} workflows / {len(policy['members'])} original members; ownership review still required.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='validate reviewed manifest against the intended staged source tree without writing')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    try:
        if args.check:
            check_source_contracts(root, json.loads((root / ci_impact.POLICY).read_text()), snapshot_inputs(root))
            print('Validated reviewed manifest against intended staged sources; no authority enabled.')
        else:
            refresh(root)
    except (ci_impact.ContractError, OSError, ValueError, KeyError, TypeError, yaml.YAMLError) as error:
        print(f'Inventory error; manifest not written: {error}', file=sys.stderr)
        sys.exit(2)
