#!/usr/bin/env python3
"""Source-bound CI impact forecasts. This program never authorizes omissions."""
from __future__ import annotations

import argparse
import fnmatch
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys

POLICY = '.github/ci/ci_impact.json'
PLANNER = '.github/scripts/ci_impact.py'
CONTROL_JOB = ('treedb-tests.yml', 'impact-shadow')
SCHEMA = 2
SHA = re.compile(r'^[0-9a-f]{40}$')


class ContractError(ValueError):
    pass


def digest(data):
    return hashlib.sha256(data).hexdigest()


def blob_id(data):
    return hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()


def typed_json(value):
    """JSON comparison preserves bool/int/float distinctions throughout receipts."""
    try:
        return json.dumps(value, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise ContractError('malformed-literal-contract') from error


def workflow_paths(inventory):
    # GitHub workflow definitions are direct children of this directory.
    return {p.rsplit('/', 1)[1]: p for p in inventory
            if p.startswith('.github/workflows/') and p.count('/') == 2 and p.endswith(('.yml', '.yaml'))}


def expected_members(workflows, *, member_limit=None):
    """Normalize reviewed static descriptors, never choose or schedule shards."""
    if not isinstance(workflows, dict) or not workflows:
        raise ContractError('incomplete-workflow-policy')
    members = []
    ids = set()
    for workflow, contract in workflows.items():
        if (not isinstance(workflow, str) or workflow not in workflow_paths({'.github/workflows/' + workflow: ''}) or
                not isinstance(contract, dict) or not isinstance(contract.get('jobs'), dict) or not contract['jobs']):
            raise ContractError('incomplete-workflow-policy')
        for job, source in contract['jobs'].items():
            if not isinstance(job, str) or not job or (workflow, job) == CONTROL_JOB or not isinstance(source, dict):
                raise ContractError('malformed-job-contract')
            runner = source.get('runs_on')
            if not (isinstance(runner, str) and runner or
                    isinstance(runner, list) and runner and all(isinstance(v, str) and v for v in runner)):
                raise ContractError('malformed-runner-contract')
            if 'matrix' not in source or type(source.get('race')) is not bool:
                raise ContractError('missing-matrix-race-contract')
            matrix = source['matrix']
            if matrix is None:
                variants = [{}]
            else:
                if not isinstance(matrix, dict) or not matrix or 'exclude' in matrix:
                    raise ContractError('unsupported-matrix')
                if 'include' in matrix:
                    if set(matrix) != {'include'} or not isinstance(matrix['include'], list) or not matrix['include']:
                        raise ContractError('unsupported-matrix')
                    variants = matrix['include']
                else:
                    if any(not isinstance(k, str) or not k or not isinstance(v, list) or not v for k, v in matrix.items()):
                        raise ContractError('unsupported-matrix')
                    variants = (dict(zip(matrix, values)) for values in itertools.product(*matrix.values()))
            seen = set()
            for variant in variants:
                # Runtime cannot expand an inconsistent Cartesian contract past
                # its claimed inventory. Refresh derives the reviewed source
                # universe; it does not accept that universe as policy authority.
                if member_limit is not None and len(members) >= member_limit:
                    raise ContractError('incomplete-or-altered-matrix-member-policy')
                if not isinstance(variant, dict) or (matrix is not None and not variant):
                    raise ContractError('malformed-matrix-variant')
                if any(not isinstance(k, str) or not k or type(v) not in (str, int, float, bool) or
                       isinstance(v, str) and '${{' in v for k, v in variant.items()):
                    raise ContractError('unsupported-matrix-literal')
                key = typed_json(variant)
                if key in seen:
                    raise ContractError('duplicate-matrix-variant')
                seen.add(key)
                label = variant.get('name') or variant.get('shard') or variant.get('os') or variant.get('profile') or 'single'
                if type(label) not in (str, int, float):
                    raise ContractError('malformed-member-label')
                member_id = f'{Path(workflow).stem}/{job}/{label}'
                if member_id in ids:
                    raise ContractError('member-label-collision')
                ids.add(member_id)
                members.append({'id': member_id, 'workflow': workflow, 'job': job,
                                'variant': {'matrix': variant, 'runs_on': runner, 'race': source['race'],
                                            'cgo': 'setup-go platform default; explicit command overrides retained',
                                            'tags': 'entry command defaults/flags retained'}})
    return members


def check_harness_bindings(bindings):
    if not isinstance(bindings, dict) or PLANNER not in bindings:
        raise ContractError('missing-reviewed-planner-binding')
    for path, descriptor in bindings.items():
        if (not isinstance(path, str) or path_text(path.encode()) != path or
                not isinstance(descriptor, dict) or set(descriptor) != {'git_blob', 'mode'} or
                not isinstance(descriptor['git_blob'], str) or not SHA.fullmatch(descriptor['git_blob']) or
                descriptor['mode'] not in ('100644', '100755')):
            raise ContractError('malformed-harness-binding')
    return bindings


def git(repo, *args):
    try:
        return subprocess.check_output(['git', '-C', str(repo), *args], stderr=subprocess.PIPE,
                                       env=dict(os.environ, GIT_OPTIONAL_LOCKS='0'))
    except (OSError, subprocess.CalledProcessError) as error:
        raise ContractError('git-discovery-error') from error


def path_text(raw):
    try:
        value = raw.decode('utf-8')
    except UnicodeError as error:
        raise ContractError('non-utf8-path') from error
    if not value or value.startswith('/') or any(p in ('', '.', '..') for p in value.split('/')):
        raise ContractError('malformed-path')
    return value


def nul_fields(raw):
    if raw and not raw.endswith(b'\0'):
        raise ContractError('partial-nul-discovery')
    return raw.split(b'\0')[:-1] if raw else []


def parse_diff(raw):
    fields = nul_fields(raw)
    changes = []
    index = 0
    while index < len(fields):
        try:
            status = fields[index].decode('ascii', errors='strict')
        except UnicodeError as error:
            raise ContractError('malformed-diff-status') from error
        index += 1
        if not re.fullmatch(r'[AMDT]|[RC](?:100|[1-9]?[0-9])', status):
            raise ContractError('malformed-diff-status')
        count = 2 if status[0] in 'RC' else 1
        if index + count > len(fields):
            raise ContractError('partial-nul-diff')
        change = {'status': status, 'path': path_text(fields[index + count - 1])}
        if count == 2:
            change['old_path'] = path_text(fields[index])
        changes.append(change)
        index += count
    paths = [c['path'] for c in changes]
    if len(paths) != len(set(paths)):
        raise ContractError('duplicate-diff-path')
    return changes


def tree_inventory(repo, commit):
    raw = git(repo, 'ls-tree', '-rz', '--full-tree', commit)
    inventory = {}
    for field in nul_fields(raw):
        try:
            metadata, name = field.split(b'\t', 1)
            mode, kind, oid = metadata.decode('ascii').split(' ')
        except (ValueError, UnicodeError) as error:
            raise ContractError('malformed-tree-discovery') from error
        path = path_text(name)
        if mode not in ('100644', '100755') or kind != 'blob' or not SHA.fullmatch(oid):
            raise ContractError('unsupported-tree-entry')
        if path in inventory:
            raise ContractError('duplicate-tree-path')
        inventory[path] = {'git_blob': oid, 'mode': mode}
    if not inventory:
        raise ContractError('empty-tree-discovery')
    return inventory, digest(raw)


def check_policy(policy, *, allow_unreviewed=False):
    if not isinstance(policy, dict) or type(policy.get('schema_version')) is not int or policy['schema_version'] != SCHEMA or policy.get('mode') != 'advisory':
        raise ContractError('unsupported-policy')
    workflows = policy.get('workflows', {})
    members = policy.get('members', [])
    if not isinstance(members, list) or any(not isinstance(m, dict) or not isinstance(m.get('id'), str) for m in members):
        raise ContractError('incomplete-member-policy')
    ids = [m['id'] for m in members]
    if not workflows or not ids or len(ids) != len(set(ids)):
        raise ContractError('incomplete-member-policy')
    expected = {m['id']: typed_json(m) for m in expected_members(workflows, member_limit=len(members))}
    for member in members:
        if (not isinstance(member.get('workflow'), str) or member['workflow'] not in workflows or
                not isinstance(member.get('variant'), dict) or not member['variant'] or
                not isinstance(member.get('job'), str) or not member['job'] or
                not isinstance(member.get('owner'), str) or not member['owner'] or
                member['job'] not in workflows[member['workflow']]['jobs']):
            raise ContractError('incomplete-member-policy')
        if not allow_unreviewed and member['owner'].startswith('UNREVIEWED'):
            raise ContractError('unreviewed-member-owner')
    if {m['workflow'] for m in members} != set(workflows):
        raise ContractError('incomplete-workflow-policy')
    actual = {m['id']: typed_json({key: m.get(key) for key in ('id', 'workflow', 'job', 'variant')}) for m in members}
    if actual != expected:
        raise ContractError('incomplete-or-altered-matrix-member-policy')
    for contract in workflows.values():
        if not isinstance(contract.get('git_blob'), str) or not SHA.fullmatch(contract['git_blob']):
            raise ContractError('malformed-workflow-binding')
    check_harness_bindings(policy.get('harness_inputs'))
    selectors = set(workflows) | {m['workflow'] + '/' + m['job'] for m in members}
    for rule in policy['rules']:
        if not rule.get('glob') or not rule.get('reason') or not rule.get('consumers'):
            raise ContractError('incomplete-consumer-policy')
        if any(w not in selectors for w in rule['consumers']):
            raise ContractError('unknown-consumer')
    if any(member not in ids for member in policy['always_members']):
        raise ContractError('unknown-always-member')
    return policy


def is_discovery_source(path):
    return (path.endswith(('.go', '.py', '.sh', '.s', '.S', '.c', '.h', '.cc', '.cpp', '.cxx', '.hpp', '.hh', '.m', '.mm', '.syso', '.mk')) or
            path.endswith(('go.mod', 'go.sum')) or path.rsplit('/', 1)[-1].startswith('go.work') or path == 'Makefile')


def discovery_digest(inventory):
    # Code can add a dynamic docs/fixture consumer without changing imports.
    # Reviewed source binding keeps stale footprints conservative on later PRs.
    hasher = hashlib.sha256()
    for path, descriptor in inventory.items():
        # Include extensionless executable readers as well as source classes.
        if is_discovery_source(path) or descriptor['mode'] == '100755':
            hasher.update(path.encode())
            hasher.update(b'\0')
            hasher.update(descriptor['mode'].encode())
            hasher.update(b'\0')
            hasher.update(descriptor['git_blob'].encode())
            hasher.update(b'\0')
    return hasher.hexdigest()


def read_policy(repo, commit):
    raw = git(repo, 'show', f'{commit}:{POLICY}')
    try:
        return check_policy(json.loads(raw)), digest(raw)
    except (ValueError, KeyError, TypeError) as error:
        raise ContractError('invalid-policy') from error


def identity_parents(repo, event):
    for key in ('base', 'head', 'candidate'):
        sha = event.get(key, '')
        if not SHA.fullmatch(sha) or sha == '0' * 40:
            raise ContractError('missing-event-identity')
        git(repo, 'cat-file', '-e', f'{sha}^{{commit}}')
    if event.get('event_name') != 'pull_request':
        raise ContractError('unsupported-event')
    raw = git(repo, 'cat-file', '-p', event['candidate']).decode('ascii', errors='replace')
    parents = re.findall(r'^parent ([0-9a-f]{40})$', raw, re.MULTILINE)
    if parents != [event['base'], event['head']]:
        raise ContractError('stale-merge-parents')
    if any(not event.get(key) for key in ('run_id', 'run_attempt', 'workflow_ref', 'repository', 'pr_number', 'event_sha256')):
        raise ContractError('missing-run-identity')
    return parents


def plan(repo, event, environment):
    """Recompute from commit objects, never API file lists or caller-supplied diffs.

    The accepted base supplies policy when available. Bootstrap uses the installed
    manifest only to describe the full universe, with an explicit full fallback.
    Reports are deterministic for the same event, objects, and environment.
    """
    repo = Path(repo)
    runtime_raw = Path(__file__).read_bytes()
    fallback = set()
    accepted_commit = None
    try:
        if not SHA.fullmatch(event.get('base', '')) or event['base'] == '0' * 40:
            raise ContractError('missing-base-policy')
        policy, policy_digest = read_policy(repo, event['base'])
        accepted_commit = event['base']
    except ContractError:
        fallback.add('bootstrap-no-accepted-policy')
        # This cannot grant authority: qualification stays false. Failure to read
        # even the reporting inventory is an error, not an empty successful plan.
        reporting_raw = (Path(__file__).resolve().parents[1] / 'ci/ci_impact.json').read_bytes()
        policy = check_policy(json.loads(reporting_raw))
        policy_digest = None
    identity = {'event': dict(event), 'environment': dict(environment),
                'accepted_policy_commit': accepted_commit, 'accepted_policy_sha256': policy_digest,
                'reporting_policy_sha256': policy_digest or digest(reporting_raw),
                'candidate_tree': None, 'harness': {}, 'parents': [],
                'planner_runtime_sha256': digest(runtime_raw),
                'workflow_ref': event.get('workflow_ref', ''),
                'member_inventory_sha256': digest(json.dumps(policy['members'], sort_keys=True).encode())}
    changes = []
    inventories = {}
    affected = set()
    try:
        identity['parents'] = identity_parents(repo, event)
        if not all(environment.get(k) for k in ('platform', 'python', 'runner_image', 'runner_os', 'gowork')):
            raise ContractError('incomplete-environment')
        identity['candidate_tree'] = git(repo, 'rev-parse', f"{event['candidate']}^{{tree}}").decode().strip()
        base_files, base_digest = tree_inventory(repo, event['base'])
        candidate_files, candidate_digest = tree_inventory(repo, event['candidate'])
        inventories = {'base_files': len(base_files), 'candidate_files': len(candidate_files),
                       'base_sha256': base_digest, 'candidate_sha256': candidate_digest}
        base_discovery = discovery_digest(base_files)
        candidate_discovery = discovery_digest(candidate_files)
        if base_discovery != policy['discovery_source_sha256'] or candidate_discovery != policy['discovery_source_sha256']:
            fallback.add('consumer-discovery-drift')
        inventories['discovery_source_sha256'] = candidate_discovery
        inventories['modules'] = [p for p in candidate_files if p == 'go.mod' or p.endswith('/go.mod')]
        for files in (base_files, candidate_files):
            if set(workflow_paths(files)) != set(policy['workflows']):
                fallback.add('workflow-discovery-drift')
            for name, contract in policy['workflows'].items():
                if files.get('.github/workflows/' + name, {}).get('git_blob') != contract['git_blob']:
                    fallback.add('workflow-contract-drift')
            for path, expected_descriptor in policy['harness_inputs'].items():
                if files.get(path) != expected_descriptor:
                    fallback.add('harness-contract-drift')
                if path not in files:
                    fallback.add('missing-harness-input')
        identity['harness'] = {p: candidate_files[p] for p in policy['harness_inputs'] if p in candidate_files}
        if blob_id(runtime_raw) != policy['harness_inputs'][PLANNER]['git_blob']:
            fallback.add('planner-runtime-drift')
        raw = git(repo, 'diff', '--name-status', '-z', '--find-renames', '--no-ext-diff',
                  '--no-textconv', event['base'], event['candidate'], '--')
        changes = parse_diff(raw)
        inventories['diff_bytes'] = len(raw)
        inventories['diff_sha256'] = digest(raw)
        for change in changes:
            paths = [change['path']]
            if 'old_path' in change:
                paths.append(change['old_path'])
            owners = set()
            reasons = set()
            for path in paths:
                if path not in base_files:
                    fallback.add('new-input')
                if any(fnmatch.fnmatchcase(path, p) for p in policy['global_inputs']):
                    fallback.add('global-or-policy-input')
                for rule in policy['rules']:
                    if fnmatch.fnmatchcase(path, rule['glob']):
                        owners.update(rule.get('consumers', []))
                        reasons.add(rule['reason'])
                        if rule.get('full'):
                            fallback.add(rule['reason'])
                        break
                else:
                    fallback.add('unowned-input')
            change['consumers'] = sorted(owners)
            change['reasons'] = sorted(reasons)
            affected.update(owners)
        # The complete tree diff is authoritative, but its discovery footprint
        # must be complete on both sides, including old rename/delete ownership.
        for change in changes:
            if change['status'][0] == 'D' and change['path'] not in base_files:
                raise ContractError('diff-ownership-discovery-error')
            if change['status'][0] != 'D' and change['path'] not in candidate_files:
                raise ContractError('diff-ownership-discovery-error')
            if 'old_path' in change and change['old_path'] not in base_files:
                raise ContractError('diff-ownership-discovery-error')
    except (ContractError, UnicodeError) as error:
        fallback.add(str(error) if isinstance(error, ContractError) else 'malformed-diff-encoding')
    members = []
    for member in policy['members']:
        run = (bool(fallback) or member['id'] in policy['always_members'] or
               member['workflow'] in affected or member['workflow'] + '/' + member['job'] in affected)
        members.append(dict(member, decision='would_run' if run else 'would_be_inapplicable',
                            reason='full-fallback' if fallback else 'input-consumer' if run else 'no-owned-input-change',
                            fallback=sorted(fallback)))
    return {'schema_version': SCHEMA, 'mode': 'advisory', 'omission_authority': False,
            'forecast_qualified': not bool(fallback), 'identity': identity,
            'inventory': inventories, 'changes': changes, 'fallback': sorted(fallback),
            'members': members, 'coverage_gaps': policy['coverage_gaps']}


def validate(repo, event, environment, receipt):
    """Exact recomputation rejects missing/duplicate/forged decisions and identity."""
    if typed_json(receipt) != typed_json(plan(repo, event, environment)):
        raise ContractError('receipt does not match event, source, policy, inventory, and decisions')


def event_identity(args, raw):
    event = json.loads(raw)
    pr = event.get('pull_request', {})
    return {'event_name': args.event_name, 'base': pr.get('base', {}).get('sha', ''),
            'head': pr.get('head', {}).get('sha', ''), 'candidate': args.candidate,
            'run_id': args.run_id, 'run_attempt': args.run_attempt,
            'workflow_ref': args.workflow_ref, 'repository': event.get('repository', {}).get('full_name', ''),
            'pr_number': pr.get('number', event.get('number')),
            'event_sha256': digest(raw),
            'execution_context': 'local-replay' if args.workflow_ref == 'local-replay' else 'hosted-shadow'}


def environment_identity():
    return {'platform': platform.platform(), 'python': platform.python_version(),
            'runner_image': os.environ.get('ImageOS', 'local') + '-' + os.environ.get('ImageVersion', 'unknown'),
            'runner_os': os.environ.get('RUNNER_OS', platform.system()),
            'gowork': os.environ.get('GOWORK', 'unset')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('plan', 'validate'))
    parser.add_argument('--repo', default='.')
    parser.add_argument('--event-name', required=True)
    parser.add_argument('--event-path', required=True)
    parser.add_argument('--candidate', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--run-attempt', required=True)
    parser.add_argument('--workflow-ref', default='local-replay')
    parser.add_argument('--environment-path', help='independently retained original environment JSON for replay; never infer from the receipt being validated')
    parser.add_argument('--output', required=True, help='receipt path (written by plan, read by validate)')
    args = parser.parse_args()
    event_raw = Path(args.event_path).read_bytes()
    event = event_identity(args, event_raw)
    environment = json.loads(Path(args.environment_path).read_bytes()) if args.environment_path else environment_identity()
    if args.command == 'validate':
        validate(args.repo, event, environment, json.loads(Path(args.output).read_bytes()))
        print('Validated advisory receipt; omission_authority=false')
    else:
        receipt = plan(args.repo, event, environment)
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        (output.parent / 'event.json').write_bytes(event_raw)
        (output.parent / 'environment.json').write_text(json.dumps(environment, indent=2, sort_keys=True) + '\n')
        with output.open('w', encoding='utf-8') as stream:
            json.dump(receipt, stream, indent=2, sort_keys=True)
            stream.write('\n')
        run = sum(m['decision'] == 'would_run' for m in receipt['members'])
        print(f"Advisory only: {run}/{len(receipt['members'])} would run; "
              f"qualified={receipt['forecast_qualified']}; fallback={','.join(receipt['fallback']) or 'none'}")


if __name__ == '__main__':
    try:
        main()
    except (ContractError, OSError, ValueError, KeyError, TypeError) as error:
        print(f'Advisory planner error; retain full execution: {error}', file=sys.stderr)
        sys.exit(2)
