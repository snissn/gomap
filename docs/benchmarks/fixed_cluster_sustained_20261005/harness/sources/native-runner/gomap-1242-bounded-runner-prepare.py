import hashlib
import json
import pathlib
import shlex
import sys

# Reuse the already exercised runner envelope; only the frozen source and commands change.
p = pathlib.Path(sys.argv[1])
cfg = json.loads((p / 'config.json').read_text())
template = pathlib.Path('/home/mikers/gomap-1242-v4-assigned-owner-semantic-red-root-v1')
assert p.is_absolute() and (p / 'source').is_dir()
assert not (p / 'run.started').exists()
for name in ('receipts', 'go-tmp'):
    (p / name).mkdir(exist_ok=True)
inventory = {
    str(f.relative_to(p / 'source')): hashlib.sha256(f.read_bytes()).hexdigest()
    for f in sorted((p / 'source').rglob('*')) if f.is_file()
}
(p / 'source-inventory.json').write_text(json.dumps(inventory, sort_keys=True, indent=2) + '\n')
cfg['source_inventory_sha256'] = hashlib.sha256((p / 'source-inventory.json').read_bytes()).hexdigest()
cfg['source_files'] = len(inventory)
cfg['runner_host'] = '192.168.0.111'
(p / 'config.json').write_text(json.dumps(cfg, indent=2) + '\n')
run = (template / 'run.sh').read_text().replace(str(template), str(p))
run = run.replace('gomap-1242-v4-assigned-red-', cfg['unit_prefix'])
run = run.replace('300s systemd-run', str(cfg['outer_timeout_seconds']) + 's systemd-run')
inner = (template / 'inner.sh').read_text().replace(str(template), str(p))
prefix = inner.split('set +e\n', 1)[0]
steps = ['result=0\n']
for i, command in enumerate(cfg['go_commands']):
    assert command['args'][0] in {'test', 'list', 'tool', 'build'}
    name = f'{i:02d}-{command["name"]}'
    env = command.get('env', {})
    assert all(k.startswith('TREEDB_COLLECTION_') for k in env)
    # Keep ambient benchmark overrides out of all commands, especially the buffered control.
    clear = 'for n in ${!TREEDB_COLLECTION_@}; do unset "$n"; done\n'
    assignments = ''.join('export ' + k + '=' + shlex.quote(str(v)) + '\n' for k, v in env.items())
    argv = ['timeout', '--kill-after=15s', str(command['timeout_seconds']) + 's', 'go', *command['args']]
    steps += [clear, assignments, 'set +e\n',
              '/usr/bin/time -v ' + shlex.join(argv) + ' > ' + shlex.quote(str(p / 'receipts' / (name + '.jsonl'))) +
              ' 2> ' + shlex.quote(str(p / 'receipts' / (name + '.stderr'))) + '\n',
              'result=$?\nset -e\n',
              'printf \'%s\\n\' "$result" > ' + shlex.quote(str(p / 'receipts' / (name + '.exit'))) + '\n',
              'if test "$result" -ne 0; then break_run=true; else break_run=false; fi\n',
              'if "$break_run"; then\n' +
              '  for n in memory.max memory.swap.max memory.peak memory.events; do cat "$cg/$n" > "$base/receipts/end-$n.txt"; done\n' +
              '  exit "$result"\nfi\n']
    steps += ['for n in memory.max memory.swap.max memory.peak memory.events; do cat "$cg/$n" > ' +
              shlex.quote(str(p / 'receipts' / (name + '-'))) + '"$n.txt"; done\n']
steps += ['for n in memory.max memory.swap.max memory.peak memory.events; do cat "$cg/$n" > "$base/receipts/end-$n.txt"; done\nexit "$result"\n']
for name, text in [('run.sh', run), ('inner.sh', prefix + ''.join(steps))]:
    (p / name).write_text(text)
    (p / name).chmod(0o755)
print(json.dumps({'status': 'PREPARED_NOT_RUN', 'packet': str(p), 'source_files': len(inventory),
                  'commands': len(cfg['go_commands']), 'inventory_sha256': cfg['source_inventory_sha256']}))
