// Fixed action handler plumbing only; no uploader or independent cleanup authority.
'use strict';
const fs = require('fs');
const crypto = require('crypto');
const path = require('path');
const {spawn} = require('child_process');
function need(ok) { if (!ok) throw new Error('fixed action binding refused'); }
function identity(file, expected) {
  const real = fs.realpathSync(file);
  const s = fs.lstatSync(real);
  need(s.isFile() && real === expected.path && s.size === expected.bytes);
  need(crypto.createHash('sha256').update(fs.readFileSync(real)).digest('hex') === expected.sha256);
  return real;
}
try {
  const root = path.join(process.env.RUNNER_TEMP, 'primary-allocation-construction');
  const harness = path.join(root, 'harness');
  const a = JSON.parse(Buffer.from(process.env.ROOT_ADMISSION_B64, 'base64').toString('utf8'));
  need(a.authority === 'ROOT' && a.actor_id === '1981537' && a.authority_thread === '01a18e7c-8c23-7fd2-8d55-b0486d938cd3' && a.head === process.env.GITHUB_SHA);
  const node = identity(process.execPath, a.tools.node);
  const python = identity(a.tools.python.path, a.tools.python);
  const manifestPath = path.join(harness, '.github/primary-construction-inputs.json');
  need(crypto.createHash('sha256').update(fs.readFileSync(manifestPath)).digest('hex') === process.env.INPUT_MANIFEST_SHA256);
  const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  for (const [name, row] of Object.entries(manifest.files)) {
    need(!path.isAbsolute(name) && !name.split('/').includes('..'));
    const file = path.join(harness, name);
    const s = fs.lstatSync(file);
    need(s.isFile() && s.nlink === 1 && s.size === row.bytes);
    need(crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex') === row.sha256);
  }
  for (const name of ['action.yml', 'index.js']) {
    const actual = fs.readFileSync(path.join(__dirname, name));
    need(actual.equals(fs.readFileSync(path.join(harness, '.github/actions/primary-construction', name))));
  }
  need(process.env.ACTIONS_RUNTIME_TOKEN && process.env.ACTIONS_RESULTS_URL && process.env.GITHUB_OUTPUT);
  const child = spawn(python, ['-I', '-B', path.join(harness, '.github/scripts/primary_allocation_construction.py'), 'launch'],
    {stdio: 'inherit', env: {...process.env, PRIMARY_NODE_PATH: node}});
  let interrupted = false;
  for (const name of ['SIGTERM', 'SIGINT']) process.on(name, () => {
    if (!interrupted) {
      interrupted = true;
      // Python's existing outer wait catches KeyboardInterrupt and cleans its guard.
      child.kill('SIGINT');
    }
  });
  child.on('error', () => { process.stderr.write('FIXED_ACTION_SPAWN_FAILED\n'); process.exitCode = 1; });
  child.on('close', (code, signal) => {
    process.exitCode = !interrupted && !signal && code === 0 ? 0 : 1;
  });
} catch (_) {
  process.stderr.write('FIXED_ACTION_BINDING_FAILED\n');
  process.exitCode = 1;
}
