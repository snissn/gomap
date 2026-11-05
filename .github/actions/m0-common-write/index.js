// Fixed standard action handler, adapted from authenticated PRIMARY e473.
// No uploader implementation or independent physical cleanup authority.
'use strict';
const fs = require('fs');
const crypto = require('crypto');
const path = require('path');
const {spawn} = require('child_process');
function need(ok) { if (!ok) throw new Error('fixed action binding refused'); }
try {
  const head = process.env.GITHUB_SHA;
  const a = JSON.parse(Buffer.from(process.env.M0_DATA_ADMISSION_B64, 'base64').toString('utf8'));
  need(a.authority === 'ROOT' && a.actor_id === '1981537' && a.authority_thread === '01a18e7c-8c23-7fd2-8d55-b0486d938cd3' && a.reader_head === head);
  need(head === process.env.EXPECTED_HEAD && head === process.env.WORKFLOW_SHA);
  const harness = process.env.GITHUB_WORKSPACE;
  const manifest = JSON.parse(fs.readFileSync(path.join(harness, '.github/m0-common-write-inputs.json'), 'utf8'));
  for (const [name, row] of Object.entries(manifest.files)) {
    need(!path.isAbsolute(name) && !name.split('/').includes('..'));
    const file = path.join(harness, name);
    const s = fs.lstatSync(file);
    need(s.isFile() && s.nlink === 1 && !s.isSymbolicLink() && s.size === row.bytes);
    need(crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex') === row.sha256);
  }
  need(process.env.ACTIONS_RUNTIME_TOKEN && process.env.ACTIONS_RESULTS_URL && process.env.GITHUB_OUTPUT);
  const child = spawn('python3', ['-B', path.join(harness, '.github/scripts/four_gate_artifact_readback.py'), 'launch'],
    {stdio: 'inherit', env: {...process.env, M0_NODE_PATH: fs.realpathSync(process.execPath)}});
  let interrupted = false;
  for (const name of ['SIGTERM', 'SIGINT']) process.on(name, () => {
    if (!interrupted) { interrupted = true; child.kill('SIGINT'); }
  });
  child.on('error', () => { process.stderr.write('FIXED_ACTION_SPAWN_FAILED\n'); process.exitCode = 1; });
  child.on('close', (code, signal) => { process.exitCode = !interrupted && !signal && code === 0 ? 0 : 1; });
} catch (_) { process.stderr.write('FIXED_ACTION_BINDING_FAILED\n'); process.exitCode = 1; }
