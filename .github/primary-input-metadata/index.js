'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const {spawn} = require('child_process');
try {
  const root = path.join(process.env.RUNNER_TEMP, 'primary-construction-metadata');
  const harness = path.join(root, 'harness');
  const pins = JSON.parse(fs.readFileSync(path.join(harness, 'inputs.json'), 'utf8'));
  for (const [name, r] of Object.entries(pins)) {
    if (path.isAbsolute(name) || name.split('/').includes('..')) throw new Error();
    const file = path.join(harness, name);
    const s = fs.lstatSync(file);
    if (!s.isFile() || s.nlink !== 1 || s.size !== r.bytes ||
        crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex') !== r.sha256) throw new Error();
  }
  for (const name of ['action.yml', 'index.js']) {
    if (!fs.readFileSync(path.join(__dirname,name)).equals(fs.readFileSync(path.join(harness,name)))) throw new Error();
  }
  const python = '/usr/bin/python3.12';
  const argv = ['-I','-B',path.join(harness,'scripts/ycsb_campaign_guard.py'),
    '--scripts-dir',path.join(harness,'scripts'),'--out',path.join(root,'guard'),
    '--cwd',root,'--filesystem','/home/runner/work','--synthetic',
    '--deadline','180','--cleanup-seconds','10','--interval','.2',
    '--rss-cap-bytes','25769803776','--free-floor-bytes','10737418240','--',
    python,'-I','-B',path.join(harness,'metadata.py')];
  const env = {...process.env, METADATA_NODE_PATH: fs.realpathSync(process.execPath)};
  for (const key of Object.keys(env)) if (key.startsWith('ACTIONS_') || ['GH_TOKEN','GITHUB_TOKEN','ROOT_METADATA_ADMISSION_B64'].includes(key)) delete env[key];
  const child = spawn(python,argv,{stdio:'inherit',env});
  let interrupted = false;
  for (const name of ['SIGINT','SIGTERM']) process.on(name,()=>{
    if (!interrupted) { interrupted = true; child.kill('SIGINT'); }
  });
  child.on('error',()=>{process.stderr.write('METADATA_SPAWN_FAILED\n');process.exitCode=1;});
  child.on('close',(code,signal)=>{
    try {
      // The joined guard has closed these original collector files. Read fixed
      // bounded bytes through our own descriptor and require a checked close.
      const readClosed = name => {
        const fd = fs.openSync(path.join(root,'guard',name),fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
        try {
          const before = fs.fstatSync(fd);
          if (!before.isFile() || before.nlink !== 1 || before.size > 2*1024*1024) throw new Error();
          const data = Buffer.alloc(before.size);
          let offset = 0;
          while (offset < data.length) {
            const count = fs.readSync(fd,data,offset,data.length-offset,null);
            if (count === 0) throw new Error();
            offset += count;
          }
          const extra = Buffer.alloc(1);
          if (fs.readSync(fd,extra,0,1,null) !== 0) throw new Error();
          const after = fs.fstatSync(fd);
          if (before.dev !== after.dev || before.ino !== after.ino || before.size !== after.size ||
              before.mtimeMs !== after.mtimeMs || before.ctimeMs !== after.ctimeMs) throw new Error();
          return {bytes:data.length,sha256:crypto.createHash('sha256').update(data).digest('hex'),
                  base64:data.toString('base64')};
        } finally { fs.closeSync(fd); }
      };
      const stdout = readClosed('stdout.log');
      const stderr = readClosed('stderr.log');
      const guardReceipt = readClosed('result.json');
      const result = JSON.parse(Buffer.from(guardReceipt.base64,'base64').toString('utf8'));
      const encoded = JSON.stringify({schema:'primary-metadata-joined-publication-v2',
        guard:result,guard_receipt:guardReceipt,guard_exit:code,guard_signal:signal,
        collector_stdout:stdout,collector_stderr:stderr});
      // Same aggregate 2 MiB console publication budget as source01. It now
      // includes every original stdout/stderr byte, never a selected subset.
      if (Buffer.byteLength(encoded+'\n','utf8') > 2*1024*1024) {
        process.stderr.write('METADATA_COMPLETE_PUBLICATION_TOO_LARGE\n');
        process.exitCode=1;
        return;
      }
      process.stdout.write(encoded+'\n');
      process.exitCode = !interrupted && !signal && code === 0 && result.errors.length === 0 &&
        result.cleanup_signals.length === 0 && result.status === 'guard-completed-pending-independent-artifact-review' ? 0 : 1;
    } catch (_) {process.stderr.write('METADATA_GUARD_RECEIPT_UNAVAILABLE\n');process.exitCode=1;}
  });
} catch (_) {process.stderr.write('METADATA_SOURCE_BINDING_FAILED\n');process.exitCode=1;}
