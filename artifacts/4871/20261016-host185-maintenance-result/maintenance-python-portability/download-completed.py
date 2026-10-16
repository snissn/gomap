"""Retain the one completed attempt, including failure or refusal; no retry."""
import datetime
import hashlib
import json
import shutil
import stat
import subprocess
import zipfile
from pathlib import Path, PurePosixPath

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
R = D / 'maintenance-portability-runtime'

def read(name):
    return json.loads((R / name).read_text())

def save(name, value):
    (R / name).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    return value

def api(endpoint):
    return json.loads(subprocess.check_output(['gh', 'api', endpoint], text=True))

def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for part in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            h.update(part)
    return h.hexdigest()

binding, grant = read('run-binding.json'), read('grant.json')
run_id = binding['run_id']
run = save('completed-run.json', api(f'repos/snissn/gomap/actions/runs/{run_id}'))
assert run['status'] == 'completed', 'Keep waiting; this is not a failure.'
assert run['head_sha'] == grant['harness_head'] and run['run_attempt'] == 1
assert run['event'] == 'workflow_dispatch' and run['head_branch'] == 'main'
jobs = save('completed-jobs.json', api(f'repos/snissn/gomap/actions/runs/{run_id}/jobs?per_page=100'))
assert jobs['total_count'] == len(jobs['jobs'])
artifacts = save('artifacts.json', api(f'repos/snissn/gomap/actions/runs/{run_id}/artifacts?per_page=100'))
assert artifacts['total_count'] == len(artifacts['artifacts'])
matches = [a for a in artifacts['artifacts'] if a['name'] == f'bounded-maintenance-{run_id}-1']
assert len(matches) == 1, 'Missing complete-attempt artifact: preserve action evidence and investigate; never rerun automatically.'
artifact = matches[0]
assert not artifact['expired'] and artifact['workflow_run']['head_sha'] == grant['harness_head']
archive = R / 'attempt.zip'
assert not archive.exists() and not (R / 'packet').exists()
try:
    with archive.open('xb') as stream:
        result = subprocess.run(['gh', 'api', f"repos/snissn/gomap/actions/artifacts/{artifact['id']}/zip"],
                                stdout=stream, stderr=subprocess.PIPE, timeout=600)
except BaseException as error:
    save('download-interrupted.json', {'error': repr(error), 'artifact_id': artifact['id'],
                                     'run_id': run_id, 'bytes': archive.stat().st_size,
                                     'partial_sha256': digest(archive), 'retry_executed': False})
    raise
receipt = {'at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'artifact_id': artifact['id'],
           'run_id': run_id, 'exit': result.returncode, 'stderr': result.stderr.decode(errors='replace'),
           'bytes': archive.stat().st_size, 'sha256': digest(archive), 'api_digest': artifact.get('digest'),
           'api_size_in_bytes': artifact['size_in_bytes'], 'source_run_conclusion': run['conclusion']}
save('download-receipt.json', receipt)
assert result.returncode == 0
assert receipt['bytes'] == artifact['size_in_bytes']
assert artifact.get('digest', '').startswith('sha256:'), 'API archive digest unavailable; do not claim verified archive identity.'
assert 'sha256:' + receipt['sha256'] == artifact['digest'], 'Downloaded archive differs from the API digest.'

packet = R / 'packet'
packet.mkdir()
files = {}
with zipfile.ZipFile(archive) as z:
    members = z.infolist()
    assert len({i.filename for i in members}) == len(members), 'Duplicate archive paths.'
    for info in members:
        path = PurePosixPath(info.filename)
        assert not path.is_absolute() and '..' not in path.parts
        assert not stat.S_ISLNK(info.external_attr >> 16)
        target = packet.joinpath(*path.parts)
        if info.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        with z.open(info) as source, target.open('xb') as dest:
            shutil.copyfileobj(source, dest)
        files[info.filename] = {'bytes': target.stat().st_size, 'sha256': digest(target)}
save('extracted-files.json', files)
receipt.update(archive_digest_verified=True, extracted_files=len(files),
               extracted_bytes=sum(f['bytes'] for f in files.values()),
               extracted_manifest_sha256=digest(R / 'extracted-files.json'))
save('download-receipt.json', receipt)
print(json.dumps(receipt, indent=2))
