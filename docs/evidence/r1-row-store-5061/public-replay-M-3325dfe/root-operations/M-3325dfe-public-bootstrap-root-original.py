import datetime, hashlib, json, platform, subprocess
from pathlib import Path

O = Path('/home/mikers/gomap-r1-final-observers-20261006')
S = Path('/home/mikers/gomap-r1-publication-stage-M-3325dfe-20261006')
sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
host = O / 'M-3325dfe-public-replay-host-original.json'
with host.open('x') as f:
    json.dump({'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
               'platform': platform.platform(), 'uname': list(platform.uname()),
               'hostname': platform.node(), 'purpose': 'actual public replay host'}, f, indent=2)
    f.write('\n')
tag = 'r1-5056-evidence-M3325dfe-20261006-v1'
bootstrap = {
    'schema': 'gomap-r1-public-replay-builder-bootstrap-v1',
    'public_artifact_git_sha': '24ac6866992dd89ab222b8bf03d60e6c0c49a2d5',
    'public_repository_https': 'https://github.com/snissn/gomap.git',
    'release_tag': tag,
    'release_https_url': f'https://github.com/snissn/gomap/releases/download/{tag}/binary-overlay.tar.gz',
    'fresh_linux111': {
        'checkout_absolute': '/home/mikers/gomap-r1-public-git-M-3325dfe-20261006',
        'restored_absolute': '/home/mikers/gomap-r1-public-restored-M-3325dfe-20261006',
        'proof_absolute': '/home/mikers/gomap-r1-public-proof-M-3325dfe-20261006',
        'timed_lock': '/home/mikers/gomap-r1-evidence-20261005/timed-capture.lock',
        'actual_host_identity_record': str(host),
        'actual_host_identity_record_sha256': sha(host),
        'no_private_capture_source': True,
    },
    'command_timeout_seconds': 600,
    'authenticated_core_SHA256SUMS_sha256': '0c378823a405dc4af75fe33228ccc29d94d3172ac8c8a63d58226b2e677616dc',
    'authenticated_base13_sha256': 'ddfa77702e3e19974c03deab5b01d40b273b05634e9d7a330b664241dbbfecd6',
}
for k in ('checkout_absolute', 'restored_absolute', 'proof_absolute'):
    assert not Path(bootstrap['fresh_linux111'][k]).exists(), k
p = O / 'M-3325dfe-public-builder-bootstrap-original.json'
with p.open('x') as f:
    json.dump(bootstrap, f, indent=2, sort_keys=True); f.write('\n')
helper = O / 'M-3325dfe-public-input-cost-identity-v3-preparation/build-draft-inputs.py'
assert sha(helper) == 'a79091f4ee3d7f265f8c330c60b2eb01139d658001f46de80cbc096a13d8a79f'
argv = ['python3', '-B', str(helper), '--stage', str(S), '--template',
        str(O / 'M-3325dfe-public-replay-provenance-v2-preparation/root-inputs.template.json'),
        '--bootstrap', str(p), '--expected-bootstrap-sha256', sha(p), '--out',
        str(O / 'M-3325dfe-public-builder-draft-original')]
start = O / 'M-3325dfe-public-builder-start-original.json'
with start.open('x') as f:
    json.dump({'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'argv': argv}, f, indent=2); f.write('\n')
r = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
with (O / 'M-3325dfe-public-builder-output-original.log').open('xb') as f: f.write(r.stdout)
with (O / 'M-3325dfe-public-builder-exit-original.json').open('x') as f:
    json.dump({'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'exit': r.returncode,
               'log_sha256': hashlib.sha256(r.stdout).hexdigest()}, f, indent=2); f.write('\n')
print(r.stdout.decode()); raise SystemExit(r.returncode)
