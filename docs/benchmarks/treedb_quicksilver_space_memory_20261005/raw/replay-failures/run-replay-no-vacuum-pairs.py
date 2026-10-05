import pathlib, subprocess

root = pathlib.Path('/mnt/fast4tb/quicksilver-space-memory-20261005')
for label, variant in [('1-baseline', 'baseline'), ('1-candidate', 'candidate'),
                       ('2-candidate', 'candidate'), ('2-baseline', 'baseline'),
                       ('3-baseline', 'baseline'), ('3-candidate', 'candidate')]:
    subprocess.run(['python3', '-u', str(root / 'run-replay-no-vacuum.py'), label, variant], check=True)
print('VALIDATED_ALL_SIX_REPLAYS', flush=True)
