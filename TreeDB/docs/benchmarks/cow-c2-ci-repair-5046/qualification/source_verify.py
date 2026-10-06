import pathlib,json,hashlib,sys
source=pathlib.Path(sys.argv[1]);identity=pathlib.Path(sys.argv[2]);out=pathlib.Path(sys.argv[3])
expected=json.loads(identity.read_text());actual={str(p.relative_to(source)):hashlib.sha256(p.read_bytes()).hexdigest() for p in source.rglob('*') if p.is_file() or p.is_symlink()}
files=expected['files'];proof={'commit':expected['head'],'files':len(actual),'manifest_sha256':hashlib.sha256(identity.read_bytes()).hexdigest(),'extra':sorted(actual.keys()-files.keys()),'missing':sorted(files.keys()-actual.keys()),'changed':sorted(k for k in files.keys()&actual.keys() if files[k]!=actual[k])}
out.write_text(json.dumps(proof,indent=2)+'\n');assert actual==files,proof
