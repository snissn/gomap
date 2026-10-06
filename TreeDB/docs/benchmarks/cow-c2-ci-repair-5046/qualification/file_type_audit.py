import sys,json,stat,hashlib,collections
from pathlib import Path
source,expected_file,out=map(Path,sys.argv[1:])
expected=json.loads(expected_file.read_text());rows=[];bad=[]
for name,entry in sorted(expected.items()):
 p=source/name;s=p.lstat();m=entry["mode"]
 actual="symlink" if stat.S_ISLNK(s.st_mode) else "regular" if stat.S_ISREG(s.st_mode) else "other"
 valid=(m=="120000" and actual=="symlink") or (m in ("100644","100755") and actual=="regular" and bool(s.st_mode&0o111)==(m=="100755"))
 row={"path":name,"expected_git_mode":m,"actual_kind":actual,"permissions":oct(stat.S_IMODE(s.st_mode)),"target":str(p.readlink()) if actual=="symlink" else None}
 rows.append(row)
 if not valid:bad.append(row)
actual_paths={p.relative_to(source).as_posix() for p in source.rglob("*") if p.is_file() or p.is_symlink()}
extra=sorted(actual_paths-set(expected));missing=sorted(set(expected)-actual_paths)
body=json.dumps(rows,indent=2)+"\n"; (out/"file-type-rows.json").write_text(body)
summary={"commit":"974d1ce9bd5cd91d17f5851d53663bf71643b288","files":len(rows),"git_modes_sha256":hashlib.sha256(expected_file.read_bytes()).hexdigest(),"actual_rows_sha256":hashlib.sha256(body.encode()).hexdigest(),"actual_kind_counts":dict(collections.Counter(r["actual_kind"] for r in rows)),"expected_mode_counts":dict(collections.Counter(r["expected_git_mode"] for r in rows)),"symlinks":[r for r in rows if r["actual_kind"]=="symlink"],"mode_or_type_mismatches":bad,"extra":extra,"missing":missing}
(out/"file-type-audit.json").write_text(json.dumps(summary,indent=2)+"\n")
print(json.dumps(summary));sys.exit(bool(bad or extra or missing))
