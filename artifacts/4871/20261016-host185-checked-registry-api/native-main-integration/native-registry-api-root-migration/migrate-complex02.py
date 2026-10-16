from pathlib import Path
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';O=D/'native-main-integration/native-registry-api-root-migration'
def edit(rel,pairs):
 p=W/rel;s=p.read_text();q=O/'before'/rel
 if not q.exists():q.parent.mkdir(parents=True,exist_ok=True);q.write_text(s)
 for a,b in pairs:
  assert a in s,(rel,a);s=s.replace(a,b)
 p.write_text(s)
edit('TreeDB/caching/snapshot.go', [('val, ptr, flags, found := s.lookupCachedRootDomainEntry(key)\n\tif found {','val, ptr, flags, found, err := s.lookupCachedRootDomainEntry(key)\n\tif err != nil {\n\t\treturn dst, err\n\t}\n\tif found {'), ('val, ptr, flags, revision, found := s.lookupCachedRootDomainEntryWithRevision(key)\n\tif found {','val, ptr, flags, revision, found, err := s.lookupCachedRootDomainEntryWithRevision(key)\n\tif err != nil {\n\t\treturn dst, page.LegacyEntryRevision, err\n\t}\n\tif found {')])
edit('TreeDB/caching/cow_read_workspace.go',[('val, ptr, flags, revision, found := s.lookupCachedRootDomainEntryWithRevision(key)\n\tif found {','val, ptr, flags, revision, found, err := s.lookupCachedRootDomainEntryWithRevision(key)\n\tif err != nil {\n\t\treturn node.LeafEntry{}, err\n\t}\n\tif found {')])
p=W/'kvstore/adapters/treedb/treedb.go';s=p.read_text();import re
pattern=r'(\t+)(probe|snap) := d.DB.AcquireSnapshot\(\)\n\1if \2 == nil \{\n\1\treturn (nil, )?kvstore.ErrUnsupported\n\1\}'
def repl(m):
 ind,name,prefix=m[1],m[2],m[3] or '';return f'{ind}{name}, {name}Err := d.DB.AcquireSnapshot()\n{ind}if {name}Err != nil {{\n{ind}\tif errors.Is({name}Err, treedb.ErrClosed) {{\n{ind}\t\treturn {prefix}kvstore.ErrUnsupported\n{ind}\t}}\n{ind}\treturn {prefix}{name}Err\n{ind}}}'
n,count=re.subn(pattern,repl,s);assert count==4;edit(str(p.relative_to(W)),[(s,n),('// Keep historical post-close behavior for ReadBatch: if no snapshot can\n\t\t\t// be acquired up front, report unsupported instead of ErrClosed.','// Keep historical post-close behavior while preserving admission failures.\n\t\t\t// Only an actual ErrClosed maps to ErrUnsupported.')])
edit('cmd/unified_bench/adapter_treedb_backend.go',[('snap = db.AcquireSnapshot()\n\t\tif snap == nil {\n\t\t\treturn kvstore.ErrUnsupported\n\t\t}\n\t\treturn nil','captured, err := db.AcquireSnapshot()\n\t\tif err != nil {\n\t\t\tif errors.Is(err, treedbdb.ErrClosed) {\n\t\t\t\treturn kvstore.ErrUnsupported\n\t\t\t}\n\t\t\treturn err\n\t\t}\n\t\tsnap = captured\n\t\treturn nil')])
