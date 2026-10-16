from pathlib import Path
import re
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';O=D/'native-main-integration/native-registry-api-root-migration'
def read(rel):
 p=W/rel;s=p.read_text();q=O/'before'/rel
 if not q.exists():q.parent.mkdir(parents=True,exist_ok=True);q.write_text(s)
 return p,s
p,s=read('TreeDB/caching/root_domain.go')
a=s.index('func (db *DB) resolveLivePublishedRootSnapshot(');b=s.index('\nfunc ',a+1);x=s[a:b]
x=x.replace('(rootDomainSnapshot, func()) {','(rootDomainSnapshot, func(), error) {').replace('return s, nil','return s, nil, nil')
x=x.replace('snap := provider.AcquireSnapshot()\n\tif snap == nil {\n\t\treturn s, nil, nil','snap, err := provider.AcquireSnapshot()\n\tif err != nil {\n\t\treturn s, nil, err')
x=x.replace('return s, func() { _ = snap.Close() }','return s, func() { _ = snap.Close() }, nil');s=s[:a]+x+s[b:]
s=s.replace('s, release := db.resolveLivePublishedRootSnapshot(s)\n\tvar (','s, release, err := db.resolveLivePublishedRootSnapshot(s)\n\tif err != nil {\n\t\treturn nil, false, err\n\t}\n\tvar (').replace('\n\t\terr error\n\t)\n\tif reverse {','\n\t)\n\tif reverse {');p.write_text(s)
# All these private helpers must carry a real registration failure to an existing public error return.
p,s=read('TreeDB/caching/range_spans.go');a=s.index('func (db *DB) lookupViewEntryWithRangeSpans(');b=s.index('\nfunc rangeSpanPrefixEnd',a);x=s[a:b]
x=re.sub(r'(?m)^(func .*\) \([^\n]+)\) \{',r'\1, err error) {',x)
x=re.sub(r'(?m)^(\t+return (?!s\.db\.lookup)[^\n]+)$',r'\1, nil',x)
x=re.sub(r'(?m)^(\t+[^\n=]+) = ((?:db|s)\.lookup[^\n]+)$',r'\1, err = \2',x)
# Forward the named error from wrapper helpers, not nil.
x=re.sub(r'(return val, ptr, flags, (?:found|found, source)), nil',r'\1, err',x)
x=x.replace('resolvedSnap, release := db.resolveLivePublishedRootSnapshot(rootSnap)','resolvedSnap, release, err := db.resolveLivePublishedRootSnapshot(rootSnap)\n\t\tif err != nil {\n\t\t\treturn nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, rootDomainEntrySourceNone, err\n\t\t}')
s=s[:a]+x+s[b:];p.write_text(s)
p,s=read('TreeDB/caching/snapshot.go');a=s.index('func (s *Snapshot) lookupQueueEntry(');b=s.index('\nfunc recordSnapshotRootDomainRead',a);x=s[a:b]
x=re.sub(r'(?m)^(func .*\) \([^\n]+)\) \{',r'\1, err error) {',x)
x=re.sub(r'(?m)^(\t+[^\n=]+) = ((?:s(?:\.db)?)\.lookup[^\n]+)$',r'\1, err = \2',x)
x=re.sub(r'(?m)^(\t+return (?!snap\.getEntryWithRevision)[^\n]+)$',r'\1, nil',x)
# Wrapper returns propagate the named error; concrete reads return nil unless acquisition failed.
for ret in ['val, ptr, flags, found','val, ptr, flags, revision, found','snap, val, ptr, flags, found, source','val, ptr, flags, found, source','val, ptr, flags, revision, found, source']:
 x=x.replace('return '+ret+', nil','return '+ret+', err')
x=x.replace('val, ptr, flags, revision, found, source, err = s.db.lookupViewEntryWithRangeSpansAndRootRevisionSource(s.view, key, false, snap, true)\n\t\tif found {','val, ptr, flags, revision, found, source, err = s.db.lookupViewEntryWithRangeSpansAndRootRevisionSource(s.view, key, false, snap, true)\n\t\tif err != nil {\n\t\t\treturn snap, nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, rootDomainEntrySourceNone, err\n\t\t}\n\t\tif found {')
x=x.replace('return snap.getEntryWithRevision(key)','val, ptr, flags, revision, found = snap.getEntryWithRevision(key)\n\treturn val, ptr, flags, revision, found, nil')
s=s[:a]+x+s[b:];p.write_text(s)
# The remaining production callers already return errors; add the direct failure exits.
p,s=read('TreeDB/caching/db.go')
for funcname,ret in [('getMemtable','nil, false, err'),('getMemtableAppend','dst, false, err'),('getMemtableAppendWithRevision','dst, page.LegacyEntryRevision, false, err'),('Has','false, err')]:
 a=s.index('func (db *DB) '+funcname+'(');b=s.index('\nfunc ',a+1);x=s[a:b]
 x,n=re.subn(r'(?m)^(\t+)([^\n]+) := (db\.lookupViewEntryWithRangeSpans[^\n]+)$',lambda m:m[1]+m[2]+', err := '+m[3]+'\n'+m[1]+'if err != nil {\n'+m[1]+'\treturn '+ret+'\n'+m[1]+'}',x);assert n==1,(funcname,n);s=s[:a]+x+s[b:]
p.write_text(s)
p,s=read('TreeDB/caching/snapshot.go')
for funcname,ret in [('getAppendOpen','dst, err'),('getVersionedAppendOpen','dst, page.LegacyEntryRevision, err'),('getUnsafeOpen','nil, err'),('hasOpen','false, err'),('GetEntry','node.LeafEntry{}, err'),('GetEntryExact','node.LeafEntry{}, err')]:
 marker='func (s *Snapshot) '+funcname+'('
 if marker not in s:
  print('NEEDS_METHOD_NAME',funcname);continue
 a=s.index(marker);b=s.find('\nfunc ',a+1);b=len(s) if b<0 else b;x=s[a:b]
 x,n=re.subn(r'(?m)^(\t+)([^\n]+) (:=|=) (s\.lookup(?:CachedRootDomainEntry|QueueEntry|RootDomainSnapshotEntry)[^\n]+)$',lambda m:m[1]+m[2]+', err '+m[3]+' '+m[4]+'\n'+m[1]+'if err != nil {\n'+m[1]+'\treturn '+ret+'\n'+m[1]+'}',x);assert n>0,(funcname,n);s=s[:a]+x+s[b:]
p.write_text(s)
