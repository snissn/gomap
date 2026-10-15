import json,subprocess,datetime,pathlib
out=pathlib.Path(__file__).parent
manifest=json.loads((out/'final-a220-source.json').read_text())
def api(path):
 return json.loads(subprocess.check_output(['gh','api',path],text=True))
pr=json.loads(subprocess.check_output(['gh','pr','view','5135','--repo','snissn/gomap','--json','state,mergedAt,mergeCommit,headRefOid,url'],text=True))
assert pr['state']=='MERGED',pr
assert pr['headRefOid']==manifest['head'],pr
merge=pr['mergeCommit']['oid']
commit=api('repos/snissn/gomap/git/commits/'+merge)
parents=[x['sha'] for x in commit['parents']]
assert manifest['head'] in parents,parents
assert manifest['base'] in parents,parents
tree=api('repos/snissn/gomap/git/trees/'+merge+'?recursive=1')
assert not tree.get('truncated'), 'truncated tree'
blobs={x['path']:x['sha'] for x in tree['tree'] if x['type']=='blob'}
verified=[]
for item in manifest['files']:
 actual=blobs.get(item['path']);assert actual==item['git_blob'],(item,actual)
 verified.append({'path':item['path'],'git_blob':actual})
main=api('repos/snissn/gomap/branches/main')['commit']['sha']
assert main==merge,(main,merge)
issues={str(n):api('repos/snissn/gomap/issues/'+str(n))['state'] for n in (5131,5132,5130)}
assert issues['5131']=='closed' and issues['5132']=='closed',issues
assert issues['5130']=='open',issues
r={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pr':pr,'main':main,'merge_parents':parents,'verified_count':len(verified),'files':verified,'issues':issues,'source_manifest':'final-a220-source.json'}
(out/'merged-readback.json').write_text(json.dumps(r,indent=2)+'\n')
print(json.dumps({k:r[k] for k in ('main','merge_parents','verified_count','issues')},indent=2))
