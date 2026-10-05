from source_paths import source_path, isolate_paths
"""Inert source-only derivation. Root reviews/pins generated sources before any run."""
import argparse, ast, hashlib, importlib.util, json, pathlib, re
TEMPLATES = {'admission': '/tmp/gomap-4994-trial14mixedc1-growth-admission-root-v2.py', 'query': '/tmp/gomap-4994-trial14mixedc1-growth-query-root-v2.py', 'lifecycle': '/tmp/gomap-4994-trial14mixedc1-growth-lifecycle-root-v2.py', 'exact-validate': '/tmp/gomap-4994-trial14mixedc1-growth-exact-validate-root-v2.py', 'artifact-verify': '/tmp/gomap-4994-trial14mixedc1-growth-artifact-verify-root-v2.py'}
TEMPLATE_SHA256 = {'admission': '9fde2650d5b2336c51fb1cf89828f49ee4a3e39b0f024b15e13093b04fce3af2', 'query': '9cf5cfd4012da653780c32ec8eacde075a6979ffd14b31c6f8df4db4590717f4', 'lifecycle': '36deaea97ae5f61a01709cb05c43c99bceded843cda23cb3adb1e50a6cb84440', 'exact-validate': 'd9cd3dfbbbc76badf18e77740fb7d0763d66be03b1324402b56f24c4ea136af6', 'artifact-verify': '41a011660d88d8809b80ead55102ffbcf45f34dc570f9188562f960d0bc366e3'}
CAMPAIGN = 'rf4trial24mixedchangingc1'
PREFIX = 'gomap-4997-4998-trial24-growth-'
def sha(raw): return hashlib.sha256(raw).hexdigest()
ARTIFACT_TEMPLATE_ROOT = '/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1'
REMOTE_TEMPLATE_ROOT = '/home/mikers/gomap-4250-twohost-rf4trial14mixedc1'
def required_path_bindings(texts):
    # Admission's deferred environment paths are removed before this scan.
    # Role sources, the artifact volume and node roots have deterministic joins.
    paths={node.value for text in texts.values() for node in ast.walk(ast.parse(text))
           if isinstance(node,ast.Constant) and isinstance(node.value,str)
           and node.value.startswith(('/tmp/gomap-4994-','/home/mikers/gomap-4994-','/Volumes/FlashDrive/gomap-4994-',REMOTE_TEMPLATE_ROOT))}
    return {path for path in paths if path not in TEMPLATES.values()
            and path!=ARTIFACT_TEMPLATE_ROOT and not path.startswith(ARTIFACT_TEMPLATE_ROOT+'/')
            and path!=REMOTE_TEMPLATE_ROOT and not path.startswith(REMOTE_TEMPLATE_ROOT+'/')}
def no_historical_paths(text):
    assert not re.search(r'/(?:tmp|home/mikers|Volumes/FlashDrive)/gomap-4994-',text), 'unreplaced historical artifact path'
    assert REMOTE_TEMPLATE_ROOT not in text, 'unreplaced historical cluster path'
def validate_path_bindings(texts,bindings):
    assert isinstance(bindings,dict) and set(bindings)==required_path_bindings(texts), 'exact required historical path bindings'
    for old,new in bindings.items():
        assert isinstance(new,str) and pathlib.Path(new).is_absolute() and new!=old, 'fresh absolute reviewed path'
        no_historical_paths(new)
PLAN_MODULE=source_path('/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-4997-4998-trial24-bootstrap-plan-prepare-root-v1.py')
PLAN_SHA='94e1506fc3f5648df9082fd44ddeebf69eb6550d109afa85a2ac22e25be6536b'
def product_module():
 assert sha(pathlib.Path(PLAN_MODULE).read_bytes())==PLAN_SHA
 spec=importlib.util.spec_from_file_location('trial24_frozen_product',PLAN_MODULE)
 pm=importlib.util.module_from_spec(spec);spec.loader.exec_module(pm);return pm
def growth_product(texts,pins):
 pm=product_module();product=pm.frozen_product()
 assert pins['source_head']==product['head'] and pins['source_tree']==product['tree']
 tree=ast.parse(texts['admission'])
 old_expected=ast.literal_eval(next(n.value for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='EXPECTED' for t in n.targets)))
 admit=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='admit')
 calls={n.targets[0].id:n.value for n in admit.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id in ('product','build')}
 def locator(old):
  for source,target in sorted(pins['reviewed_path_bindings'].items(),key=lambda x:-len(x[0])):old=old.replace(source,target)
  return old.replace(ARTIFACT_TEMPLATE_ROOT,pins['artifact_root']).replace('rf4trial14mixedc1',CAMPAIGN)
 paths={k:locator(ast.literal_eval(call.args[0])) for k,call in calls.items()}
 old_sha={k:ast.literal_eval(call.args[1]) for k,call in calls.items()}
 bindings=pins['literal_bindings']
 assert bindings[old_sha['product']]==pm.PRODUCT_SHA['source_acceptance']
 raw=pm.read(paths['build'],bindings[old_sha['build']]);reviewraw=pm.read(paths['product'],pm.PRODUCT_SHA['source_acceptance'])
 pm.normalized_product(raw,reviewraw,product)
 expected=dict(product,source_head=product['head'],source_tree=product['tree'],query_image=product['driver_image'])
 for key in ('source_head','source_tree','source_inventory_sha256','server_sha256','driver_sha256','query_image'):
  assert bindings[old_expected[key]]==expected[key],('frozen product literal',key)
 compare=next(n.test for n in ast.walk(admit) if isinstance(n,ast.Assert) and isinstance(n.test,ast.Compare) and isinstance(n.test.left,ast.Subscript) and isinstance(n.test.left.slice,ast.Constant) and n.test.left.slice.value=='server_images')
 old_images=compare.comparators[0]
 for key,value in zip(old_images.keys,old_images.values):
  if isinstance(value,ast.Constant):assert bindings[value.value]==product['server_images'][key.value],('frozen product host image',key.value)
 return pm,product,paths

def main():
    if not __debug__: raise RuntimeError('assertions required')
    ap=argparse.ArgumentParser();ap.add_argument('--pins',required=True);ap.add_argument('--output-root',required=True)
    a=ap.parse_args(); pins=json.loads(pathlib.Path(a.pins).read_bytes()); out=pathlib.Path(a.output_root)
    assert type(pins['source_pr']) is int and pins['source_pr']>0 and type(pins['predecessor_pr']) is int and pins['predecessor_pr']>0
    assert pins['campaign']==CAMPAIGN and pins['remote_root']=='/home/mikers/gomap-4250-twohost-'+CAMPAIGN
    assert all(re.fullmatch('[0-9a-f]{40}',pins[k]) for k in ('source_head','source_tree'))
    # Root binds every old provenance literal to actual fresh evidence. This map
    # changes constants only; executable assertions and workload math remain intact.
    bindings=pins['literal_bindings']; assert isinstance(bindings,dict)
    assert all(re.fullmatch('[0-9a-f]{40}|[0-9a-f]{64}|sha256:[0-9a-f]{64}',k) and isinstance(v,str) and re.fullmatch('[0-9a-f]{40}|[0-9a-f]{64}|sha256:[0-9a-f]{64}',v) for k,v in bindings.items())
    assert isinstance(pins['qualification_prefix'],int) and pins['qualification_prefix']>0
    texts={}
    for role,path in TEMPLATES.items():
        raw=pathlib.Path(source_path(path)).read_bytes();assert sha(raw)==TEMPLATE_SHA256[role]
        text=raw.decode()
        if role=='admission':
            for key,env in [('ADMISSION_PATH','GOMAP_TRIAL24_GROWTH_ADMISSION'),('ADMISSION_SHA256','GOMAP_TRIAL24_GROWTH_ADMISSION_SHA256'),('SOURCE_PREREVIEW_PATH','GOMAP_TRIAL24_GROWTH_REVIEW'),('SOURCE_PREREVIEW_SHA256','GOMAP_TRIAL24_GROWTH_REVIEW_SHA256')]:
                text=re.sub(r'^'+key+r' = .*$',key+" = os.environ.get("+repr(env)+")",text,flags=re.M)
        texts[role]=text
    validate_path_bindings(texts,pins['reviewed_path_bindings'])
    assert isinstance(pins['artifact_root'],str) and pathlib.Path(pins['artifact_root']).is_absolute()
    no_historical_paths(pins['artifact_root'])
    isolate_paths([pins['artifact_root']],['__ROOT_FROZEN_SOURCE_ROOT__',pathlib.Path(__file__).resolve().parent])
    old_source_hashes={TEMPLATE_SHA256[k] for k in ('query','lifecycle','exact-validate')}
    required=set()
    for text in texts.values():
        required.update(re.findall(r'(?<![0-9a-f])(?:sha256:)?[0-9a-f]{64}(?![0-9a-f])|(?<![0-9a-f])[0-9a-f]{40}(?![0-9a-f])',text))
    assert not (old_source_hashes & set(bindings)), 'generated source hashes must be computed, not supplied'
    assert required-old_source_hashes<=set(bindings), 'all source/image/CID/evidence literals require actual root bindings'
    assert bindings['b625421c06ae6f9398c384686f3c50ce2389ac21']==pins['source_head']
    assert bindings['223df16137b4d11565a4a5ed033973c40ffa7556']==pins['source_tree']
    pm,product,product_paths=growth_product(texts,pins)
    destinations={role:out/(PREFIX+role+'-root-v1.py') for role in texts}
    sources={};generated_hashes={}
    # Exact validator has no dependency; source hashes are computed before they
    # are installed in admission/artifact validation, never guessed.
    for role in ('exact-validate','query','lifecycle','admission','artifact-verify'):
        text=texts[role]
        for old,new in sorted(pins['reviewed_path_bindings'].items(),key=lambda x:-len(x[0])): text=text.replace(old,new)
        for oldrole,oldpath in TEMPLATES.items(): text=text.replace(oldpath,str(destinations[oldrole]))
        text=text.replace('/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1',pins['artifact_root'])
        text=text.replace('rf4trial14mixedc1',CAMPAIGN).replace('gomap-4994-trial14mixedc1-growth-',PREFIX).replace('trial14mixedc1','trial24mixedchangingc1').replace('/growth-query-root-v2','/growth-query-root-v1').replace('/growth-lifecycle-root-v2','/growth-lifecycle-root-v1')
        for old,new in sorted(bindings.items(),key=lambda x:-len(x[0])): text=text.replace(old,new)
        for dependent in ('query','lifecycle','exact-validate'):
            if dependent in generated_hashes: text=text.replace(TEMPLATE_SHA256[dependent],generated_hashes[dependent])
        if role=='artifact-verify':
            # The retained verifier uses split T/basename expressions, not
            # absolute source paths. Bind every read (including proof hashes)
            # directly to the emitted path; keep T's unrelated meaning intact.
            for dependent in ('exact-validate','query','lifecycle'):
                split_path='T/'+repr(PREFIX+dependent+'-root-v2.py')
                assert text.count(split_path)==2, (dependent,'retained source-read shape drift')
                text=text.replace(split_path,'Path('+repr(str(destinations[dependent]))+')')
            reads={str(destinations[k]):0 for k in ('exact-validate','query','lifecycle')}
            checked={}
            def source_read(node):
                if not isinstance(node,ast.Call) or not isinstance(node.func,ast.Attribute) or node.func.attr not in ('read_bytes','read_text'): return None
                receiver=node.func.value
                if not isinstance(receiver,ast.Call) or not isinstance(receiver.func,ast.Name) or receiver.func.id!='Path' or len(receiver.args)!=1 or not isinstance(receiver.args[0],ast.Constant): return None
                return receiver.args[0].value
            verifier=ast.parse(text)
            for node in ast.walk(verifier):
                path=source_read(node)
                if isinstance(path,str) and path.endswith('.py'):
                    assert path in reads, ('unexpected source read',path)
                    reads[path]+=1
                if isinstance(node,ast.Assert) and isinstance(node.test,ast.Compare):
                    compare=node.test;left=compare.left
                    if isinstance(left,ast.Call) and isinstance(left.func,ast.Name) and left.func.id=='sha' and len(left.args)==1:
                        path=source_read(left.args[0])
                        if path in reads:
                            assert len(compare.ops)==len(compare.comparators)==1 and isinstance(compare.ops[0],ast.Eq) and isinstance(compare.comparators[0],ast.Constant)
                            checked[path]=compare.comparators[0].value
            assert all(count==2 for count in reads.values()), reads
            assert checked=={str(destinations[k]):generated_hashes[k] for k in ('exact-validate','query','lifecycle')}, 'source hashes must match emitted bytes'
        if role=='admission':
            lines=text.splitlines();index=next(i for i,line in enumerate(lines) if line.startswith(' build=pinned('));lines.insert(index+1,' assert build=='+repr(product));text='\n'.join(lines)+'\n'
            text=text.replace("landed['pr']==4995", "landed['pr']=="+str(pins['source_pr']))
            text=text.replace("chain['qualification_prefix']==88", "chain['qualification_prefix']=="+str(pins['qualification_prefix']))
            # No approval/pre-review is known when constructing this packet.
            # Separate actual root admission/source review pins are supplied at
            # execution; all prior admit() semantic assertions are retained.
            text='import os\n'+text
            for key,env in [('ADMISSION_PATH','GOMAP_TRIAL24_GROWTH_ADMISSION'),('ADMISSION_SHA256','GOMAP_TRIAL24_GROWTH_ADMISSION_SHA256'),('SOURCE_PREREVIEW_PATH','GOMAP_TRIAL24_GROWTH_REVIEW'),('SOURCE_PREREVIEW_SHA256','GOMAP_TRIAL24_GROWTH_REVIEW_SHA256')]:
                text=re.sub(r'^'+key+r' = .*$',key+" = os.environ.get("+repr(env)+")",text,flags=re.M)
        assert 'rf4trial14mixedc1' not in text and 'landed[\'pr\']==4995' not in text
        no_historical_paths(text)
        ast.parse(text,str(destinations[role]));sources[role]=text;generated_hashes[role]=sha(text.encode())
    assert not out.exists() and not out.is_symlink()
    isolate_paths([out],['__ROOT_FROZEN_SOURCE_ROOT__',pathlib.Path(__file__).resolve().parent,a.pins,pins['artifact_root']]+list(pins['reviewed_path_bindings'].values())+list(pm.PRODUCT_PATHS.values())+list(product_paths.values()))
    out.mkdir()
    for role,text in sources.items(): destinations[role].write_text(text)
    receipt=dict(state='PREPARED_UNEXECUTED_REQUIRES_INDEPENDENT_SOURCE_REVIEW_AND_ACTUAL_ADMISSION',campaign=CAMPAIGN,source_head=pins['source_head'],source_tree=pins['source_tree'],pins_sha256=sha(pathlib.Path(a.pins).read_bytes()),sources={str(destinations[k]):v for k,v in generated_hashes.items()},templates=TEMPLATE_SHA256,workload=dict(operations=70,mutation_attempts=2,searches=68,fresh_ids=1,post_rows=10005),limits=['Source AST checks only; not executed or accepted runtime evidence','Root must verify each supplied literal/path binding against retained actual raw provenance','Derived growth-landed receipt remains separate from collector final landed receipt; both raw provenance must be retained','No automatic replay, store deletion or lifecycle change'])
    (out/'preparation.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt))
if __name__=='__main__': main()
