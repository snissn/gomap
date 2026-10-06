if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
from source_paths import source_path, isolate_paths
"""Inert local-only derivation: seal fresh inputs, native Go oracle, finalize inactive manifest."""
import argparse, ast, hashlib, importlib.util, json, pathlib, re
TEMPLATE = source_path('/tmp/gomap-4994-trial14mixedc1-post-input-manifest-prepare-root-v1.py')
TEMPLATE_SHA256 = '8586f38a885357a1e042d3507a57396c64ac88d08e626188e74a33c46f18e7b5'
COLLECTOR = source_path('/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-4997-4998-trial24mixedchangingc1-collector-root-v1.py')
COLLECTOR_SHA256 = 'e4cc620c52f3b0e3904747c2a6a8f922a62dc56277e0741abd3c4a5d1b70fdf1'
def sha(raw): return hashlib.sha256(raw).hexdigest()
def collector():
    assert sha(pathlib.Path(COLLECTOR).read_bytes()) == COLLECTOR_SHA256
    spec = importlib.util.spec_from_file_location('trial24_inert_collector', COLLECTOR)
    c = importlib.util.module_from_spec(spec); spec.loader.exec_module(c); return c

PLAN_MODULE=source_path('/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-4997-4998-trial24-bootstrap-plan-prepare-root-v1.py')
PLAN_SHA='75d6e2bbc45ced856aca3b752807470e7b641d3eb23bb286718d93610ff76f63'
def product_module():
 assert sha(pathlib.Path(PLAN_MODULE).read_bytes())==PLAN_SHA
 spec=importlib.util.spec_from_file_location('trial24_frozen_product',PLAN_MODULE)
 pm=importlib.util.module_from_spec(spec);spec.loader.exec_module(pm);return pm
def post_product(constants,pins):
 pm=product_module();product=pm.frozen_product()
 expected={'HEAD':product['head'],'TREE':product['tree'],'GO_SHA':product['source_inventory_sha256'],'SERVER_SHA':product['server_sha256'],'DRIVER_SHA':product['driver_sha256'],'SOURCE_REVIEW_SHA':pm.PRODUCT_SHA['source_acceptance']}
 for key,value in expected.items():assert constants[key]==value,('frozen product constant',key)
 assert pins['source_head']==product['head'] and pins['source_tree']==product['tree']
 raw=pm.read(constants['BUILD'],constants['BUILD_SHA']);review=pm.read(constants['SOURCE_REVIEW'],constants['SOURCE_REVIEW_SHA'])
 pm.normalized_product(raw,review,product)
 assert sha(pm.read(constants['SOURCE_INV']))==product['source_inventory_sha256']
 return pm,product

def main():
    if not __debug__: raise RuntimeError('assertions required')
    ap = argparse.ArgumentParser(); ap.add_argument('--pins', required=True); ap.add_argument('--output', required=True); ap.add_argument('--finalize-pending')
    a = ap.parse_args(); c = collector(); pins = c.strict_json(pathlib.Path(a.pins).read_bytes()); out = pathlib.Path(a.output)
    assert not out.exists() and not out.is_symlink()
    protected=['__ROOT_FROZEN_SOURCE_ROOT__',pathlib.Path(__file__).resolve().parent,a.pins]
    if a.finalize_pending:protected += [c.LOCAL_INPUT_ROOT,a.finalize_pending]+list(pins['additional_local_pins'])+list(pins['receipts'].values())
    isolate_paths([out],protected)
    if a.finalize_pending:
        raw = pathlib.Path(a.finalize_pending).read_bytes(); assert sha(raw) == pins['pending_manifest_sha256']
        m = c.strict_json(raw); assert all(m[k] is False for k in c.FLAGS)
        isolate_paths([out],protected+list(m['local_pins'])+list(m['receipts'].values()))
        assert m['collector_sha256'] == COLLECTOR_SHA256
        assert set(pins['receipts']) == set(c.RECEIPTS) - set(m['receipts'])
        assert not set(m['local_pins']) & set(pins['additional_local_pins'])
        m['receipts'].update(pins['receipts']); m['local_pins'].update(pins['additional_local_pins'])
        m['population_limits'] = pins['population_limits']; m['server_cli_path'] = '/treedb-fixed-peer'
        pm=product_module();product=pm.frozen_product()
        isolate_paths([out],protected+list(pm.PRODUCT_PATHS.values())+pm.product_review_paths()+[m['receipts']['build'],m['receipts']['source_prereview']])
        pm.normalized_product(pm.read(m['receipts']['build'],m['local_pins'][m['receipts']['build']]),pm.read(m['receipts']['source_prereview'],m['local_pins'][m['receipts']['source_prereview']]),product)
        # Validate the eventual schema locally; grant no flags and run no network.
        schema = dict(m); schema.update({k: True for k in c.FLAGS})
        c.validate_manifest(schema, COLLECTOR_SHA256); c.prepare_local(m)
        assert all(m[k] is False for k in c.FLAGS)
        out.write_text(json.dumps(m, indent=2)+'\n')
        print(json.dumps(dict(state='FINALIZED_INACTIVE_MANIFEST_NOT_RUNTIME_ACCEPTANCE', path=str(out), sha256=sha(out.read_bytes())))); return
    raw = pathlib.Path(TEMPLATE).read_bytes(); assert sha(raw) == TEMPLATE_SHA256
    text = raw.decode().replace('rf4trial14mixedc1', 'rf4trial24mixedchangingc1')
    constants = pins['constants']
    required = {'V','PRE','OUT','ARCHIVE','MANIFEST','PROMOTION_PROOF','PROBE','PROOF','LIFECYCLE','ROOT_PROBE_PROOF_SHA256','ARTIFACT_REVIEW_PATH','ARTIFACT_REVIEW_SHA256','FINAL_INSPECT_WRAPPERS','HEAD','TREE','GO_SHA','DRIVER_SHA','SERVER_SHA','PRE_INV_SHA','BOOT_SHA','CONFIG_SHA','PLAN_SHA','BOOT_REVIEW','BOOT_REVIEW_SHA','LANDED','LANDED_SHA','BUILD','BUILD_SHA','SOURCE_REVIEW','SOURCE_REVIEW_SHA','SOURCE_INV','CIDS'}
    assert set(constants) == required, 'exact required sealer constants'
    protected += [constants[k] for k in ('PRE','PROBE','PROOF','LIFECYCLE','BOOT_REVIEW','LANDED','BUILD','SOURCE_REVIEW','SOURCE_INV','ARTIFACT_REVIEW_PATH')]
    isolate_paths([out],protected+[constants[k] for k in ('OUT','ARCHIVE','MANIFEST','PROMOTION_PROOF')])
    isolate_paths([constants[k] for k in ('OUT','ARCHIVE','MANIFEST','PROMOTION_PROOF')],protected)
    assert constants['OUT'] == c.LOCAL_INPUT_ROOT and constants['HEAD'] == pins['source_head'] and constants['TREE'] == pins['source_tree']
    assert type(pins['pre_input_count']) is int and pins['pre_input_count'] > 0
    assert type(pins['source_pr']) is int and pins['source_pr']>0 and type(pins['predecessor_pr']) is int and pins['predecessor_pr']>0
    pathkeys = {'V','PRE','OUT','ARCHIVE','MANIFEST','PROMOTION_PROOF','PROBE','PROOF','LIFECYCLE','BOOT_REVIEW','LANDED','BUILD','SOURCE_REVIEW','SOURCE_INV','COLLECTOR'}
    replacements = dict(constants, COLLECTOR=COLLECTOR, COLLECTOR_SHA256=COLLECTOR_SHA256)
    class Replace(ast.NodeTransformer):
        def visit_Assign(self, node):
            if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and node.targets[0].id in replacements:
                key = node.targets[0].id; value = repr(replacements[key])
                node.value = ast.parse('Path('+value+')' if key in pathkeys else value, mode='eval').body
            return node
    text = ast.unparse(ast.fix_missing_locations(Replace().visit(ast.parse(text))))+'\n'
    text = text.replace('assert len(inv) == 74', 'assert len(inv) == '+str(pins['pre_input_count']))
    # Seal-before-oracle stage retains all source binding checks. Finalization
    # itself is checked only after sealing, native Go oracle generation and review.
    original = ast.parse(pathlib.Path(COLLECTOR).read_text())
    fn = next(x for x in original.body if isinstance(x, ast.FunctionDef) and x.name == 'validate_source_bindings')
    fn.name = 'validate_sealing_source_bindings'
    fn.body = [x for x in fn.body if not (isinstance(x, ast.Expr) and isinstance(x.value, ast.Call) and isinstance(x.value.func, ast.Name) and x.value.func.id == 'validate_finalization')]
    helper = ast.unparse(fn)
    for name in ('strict_json','input_name','digest_valid'): helper = helper.replace(name+'(', 'c.'+name+'(')
    helper = helper.replace('set(HOSTS)', 'set(c.HOSTS)')
    marker = '    c.validate_source_bindings(manifest, '; assert marker in text
    text = text.replace(marker, '    '+helper.replace('\n','\n    ')+'\n    validate_sealing_source_bindings(manifest, ', 1)
    assert 'c.prepare_local(manifest)' in text
    text = text.replace('c.prepare_local(manifest)', "assert c.population_identity(c.build_initial_population(files, boot, baseline), 128)['SHA256'] == baseline['PopulationSHA256']")
    text = text.replace('PREPARED_LOCAL_INPUTS_INACTIVE_MANIFEST','SEALED_INPUTS_PENDING_NATIVE_PREFIX_ORACLE_AND_FINALIZATION')
    assert not re.search(r'/(?:tmp|home/mikers|Volumes/FlashDrive)/gomap-4994-',text), 'unreplaced historical artifact path'
    assert 'rf4trial14mixedc1' not in text
    pm,product=post_product(constants,pins)
    protected+=list(pm.PRODUCT_PATHS.values())+pm.product_review_paths()
    isolate_paths([out],protected)
    marker='    build = load(BUILD, BUILD_SHA)'
    assert marker in text
    text=text.replace(marker,marker+'\n    assert build == '+repr(product),1)
    guard=ast.unparse(next(n for n in ast.parse(pathlib.Path(__file__).with_name('source_paths.py').read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='isolate_paths'))
    marker='    OUT.mkdir()';assert marker in text
    text=text.replace(marker, "    isolate_paths([OUT,ARCHIVE,MANIFEST,PROMOTION_PROOF],["+repr('__ROOT_FROZEN_SOURCE_ROOT__')+",Path("+repr(str(pathlib.Path(__file__).resolve().parent))+"),PRE,PROBE,PROOF,LIFECYCLE,BOOT_REVIEW,LANDED,BUILD,SOURCE_REVIEW,SOURCE_INV,ARTIFACT_REVIEW_PATH])\n"+marker,1)
    text=guard+'\n'+text
    ast.parse(text, str(out)); out.write_text(text)
    print(json.dumps(dict(state='UNEXECUTED_SEALER_REQUIRES_INDEPENDENT_SOURCE_REVIEW', source=str(out), sha256=sha(out.read_bytes()), template_sha256=TEMPLATE_SHA256, collector_sha256=COLLECTOR_SHA256)))
if __name__ == '__main__': main()
