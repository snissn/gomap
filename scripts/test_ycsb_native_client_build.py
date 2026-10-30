"""Synthetic compiler controls; never builds a product."""
import copy
import importlib.util
import json
import os
import pathlib
import subprocess
import shutil
import tempfile
import unittest

ROOT=pathlib.Path(__file__).resolve().parent
SPEC=importlib.util.spec_from_file_location('native_build',ROOT/'ycsb_native_client_build.py')
B=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(B)

class BuildTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir=os.environ.get('TMPDIR','/home/mikers'),prefix='native-build-synthetic-')
        self.addCleanup(self.tmp.cleanup)
        self.root=pathlib.Path(self.tmp.name); self.repo=self.root/'repo';self.repo.mkdir()
        def git(*args):return subprocess.check_output(['git','-C',str(self.repo),*args])
        self.git=git
        git('init','-q');git('config','user.name','Synthetic');git('config','user.email','synthetic@example.invalid')
        (self.repo/'db/treedbnative').mkdir(parents=True)
        (self.repo/'db/treedbnative/nativewire.go').write_text('original\n')
        (self.repo/'go.mod').write_text('module synthetic\n')
        git('add','.');git('commit','-qm','synthetic')
        self.commit=git('rev-parse','HEAD').decode().strip()
        (self.repo/'db/treedbnative/nativewire.go').write_text('patched\n')
        self.old=(B.PRODUCER,B.WIRE_SHA)
        B.PRODUCER=self.commit;B.WIRE_SHA=B.digest(self.repo/'db/treedbnative/nativewire.go')
        self.addCleanup(lambda:setattr(B,'PRODUCER',self.old[0]));self.addCleanup(lambda:setattr(B,'WIRE_SHA',self.old[1]))
        self.go=self.root/'fake-go'; self.cc=self.root/'fake-cc'
        self.cc.write_text('#!/bin/sh\necho synthetic-cc\n');self.cc.chmod(0o755)
        self.go.write_text('''#!/usr/bin/python3
import json,os,pathlib,sys
args=sys.argv[1:]
if args==['version']:print('go version go1.synthetic linux/amd64')
elif args[:2]==['env','-json']:print(json.dumps({'GOROOT':str(pathlib.Path(__file__).parent/'goroot'),'GOVERSION':'go1.synthetic'}))
elif args[:2]==['list','-m']:print(json.dumps({'Path':'synthetic','Main':True,'Dir':os.getcwd()}))
elif args[:2]==['list','-deps']:print(json.dumps({'Dir':os.getcwd(),'GoFiles':['go.mod']}))
elif args[0]=='build':
 assert os.environ['GOENV']=='off' and os.environ['GOFLAGS']=='' and os.environ['GOWORK']=='off'
 assert 'EVIL_OVERLAY' not in os.environ
 pathlib.Path(args[args.index('-o')+1]).write_bytes(b'synthetic executable')
elif args[:2]==['version','-m']:print('synthetic buildinfo')
else:raise SystemExit(91)
''');self.go.chmod(0o755)
        (self.root/'goroot').mkdir();(self.root/'goroot/VERSION').write_text('synthetic')
        self.request=dict(schema_version=1,source_dir=str(self.repo),go_binary=str(self.go),cc_binary=str(self.cc),build_id='synthetic',source_patch_sha256=B.sha(self.git('diff','--binary','HEAD')))
        self.out=self.root/'packet'
    def build(self):B.build(self.request,self.out)
    def test_sanitized_success_and_validation(self):
        os.environ['EVIL_OVERLAY']='ignored';self.addCleanup(os.environ.pop,'EVIL_OVERLAY',None)
        self.build();m=B.read(self.out/'client-build.json');B.validate(self.out,m)
        receipt=B.read(self.out/'receipt.json')
        self.assertEqual(receipt['environment']['GOFLAGS'],'')
        self.assertEqual(receipt['argv'][-1],'./cmd/go-ycsb')
        self.assertEqual(m['controlled_build']['receipt_sha256'],B.digest(self.out/'receipt.json'))
    def test_untracked_input_rejected_before_compiler(self):
        (self.repo/'hidden.go').write_text('x');self.assertRaisesRegex(ValueError,'untracked',self.build)
        self.assertFalse(self.out.exists())
    def test_ignored_input_rejected_before_compiler(self):
        (self.repo/'.git/info').mkdir(exist_ok=True);(self.repo/'.git/info/exclude').write_text('hidden.go\n');(self.repo/'hidden.go').write_text('x')
        self.assertRaisesRegex(ValueError,'untracked',self.build)
    def test_hidden_tracked_change_rejected(self):
        self.git('update-index','--assume-unchanged','go.mod')
        (self.repo/'go.mod').write_text('hidden original close replacement')
        self.assertRaisesRegex(ValueError,'actual producer source differs',self.build)
    def test_wire_clean_filter_attributes_rejected(self):
        (self.repo/'.git/info').mkdir(exist_ok=True)
        (self.repo/'.git/info/attributes').write_text('db/treedbnative/nativewire.go filter=unbound\n')
        self.assertRaisesRegex(ValueError,'attributes',self.build)
    def test_bad_request_field_rejected(self):
        self.request['GOFLAGS']='-overlay=evil';self.assertRaisesRegex(ValueError,'request fields',self.build)
    def test_patch_mismatch_rejected(self):
        self.request['source_patch_sha256']='0'*64;self.assertRaisesRegex(ValueError,'patch',self.build)
    def test_receipt_and_evidence_tamper_rejected(self):
        self.build();m=B.read(self.out/'client-build.json')
        (self.out/'buildinfo.txt').write_text('tampered');self.assertRaisesRegex(ValueError,'evidence',B.validate,self.out,m)
    def test_receipt_without_manifest_binding_rejected(self):
        self.build();m=B.read(self.out/'client-build.json');m['controlled_build']['receipt_sha256']='0'*64
        self.assertRaisesRegex(ValueError,'receipt',B.validate,self.out,m)
    def test_required_comparison_rejects_old_manifest(self):
        manifest=dict(targets=['treedb_native'],client={},controlled_native_client_build_required=True)
        self.assertRaisesRegex(ValueError,'required',B.comparison,self.root,manifest)
    def test_optional_old_contract_is_unchanged(self):
        B.comparison(self.root,dict(targets=['mongodb'],client={}))
    def test_required_packet_copy_and_tamper(self):
        self.build();m=B.read(self.out/'client-build.json');dest=self.root/'comparison';dest.mkdir()
        B.retain(self.out,dest,m)
        manifest=dict(targets=['treedb_native'],client=m,controlled_native_client_build_required=True)
        B.comparison(dest,manifest)
        (dest/'native-client-build/source.patch').write_text('tampered')
        self.assertRaisesRegex(ValueError,'evidence',B.comparison,dest,manifest)
    def test_rehashed_unsafe_environment_is_rejected(self):
        self.build();m=B.read(self.out/'client-build.json');receipt=B.read(self.out/'receipt.json')
        receipt['environment']['GOFLAGS']='-overlay=/unbound'
        (self.out/'receipt.json').write_text(json.dumps(receipt))
        m['controlled_build']['receipt_sha256']=B.digest(self.out/'receipt.json')
        self.assertRaisesRegex(ValueError,'unsafe',B.validate,self.out,m)
    def test_rehashed_extra_argv_is_rejected(self):
        self.build();m=B.read(self.out/'client-build.json');receipt=B.read(self.out/'receipt.json')
        receipt['argv'].insert(2,'-overlay=/unbound')
        (self.out/'receipt.json').write_text(json.dumps(receipt))
        m['controlled_build']['receipt_sha256']=B.digest(self.out/'receipt.json')
        self.assertRaisesRegex(ValueError,'argv',B.validate,self.out,m)
    def test_shell_requires_packet_before_any_build(self):
        client=self.root/'client.json';client.write_text('{}')
        server=self.root/'server.json';server.write_text('{}')
        env=dict(os.environ,PARSE_ONLY='false',BUILD='true',BUILD_GOYCSB='false',TARGETS='treedb_native',MONGODB_MODE='skip',OUT_DIR=str(self.root/'shell-out'),CLIENT_BUILD_MANIFEST=str(client),SERVER_BUILD_MANIFEST=str(server),REQUIRE_CONTROLLED_NATIVE_CLIENT_BUILD='true',NATIVE_CLIENT_BUILD_PACKET='',GOYCSB_BIN='/must-not-execute-client',TREEDB_NATIVE_SERVER_BIN='/must-not-build-server',TREEDB_MONGO_GATEWAY_BIN='/must-not-build-gateway')
        result=subprocess.run(['bash',str(ROOT/'ycsb_compare_mongodb_treedb.sh')],env=env,capture_output=True,text=True)
        self.assertEqual(result.returncode,2)
        self.assertIn('absolute NATIVE_CLIENT_BUILD_PACKET is required',result.stderr)
    def test_preflight_binds_explicit_source_and_client_paths(self):
        self.build();manifest=B.read(self.out/'client-build.json')
        B.preflight(self.out,manifest,self.repo,self.out/'go-ycsb','false','false')
        self.assertRaisesRegex(ValueError,'source directory',B.preflight,self.out,manifest,self.root,self.out/'go-ycsb','false','false')
        other=self.root/'unrelated-client';other.write_bytes((self.out/'go-ycsb').read_bytes())
        B.preflight(self.out,manifest,self.repo,other,'false','false')
        relocated=self.root/'relocated-source';shutil.copytree(self.repo,relocated)
        B.preflight(self.out,manifest,relocated,other,'false','false')
        other.write_bytes(b'wrong executable')
        self.assertRaisesRegex(ValueError,'client executable',B.preflight,self.out,manifest,self.repo,other,'false','false')
    def test_preflight_refuses_all_implicit_builds(self):
        self.build();manifest=B.read(self.out/'client-build.json')
        for server,client in [('true','false'),('false','true')]:
            self.assertRaisesRegex(ValueError,'BUILD=false and BUILD_GOYCSB=false',B.preflight,self.out,manifest,self.repo,self.out/'go-ycsb',server,client)
    def test_shell_controlled_packet_refuses_server_build(self):
        self.build();server=self.root/'server.json';server.write_text('{}')
        # Exercise the later BUILD guard after the earlier runtime-hash gate.
        # This deliberately synthetic manifest is never used to launch a client:
        # preflight rejects BUILD=true before it inspects or executes the ELF.
        client_manifest=self.out/'client-build.json'
        manifest=json.loads(client_manifest.read_text())
        manifest['exe_sha256']='7dd046a58f9a3deb2560a55bf9f6b2cd5e167787aace0469e0fa671cade9a00e'
        client_manifest.write_text(json.dumps(manifest))
        env=dict(os.environ,PARSE_ONLY='false',BUILD='true',BUILD_GOYCSB='false',TARGETS='treedb_native',MONGODB_MODE='skip',OUT_DIR=str(self.root/'controlled-shell-out'),CLIENT_BUILD_MANIFEST=str(self.out/'client-build.json'),SERVER_BUILD_MANIFEST=str(server),REQUIRE_CONTROLLED_NATIVE_CLIENT_BUILD='true',NATIVE_CLIENT_BUILD_PACKET=str(self.out),GOYCSB_DIR=str(self.repo),GOYCSB_BIN=str(self.out/'go-ycsb'),TREEDB_NATIVE_SERVER_BIN='/must-not-build-server',TREEDB_MONGO_GATEWAY_BIN='/must-not-build-gateway')
        result=subprocess.run(['bash',str(ROOT/'ycsb_compare_mongodb_treedb.sh')],env=env,capture_output=True,text=True)
        self.assertNotEqual(result.returncode,0)
        self.assertIn('BUILD=false and BUILD_GOYCSB=false',result.stderr)
        self.assertEqual(list((self.root/'controlled-shell-out').iterdir()),[])
    def test_failed_compiler_retains_failure_no_manifest(self):
        self.go.write_text('#!/bin/sh\nexit 37\n')
        self.assertRaises(subprocess.CalledProcessError,self.build)
        self.assertTrue((self.out/'FAILED.json').exists());self.assertFalse((self.out/'client-build.json').exists())

if __name__=='__main__':unittest.main()
