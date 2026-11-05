"""Fixed pure synthetic controls only; no original artifact/tool/helper execution."""
import ast
import hashlib
import zipfile
from pathlib import Path
import copy
import io
import unittest
import m0_trace_consumer as c
import m0_common_write_projection as p
from test_m0_state_projection import fixture


def synthetic():
    owner,a,lines=fixture()
    # The target worker's initial state and loop stack are separate from the
    # executing waker's result-send stack. No actual payload is used.
    lines[0:0]=[b'M=1 P=0 G=7 StateTransition Time=0 GoID=99 NotExist->Waiting Reason=""\n',
        b'TransitionStack=\n',b'\t'+p.LOOP.encode()+b' @ 0xab\n',b'\t\t/src/'+p.FILE.encode()+b':280\n',b'\n']
    i=lines.index(b'M=2 P=1 G=99 StateTransition Time=260 GoID=7 Waiting->Runnable Reason=""\n')
    lines[i+1:i+1]=[b'Stack=\n',b'\t'+p.SEND.encode()+b' @ 0xaf\n',b'\t\t/src/'+p.FILE.encode()+b':227\n',b'\n']
    return owner,a,lines


class Controls(unittest.TestCase):
    def setUp(self):
        self.owner,self.a,self.lines=synthetic();self.out={n:[]for n in ('links','operations','segments','stacks')}
    def emit(self,n,r):self.out[n].append(r)
    def link(self):return p.link_worker(self.a,iter(self.lines),self.emit)
    def test_unique_positive_link(self):
        row=self.link();self.assertEqual(row['status'],'POSITIVELY_LINKED_ORIGINAL_WORKER')
        self.assertEqual(row['worker_goroutine'],99);self.assertEqual(row['result_wake_links'],1)
    def test_event_target_and_event_stacks_separate(self):
        tail=['TransitionStack=','\ttarget @ 0xab','\t\t/t.go:1','Stack=','\twaker @ 0xac','\t\t/w.go:2']
        t,e=p.sections(tail);self.assertEqual(t[0]['function'],'target');self.assertEqual(e[0]['function'],'waker')
    def test_transition_stack_cannot_substitute_waker(self):
        self.lines=[l.replace(b'Stack=\n',b'TransitionStack=\n') if l==b'Stack=\n' else l for l in self.lines]
        # Existing earlier target/event sections now duplicate => strict refusal.
        with self.assertRaises(ValueError):self.link()
    def test_missing_link_explicit_unknown_all2048(self):
        self.lines=[l.replace(p.SEND.encode(),b'other.function') for l in self.lines]
        r=self.link();self.assertEqual(r['reason'],'ORIGINAL_RESULT_WAKE_LINK_MISSING')
        p.unknown_worker(self.a,r['reason'],self.emit)
        self.assertEqual(len(self.out['operations']),2048)
        self.assertTrue(all(r['state_ns'] is None for r in self.out['operations']))
    def test_missing_initial_event(self):
        del self.lines[:5]
        self.assertEqual(self.link()['reason'],'ORIGINAL_WORKER_INITIAL_EVENT_MISSING')
    def test_missing_target_loop(self):
        self.lines=[l.replace(p.LOOP.encode(),b'other.loop') for l in self.lines]
        self.assertEqual(self.link()['reason'],'ORIGINAL_WORKER_TARGET_LOOP_STACK_MISSING')
    def test_result_send_exact_source_line(self):
        self.lines=[l.replace(b':227\n',b':228\n')for l in self.lines]
        self.assertEqual(self.link()['reason'],'ORIGINAL_RESULT_WAKE_LINK_MISSING')
    def test_ambiguous_wakers(self):
        extra=[b'M=2 P=1 G=100 StateTransition Time=2260 GoID=7 Waiting->Runnable Reason=""\n',
               b'Stack=\n',b'\t'+p.SEND.encode()+b' @ 0xaf\n',b'\t\t/src/'+p.FILE.encode()+b':227\n']
        self.lines.extend(extra)
        self.assertEqual(self.link()['reason'],'ORIGINAL_RESULT_WAKE_WORKER_AMBIGUOUS')
    def test_outside_set_envelope_not_nearest_match(self):
        self.lines=[l.replace(b'Time=260 GoID=7',b'Time=1260 GoID=7') for l in self.lines]
        self.assertEqual(self.link()['reason'],'RESULT_WAKE_OUTSIDE_SET_ENVELOPE')
    def test_worker_waiting_endpoints_complete_not_cpu(self):
        r=p.project_worker(self.a,99,iter(self.lines),self.emit)
        self.assertEqual(r['records'],2048);self.assertEqual(r['state_ns']['Waiting'],2048*400)
        self.assertFalse(r['running_is_cpu']);self.assertFalse(r['exclusive_write'])
        self.assertEqual(len(self.out['operations']),2048)
    def test_missing_chain_not_zero(self):
        self.lines.insert(5,b'M=1 P=0 G=99 StateTransition Time=50 GoID=99 Running->Waiting Reason=""\n')
        r=p.project_worker(self.a,99,iter(self.lines),self.emit)
        self.assertEqual(r['status'],'INCONCLUSIVE_STOP');self.assertIsNone(r['state_ns'])
    def test_nonexistent_inside_envelope_unknown(self):
        self.lines.insert(5,b'M=1 P=0 G=99 StateTransition Time=50 GoID=99 Waiting->NotExist Reason=""\n')
        r=p.project_worker(self.a,99,iter(self.lines),self.emit)
        self.assertEqual(r['status'],'INCONCLUSIVE_STOP');self.assertIsNone(r['state_ns'])
    def test_output_error_not_swallowed_as_data_unknown(self):
        def fail(n,r):raise OSError('synthetic short write')
        with self.assertRaises(OSError):p.project_worker(self.a,99,iter(self.lines),fail)
    def test_event_stack_never_in_worker_dictionary(self):
        p.project_worker(self.a,99,iter(self.lines),self.emit)
        funcs=[f['function'] for s in self.out['stacks'] for f in s['frames']]
        self.assertNotIn(p.SEND,funcs)
    def test_bad_stack_grammar_refused(self):
        self.lines.insert(2,b'not a frame\n')
        with self.assertRaises(ValueError):self.link()

    def auth_env(self):
        module=ast.parse(Path(__file__).with_name('four_gate_artifact_readback.py').read_text())
        selected=[n for n in module.body if isinstance(n,ast.FunctionDef) and n.name in ('need','authenticate_members')]
        env={'MIB':1024**2,'hashlib':hashlib,'MEMBERS':{'fixed.txt':(4,hashlib.sha256(b'data').hexdigest())}}
        exec(compile(ast.Module(body=selected,type_ignores=[]),'pure-extracted-member-auth','exec'),env)
        raw=io.BytesIO()
        with zipfile.ZipFile(raw,'w') as z:z.writestr('fixed.txt',b'data')
        return env,raw
    def test_full_member_hash_crc_before_derivation(self):
        env,raw=self.auth_env()
        class Writer:
            def check(self):pass
        with zipfile.ZipFile(raw) as z:env['authenticate_members'](z,Writer())
        source=Path(__file__).with_name('four_gate_artifact_readback.py').read_text()
        self.assertLess(source.index('authenticate_members(z,writer)',source.index('def main():')),
                        source.index("owner=json.loads"))
        self.assertIn('stream.readline(MIB+1)',source)
    def test_hash_and_length_refusal(self):
        class Writer:
            def check(self):pass
        for expected in ((5,hashlib.sha256(b'data').hexdigest()),(4,'0'*64)):
            env,raw=self.auth_env();env['MEMBERS']['fixed.txt']=expected
            with zipfile.ZipFile(raw) as z:
                with self.assertRaises(ValueError):env['authenticate_members'](z,Writer())
    def test_crc_refusal_not_accepted_on_hash(self):
        env,raw=self.auth_env()
        class Writer:
            def check(self):pass
        with zipfile.ZipFile(raw) as z:
            z.getinfo('fixed.txt').CRC^=1
            with self.assertRaises(zipfile.BadZipFile):env['authenticate_members'](z,Writer())
    def test_member_reader_failure_propagates(self):
        env,raw=self.auth_env()
        class Writer:
            def check(self):raise OSError('synthetic resource refusal')
        with zipfile.ZipFile(raw) as z:
            with self.assertRaises(OSError):env['authenticate_members'](z,Writer())
    def test_retained_close_first_operational_error_source(self):
        source=Path(__file__).with_name('four_gate_artifact_readback.py').read_text()
        self.assertIn("BaseExceptionGroup('original failure then close failure',[error,caught])",source)
        self.assertIn("BaseExceptionGroup('original failure then receipt failure',[error,receipt_error])",source)
    def test_exact_source_no_product_or_clockfit(self):
        source=Path(__file__).with_name('m0_common_write_projection.py').read_text()
        tree=ast.parse(source)
        self.assertFalse(any(isinstance(n,ast.Import) and any(a.name in ('subprocess','socket','urllib')for a in n.names) for n in ast.walk(tree)))
        self.assertNotIn("['NS']",source)
        self.assertIn("state_ns':None",source)
        self.assertIn("partial_segments_not_complete_coverage",source)


if __name__=='__main__':
    suite=unittest.defaultTestLoader.loadTestsFromTestCase(Controls)
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful() or result.skipped or result.testsRun!=21:raise SystemExit(1)
