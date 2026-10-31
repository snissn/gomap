"""Offline framing controls, including execution with Python assertions disabled."""
import pathlib
import socket
import threading
import time
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import ycsb_reference_wire as wire


def raw_document(elements):
    return struct.pack('<i', len(elements)+5)+elements+b'\0'


class Socket:
    def __init__(self, response):
        self.response=response

    def settimeout(self, timeout):
        self.timeout=timeout

    def close(self):
        self.closed=True

    def recv(self, size):
        result,self.response=self.response[:size],self.response[size:]
        return result

    def sendall(self, data):
        self.sent=data


def response_client(raw,directory):
    with mock.patch.object(wire.socket,'create_connection',return_value=Socket(raw)):
        return wire.Client('127.0.0.1:1',pathlib.Path(directory))


class WireValidation(unittest.TestCase):
    def test_slow_drip_response_has_total_deadline_and_cleanup(self):
        listener=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        listener.bind(('127.0.0.1',0));listener.listen(1);listener.settimeout(2)
        stop=threading.Event();errors=[];sent=[]
        body=b'\0'*5+wire.document(dict(ok=1))
        header=struct.pack('<iiii',len(body)+16,2,1,2013)
        def peer():
            try:
                conn,_=listener.accept()
                with conn:
                    conn.settimeout(2)
                    request=conn.recv(65536)
                    if not request:raise ValueError('peer received no request')
                    conn.sendall(header)
                    # Every byte arrives well inside the three-second inactivity
                    # timeout; the complete valid response takes about four seconds.
                    for byte in body:
                        if stop.wait(.22):return
                        conn.sendall(bytes([byte]));sent.append(byte)
            except (BrokenPipeError,ConnectionResetError):
                pass  # Expected when the deadline closes the actual client socket.
            except BaseException as exc:errors.append(exc)
        thread=threading.Thread(target=peer);thread.start()
        started=time.monotonic();client=None;closed=False
        try:
            with tempfile.TemporaryDirectory() as directory:
                client=wire.Client('127.0.0.1:'+str(listener.getsockname()[1]),pathlib.Path(directory))
                try:
                    with self.assertRaises(TimeoutError):
                        reply=client.call(dict(ping=1))
                        print('slow-drip accepted valid reply before repair:',reply,flush=True)
                finally:
                    client.close();closed=client.sock.fileno()==-1
        finally:
            stop.set();listener.close();thread.join(2)
        self.assertFalse(thread.is_alive(),'synthetic peer must be joined')
        self.assertFalse(errors,errors)
        self.assertTrue(closed,'actual client socket must close after deadline')
        self.assertLess(time.monotonic()-started,4,'total deadline must return control')
        self.assertGreater(len(sent),1,'actual response must drip rather than remain idle')
        print('slow-drip timed out, client closed, peer joined; response bytes:',len(sent),flush=True)

    def test_total_budget_includes_connect_send_and_complete_frame(self):
        body=b'\0'*5+wire.document(dict(ok=1))
        raw=struct.pack('<iiii',len(body)+16,2,1,2013)+body
        clock=[10.0]
        class BudgetSocket(Socket):
            def sendall(self,data):
                self.before_send_timeout=self.timeout
                clock[0]+=1.5
                super().sendall(data)
            def recv(self,size):
                self.before_recv_timeouts=getattr(self,'before_recv_timeouts',[])+[self.timeout]
                return super().recv(size)
        sock=BudgetSocket(raw)
        def connect(*args,**kwargs):
            self.assertEqual(kwargs['timeout'],3)
            clock[0]+=1 # Connection time consumes the same total budget.
            return sock
        with tempfile.TemporaryDirectory() as directory,mock.patch.object(wire.time,'monotonic',side_effect=lambda:clock[0]),mock.patch.object(wire.socket,'create_connection',side_effect=connect):
            client=wire.Client('127.0.0.1:1',pathlib.Path(directory))
            self.assertEqual(client.call(dict(ping=1)),dict(ok=1))
            self.assertEqual(sock.before_send_timeout,2)
            self.assertEqual(sock.before_recv_timeouts,[.5,.5])
            clock[0]=13
            with self.assertRaisesRegex(TimeoutError,'total deadline'):
                client.call(dict(ping=1))
            self.assertEqual(client.counter,1,'expired budget must refuse another send')
            client.close();self.assertTrue(sock.closed)

    def test_expired_connect_closes_socket_and_invalid_budget_refuses(self):
        sock=Socket(b'')
        with mock.patch.object(wire.socket,'create_connection',return_value=sock),mock.patch.object(wire.time,'monotonic',side_effect=[0,3]):
            with self.assertRaisesRegex(TimeoutError,'total deadline'):
                wire.Client('127.0.0.1:1',pathlib.Path('.'))
        self.assertTrue(sock.closed,'expired connect must close an acquired socket')
        for budget in (0,-1,float('inf'),float('nan')):
            with self.subTest(budget=budget),mock.patch.object(wire.socket,'create_connection') as connect:
                with self.assertRaisesRegex(ValueError,'total timeout'):
                    wire.Client('127.0.0.1:1',pathlib.Path('.'),timeout=budget)
                connect.assert_not_called()

    def test_supported_document_round_trip(self):
        value=dict(ok=1, text='binary', payload=b'\0\xff', enabled=True,
                   nested=dict(n=2), items=[1,'two'])
        raw=wire.document(value)
        got,end=wire.decode(raw)
        self.assertEqual(end,len(raw))
        self.assertEqual(got,dict(value,payload=dict(binary_hex='00ff',subtype=0)))

    def test_duplicate_framing_array_and_value_bounds_reject(self):
        integer=b'\x10n\0'+struct.pack('<i',1)
        malformed=[raw_document(integer+integer), b'\x04\0\0\0\0',
                   wire.document(dict(n=1))[:-1]+b'x',
                   raw_document(b'\x08b\0\x02'),
                   raw_document(b'\x02s\0'+struct.pack('<i',4)+b'x\0'),
                   raw_document(b'\x05b\0'+struct.pack('<i',5)+b'\0x')]
        for raw in malformed:
            with self.subTest(raw=raw),self.assertRaises((ValueError,AssertionError,struct.error)):
                wire.decode(raw)
        with self.assertRaises((ValueError,AssertionError)):
            wire.decode(wire.document({'1':1}),array=True)

    def test_response_header_section_trailing_and_EOF_reject(self):
        def frame(body,to=1,opcode=2013):
            return struct.pack('<iiii',len(body)+16,2,to,opcode)+body
        body=b'\0'*5+wire.document(dict(ok=1))
        for raw in [frame(body,to=2),frame(body,opcode=1),
                    frame(b'\1'+body[1:]),frame(body[:4]+b'\1'+body[5:]),
                    frame(body+b'\0'),frame(body)[:-1]]:
            with self.subTest(raw=raw),tempfile.TemporaryDirectory() as directory:
                client=response_client(raw,directory)
                with self.assertRaises((ValueError,AssertionError,struct.error)):
                    client.call(dict(ping=1))

    def test_optimized_interpreter_keeps_duplicate_and_framing_rejections(self):
        code='''
import pathlib, struct, tempfile
import ycsb_reference_wire as wire
from test_ycsb_reference_wire import raw_document, response_client
integer=b'\\x10n\\0'+struct.pack('<i',1)
try:
    wire.decode(raw_document(integer+integer))
except ValueError:
    pass
else:
    raise SystemExit('optimized interpreter accepted duplicate BSON key')
body=b'\\0'*5+wire.document(dict(ok=1))
wrong=struct.pack('<iiii',len(body)+16,2,99,2013)+body
with tempfile.TemporaryDirectory() as directory:
    client=response_client(wrong,directory)
    try:
        client.call(dict(ping=1))
    except ValueError:
        pass
    else:
        raise SystemExit('optimized interpreter accepted response-to mismatch')
print('optimized duplicate and framing controls rejected')
'''
        result=subprocess.run([sys.executable,'-B','-O','-c',code],cwd=pathlib.Path(__file__).parent,
                              text=True,capture_output=True,timeout=10)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertIn('optimized duplicate and framing controls rejected',result.stdout)


if __name__=='__main__':
    unittest.main()
