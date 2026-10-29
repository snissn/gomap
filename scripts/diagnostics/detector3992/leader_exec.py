"""Exact-parent session leader; custody is supplied by reviewed supervisors."""
import os,signal,sys
if len(sys.argv)<5 or sys.argv[1]!="--parent" or sys.argv[3]!="--":raise SystemExit(125)
expected=int(sys.argv[2])
if os.getppid()!=expected:raise SystemExit(125)
signal.pthread_sigmask(signal.SIG_UNBLOCK,{signal.SIGINT,signal.SIGTERM,signal.SIGHUP})
if os.getppid()!=expected:raise SystemExit(125)
os.execvpe(sys.argv[4],sys.argv[4:],os.environ)
