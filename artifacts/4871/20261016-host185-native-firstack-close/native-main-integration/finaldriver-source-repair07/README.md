Repair07: genuine singleton coordinator test fixture, SOURCE ONLY.

The failed repair06 helper successfully admitted Begin with FakeClock but its
empty-Set-only candidate had no DurableRoot transaction. Release correctly
rejected group.members length0 before its clock/success processing. Preliminary
claims that Begin rejected FakeClock, or Release called the separate realClock-
only AdmitNativePublicationCompletionV6, were incorrect on this exact source.
The original failed raw packet remains immutable; diagnosis.json binds it.

This one-file test repair uses the existing genuine ordinary PreparedCOW and
DurableRootTransaction fixture (no payload), Enqueues it normally, checks raw
transaction phase/owner and original Set owner, and executes its original ordinary
Consume callback before successful Release. The ambiguous arm does not Consume;
it verifies original transaction+Set recovery transfer and terminal release.
No phase fields, group membership, fence fields or authority are fabricated.
All previous physical-refusal, Stop, successor, first-error and receipt assertions
remain; explicit transaction callback/custody assertions supplement them.

Default realClock replaces FakeClock. StartupSynchronous keeps the background
scheduler from stealing the prepared turn without a delay/sleep. It remains
true while native physical completion is paused: CompleteStartup correctly
requires activeDispatch0 and is not called. Original WaitThrough dispatches the
ordinary successor through the maintained synchronous startup engine. This is
ordinary-engine progress, not a new background-scheduler qualification.

Original shared Begin/Release work is still32 records/1MiB. Begin5 + success
Release24 or26 =29 or31 (no timer or installed timer). FakeClock would add2 total
and could require33. The exact per-source typed byte expressions and error branch
are in pricing-and-lifetime.json. Existing ordinary Prepare/Enqueue/Consume and
recovery callbacks run outside that synthetic vector, as explicitly identified;
this is not proof that a full native transaction/fence/Consume fitsQ32. Public
Q32 remains RED/ACK0; F/G/P and all production/layout/lifetime bytes are unchanged.

Freeze binds all10,433 source files; only native_publication_v6_test.go differs
from repair06. Full and changed archives are reconstructable; delta and both
source manifests bind exact parent/current bytes. Raw index4c6b6335 unchanged.
Only pinned gofmt ran; no compiler, test body or runtime was executed here.
Source and runtime leases are RELEASED. Root review and explicit runtime grant
are required for the proposed two focused calls (8tops/6children each) and the
separate later actual onceACK/pausedFinishClose calls. All proposal executed
flags remain false. No Git/index/GH action occurred.

The repair06 concurrent containing-admission boundary remains explicit: synthetic
mutex/callback tests with externally kept-alive owners do not prove admission
lifetime under concurrent public Close. No owner/registry/keeper was added.
