"""Synthetic consumer controls; these fixtures are not performance evidence."""
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

import command_startup_trace_validate as consumer
from ycsb_evidence_json import loads

HASHES = {"HTTP": "a" * 64, "Native": "b" * 64}


def trace(kind, retry=False):
    events = [dict(kind="prepared", start_ns=3, end_ns=3),
              dict(kind="spawn", start_ns=4, end_ns=8)]
    if retry:
        events += [dict(kind=kind, start_ns=9, end_ns=11, error="not ready"),
                   dict(kind="poll_sleep", start_ns=12, end_ns=10000012)]
    start = events[-1]["end_ns"] + 1
    events += [dict(kind=kind, start_ns=start, end_ns=start + 2),
               dict(kind="terminal", start_ns=0, end_ns=start + 3)]
    totals = {}
    for event in events:
        value = totals.setdefault(event["kind"], dict(count=0, failures=0, nanoseconds=0))
        value["count"] += 1
        value["failures"] += int(bool(event.get("error")))
        value["nanoseconds"] += event["end_ns"] - event["start_ns"]
    return dict(probe_boundary=consumer.BOUNDARIES[kind], unavailable=list(consumer.UNAVAILABLE),
                evidence=consumer.EVIDENCE, events=events, totals=totals, truncated=False, status="ready")


def report(command="native"):
    probe = "native_hello" if command == "native" else "http_health"
    return dict(version="actual-command-v2", Command=command, Operation="dense", Phase="complete",
                Profile="command_wal_durable", MutationACK="nativewire AckSynced; seed HTTP uses selected durable profile",
                Requested=3, Completed=3, SeedRows=4096,
                HTTPBinarySHA256=HASHES["HTTP"], NativeBinarySHA256=HASHES["Native"],
                DurationSeconds=1.0, OperationsPerSecond=3.0, OperationNanoseconds=[10, 30, 20],
                LatencyNanoseconds=dict(P50=20, P95=30, P99=30, Max=30),
                CloseNanoseconds=10, MaintenanceNanoseconds=10, ReopenNanoseconds=10, ReopenCloseNanoseconds=10,
                Verification=dict(after_requests={"fixture": "producer-owned"}, second_reopen={"fixture": "producer-owned"}),
                startup_trace=dict(seed=trace("http_health"), serve=trace(probe, True), reopen=trace(probe)))


class StartupConsumerTests(unittest.TestCase):
    def check(self, value, **kwargs):
        return consumer.validate(value, value.get("Command", "native"), "dense", HASHES, operations=3, **kwargs)

    def test_enabled_protocols_and_retry(self):
        for command in ("http", "native"):
            with self.subTest(command=command):
                result = self.check(report(command))
                self.assertEqual(result["status"], "valid")
                self.assertIn("database_open_duration", result["unavailable"])

    def test_disabled_requires_omission(self):
        value = report()
        del value["startup_trace"]
        self.check(value, trace_mode="disabled")
        for trace_value in (None, {}, report()["startup_trace"]):
            with self.subTest(trace=trace_value):
                value["startup_trace"] = trace_value
                with self.assertRaises(ValueError):
                    self.check(value, trace_mode="disabled")

    def test_required_trace_rejections(self):
        mutations = {
            "missing": lambda v: v.pop("startup_trace"),
            "null": lambda v: v.update(startup_trace=None),
            "missing-phase": lambda v: v["startup_trace"].pop("reopen"),
            "extra-phase": lambda v: v["startup_trace"].update(extra=trace("http_health")),
            "wrong-protocol": lambda v: v["startup_trace"].update(reopen=trace("http_health")),
        }
        for name, mutation in mutations.items():
            with self.subTest(name=name):
                value = report()
                mutation(value)
                with self.assertRaises(ValueError):
                    self.check(value)

    def test_malformed_trace_rejections(self):
        mutations = {
            "failed": lambda t: t.update(status="failed"),
            "incomplete": lambda t: t.update(status="incomplete"),
            "truncated": lambda t: t.update(truncated=True),
            "truncated-number": lambda t: t.update(truncated=0),
            "unavailable": lambda t: t.update(unavailable=[]),
            "causal-claim": lambda t: t.update(evidence="database opening measured"),
            "probe-boundary": lambda t: t.update(probe_boundary="TCP connected"),
            "missing-events": lambda t: t.pop("events"),
            "excess-events": lambda t: t.update(events=t["events"] * 1025),
            "negative-time": lambda t: t["events"][0].update(start_ns=-1),
            "boolean-time": lambda t: t["events"][0].update(start_ns=True),
            "float-time": lambda t: t["events"][0].update(start_ns=3.0),
            "bad-order": lambda t: t["events"][1].update(start_ns=0),
            "missing-spawn": lambda t: t["events"][1].update(kind="prepared"),
            "missing-probe": lambda t: t["events"][-2].update(kind="poll_sleep"),
            "terminal-bounds": lambda t: t["events"][-1].update(end_ns=0),
            "terminal-origin": lambda t: t["events"][-1].update(start_ns=1),
            "failed-terminal": lambda t: t["events"][-1].update(error="cleanup"),
            "failed-spawn": lambda t: t["events"][1].update(error="spawn failed"),
            "long-error": lambda t: t["events"][-2].update(error="x" * 257),
            "long-unicode-error": lambda t: t["events"][-2].update(error="界" * 100),
            "invented-owner-span": lambda t: t.update(database_open_duration=1),
            "missing-totals": lambda t: t.pop("totals"),
            "wrong-total": lambda t: t["totals"]["terminal"].update(nanoseconds=99),
            "boolean-total": lambda t: t["totals"]["terminal"].update(count=True),
            "wrong-failures": lambda t: t["totals"]["native_hello"].update(failures=0),
            "missing-failed-probe": lambda t: t["events"][2].pop("error"),
        }
        for name, mutation in mutations.items():
            with self.subTest(name=name):
                value = report()
                mutation(value["startup_trace"]["serve"])
                with self.assertRaises(ValueError):
                    self.check(value)

    def test_public_result_and_lifecycle_rejections(self):
        mutations = {
            "original-close-failure-after-counts-pass": lambda v: v.update(cleanup_errors=["original close failed"]),
            "operation-failure": lambda v: v.update(failure="failed"),
            "failed-index": lambda v: v.update(failed_operation=0),
            "incomplete-lifecycle": lambda v: v.update(Phase="serve-close"),
            "missing-reopen-close": lambda v: v.pop("ReopenCloseNanoseconds"),
            "missing-reopen-verification": lambda v: v["Verification"].pop("second_reopen"),
            "null-verification": lambda v: v["Verification"].update(second_reopen=None),
            "missing-samples": lambda v: v.pop("OperationNanoseconds"),
            "wrong-count": lambda v: v.update(Completed=2),
            "bool-count": lambda v: v.update(Completed=True),
            "wrong-percentile": lambda v: v["LatencyNanoseconds"].update(P95=20),
            "nonfinite-rate": lambda v: v.update(OperationsPerSecond=float("inf")),
            "wrong-rate": lambda v: v.update(OperationsPerSecond=4),
            "wrong-binary": lambda v: v.update(HTTPBinarySHA256="c" * 64),
            "wrong-fixture": lambda v: v.update(SeedRows=4000),
            "wrong-profile": lambda v: v.update(Profile="command_wal_relaxed"),
        }
        for name, mutation in mutations.items():
            with self.subTest(name=name):
                value = report()
                mutation(value)
                with self.assertRaises(ValueError):
                    self.check(value)

    def test_strict_json_rejects_duplicates_nonfinite_and_truncation(self):
        for raw in ('{"startup_trace":{},"startup_trace":{}}', '{"x":NaN}', '{"x":1e999}', '{'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                loads(raw)

    def test_trace_int64_boundaries_with_consistent_accounting(self):
        for endpoint in (0, consumer.INT64_MAX, consumer.INT64_MAX + 1):
            for placement in ("start-and-end", "duration"):
                with self.subTest(endpoint=endpoint, placement=placement):
                    value = report()
                    candidate = value["startup_trace"]["seed"]
                    for event in candidate["events"]:
                        event["start_ns"] = endpoint if placement == "start-and-end" else 0
                        event["end_ns"] = endpoint if placement == "start-and-end" else 0
                    if placement == "duration":
                        candidate["events"][-2]["end_ns"] = endpoint
                    candidate["events"][-1].update(start_ns=0, end_ns=endpoint)
                    for kind, total in candidate["totals"].items():
                        total["nanoseconds"] = sum(e["end_ns"] - e["start_ns"]
                                                   for e in candidate["events"] if e["kind"] == kind)
                    if endpoint <= consumer.INT64_MAX:
                        self.check(value)
                    else:
                        with self.assertRaisesRegex(ValueError, "invalid event (start|end)"):
                            self.check(value)

    def test_total_nanoseconds_int64_rejection(self):
        value = report()
        value["startup_trace"]["seed"]["totals"]["terminal"]["nanoseconds"] = consumer.INT64_MAX + 1
        with self.assertRaisesRegex(ValueError, "invalid terminal.nanoseconds"):
            self.check(value)

    def test_public_nanoseconds_int64_boundaries(self):
        lifecycle = ("CloseNanoseconds", "MaintenanceNanoseconds", "ReopenNanoseconds", "ReopenCloseNanoseconds")
        for key in lifecycle:
            with self.subTest(field=key):
                value = report()
                value[key] = consumer.INT64_MAX
                self.check(value)
                value[key] += 1
                with self.assertRaisesRegex(ValueError, "invalid " + key):
                    self.check(value)
                value[key] = 0
                with self.assertRaisesRegex(ValueError, "invalid " + key):
                    self.check(value)
        for sample in (0, consumer.INT64_MAX, consumer.INT64_MAX + 1):
            with self.subTest(sample=sample):
                value = report()
                value["OperationNanoseconds"] = [sample] * 3
                value["LatencyNanoseconds"] = dict.fromkeys(("P50", "P95", "P99", "Max"), sample)
                if sample <= consumer.INT64_MAX:
                    self.check(value)
                else:
                    with self.assertRaisesRegex(ValueError, "invalid operation duration"):
                        self.check(value)
        for key in ("P50", "P95", "P99", "Max"):
            with self.subTest(latency=key):
                value = report()
                value["LatencyNanoseconds"][key] = consumer.INT64_MAX + 1
                with self.assertRaisesRegex(ValueError, "invalid " + key):
                    self.check(value)

    def test_cli_reports_failure(self):
        with tempfile.TemporaryDirectory(prefix="command-startup-consumer-") as directory:
            path = pathlib.Path(directory) / "report.json"
            value = report()
            value["cleanup_errors"] = ["original close failed"]
            path.write_text(json.dumps(value))
            result = subprocess.run([sys.executable, str(pathlib.Path(consumer.__file__)), str(path),
                                     "--command", "native", "--operation", "dense", "--operations", "3",
                                     "--http-binary-sha256", HASHES["HTTP"],
                                     "--native-binary-sha256", HASHES["Native"]], capture_output=True, text=True)
            self.assertEqual(result.returncode, 1)
            self.assertIn("cleanup", result.stderr)
            self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
