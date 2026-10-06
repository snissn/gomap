"""Exercise exact row/refusal rules using retained real construction stdout.

Extracted per-leaf framing and deliberately damaged copies are parser fixtures,
never performance samples. The original complete stdout is retained unchanged.
"""
import argparse
import copy
import json
from pathlib import Path
import tempfile

from prepare_config import draft
from protocol import identity, drift, row, schedule, sha, write

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stdout", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    lines = args.stdout.read_text().splitlines()
    metadata = [line for line in lines if line.startswith(("goos:", "goarch:", "pkg:", "cpu:"))]
    benchmarks = [line for line in lines if line.startswith("BenchmarkC3PublicReadAdmission/")]
    cases = draft()["cases"]
    assert len(metadata) == 4 and len(benchmarks) == len(cases) == 54
    results = []
    def check(label, text, case, expect_success, error_text=""):
        stdout, stderr = out / (label + ".stdout"), out / (label + ".stderr")
        stdout.write_text(text)
        stderr.write_text(error_text)
        try:
            parsed = row(stdout, stderr, case, "candidate", "measured")
        except (ValueError, KeyError) as error:
            assert not expect_success, (label, str(error))
            results.append({"label": label, "refused": True, "reason": str(error)})
        else:
            assert expect_success, label
            results.append({"label": label, "accepted": True, "metric_count": len(parsed["metrics"])})
    selected = None
    for case in cases:
        case = copy.deepcopy(case)
        case["iterations"] = 1
        matching = [line for line in benchmarks if line.split()[0] == case["benchmark"] + "-4"]
        assert len(matching) == 1
        framed = "\n".join(metadata + matching + ["PASS"]) + "\n"
        check(case["id"], framed, case, True)
        if selected is None:
            selected = (case, matching[0], framed)
    case, original, framed = selected
    tokens = original.split()
    def metric_change(unit, number):
        changed = list(tokens)
        changed[changed.index(unit) - 1] = number
        return "\n".join(metadata + ["\t".join(changed), "PASS"]) + "\n"
    check("missing-row", "\n".join(metadata + ["PASS"]), case, False)
    check("duplicate-row", "\n".join(metadata + [original, original, "PASS"]), case, False)
    check("extra-row", framed + original.replace(case["benchmark"], case["benchmark"] + "wrong") + "\n", case, False)
    check("unexpected-stdout", framed + "unexpected diagnostic\n", case, False)
    check("unexpected-stderr", framed, case, False, "unexpected diagnostic\n")
    check("nonfinite", metric_change("ns/op", "NaN"), case, False)
    check("changed-work", metric_change("output/op", "2"), case, False)
    check("required-zero", metric_change("snapshot_rotations/op", "1"), case, False)
    check("close-failure", metric_change("close_ok", "0"), case, False)
    check("missing-metric", framed.replace(original, "\t".join(tokens[:-2])), case, False)
    check("extra-metric", framed.replace(original, original + " 1 extra_metric"), case, False)
    check("duplicate-metric", framed.replace(original, original + " 1 ns/op"), case, False)
    wrong_count = list(tokens)
    wrong_count[1] = "2"
    check("wrong-count", framed.replace(original, "\t".join(wrong_count)), case, False)
    with tempfile.TemporaryDirectory(prefix="c3-parser-source-") as temporary:
        source = Path(temporary) / "source"
        source.mkdir()
        fixture = source / "fixture.go"
        fixture.write_text("frozen\n")
        manifest = Path(temporary) / "manifest.json"
        write(manifest, {"files": [{"path": "fixture.go", "sha256": sha(fixture)}]})
        bound = identity(manifest)
        assert not drift(source, bound)
        fixture.write_text("drift\n")
        assert drift(source, bound) == ["fixture.go"]
        fixture.write_text("frozen\n")
        (source / "extra.go").write_text("extra\n")
        assert drift(source, bound) == ["EXTRA:extra.go"]
    planned = list(schedule(draft()))
    assert len(planned) == 756 and sum(item["phase"] == "measured" for item in planned) == 648
    write(out / "result.json", {"scope": "parser-only synthetic framing of actual retained candidate construction rows; no performance acceptance",
        "original_stdout_sha256": sha(args.stdout), "accepted_leaf_count": 54,
        "checks": results, "source_drift_and_extra_path_refused": True,
        "schedule_count": len(planned), "scripts": {name: sha(Path(__file__).parent / name) for name in ("protocol.py", "prepare_config.py", "parser_smoke.py")}})
    print(json.dumps({"leaves": 54, "refusals": sum(bool(item.get("refused")) for item in results)}))

if __name__ == "__main__":
    main()
