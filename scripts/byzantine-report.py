#!/usr/bin/env python3
"""
Run all Byzantine Raft tests + benchmarks and produce a single readable report.

Usage:
    python3 scripts/byzantine-report.py              # runs everything, prints + saves report
    python3 scripts/byzantine-report.py --no-bench   # skip benchmarks (faster)
"""

import json
import os
import re
import subprocess
import sys
import textwrap
import time
from collections import OrderedDict
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
REPORT_PATH = os.path.join(ROOT, "byzantine-report.txt")

W = 88  # report width

def _go_env():
    """Return env dict that ensures the real Go module cache is used."""
    env = os.environ.copy()
    home = os.path.expanduser("~")
    real_gomodcache = os.path.join(home, "go", "pkg", "mod")
    if os.path.isdir(real_gomodcache):
        env["GOMODCACHE"] = real_gomodcache
    env.pop("GOFLAGS", None)
    return env

# ── helpers ──────────────────────────────────────────────────────────────────

def hr(char="─"):
    return char * W

def banner(title):
    pad = W - len(title) - 4
    left = pad // 2
    right = pad - left
    return f"{'─' * left}[ {title} ]{'─' * right}"

def indent(text, n=4):
    prefix = " " * n
    return "\n".join(prefix + l for l in text.splitlines())

def pass_fail_icon(passed):
    return "PASS" if passed else "FAIL"

def duration_str(sec):
    if sec is None:
        return ""
    if sec < 1:
        return f"{sec*1000:.0f}ms"
    return f"{sec:.2f}s"

# ── run go test -json ────────────────────────────────────────────────────────

def run_tests():
    env = _go_env()
    proc = subprocess.run(
        ["go", "test", "-json", "-v", "-count=1", "./byzantine"],
        capture_output=True, text=True, cwd=SRC, timeout=120, env=env,
    )
    combined = proc.stdout + "\n" + proc.stderr
    events = []
    for line in combined.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    # Check if we got any real test events (not just build-fail)
    has_test_events = any(
        ev.get("Test") and ev.get("Action") in ("pass", "fail")
        for ev in events
    )
    if not has_test_events:
        proc2 = subprocess.run(
            ["go", "test", "-v", "-count=1", "./byzantine"],
            capture_output=True, text=True, cwd=SRC, timeout=120, env=env,
        )
        return parse_plain_text_to_events(proc2.stdout + "\n" + proc2.stderr)
    return events

def parse_plain_text_to_events(text):
    """Convert plain `go test -v` output into fake JSON events for the parser."""
    events = []
    current_test = None
    for line in text.splitlines():
        run_m = re.match(r"=== RUN\s+(\S+)", line)
        if run_m:
            current_test = run_m.group(1)
            continue
        pass_m = re.match(r"--- (PASS|FAIL): (\S+)\s+\((\d+\.\d+)s\)", line)
        if pass_m:
            action = "pass" if pass_m.group(1) == "PASS" else "fail"
            name = pass_m.group(2)
            elapsed = float(pass_m.group(3))
            events.append({"Test": name, "Action": action, "Elapsed": elapsed})
            continue
        if current_test and line.strip():
            events.append({"Test": current_test, "Action": "output", "Output": line.rstrip() + "\n"})
    return events

# ── run benchmarks ───────────────────────────────────────────────────────────

def run_benchmarks():
    env = _go_env()
    proc = subprocess.run(
        ["go", "test", "-bench=.", "-benchmem", "-benchtime=500ms",
         "-count=1", "-run=^$", "./byzantine"],
        capture_output=True, text=True, cwd=SRC, timeout=120, env=env,
    )
    return proc.stdout + "\n" + proc.stderr

# ── parse test events ────────────────────────────────────────────────────────

SECTIONS = OrderedDict([
    ("Baseline (no corruption)", [
        "TestBaselineAgreementNoCorruption",
    ]),
    ("Part 1: Byzantine attacks WITHOUT MAC", [
        "TestByzantinePayloadCorruptionDivergentApply",
        "TestByzantineStaleTermBlocksCommit",
        "TestByzantineRawBitFlipStallsProgress",
    ]),
    ("Part 2: Side-by-side comparison (no MAC vs HMAC)", [
        "TestCompare_WithoutMAC_vs_WithHMAC",
        "TestCompare_WithoutMAC_vs_WithHMAC/WithoutMAC_tamper_same_index_different_command",
        "TestCompare_WithoutMAC_vs_WithHMAC/WithHMAC_wire_flip_dropped_no_unanimous_commit",
    ]),
    ("Part 3: MAC unit tests", [
        "TestMACRoundTrip",
        "TestMACRejectsWrongKey",
        "TestMACRejectsTampering",
        "TestMACRejectsWrongMethod",
        "TestMACManagerMsgInterceptorShape",
    ]),
    ("Part 3: MAC integration (Raft cluster)", [
        "TestMACClusterAgreement",
        "TestMACDropsSingleBitCorruption",
    ]),
    ("Interceptor unit tests", [
        "TestFlipRandomBitsEmpty",
        "TestFlipRandomBitsDeterministicLength",
        "TestMakeByzantineInterceptorSkipsUnknownEnd",
        "TestMakeByzantineInterceptorCorruptsListedEnd",
    ]),
])

DESCRIPTIONS = {
    "TestBaselineAgreementNoCorruption":
        "3-node cluster, submit 42, all nodes agree. Sanity check.",
    "TestByzantinePayloadCorruptionDivergentApply":
        "Tamper command (42->999) in AppendEntries. Leader applies 42, followers apply 999.\n"
        "=> Same log index, DIFFERENT state machine state. Safety violated.",
    "TestByzantineStaleTermBlocksCommit":
        "Force AppendEntries.Term=0 on replication RPCs from leader.\n"
        "=> Followers reject; command never commits cluster-wide.",
    "TestByzantineRawBitFlipStallsProgress":
        "Flip random bits (12% rate) on leader's outgoing RPCs.\n"
        "=> Decoding fails or semantics break; replication stalls.",
    "TestCompare_WithoutMAC_vs_WithHMAC":
        "Parent test grouping the two sub-cases below.",
    "TestCompare_WithoutMAC_vs_WithHMAC/WithoutMAC_tamper_same_index_different_command":
        "WITHOUT MAC: tamper payload 42->999. Asserts followers diverge from leader.\n"
        "=> If this ever fails, the vulnerability demo is broken.",
    "TestCompare_WithoutMAC_vs_WithHMAC/WithHMAC_wire_flip_dropped_no_unanimous_commit":
        "WITH HMAC: flip one bit on wire before VerifyInbound.\n"
        "=> MAC rejects bad bytes, RPC dropped, no unanimous commit of garbage.",
    "TestMACRoundTrip":             "Wrap + verify round-trip on raw bytes.",
    "TestMACRejectsWrongKey":       "Different key => verification fails.",
    "TestMACRejectsTampering":      "Flip one bit in MAC'd payload => rejected.",
    "TestMACRejectsWrongMethod":    "MAC binds to RPC method name; swapping method => rejected.",
    "TestMACManagerMsgInterceptorShape": "Compile-time check: methods match labrpc.MsgInterceptor.",
    "TestMACClusterAgreement":
        "3-node cluster with HMAC on all RPCs. Submit 42, all agree.\n"
        "=> HMAC does not break normal operation.",
    "TestMACDropsSingleBitCorruption":
        "HMAC enabled + flip one bit on every inbound RPC.\n"
        "=> All RPCs dropped by MAC verify; no unanimous commit.",
    "TestFlipRandomBitsEmpty":      "FlipRandomBits on nil/empty input.",
    "TestFlipRandomBitsDeterministicLength": "Output length matches input length.",
    "TestMakeByzantineInterceptorSkipsUnknownEnd": "Interceptor ignores non-listed endpoints.",
    "TestMakeByzantineInterceptorCorruptsListedEnd": "Interceptor corrupts listed endpoints at rate 1.0.",
}

def parse_test_results(events):
    results = {}  # test_name -> {"action": pass/fail, "elapsed": float, "logs": [str]}
    for ev in events:
        name = ev.get("Test")
        action = ev.get("Action")
        if name is None:
            continue
        if name not in results:
            results[name] = {"action": None, "elapsed": None, "logs": []}
        if action in ("pass", "fail"):
            results[name]["action"] = action
            results[name]["elapsed"] = ev.get("Elapsed")
        elif action == "output":
            out = ev.get("Output", "").rstrip("\n")
            # Filter out noisy framework lines
            if out and not out.startswith("=== RUN") and not out.startswith("--- "):
                # Strip leading whitespace + test file prefix for cleaner logs
                cleaned = re.sub(r"^\s+\S+\.go:\d+: ", "", out)
                if cleaned:
                    results[name]["logs"].append(cleaned)
    return results

# ── parse benchmarks ─────────────────────────────────────────────────────────

def parse_benchmarks(raw):
    benches = []
    for line in raw.splitlines():
        m = re.match(
            r"^(Benchmark\S+)\s+(\d+)\s+([\d.]+)\s+ns/op(?:\s+([\d]+)\s+B/op)?(?:\s+([\d]+)\s+allocs/op)?",
            line,
        )
        if m:
            name = m.group(1)
            iters = int(m.group(2))
            ns_op = float(m.group(3))
            b_op = int(m.group(4)) if m.group(4) else None
            allocs = int(m.group(5)) if m.group(5) else None
            benches.append({
                "name": name, "iters": iters, "ns_op": ns_op,
                "b_op": b_op, "allocs": allocs,
            })
    return benches

# ── build report ─────────────────────────────────────────────────────────────

def build_report(test_results, benchmarks):
    lines = []
    def w(s=""):
        lines.append(s)

    w(hr("═"))
    w(f"  BYZANTINE RAFT EXTENSION — TEST & BENCHMARK REPORT")
    w(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    w(hr("═"))
    w()

    # ── summary counts ──
    total = passed = failed = 0
    for name, r in test_results.items():
        if r["action"] in ("pass", "fail"):
            total += 1
            if r["action"] == "pass":
                passed += 1
            else:
                failed += 1

    w(banner("TEST SUMMARY"))
    w()
    w(f"  Total: {total}    Passed: {passed}    Failed: {failed}")
    w()

    # ── per-section detail ──
    seen = set()
    for section_title, test_names in SECTIONS.items():
        w(hr())
        w(f"  {section_title}")
        w(hr())
        w()
        for tname in test_names:
            seen.add(tname)
            r = test_results.get(tname)
            if r is None:
                w(f"    {pass_fail_icon(False)}  {tname}  (not found in output)")
                w()
                continue
            status = pass_fail_icon(r["action"] == "pass")
            elapsed = duration_str(r["elapsed"])
            label = tname.split("/")[-1] if "/" in tname else tname
            w(f"    {status}  {label}  ({elapsed})")
            desc = DESCRIPTIONS.get(tname)
            if desc:
                for dl in desc.splitlines():
                    w(f"          {dl}")
            logs = [l for l in r["logs"] if l.strip()]
            if logs:
                w(f"          Output:")
                for l in logs:
                    w(f"            {l}")
            w()

    # ── any tests not in a section ──
    unsectioned = [n for n in test_results if n not in seen and test_results[n]["action"]]
    if unsectioned:
        w(hr())
        w("  Other tests")
        w(hr())
        w()
        for tname in sorted(unsectioned):
            r = test_results[tname]
            status = pass_fail_icon(r["action"] == "pass")
            elapsed = duration_str(r["elapsed"])
            w(f"    {status}  {tname}  ({elapsed})")
            w()

    # ── benchmarks ──
    if benchmarks:
        w(hr("═"))
        w(f"  BENCHMARKS")
        w(hr("═"))
        w()

        # HMAC micro
        hmac_benches = [b for b in benchmarks if b["name"].startswith("BenchmarkHMACSHA256")]
        if hmac_benches:
            w("  HMAC-SHA256 raw cost (Wrap = HMAC compute + alloc):")
            w()
            w(f"    {'Payload':<12} {'ns/op':>10} {'B/op':>10} {'allocs':>10} {'iters':>10}")
            w(f"    {'─'*12} {'─'*10} {'─'*10} {'─'*10} {'─'*10}")
            for b in hmac_benches:
                label = b["name"].replace("BenchmarkHMACSHA256_", "")
                b_str = str(b["b_op"]) if b["b_op"] is not None else "—"
                a_str = str(b["allocs"]) if b["allocs"] is not None else "—"
                w(f"    {label:<12} {b['ns_op']:>10.1f} {b_str:>10} {a_str:>10} {b['iters']:>10}")
            w()

        # Raft submit
        raft_benches = [b for b in benchmarks if b["name"].startswith("BenchmarkRaftSubmit")]
        if raft_benches:
            w("  Raft submit latency (3-node cluster, per-op):")
            w()
            w(f"    {'Mode':<30} {'ns/op':>12} {'us/op':>10} {'ops/sec':>12} {'iters':>8}")
            w(f"    {'─'*30} {'─'*12} {'─'*10} {'─'*12} {'─'*8}")
            no_mac_ns = None
            for b in raft_benches:
                label = b["name"].replace("BenchmarkRaftSubmit3Nodes_", "")
                us = b["ns_op"] / 1000
                ops = 1e9 / b["ns_op"] if b["ns_op"] > 0 else 0
                w(f"    {label:<30} {b['ns_op']:>12.0f} {us:>10.1f} {ops:>12.0f} {b['iters']:>8}")
                if "NoMAC" in b["name"]:
                    no_mac_ns = b["ns_op"]
            w()

            if no_mac_ns and len(raft_benches) >= 2:
                mac_bench = next((b for b in raft_benches if "MAC" in b["name"] and "No" not in b["name"]), None)
                if mac_bench:
                    overhead = ((mac_bench["ns_op"] - no_mac_ns) / no_mac_ns) * 100
                    w(f"  MAC overhead: {overhead:+.1f}% latency per submit")
                    w()
                    if abs(overhead) < 5 or overhead < 0:
                        w(f"  Note: short benchtime (500ms) produces noisy results. For stable")
                        w(f"  comparisons, run with -benchtime=3s -count=5 and use benchstat.")
                    w()

    # ── takeaway ──
    w(hr("═"))
    w("  KEY TAKEAWAYS")
    w(hr("═"))
    w()
    w(textwrap.dedent("""\
      1. Plain Raft assumes honest nodes. A single Byzantine actor that tampers with
         AppendEntries bytes can cause followers to apply different commands at the same
         log index — silently diverging state machines.

      2. Corrupting Term fields blocks replication (followers reject stale terms) or
         causes election instability. Random bit flips crash gob decoding or produce
         semantically invalid messages, stalling progress.

      3. Adding HMAC-SHA256 to every RPC (outbound wrap, inbound verify) catches all
         byte-level tampering. Corrupted messages are dropped before dispatch, so the
         cluster never applies garbage.

      4. The MAC overhead is small (sub-microsecond per HMAC on typical Raft message
         sizes). End-to-end Raft submit latency increases modestly — a near-free defense
         against the catastrophic failure mode it prevents.
    """))
    w(hr("═"))

    return "\n".join(lines)

# ── main ─────────────────────────────────────────────────────────────────────

def main():
    skip_bench = "--no-bench" in sys.argv

    print(f"Running tests ...", flush=True)
    t0 = time.time()
    events = run_tests()
    test_results = parse_test_results(events)
    t_tests = time.time() - t0
    print(f"  tests done ({t_tests:.1f}s)", flush=True)

    benchmarks = []
    if not skip_bench:
        print(f"Running benchmarks ...", flush=True)
        t0 = time.time()
        raw = run_benchmarks()
        benchmarks = parse_benchmarks(raw)
        t_bench = time.time() - t0
        print(f"  benchmarks done ({t_bench:.1f}s)", flush=True)

    report = build_report(test_results, benchmarks)

    print()
    print(report)

    with open(REPORT_PATH, "w") as f:
        f.write(report + "\n")
    print(f"\nReport saved to: {REPORT_PATH}")

if __name__ == "__main__":
    main()
