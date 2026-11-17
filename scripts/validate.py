#!/usr/bin/env python3
"""Linux end-to-end acceptance: real files, metrics, SIGTERM, CPU and peak RSS.

Build bin/loadgen first. Each five-second case writes approximately 410 MB;
only this script's uniquely created temporary log directories are removed.
"""
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "validation"
OUTPUT.mkdir(exist_ok=True)
PATTERN = b"x" * 8191 + b"\n"
checks = []

for mode, flags in [("buffered", []), ("durable", ["-sync"])]:
    data = Path(tempfile.mkdtemp(prefix=".acceptance-", dir=ROOT))
    proc = None
    try:
        out = OUTPUT / f"{mode}.json"
        with out.open("w") as stdout, (OUTPUT / f"{mode}.stderr.txt").open("w") as stderr:
            cmd = [str(ROOT / "bin/loadgen"), "-base", str(data), "-name", "request",
                   "-duration", "5s", "-rps", "10000", "-metrics", "127.0.0.1:19091", *flags]
            proc = subprocess.Popen(cmd, stdout=stdout, stderr=stderr)
            exposition = None
            for _ in range(40):
                try:
                    with urllib.request.urlopen("http://127.0.0.1:19091/metrics", timeout=1) as response:
                        exposition = response.read().decode()
                    break
                except OSError:
                    time.sleep(.05)
            assert exposition is not None, "metrics endpoint did not start"
            for kind in ["received", "accepted", "written", "failed", "rejected"]:
                for measure in ["records", "bytes"]:
                    metric = f"qlog_stream_{kind}_{measure}_total"
                    assert metric in exposition, f"missing metric: {metric}"
            for metric in ["process_resident_memory_bytes", "process_cpu_seconds_total", "go_memstats_heap_alloc_bytes"]:
                assert metric in exposition
            (OUTPUT / f"{mode}.metrics.txt").write_text(exposition)
            deadline = time.monotonic() + 40
            while True:
                pid, status, usage = os.wait4(proc.pid, os.WNOHANG)
                if pid:
                    proc.returncode = os.waitstatus_to_exitcode(status)
                    assert proc.returncode == 0, proc.returncode
                    break
                assert time.monotonic() < deadline, "loadgen timeout"
                time.sleep(.05)
        result = json.loads(out.read_text())
        stats = result["stats"]
        assert stats["AcceptedRecords"] == stats["WrittenRecords"]
        assert stats["FailedRecords"] == 0 and stats["PendingRecords"] == 0
        total = lines = 0
        for path in data.glob("request/*.log"):
            with path.open("rb") as source:
                while chunk := source.read(4 << 20):
                    assert len(chunk) % len(PATTERN) == 0
                    assert chunk == PATTERN * (len(chunk) // len(PATTERN))
                    total += len(chunk)
                    lines += len(chunk) // len(PATTERN)
        assert stats["SealedBytes"] == stats["WrittenBytes"]
        assert stats["SealedRecords"] == stats["WrittenRecords"]
        assert total == stats["SealedBytes"] and lines == stats["SealedRecords"]
        checks.append({"mode": mode, "records": lines, "file_bytes": total,
                       "records_per_second": result["written_records_per_second"],
                       "MB_per_second": result["written_MB_per_second"],
                       "heap_bytes": result["heap_bytes"],
                       "peak_RSS_bytes": usage.ru_maxrss * 1024,
                       "user_CPU_seconds": usage.ru_utime,
                       "system_CPU_seconds": usage.ru_stime,
                       "CPU_cores": (usage.ru_utime + usage.ru_stime) / result["elapsed_seconds_including_shutdown"],
                       "verified": True})
    finally:
        if proc is not None and proc.returncode is None:
            proc.kill()
            proc.wait()
        shutil.rmtree(data)

# Verify the command's actual signal handling in addition to the library tests.
data = Path(tempfile.mkdtemp(prefix=".signal-", dir=ROOT))
proc = None
try:
    proc = subprocess.Popen([str(ROOT / "bin/loadgen"), "-base", str(data), "-duration", "30s",
                             "-rps", "2000", "-metrics", ""],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(.3)
    proc.send_signal(signal.SIGTERM)
    stdout, stderr = proc.communicate(timeout=10)
    assert proc.returncode == 0, (proc.returncode, stderr)
    result = json.loads(stdout)
    stats = result["stats"]
    assert stats["WrittenRecords"] == stats["AcceptedRecords"] and stats["WrittenRecords"] > 0
    assert stats["PendingRecords"] == 0 and stats["FailedRecords"] == 0
    assert stats["SealedBytes"] == stats["WrittenBytes"]
    assert sum(path.stat().st_size for path in data.glob("request/*.log")) == stats["SealedBytes"]
    (OUTPUT / "sigterm.json").write_text(stdout)
    checks.append({"mode": "SIGTERM", "records": stats["WrittenRecords"], "verified": True})
finally:
    if proc is not None and proc.returncode is None:
        proc.kill()
        proc.wait()
    shutil.rmtree(data)

(OUTPUT / "checks.json").write_text(json.dumps(checks, indent=2) + "\n")
print(json.dumps(checks, indent=2))
