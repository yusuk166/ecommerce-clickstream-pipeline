import argparse
import json
import re
from datetime import datetime, timedelta
from pathlib import Path


def load(p):
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]


def ts(r):
    return datetime.strptime(r["ts"], "%Y-%m-%dT%H:%M:%S")


def steps(p, day):
    out = {}
    for line in p.read_text(encoding="utf-8").splitlines():
        m = re.match(r"(\S+) step rate=(\d+)(?: limit=(\d+))?", line)
        if not m:
            continue
        t = datetime.combine(day, datetime.strptime(m[1], "%H:%M:%S").time())
        if m[3]:
            out[int(m[2])] = {"rate": int(m[2]), "limit": int(m[3]), "start": t, "end": None}
        else:
            out[int(m[2])]["end"] = t
    return [out[k] for k in sorted(out)]


ap = argparse.ArgumentParser()
ap.add_argument("--metrics-dir", default="data/metrics")
ap.add_argument("--drain-seconds", type=int, default=30)
a = ap.parse_args()
d = Path(a.metrics_dir)
progress, batches = load(d / "progress.jsonl"), load(d / "batches.jsonl")
S = steps(d / "steps.log", ts(progress[0]).date())

print("| 입력 속도 (목표 / 실측) | 처리 속도 | 배치 시간 평균 / 최대 | 지연 p95 평균 / 최대 | lag 최대 | 판단 |")
print("|---|---|---|---|---|---|")
for i, s in enumerate(S):
    hi = (s["end"] or datetime.max) + timedelta(seconds=a.drain_seconds)
    if i + 1 < len(S):
        hi = min(hi, S[i + 1]["start"])
    pr = [r for r in progress if s["start"] <= ts(r) <= hi and r["input_rows"]]
    p95 = [float(r["lat_p95"]) for r in batches if s["start"] <= ts(r) <= hi and r["lat_p95"]]
    if not pr:
        continue
    dur = ((s["end"] or ts(pr[-1])) - s["start"]).total_seconds() or 1
    in_rps = sum(r["input_rows"] for r in pr) / dur
    proc = sum(r["processed_rps"] for r in pr) / len(pr)
    bt = [r["batch_ms"] / 1000 for r in pr]
    lag, done = 0, 0
    for r in pr:
        done += r["input_rows"]
        lag = max(lag, int(min(s["limit"], s["rate"] * (ts(r) - s["start"]).total_seconds()) - done))
    verdict = "한계" if lag > s["rate"] * 20 or sum(bt) / len(bt) > 10 else "여유"
    print(f"| {s['rate']:,} / {in_rps:,.0f} | {proc:,.0f} | {sum(bt) / len(bt):.1f}s / {max(bt):.1f}s "
          f"| {sum(p95) / len(p95) if p95 else 0:.1f}s / {max(p95) if p95 else 0:.1f}s | {lag:,} | {verdict} |")
