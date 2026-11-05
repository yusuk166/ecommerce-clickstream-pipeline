"""로드 테스트 결과 집계 — preprocess.py 가 남긴 지표를 단계별 표로 만든다.

입력 (data/metrics/)
  steps.log       단계 시작 · 끝 시각 (run_steps.sh 가 기록)
  progress.jsonl  배치마다 Spark lastProgress: 입력 속도 · 처리 속도 · 배치 시간 · lag
  batches.jsonl   배치마다 건수 · end-to-end 지연 p50 · p95 · max

예)
  python loadtest_report.py                  # 표 출력 (markdown)
  python loadtest_report.py --drain-seconds 60   # 단계 끝난 뒤 60초까지를 그 단계로 침
"""
import argparse
import json
import re
from datetime import datetime, timedelta
from pathlib import Path


def load_jsonl(p: Path) -> list[dict]:
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def parse_steps(p: Path, day: datetime) -> list[dict]:
    """'10:35:46 step rate=100 limit=12000 offset=...' / '10:37:48 step rate=100 done' → [{rate, start, end}]"""
    steps: dict[int, dict] = {}
    for line in p.read_text(encoding="utf-8").splitlines():
        m = re.match(r"(\d\d:\d\d:\d\d) step rate=(\d+)(.*)", line)
        if not m:
            continue
        t = datetime.combine(day.date(), datetime.strptime(m.group(1), "%H:%M:%S").time())
        rate = int(m.group(2))
        if "done" in m.group(3):
            steps[rate]["end"] = t
        else:
            steps[rate] = {"rate": rate, "start": t, "end": None,
                           "limit": int(re.search(r"limit=(\d+)", m.group(3)).group(1))}
    return [s for _, s in sorted(steps.items())]


def ts(row: dict) -> datetime:
    return datetime.strptime(row["ts"], "%Y-%m-%dT%H:%M:%S")


def pct(xs: list[float], q: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(q * (len(xs) - 1))))] if xs else 0.0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--metrics-dir", default="data/metrics")
    ap.add_argument("--drain-seconds", type=int, default=30, help="단계가 끝난 뒤 이 시간까지의 배치를 그 단계 결과에 포함")
    args = ap.parse_args()
    d = Path(args.metrics_dir)

    progress = load_jsonl(d / "progress.jsonl")
    batches = load_jsonl(d / "batches.jsonl")
    day = ts(progress[0]) if progress else datetime.now()
    steps = parse_steps(d / "steps.log", day)

    rows = []
    for i, s in enumerate(steps):
        lo = s["start"]
        hi = (s["end"] or datetime.max) + timedelta(seconds=args.drain_seconds)
        if i + 1 < len(steps):  # 다음 단계가 바로 이어지면 그 시작 전까지만
            hi = min(hi, steps[i + 1]["start"])
        pr = [r for r in progress if lo <= ts(r) <= hi and r["input_rows"]]
        br = [r for r in batches if lo <= ts(r) <= hi and r["kafka"]]
        if not pr:
            continue
        dur = max(1.0, ((s["end"] or ts(pr[-1])) - s["start"]).total_seconds())
        in_rps = sum(r["input_rows"] for r in pr) / dur            # 단계 동안 실제로 들어온 평균 속도
        proc_rps = sum(r["processed_rps"] for r in pr) / len(pr)   # 배치 처리 속도 평균
        batch_ms = [r["batch_ms"] for r in pr]
        # lag(밀린 양) = 배치가 끝난 시점까지 생성기가 보낸 건수 - 지금까지 처리한 건수.
        # Spark 가 알려 주는 latestOffset 은 배치를 계획할 때 읽은 값이라, 입력량 제한이 없으면 endOffset 과 같아 0 으로 나온다.
        # 그래서 생성기 속도(고정 rate)로 보낸 양을 추정해 계산한다.
        lags, cum = [], 0
        for r in pr:
            cum += r["input_rows"]
            sent = min(s["limit"], s["rate"] * (ts(r) - s["start"]).total_seconds())
            lags.append(max(0, int(sent - cum)))
        p95s = [r["lat_p95"] for r in br if r["lat_p95"] is not None]
        peak = max(lags) if lags else 0
        lag_trend = "계속 증가" if peak > s["rate"] * 20 else "일정"   # 20초치 이상 밀리면 못 따라간 것
        rows.append({
            "rate": s["rate"], "sent": s["limit"], "batches": len(pr),
            "in_rps": in_rps, "proc_rps": proc_rps,
            "batch_avg_s": sum(batch_ms) / len(batch_ms) / 1000, "batch_max_s": max(batch_ms) / 1000,
            "lat_p95_avg": (sum(p95s) / len(p95s)) if p95s else 0, "lat_p95_max": max(p95s) if p95s else 0,
            "lag_max": max(lags) if lags else 0, "lag_end": lags[-1] if lags else 0, "lag_trend": lag_trend,
        })

    print("| 입력 속도 (건/s) | 처리 속도 (건/s) | 배치 처리 시간 평균 / 최대 | 지연 p95 평균 / 최대 | lag 최대 (추정) | lag 추세 | 판단 |")
    print("|---|---|---|---|---|---|---|")
    for r in rows:
        verdict = "한계" if r["lag_trend"] != "일정" or r["batch_avg_s"] > 10 else "여유"
        print(f"| {r['rate']:,} (실측 {r['in_rps']:,.0f}) | {r['proc_rps']:,.0f} | {r['batch_avg_s']:.1f}s / {r['batch_max_s']:.1f}s "
              f"| {r['lat_p95_avg']:.1f}s / {r['lat_p95_max']:.1f}s | {r['lag_max']:,} | {r['lag_trend']} | {verdict} |")


if __name__ == "__main__":
    main()
