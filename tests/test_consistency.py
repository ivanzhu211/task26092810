"""随机一致性测试：乱序补传、重复点、非法行、穿插清理、非对齐窗口查询。

服务结果与测试内独立的参考实现（从当前保留的原始点从头算）逐项一致。
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

import pytest

from container_metrics import MetricsService
from container_metrics.validation import validate_row

DAY = 86400


def _ts(d: date, seconds: int = 0) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()) + seconds


class Reference:
    """独立参考实现：内存中保存当前保留的原始点，查询时从头算。"""

    def __init__(self) -> None:
        # (host, container, sample_ts) -> (cpu_centi, mem_mb, ingest_date)
        self.points: dict[tuple[str, str, int], tuple[int, int, str]] = {}

    def ingest(self, rows, receive_ts: int):
        ingest_date = datetime.fromtimestamp(receive_ts, tz=timezone.utc).date().isoformat()
        for row in rows:
            valid, _ = validate_row(row, receive_ts)
            if valid is None:
                continue
            key = (valid.host_id, valid.container_id, valid.sample_ts)
            if key not in self.points:  # 只认第一次
                self.points[key] = (valid.cpu_centi, valid.mem_mb, ingest_date)

    def cleanup(self, current_date: date):
        min_date = (current_date - timedelta(days=6)).isoformat()
        self.points = {k: v for k, v in self.points.items() if v[2] >= min_date}

    def query(self, host_id: str, start: int, end: int):
        per_container: dict[str, list[tuple[int, int, int]]] = {}
        for (h, c, ts), (cpu_centi, mem, _d) in self.points.items():
            if h == host_id and start <= ts < end:
                per_container.setdefault(c, []).append((ts, cpu_centi, mem))

        cpu_entries = []
        mem_entries = []
        for cid, pts in per_container.items():
            cnt = len(pts)
            cpu_sum = sum(p[1] for p in pts)
            avg = (Decimal(cpu_sum) / Decimal(100 * cnt)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            cpu_entries.append((cid, cnt, f"{avg:.2f}", Decimal(cpu_sum) / Decimal(cnt)))
            peak = max(p[2] for p in pts)
            peak_ts = min(p[0] for p in pts if p[2] == peak)
            mem_entries.append((cid, peak, peak_ts))

        cpu_entries.sort(key=lambda e: (-e[3], e[0]))
        mem_entries.sort(key=lambda e: (-e[1], e[0]))
        return (
            [(c, n, a) for c, n, a, _ in cpu_entries[:5]],
            [(c, p, t) for c, p, t in mem_entries[:5]],
        )


def _make_row(rng, hosts, containers, receive_ts):
    host = rng.choice(hosts)
    cid = rng.choice(containers)
    # 大部分点靠近接收时间，小部分是几小时到几天前的补传
    lag = rng.choice(
        [rng.randint(0, 3600), rng.randint(0, 86400), rng.randint(0, 604800)]
    )
    sample_ts = receive_ts - lag
    cpu = f"{rng.randint(0, 10000) / 100:.2f}"
    mem = rng.randint(0, 1048576)
    return {"host_id": host, "container_id": cid, "sample_ts": sample_ts,
            "cpu": cpu, "mem": mem}


def _make_bad_row(rng, hosts, containers, receive_ts):
    kind = rng.randint(0, 5)
    row = _make_row(rng, hosts, containers, receive_ts)
    if kind == 0:
        row["host_id"] = ""
    elif kind == 1:
        row["container_id"] = ""
    elif kind == 2:
        row["cpu"] = rng.choice(["100.01", "-1", "1.005", "abc", "1."])
    elif kind == 3:
        row["mem"] = rng.choice([-1, 1048577, 1.5, "100"])
    elif kind == 4:
        row["sample_ts"] = receive_ts - 604801
    else:
        row["sample_ts"] = receive_ts + 301
    return row


def _assert_query_equal(svc, ref, host, start, end):
    result = svc.query_top(host, start, end)
    exp_cpu, exp_mem = ref.query(host, start, end)
    got_cpu = [(e.container_id, e.samples, e.avg) for e in result.cpu_top]
    got_mem = [(e.container_id, e.peak, e.peak_ts) for e in result.mem_top]
    assert got_cpu == exp_cpu, f"cpu top mismatch {host} [{start},{end})"
    assert got_mem == exp_mem, f"mem top mismatch {host} [{start},{end})"


def test_randomized_consistency(tmp_path):
    rng = random.Random(20260928)
    svc = MetricsService(str(tmp_path / "metrics.db"))
    ref = Reference()

    hosts = [f"host{i}" for i in range(3)]
    containers = [f"c{j}" for j in range(6)]
    day0 = date(2026, 9, 1)
    seen_rows: list[dict] = []

    for day_offset in range(10):
        current = day0 + timedelta(days=day_offset)
        for _ in range(4):
            receive_ts = _ts(current, rng.randint(0, DAY - 1))
            batch = []
            for _ in range(rng.randint(1, 120)):
                r = rng.random()
                if r < 0.08 and seen_rows:
                    # 重发旧行（可能改数值），应判重
                    old = dict(rng.choice(seen_rows))
                    if rng.random() < 0.5:
                        old["cpu"] = f"{rng.randint(0, 10000) / 100:.2f}"
                        old["mem"] = rng.randint(0, 1048576)
                    batch.append(old)
                elif r < 0.16:
                    batch.append(_make_bad_row(rng, hosts, containers, receive_ts))
                else:
                    row = _make_row(rng, hosts, containers, receive_ts)
                    seen_rows.append(row)
                    batch.append(row)
            rng.shuffle(batch)
            svc.ingest_batch(batch, receive_ts)
            ref.ingest(batch, receive_ts)

        if rng.random() < 0.6:
            svc.cleanup(current)
            ref.cleanup(current)

        for _ in range(30):
            host = rng.choice(hosts)
            base = _ts(day0) - 604800
            span = _ts(current) + 300 - base
            start = base + rng.randint(0, span - 1)
            length = rng.randint(1, min(86400, span - (start - base)))
            _assert_query_equal(svc, ref, host, start, start + length)

    svc.close()

    # 关闭重开后结果仍一致
    svc = MetricsService(str(tmp_path / "metrics.db"))
    for _ in range(30):
        host = rng.choice(hosts)
        base = _ts(day0) - 604800
        span = _ts(day0 + timedelta(days=9)) + 300 - base
        start = base + rng.randint(0, span - 1)
        length = rng.randint(1, min(86400, span - (start - base)))
        _assert_query_equal(svc, ref, host, start, start + length)
    svc.close()


def test_ingest_result_counts(tmp_path):
    svc = MetricsService(str(tmp_path / "m.db"))
    recv = _ts(date(2026, 9, 28), 3600)
    rows = [
        {"host_id": "h", "container_id": "a", "sample_ts": recv - 10, "cpu": "1", "mem": 1},
        {"host_id": "h", "container_id": "a", "sample_ts": recv - 10, "cpu": "2", "mem": 2},
        {"host_id": "h", "container_id": "", "sample_ts": recv - 10, "cpu": "1", "mem": 1},
        {"host_id": "h", "container_id": "b", "sample_ts": recv - 5, "cpu": "1.005", "mem": 1},
    ]
    res = svc.ingest_batch(rows, recv)
    assert res.accepted == 1
    assert res.duplicates == 1
    assert [r.line_no for r in res.rejected] == [3, 4]
    svc.close()
