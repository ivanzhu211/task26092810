"""性能验收：1 台主机 200 个容器连续 24 小时每 5 秒一个点（约 346 万点）。

- 单批 500 行写入 < 200ms
- 1 小时与 24 小时非对齐窗口查询均 < 200ms，且耗时差异不大
- 关闭重开后第一次查询同样达标且结果一致
"""

from __future__ import annotations

import time
from datetime import date, datetime, timezone

import pytest

from container_metrics import MetricsService

HOST = "perf-host"
N_CONTAINERS = 200
INTERVAL = 5
SPAN = 86400
BASE = int(datetime(2026, 9, 28, 0, 0, 0, tzinfo=timezone.utc).timestamp())
N_POINTS = SPAN // INTERVAL  # 每容器 17280 点
BATCH = 500


def _row(seq: int) -> dict:
    container_seq, point_seq = divmod(seq, N_POINTS)
    return {
        "host_id": HOST,
        "container_id": f"c{container_seq:03d}",
        "sample_ts": BASE + point_seq * INTERVAL,
        "cpu": f"{(seq * 37 % 10000) / 100:.2f}",
        "mem": seq * 131 % 1048577,
    }


@pytest.fixture(scope="module")
def loaded_db(tmp_path_factory):
    path = str(tmp_path_factory.mktemp("perf") / "metrics.db")
    svc = MetricsService(path)
    total = N_CONTAINERS * N_POINTS
    worst_batch = 0.0
    start_all = time.perf_counter()
    for batch_start in range(0, total, BATCH):
        rows = [_row(i) for i in range(batch_start, min(batch_start + BATCH, total))]
        t0 = time.perf_counter()
        svc.ingest_batch(rows, BASE + SPAN)
        worst_batch = max(worst_batch, time.perf_counter() - t0)
    ingest_seconds = time.perf_counter() - start_all
    print(f"\ningest {total} points in {ingest_seconds:.1f}s, "
          f"worst 500-row batch {worst_batch * 1000:.1f}ms")
    assert worst_batch < 0.2, f"batch write too slow: {worst_batch * 1000:.1f}ms"
    yield svc, path
    svc.close()


def test_query_1h_and_24h_windows(loaded_db):
    svc, _ = loaded_db
    # 非对齐窗口起点
    start_1h = BASE + 12345
    t0 = time.perf_counter()
    q1 = svc.query_top(HOST, start_1h, start_1h + 3600)
    t_1h = time.perf_counter() - t0

    start_24h = BASE + 12345
    t0 = time.perf_counter()
    q24 = svc.query_top(HOST, start_24h, start_24h + 86400 - 12345)
    t_24h = time.perf_counter() - t0

    print(f"\n1h window {t_1h * 1000:.1f}ms, 24h window {t_24h * 1000:.1f}ms")
    assert t_1h < 0.2, f"1h window too slow: {t_1h * 1000:.1f}ms"
    assert t_24h < 0.2, f"24h window too slow: {t_24h * 1000:.1f}ms"
    assert len(q1.cpu_top) == 5 and len(q1.mem_top) == 5
    assert len(q24.cpu_top) == 5 and len(q24.mem_top) == 5
    # 24h 窗口内每个容器点数 = (86400-12345)/5 取整附近
    assert q24.cpu_top[0].samples > 14000


def test_reopen_first_query(loaded_db):
    svc, path = loaded_db
    start = BASE + 54321
    before = svc.query_top(HOST, start, start + 86400 - 54321)
    svc.close()

    svc2 = MetricsService(path)
    t0 = time.perf_counter()
    after = svc2.query_top(HOST, start, start + 86400 - 54321)
    t_first = time.perf_counter() - t0
    print(f"\nfirst query after reopen {t_first * 1000:.1f}ms")
    assert t_first < 0.2, f"first query after reopen too slow: {t_first * 1000:.1f}ms"
    assert after == before
    svc2.close()
