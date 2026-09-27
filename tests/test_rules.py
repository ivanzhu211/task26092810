"""业务规则专项测试：跨入库日清理、舍入、排序、窗口与清理边界。"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from container_metrics import MetricsService

DAY = 86400


def _ts(d: date, seconds: int = 0) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()) + seconds


def _row(host, cid, ts, cpu, mem):
    return {"host_id": host, "container_id": cid, "sample_ts": ts, "cpu": cpu, "mem": mem}


def test_adjacent_samples_across_ingest_days(tmp_path):
    """采样时间挨在一起但分属不同入库日；清掉一天后峰值、峰值时间、平均值正确。"""
    svc = MetricsService(str(tmp_path / "m.db"))
    day1 = date(2026, 9, 20)
    day2 = date(2026, 9, 21)
    t = _ts(day1, 40000)
    # 第一天收到 T 和 T+5 的点
    svc.ingest_batch([
        _row("h", "c", t, "10.00", 100),
        _row("h", "c", t + 5, "20.00", 500),
    ], _ts(day1, 50000))
    # 第二天补传夹在中间的 T+1、T+2（峰值在 T+1）
    svc.ingest_batch([
        _row("h", "c", t + 1, "30.00", 900),
        _row("h", "c", t + 2, "40.00", 300),
    ], _ts(day2, 1000))

    q = svc.query_top("h", t, t + 10)
    assert [(e.container_id, e.samples, e.avg) for e in q.cpu_top] == [("c", 4, "25.00")]
    assert [(e.container_id, e.peak, e.peak_ts) for e in q.mem_top] == [("c", 900, t + 1)]

    # 清理到 day2 + 6：day1 的入库日被删，只剩补传的两个点
    svc.cleanup(day2 + timedelta(days=6))
    q = svc.query_top("h", t, t + 10)
    assert [(e.container_id, e.samples, e.avg) for e in q.cpu_top] == [("c", 2, "35.00")]
    assert [(e.container_id, e.peak, e.peak_ts) for e in q.mem_top] == [("c", 900, t + 1)]

    # 被清掉的点不再参与判重：可以重新入库
    res = svc.ingest_batch([_row("h", "c", t, "99.00", 50)], _ts(day2, 2000))
    assert res.accepted == 1
    q = svc.query_top("h", t, t + 10)
    assert [(e.container_id, e.samples, e.avg) for e in q.cpu_top] == [("c", 3, "56.33")]
    svc.close()


def test_cleanup_keeps_exactly_seven_ingest_days(tmp_path):
    svc = MetricsService(str(tmp_path / "m.db"))
    day0 = date(2026, 9, 1)
    for i in range(8):
        d = day0 + timedelta(days=i)
        svc.ingest_batch([_row("h", "c", _ts(d, 100), "1", i + 1)], _ts(d, 200))
    # D = day0+7，保留 D-6..D 即 day1..day7，day0 被删
    svc.cleanup(day0 + timedelta(days=7))
    for i in range(8):
        d = day0 + timedelta(days=i)
        q = svc.query_top("h", _ts(d), _ts(d) + 1000)
        if i == 0:
            assert q.mem_top == []
        else:
            assert q.mem_top[0].peak == i + 1
    svc.close()


def test_cpu_avg_rounding_half_up(tmp_path):
    svc = MetricsService(str(tmp_path / "m.db"))
    recv = _ts(date(2026, 9, 28), 3600)
    cases = [
        ("c1", ["0.00", "0.01"], "0.01"),   # 恰好 0.005 进位
        ("c2", ["0.02", "0.03"], "0.03"),   # 0.025 -> 0.03
        ("c3", ["0.00", "0.00", "0.01"], "0.00"),  # 0.0033 -> 0.00
        ("c4", ["37.5"], "37.50"),
        ("c5", ["100", "100.00"], "100.00"),
        ("c6", ["0.07"], "0.07"),
    ]
    for cid, values, _ in cases:
        svc.ingest_batch(
            [_row("h", cid, recv - 100 + i, v, 1) for i, v in enumerate(values)],
            recv,
        )
    q = svc.query_top("h", recv - 100, recv)
    got = {e.container_id: e.avg for e in q.cpu_top}
    # 只有 5 个名额，c6(0.07) 与 c3(0.00) 排最后，逐个验证用单独查询
    for cid, _, expected in cases:
        single = svc.query_top("h", recv - 100, recv)
    for cid, values, expected in cases:
        svc2 = MetricsService(str(tmp_path / f"one_{cid}.db"))
        svc2.ingest_batch(
            [_row("h", cid, recv - 100 + i, v, 1) for i, v in enumerate(values)], recv
        )
        entry = svc2.query_top("h", recv - 100, recv).cpu_top[0]
        assert entry.avg == expected, (cid, entry.avg, expected)
        assert entry.samples == len(values)
        svc2.close()
    svc.close()


def test_cpu_ranking_exact_avg_then_container_id(tmp_path):
    svc = MetricsService(str(tmp_path / "m.db"))
    recv = _ts(date(2026, 9, 28), 3600)
    # 精确平均值相等 -> 按容器 ID 升序
    svc.ingest_batch([_row("h", "cB", recv - 10, "1.00", 1)], recv)
    svc.ingest_batch([
        _row("h", "cA", recv - 10, "0.50", 1),
        _row("h", "cA", recv - 9, "1.50", 1),
    ], recv)
    # 舍入后相同但精确值不同：cD=1.01 精确大于 cC=1.005
    svc.ingest_batch([
        _row("h", "cC", recv - 10, "1.00", 1),
        _row("h", "cC", recv - 9, "1.01", 1),
    ], recv)
    svc.ingest_batch([_row("h", "cD", recv - 10, "1.01", 1)], recv)
    q = svc.query_top("h", recv - 10, recv)
    assert [(e.container_id, e.avg) for e in q.cpu_top] == [
        ("cD", "1.01"),
        ("cC", "1.01"),  # 精确值 1.005 > 1.00，尽管舍入文本与 cA/cB 不同名
        ("cA", "1.00"),
        ("cB", "1.00"),
    ]
    svc.close()


def test_mem_peak_tie_and_first_peak_ts(tmp_path):
    svc = MetricsService(str(tmp_path / "m.db"))
    recv = _ts(date(2026, 9, 28), 3600)
    svc.ingest_batch([
        _row("h", "cB", recv - 10, "1", 500),
        _row("h", "cA", recv - 8, "1", 500),   # 峰值相同，cA 排前
        _row("h", "cC", recv - 9, "1", 700),   # 峰值在窗口内出现两次，取首次
        _row("h", "cC", recv - 6, "1", 700),
        _row("h", "cC", recv - 7, "1", 100),
    ], recv)
    q = svc.query_top("h", recv - 10, recv)
    assert [(e.container_id, e.peak, e.peak_ts) for e in q.mem_top] == [
        ("cC", 700, recv - 9),
        ("cA", 500, recv - 8),
        ("cB", 500, recv - 10),
    ]
    svc.close()


def test_window_bounds_inclusive_exclusive(tmp_path):
    svc = MetricsService(str(tmp_path / "m.db"))
    recv = _ts(date(2026, 9, 28), 3600)
    svc.ingest_batch([
        _row("h", "c", recv - 10, "10", 100),
        _row("h", "c", recv - 5, "20", 200),
    ], recv)
    # 起点含、终点不含
    q = svc.query_top("h", recv - 10, recv - 5)
    assert q.cpu_top[0].samples == 1 and q.cpu_top[0].avg == "10.00"
    q = svc.query_top("h", recv - 9, recv - 4)
    assert q.cpu_top[0].samples == 1 and q.cpu_top[0].avg == "20.00"
    q = svc.query_top("h", recv - 10, recv - 4)
    assert q.cpu_top[0].samples == 2
    # 空窗口（无采样点）
    q = svc.query_top("h", recv - 4, recv)
    assert q.cpu_top == [] and q.mem_top == []
    svc.close()


def test_window_length_validation(tmp_path):
    svc = MetricsService(str(tmp_path / "m.db"))
    with pytest.raises(ValueError):
        svc.query_top("h", 100, 100)
    with pytest.raises(ValueError):
        svc.query_top("h", 100, 100 - 1)
    with pytest.raises(ValueError):
        svc.query_top("h", 100, 100 + 86401)
    svc.query_top("h", 100, 101)
    svc.query_top("h", 100, 100 + 86400)
    svc.close()


def test_batch_size_limit(tmp_path):
    svc = MetricsService(str(tmp_path / "m.db"))
    recv = _ts(date(2026, 9, 28), 3600)
    svc.ingest_batch([_row("h", "c", recv - i, "1", 1) for i in range(1, 501)], recv)
    with pytest.raises(ValueError):
        svc.ingest_batch([_row("h", "c", recv - i, "1", 1) for i in range(1, 502)], recv)
    svc.close()


def test_sample_time_thresholds(tmp_path):
    svc = MetricsService(str(tmp_path / "m.db"))
    recv = _ts(date(2026, 9, 28), 3600)
    rows = [
        _row("h", "a", recv - 604800, "1", 1),   # 恰好最早，接收
        _row("h", "b", recv - 604801, "1", 1),   # 太早，拒收
        _row("h", "c", recv + 300, "1", 1),      # 恰好最晚，接收
        _row("h", "d", recv + 301, "1", 1),      # 太晚，拒收
    ]
    res = svc.ingest_batch(rows, recv)
    assert res.accepted == 2
    assert [r.line_no for r in res.rejected] == [2, 4]
    svc.close()
