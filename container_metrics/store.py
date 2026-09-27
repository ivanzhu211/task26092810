"""SQLite 存储与关联查询。

表结构：
- points：原始点，主键 (host_id, container_id, sample_ts)，天然去重；
  ingest_date 为接收时间对应的 UTC 日期（YYYYMMDD 整数），用于 7 天保留清理。
- buckets：按 600 秒对齐的预聚合桶（CPU 和/计数、内存峰值及首达时间），
  写入时同步维护，清理时按受影响桶重算，保证任意时刻查询结果与
  从当前保留原始点重算完全一致。
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone
from fractions import Fraction
from typing import Iterable

from .models import (
    CpuEntry,
    IngestResult,
    MemEntry,
    MetricRow,
    QueryResult,
    RejectedRow,
)
from .validation import validate_row

BUCKET_S = 600  # 预聚合桶宽度（秒）
RETENTION_DAYS = 7  # 保留最近 7 个入库日
MAX_BATCH_ROWS = 500
TOP_N = 5
MAX_WINDOW_S = 86400

_SCHEMA = """
CREATE TABLE IF NOT EXISTS points (
    host_id      TEXT NOT NULL,
    container_id TEXT NOT NULL,
    sample_ts    INTEGER NOT NULL,
    cpu_x100     INTEGER NOT NULL,
    mem_mb       INTEGER NOT NULL,
    ingest_date  INTEGER NOT NULL,  -- 接收时间的 UTC 日期，YYYYMMDD
    bucket_start INTEGER NOT NULL,
    PRIMARY KEY (host_id, container_id, sample_ts)
);
CREATE INDEX IF NOT EXISTS idx_points_host_ts ON points (host_id, sample_ts);
CREATE INDEX IF NOT EXISTS idx_points_ingest_date ON points (ingest_date);

CREATE TABLE IF NOT EXISTS buckets (
    host_id          TEXT NOT NULL,
    container_id     TEXT NOT NULL,
    bucket_start     INTEGER NOT NULL,
    cpu_sum_x100     INTEGER NOT NULL,
    cpu_count        INTEGER NOT NULL,
    mem_max          INTEGER NOT NULL,
    mem_max_first_ts INTEGER NOT NULL,
    PRIMARY KEY (host_id, container_id, bucket_start)
);
CREATE INDEX IF NOT EXISTS idx_buckets_host_ts ON buckets (host_id, bucket_start);
"""

_UPSERT_BUCKET = """
INSERT INTO buckets (host_id, container_id, bucket_start,
                     cpu_sum_x100, cpu_count, mem_max, mem_max_first_ts)
VALUES (?, ?, ?, ?, 1, ?, ?)
ON CONFLICT (host_id, container_id, bucket_start) DO UPDATE SET
    cpu_sum_x100 = cpu_sum_x100 + excluded.cpu_sum_x100,
    cpu_count    = cpu_count + 1,
    mem_max_first_ts = CASE
        WHEN excluded.mem_max > mem_max THEN excluded.mem_max_first_ts
        WHEN excluded.mem_max = mem_max THEN MIN(mem_max_first_ts, excluded.mem_max_first_ts)
        ELSE mem_max_first_ts
    END,
    mem_max = MAX(mem_max, excluded.mem_max)
"""


def _ingest_date_of(received_at: int) -> int:
    """接收时间对应的 UTC 日期，YYYYMMDD 整数。"""
    dt = datetime.fromtimestamp(received_at, tz=timezone.utc)
    return dt.year * 10000 + dt.month * 100 + dt.day


def _date_to_int(day: date) -> int:
    return day.year * 10000 + day.month * 100 + day.day


class MetricsStore:
    """容器指标库。所有调用须在同一进程内串行发生。"""

    def __init__(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA synchronous = NORMAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "MetricsStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def ingest_batch(self, rows: Iterable[MetricRow], received_at: int) -> IngestResult:
        """接收一批上报。超过 500 行整批拒收（抛 ValueError）。"""
        rows = list(rows)
        if len(rows) > MAX_BATCH_ROWS:
            raise ValueError(f"一批最多 {MAX_BATCH_ROWS} 行，整批拒收")
        if isinstance(received_at, bool) or not isinstance(received_at, int):
            raise ValueError("received_at 必须为整数（Unix 秒）")
        ingest_date = _ingest_date_of(received_at)
        accepted = 0
        duplicates = 0
        rejected: list[RejectedRow] = []
        with self._conn:  # 单事务：批内原子提交
            for index, row in enumerate(rows):
                try:
                    valid = validate_row(row, received_at)
                except ValueError as exc:
                    rejected.append(RejectedRow(row_index=index, reason=str(exc)))
                    continue
                bucket_start = valid.sample_ts - valid.sample_ts % BUCKET_S
                cursor = self._conn.execute(
                    "INSERT OR IGNORE INTO points"
                    " (host_id, container_id, sample_ts, cpu_x100, mem_mb,"
                    "  ingest_date, bucket_start)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        valid.host_id,
                        valid.container_id,
                        valid.sample_ts,
                        valid.cpu_x100,
                        valid.mem_mb,
                        ingest_date,
                        bucket_start,
                    ),
                )
                if cursor.rowcount == 0:
                    duplicates += 1  # 同主机同容器同采样时间：只认第一次
                    continue
                accepted += 1
                self._conn.execute(
                    _UPSERT_BUCKET,
                    (
                        valid.host_id,
                        valid.container_id,
                        bucket_start,
                        valid.cpu_x100,
                        valid.mem_mb,
                        valid.sample_ts,
                    ),
                )
        return IngestResult(
            accepted=accepted, duplicates=duplicates, rejected=tuple(rejected)
        )

    def cleanup(self, today: date) -> int:
        """删除入库日期早于 today-6 天的全部数据，返回删除的点数。"""
        if not isinstance(today, date):
            raise ValueError("today 必须为 datetime.date")
        threshold = _date_to_int(today - timedelta(days=RETENTION_DAYS - 1))
        with self._conn:
            self._conn.execute(
                "CREATE TEMPORARY TABLE IF NOT EXISTS affected_buckets ("
                " host_id TEXT NOT NULL, container_id TEXT NOT NULL,"
                " bucket_start INTEGER NOT NULL,"
                " PRIMARY KEY (host_id, container_id, bucket_start))"
            )
            self._conn.execute("DELETE FROM affected_buckets")
            self._conn.execute(
                "INSERT INTO affected_buckets"
                " SELECT DISTINCT host_id, container_id, bucket_start"
                " FROM points WHERE ingest_date < ?",
                (threshold,),
            )
            deleted = self._conn.execute(
                "DELETE FROM points WHERE ingest_date < ?", (threshold,)
            ).rowcount
            if deleted:
                self._conn.execute(
                    "DELETE FROM buckets WHERE (host_id, container_id, bucket_start)"
                    " IN (SELECT host_id, container_id, bucket_start"
                    "     FROM affected_buckets)"
                )
                # 受影响桶从剩余原始点重算，保证与从头算一致
                self._conn.execute(
                    "INSERT INTO buckets"
                    " SELECT p.host_id, p.container_id, a.bucket_start,"
                    "        SUM(p.cpu_x100), COUNT(*), MAX(p.mem_mb), -1"
                    " FROM affected_buckets a JOIN points p"
                    "   ON p.host_id = a.host_id AND p.container_id = a.container_id"
                    "  AND p.sample_ts >= a.bucket_start"
                    "  AND p.sample_ts < a.bucket_start + ?"
                    " GROUP BY p.host_id, p.container_id, a.bucket_start",
                    (BUCKET_S,),
                )
                self._conn.execute(
                    "UPDATE buckets SET mem_max_first_ts = ("
                    " SELECT MIN(p.sample_ts) FROM points p"
                    "  WHERE p.host_id = buckets.host_id"
                    "    AND p.container_id = buckets.container_id"
                    "    AND p.sample_ts >= buckets.bucket_start"
                    "    AND p.sample_ts < buckets.bucket_start + ?"
                    "    AND p.mem_mb = buckets.mem_max)"
                    " WHERE mem_max_first_ts = -1",
                    (BUCKET_S,),
                )
            self._conn.execute("DELETE FROM affected_buckets")
        return deleted

    def query(self, host_id: str, start_ts: int, end_ts: int) -> QueryResult:
        """关联查询：[start_ts, end_ts)，窗口长度须在 1..86400 秒。"""
        for name, value in (("start_ts", start_ts), ("end_ts", end_ts)):
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"{name} 必须为整数（Unix 秒）")
        window = end_ts - start_ts
        if not 1 <= window <= MAX_WINDOW_S:
            raise ValueError(f"窗口长度须在 1 到 {MAX_WINDOW_S} 秒之间")

        # 整桶区间 [first_full, tail_start) 走预聚合；两端零头走原始点。
        first_full = start_ts if start_ts % BUCKET_S == 0 else (
            (start_ts // BUCKET_S) + 1
        ) * BUCKET_S
        tail_start = (end_ts // BUCKET_S) * BUCKET_S
        head_end = min(first_full, end_ts)

        # container_id -> [cpu_sum_x100, cpu_count, mem_peak, mem_peak_ts]
        stats: dict[str, list[int]] = {}

        def add(container_id: str, cpu_x100: int, mem_mb: int, ts: int) -> None:
            entry = stats.get(container_id)
            if entry is None:
                stats[container_id] = [cpu_x100, 1, mem_mb, ts]
                return
            entry[0] += cpu_x100
            entry[1] += 1
            if mem_mb > entry[2]:
                entry[2] = mem_mb
                entry[3] = ts
            elif mem_mb == entry[2] and ts < entry[3]:
                entry[3] = ts

        if first_full < tail_start:
            for container_id, cpu_sum, cpu_cnt, mem_max, mem_ts in self._conn.execute(
                "SELECT container_id, cpu_sum_x100, cpu_count, mem_max,"
                " mem_max_first_ts FROM buckets"
                " WHERE host_id = ? AND bucket_start >= ? AND bucket_start < ?",
                (host_id, first_full, tail_start),
            ):
                entry = stats.get(container_id)
                if entry is None:
                    stats[container_id] = [cpu_sum, cpu_cnt, mem_max, mem_ts]
                else:
                    entry[0] += cpu_sum
                    entry[1] += cpu_cnt
                    if mem_max > entry[2]:
                        entry[2] = mem_max
                        entry[3] = mem_ts
                    elif mem_max == entry[2] and mem_ts < entry[3]:
                        entry[3] = mem_ts

        raw_ranges = [(start_ts, head_end)]
        if tail_start >= head_end and tail_start < end_ts:
            raw_ranges.append((tail_start, end_ts))
        for range_start, range_end in raw_ranges:
            if range_start >= range_end:
                continue
            for container_id, cpu_x100, mem_mb, ts in self._conn.execute(
                "SELECT container_id, cpu_x100, mem_mb, sample_ts FROM points"
                " WHERE host_id = ? AND sample_ts >= ? AND sample_ts < ?",
                (host_id, range_start, range_end),
            ):
                add(container_id, cpu_x100, mem_mb, ts)

        cpu_ranked = sorted(
            stats.items(),
            key=lambda item: (Fraction(-item[1][0], item[1][1]), item[0]),
        )[:TOP_N]
        mem_ranked = sorted(
            stats.items(), key=lambda item: (-item[1][2], item[0])
        )[:TOP_N]

        cpu_top = tuple(
            CpuEntry(
                container_id=cid,
                samples=entry[1],
                avg_cpu=_format_avg(entry[0], entry[1]),
            )
            for cid, entry in cpu_ranked
        )
        mem_top = tuple(
            MemEntry(container_id=cid, peak_mb=entry[2], peak_ts=entry[3])
            for cid, entry in mem_ranked
        )
        return QueryResult(cpu_top=cpu_top, mem_top=mem_top)


def _format_avg(cpu_sum_x100: int, count: int) -> str:
    """精确平均值四舍五入到两位小数（0.005 进位），返回十进制文本。"""
    hundredths = (2 * cpu_sum_x100 + count) // (2 * count)
    return f"{hundredths // 100}.{hundredths % 100:02d}"
