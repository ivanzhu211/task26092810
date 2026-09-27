"""SQLite 存储层：原始点表 + 按入库日切分的桶级预聚合表。

设计要点：
- raw_points 以 (host_id, container_id, sample_ts) 为主键，INSERT OR IGNORE 天然去重，
  且支持按容器做采样时间范围扫描（窗口边界桶用）。
- rollup 按 (host_id, container_id, bucket_start, ingest_date) 预聚合，
  窗口内部的完整桶直接聚合，清理时按 ingest_date 整行删除，与原始点保持一致。
- containers 是 (host_id, container_id) 的候选登记表，仅供查询枚举容器用。
"""

from __future__ import annotations

import sqlite3
from typing import Optional

from .validation import ValidRow

BUCKET_SECONDS = 300

_SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_points (
    host_id      TEXT NOT NULL,
    container_id TEXT NOT NULL,
    sample_ts    INTEGER NOT NULL,
    cpu_centi    INTEGER NOT NULL,
    mem_mb       INTEGER NOT NULL,
    ingest_date  TEXT NOT NULL,
    PRIMARY KEY (host_id, container_id, sample_ts)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_raw_ingest ON raw_points (ingest_date);

CREATE TABLE IF NOT EXISTS rollup (
    host_id      TEXT NOT NULL,
    container_id TEXT NOT NULL,
    bucket_start INTEGER NOT NULL,
    ingest_date  TEXT NOT NULL,
    cpu_sum      INTEGER NOT NULL,
    cnt          INTEGER NOT NULL,
    mem_max      INTEGER NOT NULL,
    mem_max_ts   INTEGER NOT NULL,
    PRIMARY KEY (host_id, container_id, bucket_start, ingest_date)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_rollup_ingest ON rollup (ingest_date);

CREATE TABLE IF NOT EXISTS containers (
    host_id      TEXT NOT NULL,
    container_id TEXT NOT NULL,
    PRIMARY KEY (host_id, container_id)
) WITHOUT ROWID;
"""

_INSERT_RAW = (
    "INSERT OR IGNORE INTO raw_points"
    " (host_id, container_id, sample_ts, cpu_centi, mem_mb, ingest_date)"
    " VALUES (?, ?, ?, ?, ?, ?)"
)

_UPSERT_ROLLUP = """
INSERT INTO rollup (host_id, container_id, bucket_start, ingest_date,
                    cpu_sum, cnt, mem_max, mem_max_ts)
VALUES (?, ?, ?, ?, ?, 1, ?, ?)
ON CONFLICT (host_id, container_id, bucket_start, ingest_date) DO UPDATE SET
    cpu_sum = cpu_sum + excluded.cpu_sum,
    cnt = cnt + 1,
    mem_max_ts = CASE
        WHEN excluded.mem_max > mem_max THEN excluded.mem_max_ts
        WHEN excluded.mem_max = mem_max AND excluded.mem_max_ts < mem_max_ts
            THEN excluded.mem_max_ts
        ELSE mem_max_ts END,
    mem_max = MAX(mem_max, excluded.mem_max)
"""

_INSERT_CONTAINER = "INSERT OR IGNORE INTO containers (host_id, container_id) VALUES (?, ?)"


class MetricsStore:
    """封装 SQLite 连接与表结构。所有调用假设在同一进程内串行发生。"""

    def __init__(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "MetricsStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def begin(self) -> None:
        self._conn.execute("BEGIN")

    def commit(self) -> None:
        self._conn.execute("COMMIT")

    def rollback(self) -> None:
        self._conn.execute("ROLLBACK")

    def insert_point(self, row: ValidRow, ingest_date: str) -> bool:
        """插入一个原始点并维护预聚合。返回 True 表示新点，False 表示重复被忽略。

        调用方需已用 begin() 开启事务。
        """
        cur = self._conn.execute(
            _INSERT_RAW,
            (row.host_id, row.container_id, row.sample_ts,
             row.cpu_centi, row.mem_mb, ingest_date),
        )
        if cur.rowcount == 0:
            return False
        bucket_start = (row.sample_ts // BUCKET_SECONDS) * BUCKET_SECONDS
        self._conn.execute(
            _UPSERT_ROLLUP,
            (row.host_id, row.container_id, bucket_start, ingest_date,
             row.cpu_centi, row.mem_mb, row.sample_ts),
        )
        self._conn.execute(_INSERT_CONTAINER, (row.host_id, row.container_id))
        return True

    def delete_before(self, min_ingest_date: str) -> None:
        """删除入库日期早于 min_ingest_date 的全部数据（含预聚合）。"""
        self._conn.execute("BEGIN")
        try:
            self._conn.execute("DELETE FROM raw_points WHERE ingest_date < ?", (min_ingest_date,))
            self._conn.execute("DELETE FROM rollup WHERE ingest_date < ?", (min_ingest_date,))
            self._conn.execute("COMMIT")
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise

    def containers_for_host(self, host_id: str) -> list[str]:
        """返回该主机已知的全部容器 ID（候选集合）。"""
        cur = self._conn.execute(
            "SELECT container_id FROM containers WHERE host_id = ?", (host_id,)
        )
        return [r[0] for r in cur]

    def rollup_cpu_sums(
        self, host_id: str, bucket_lo: int, bucket_last: int
    ) -> dict[str, tuple[int, int]]:
        """窗口内部完整桶的 CPU 合计：container_id -> (cpu_sum, cnt)。"""
        cur = self._conn.execute(
            "SELECT container_id, SUM(cpu_sum), SUM(cnt) FROM rollup"
            " WHERE host_id = ? AND bucket_start >= ? AND bucket_start <= ?"
            " GROUP BY container_id",
            (host_id, bucket_lo, bucket_last),
        )
        return {cid: (s, c) for cid, s, c in cur}

    def rollup_peaks(
        self, host_id: str, bucket_lo: int, bucket_last: int
    ) -> dict[str, int]:
        """窗口内部完整桶的内存峰值：container_id -> mem_max。"""
        cur = self._conn.execute(
            "SELECT container_id, MAX(mem_max) FROM rollup"
            " WHERE host_id = ? AND bucket_start >= ? AND bucket_start <= ?"
            " GROUP BY container_id",
            (host_id, bucket_lo, bucket_last),
        )
        return {cid: peak for cid, peak in cur}

    def rollup_peak_first_ts(
        self, host_id: str, container_id: str,
        bucket_lo: int, bucket_last: int, peak: int,
    ) -> Optional[int]:
        """完整桶内达到指定峰值的最早采样时间。"""
        cur = self._conn.execute(
            "SELECT MIN(mem_max_ts) FROM rollup"
            " WHERE host_id = ? AND container_id = ?"
            " AND bucket_start >= ? AND bucket_start <= ? AND mem_max = ?",
            (host_id, container_id, bucket_lo, bucket_last, peak),
        )
        return cur.fetchone()[0]

    def raw_range(
        self, host_id: str, container_id: str, ts_lo: int, ts_hi: int
    ) -> list[tuple[int, int, int]]:
        """[ts_lo, ts_hi) 内的原始点：(sample_ts, cpu_centi, mem_mb)，按时间升序。"""
        cur = self._conn.execute(
            "SELECT sample_ts, cpu_centi, mem_mb FROM raw_points"
            " WHERE host_id = ? AND container_id = ?"
            " AND sample_ts >= ? AND sample_ts < ?"
            " ORDER BY sample_ts",
            (host_id, container_id, ts_lo, ts_hi),
        )
        return cur.fetchall()
