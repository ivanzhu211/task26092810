"""对上层采集服务暴露的库接口：批量入库、清理、关联查询。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from fractions import Fraction
from typing import Any, Optional, Sequence

from .store import BUCKET_SECONDS, MetricsStore
from .validation import (
    MAX_BATCH_SIZE,
    Rejection,
    validate_row,
)

RETENTION_DAYS = 7
MAX_WINDOW_SECONDS = 86400
TOP_N = 5


@dataclass(frozen=True)
class IngestResult:
    accepted: int
    duplicates: int
    rejected: list[Rejection]


@dataclass(frozen=True)
class CpuTopEntry:
    container_id: str
    samples: int
    avg: str  # 十进制文本，四舍五入到两位小数（0.005 进位）


@dataclass(frozen=True)
class MemTopEntry:
    container_id: str
    peak: int
    peak_ts: int  # 窗口内首次达到峰值的采样时间


@dataclass(frozen=True)
class QueryResult:
    cpu_top: list[CpuTopEntry]
    mem_top: list[MemTopEntry]


def _ingest_date_of(receive_ts: int) -> str:
    """接收时间对应的 UTC 日期（入库日期）。"""
    return datetime.fromtimestamp(receive_ts, tz=timezone.utc).date().isoformat()


def _format_avg_centi(cpu_sum: int, cnt: int) -> str:
    """平均值四舍五入到两位小数（恰好 0.005 进位），返回十进制文本。

    cpu_sum 是百分之一单位的整数和，cnt 是点数；用整数运算避免浮点误差。
    """
    rounded_centi = (2 * cpu_sum + cnt) // (2 * cnt)
    return f"{rounded_centi // 100}.{rounded_centi % 100:02d}"


class MetricsService:
    """容器指标关联查询服务。db_path 指向本地 SQLite 文件。"""

    def __init__(self, db_path: str) -> None:
        self._store = MetricsStore(db_path)

    def close(self) -> None:
        self._store.close()

    def __enter__(self) -> "MetricsService":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def ingest_batch(self, rows: Sequence[Any], receive_ts: int) -> IngestResult:
        """入库一批（<=500 行）。超过 500 行整批拒收（抛 ValueError）。"""
        if not isinstance(receive_ts, int) or isinstance(receive_ts, bool):
            raise ValueError("receive_ts must be an integer (Unix seconds)")
        if len(rows) > MAX_BATCH_SIZE:
            raise ValueError(
                f"batch too large: {len(rows)} rows > {MAX_BATCH_SIZE}"
            )
        ingest_date = _ingest_date_of(receive_ts)
        accepted = 0
        duplicates = 0
        rejected: list[Rejection] = []
        self._store.begin()
        try:
            for line_no, row in enumerate(rows, start=1):
                valid, reason = validate_row(row, receive_ts)
                if valid is None:
                    rejected.append(Rejection(line_no=line_no, reason=reason))
                    continue
                if self._store.insert_point(valid, ingest_date):
                    accepted += 1
                else:
                    duplicates += 1
            self._store.commit()
        except BaseException:
            self._store.rollback()
            raise
        return IngestResult(accepted=accepted, duplicates=duplicates, rejected=rejected)

    def cleanup(self, current_date: date) -> None:
        """只保留最近 7 个入库日：删除入库日期早于 current_date - 6 天的数据。"""
        min_ingest_date = (current_date - timedelta(days=RETENTION_DAYS - 1)).isoformat()
        self._store.delete_before(min_ingest_date)

    def query_top(
        self, host_id: str, window_start: int, window_end: int
    ) -> QueryResult:
        """查询 [window_start, window_end) 窗口内的 CPU/内存榜单。"""
        if not isinstance(window_start, int) or not isinstance(window_end, int):
            raise ValueError("window bounds must be integers (Unix seconds)")
        window_len = window_end - window_start
        if window_len < 1 or window_len > MAX_WINDOW_SECONDS:
            raise ValueError(
                f"window length must be in [1, {MAX_WINDOW_SECONDS}] seconds"
            )

        bucket = BUCKET_SECONDS
        bucket_lo = ((window_start + bucket - 1) // bucket) * bucket
        has_interior = bucket_lo + bucket <= window_end
        if has_interior:
            bucket_last = ((window_end - bucket) // bucket) * bucket
            head = (window_start, bucket_lo)
            tail = (bucket_last + bucket, window_end)
        else:
            bucket_last = None
            head = (window_start, window_end)
            tail = None

        cpu_sums: dict[str, tuple[int, int]] = {}
        peaks: dict[str, int] = {}
        if has_interior:
            cpu_sums = self._store.rollup_cpu_sums(host_id, bucket_lo, bucket_last)
            peaks = self._store.rollup_peaks(host_id, bucket_lo, bucket_last)

        # 边界不完整桶扫原始点；boundary_peaks 记录 (峰值, 首次达到时间)
        boundary_peaks: dict[str, tuple[int, int]] = {}
        ranges = [head] + ([tail] if tail is not None else [])
        for container_id in self._store.containers_for_host(host_id):
            for ts_lo, ts_hi in ranges:
                if ts_lo >= ts_hi:
                    continue
                for sample_ts, cpu_centi, mem_mb in self._store.raw_range(
                    host_id, container_id, ts_lo, ts_hi
                ):
                    s, c = cpu_sums.get(container_id, (0, 0))
                    cpu_sums[container_id] = (s + cpu_centi, c + 1)
                    prev = boundary_peaks.get(container_id)
                    if prev is None or mem_mb > prev[0]:
                        boundary_peaks[container_id] = (mem_mb, sample_ts)

        cpu_rank: list[tuple[Fraction, str, int, int]] = []  # (精确均值, cid, sum, cnt)
        mem_rank: list[tuple[int, str]] = []  # (峰值, cid)
        final_peak: dict[str, int] = {}
        for container_id, (cpu_sum, cnt) in cpu_sums.items():
            if cnt == 0:
                continue
            cpu_rank.append((Fraction(cpu_sum, cnt), container_id, cpu_sum, cnt))
            rollup_peak = peaks.get(container_id)
            boundary = boundary_peaks.get(container_id)
            candidates = [p for p in (rollup_peak, boundary and boundary[0]) if p is not None]
            peak = max(candidates)
            final_peak[container_id] = peak
            mem_rank.append((peak, container_id))

        cpu_rank.sort(key=lambda item: (-item[0], item[1]))
        mem_rank.sort(key=lambda item: (-item[0], item[1]))

        cpu_top = [
            CpuTopEntry(
                container_id=cid,
                samples=cnt,
                avg=_format_avg_centi(cpu_sum, cnt),
            )
            for _, cid, cpu_sum, cnt in cpu_rank[:TOP_N]
        ]

        mem_top: list[MemTopEntry] = []
        for peak, cid in mem_rank[:TOP_N]:
            first_ts_candidates: list[int] = []
            boundary = boundary_peaks.get(cid)
            if boundary is not None and boundary[0] == peak:
                first_ts_candidates.append(boundary[1])
            if has_interior and peaks.get(cid) == peak:
                ts = self._store.rollup_peak_first_ts(
                    host_id, cid, bucket_lo, bucket_last, peak
                )
                if ts is not None:
                    first_ts_candidates.append(ts)
            mem_top.append(
                MemTopEntry(container_id=cid, peak=peak, peak_ts=min(first_ts_candidates))
            )

        return QueryResult(cpu_top=cpu_top, mem_top=mem_top)
