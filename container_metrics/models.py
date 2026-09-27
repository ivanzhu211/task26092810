"""数据类型定义：上报行、校验结果、查询结果。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MetricRow:
    """一条上报的原始行（调用方构造）。"""

    host_id: object
    container_id: object
    sample_ts: object  # Unix 秒
    cpu: object  # 十进制文本，如 "37.5"
    mem_mb: object  # 整数 MB


@dataclass(frozen=True)
class ValidatedRow:
    """通过校验、等待入库的行。cpu_x100 为 CPU 百分比的 100 倍（整数，保证精确）。"""

    host_id: str
    container_id: str
    sample_ts: int
    cpu_x100: int
    mem_mb: int


@dataclass(frozen=True)
class RejectedRow:
    """被拒收的行。row_index 为批内从 0 开始的行号。"""

    row_index: int
    reason: str


@dataclass(frozen=True)
class IngestResult:
    """一批上报的处理结果。"""

    accepted: int = 0
    duplicates: int = 0
    rejected: tuple[RejectedRow, ...] = ()


@dataclass(frozen=True)
class CpuEntry:
    """CPU 榜条目：container_id、窗口内采样点数、平均值（十进制文本，两位小数）。"""

    container_id: str
    samples: int
    avg_cpu: str


@dataclass(frozen=True)
class MemEntry:
    """内存榜条目：container_id、峰值 MB、窗口内首次达到峰值的采样时间。"""

    container_id: str
    peak_mb: int
    peak_ts: int


@dataclass(frozen=True)
class QueryResult:
    """关联查询结果：CPU 榜与内存榜，各最多 5 条。"""

    cpu_top: tuple[CpuEntry, ...] = ()
    mem_top: tuple[MemEntry, ...] = ()
