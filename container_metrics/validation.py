"""逐行校验上报的指标行。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping, Optional

MAX_BATCH_SIZE = 500
MAX_CPU = 100
MAX_MEM_MB = 1048576
MAX_SAMPLE_LAG_SECONDS = 604800
MAX_SAMPLE_FUTURE_SECONDS = 300

_CPU_RE = re.compile(r"^(\d+)(?:\.(\d{1,2}))?$")


@dataclass(frozen=True)
class ValidRow:
    """通过校验的一行，数值已规范化。"""

    host_id: str
    container_id: str
    sample_ts: int
    cpu_centi: int  # CPU 百分比 * 100 的整数，避免浮点误差
    mem_mb: int


@dataclass(frozen=True)
class Rejection:
    """被拒收的一行，line_no 为批内行号（从 1 开始）。"""

    line_no: int
    reason: str


def _parse_cpu_centi(value: Any) -> Optional[int]:
    """解析十进制文本 CPU（0..100，最多两位小数）为百分之一单位的整数。"""
    if not isinstance(value, str):
        return None
    match = _CPU_RE.match(value)
    if match is None:
        return None
    int_part, frac_part = match.group(1), match.group(2) or ""
    centi = int(int_part) * 100 + int(frac_part.ljust(2, "0") or "0")
    if centi > MAX_CPU * 100:
        return None
    return centi


def validate_row(row: Any, receive_ts: int) -> tuple[Optional[ValidRow], Optional[str]]:
    """校验单行。通过返回 (ValidRow, None)，否则返回 (None, 原因)。"""
    if not isinstance(row, Mapping):
        return None, "row is not a mapping"

    host_id = row.get("host_id")
    if not isinstance(host_id, str) or host_id == "":
        return None, "host_id must be a non-empty string"

    container_id = row.get("container_id")
    if not isinstance(container_id, str) or container_id == "":
        return None, "container_id must be a non-empty string"

    sample_ts = row.get("sample_ts")
    if not isinstance(sample_ts, int) or isinstance(sample_ts, bool):
        return None, "sample_ts must be an integer"
    if sample_ts < receive_ts - MAX_SAMPLE_LAG_SECONDS:
        return None, "sample_ts too old"
    if sample_ts > receive_ts + MAX_SAMPLE_FUTURE_SECONDS:
        return None, "sample_ts too far in the future"

    cpu_centi = _parse_cpu_centi(row.get("cpu"))
    if cpu_centi is None:
        return None, "cpu must be decimal text in [0, 100] with at most 2 decimals"

    mem_mb = row.get("mem")
    if not isinstance(mem_mb, int) or isinstance(mem_mb, bool):
        return None, "mem must be an integer"
    if mem_mb < 0 or mem_mb > MAX_MEM_MB:
        return None, "mem out of range [0, 1048576]"

    return ValidRow(
        host_id=host_id,
        container_id=container_id,
        sample_ts=sample_ts,
        cpu_centi=cpu_centi,
        mem_mb=mem_mb,
    ), None
