"""逐行校验：把原始上报行转成 ValidatedRow，或给出拒收原因。"""

from __future__ import annotations

import re

from .models import MetricRow, ValidatedRow

# 采样时间允许偏离接收时间的范围（秒）
MAX_SKEW_PAST_S = 604800  # 最多早 7 天
MAX_SKEW_FUTURE_S = 300  # 最多晚 5 分钟

MAX_MEM_MB = 1048576

_CPU_RE = re.compile(r"^(\d{1,3})(?:\.(\d{1,2}))?$")


def _parse_cpu_x100(cpu: object) -> int:
    """把十进制文本 CPU 解析为百分数*100 的整数，非法时抛 ValueError。"""
    if not isinstance(cpu, str):
        raise ValueError("cpu 必须为十进制文本")
    match = _CPU_RE.match(cpu)
    if match is None:
        raise ValueError("cpu 须为 0 到 100 之间、最多两位小数的十进制文本")
    int_part = int(match.group(1))
    frac = match.group(2) or ""
    cpu_x100 = int_part * 100 + int(frac.ljust(2, "0"))
    if cpu_x100 > 10000:
        raise ValueError("cpu 须在 0 到 100 之间（含两端）")
    return cpu_x100


def _require_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} 必须为整数")
    return value


def validate_row(row: MetricRow, received_at: int) -> ValidatedRow:
    """校验单行，不合法时抛出 ValueError（消息为拒收原因）。"""
    host_id = row.host_id
    if not isinstance(host_id, str) or host_id == "":
        raise ValueError("host_id 不能为空")
    container_id = row.container_id
    if not isinstance(container_id, str) or container_id == "":
        raise ValueError("container_id 不能为空")
    sample_ts = _require_int(row.sample_ts, "sample_ts")
    mem_mb = _require_int(row.mem_mb, "mem_mb")
    if not 0 <= mem_mb <= MAX_MEM_MB:
        raise ValueError(f"mem_mb 须在 0 到 {MAX_MEM_MB} 之间")
    cpu_x100 = _parse_cpu_x100(row.cpu)
    if received_at - sample_ts > MAX_SKEW_PAST_S:
        raise ValueError("sample_ts 比接收时间早超过 604800 秒")
    if sample_ts - received_at > MAX_SKEW_FUTURE_S:
        raise ValueError("sample_ts 比接收时间晚超过 300 秒")
    return ValidatedRow(
        host_id=host_id,
        container_id=container_id,
        sample_ts=sample_ts,
        cpu_x100=cpu_x100,
        mem_mb=mem_mb,
    )
