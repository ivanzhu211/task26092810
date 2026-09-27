# task26092810

## container_metrics：容器指标关联查询库

供上层采集服务直接调用的 Python 库（无接口层），数据持久化在本地 SQLite 文件。

### 用法

```python
from datetime import date
from container_metrics import MetricsService

svc = MetricsService("/data/metrics.db")

# 批量入库（每批 <= 500 行，超过抛 ValueError 整批拒收）
result = svc.ingest_batch(rows, receive_ts)
# rows: [{"host_id", "container_id", "sample_ts", "cpu", "mem"}, ...]
#   cpu 为十进制文本（"37.5"、"0.07"），mem 为整数（MB）
# result.accepted / result.duplicates / result.rejected（含行号 line_no 与原因 reason）

# 关联查询：[start, end)，长度 1..86400 秒，否则 ValueError
q = svc.query_top("host-1", window_start, window_end)
# q.cpu_top: [CpuTopEntry(container_id, samples, avg)]  avg 为两位小数十进制文本
# q.mem_top: [MemTopEntry(container_id, peak, peak_ts)]

# 清理：只保留最近 7 个入库日（入库日 = 接收时间的 UTC 日期）
svc.cleanup(date.today())

svc.close()
```

### 设计

- `raw_points`：原始点，主键 `(host_id, container_id, sample_ts)`，
  `INSERT OR IGNORE` 实现「只认第一次」的去重；清理后重新入库自然生效。
- `rollup`：按 `(host_id, container_id, bucket_start=300s, ingest_date)` 预聚合
  （CPU 和、点数、内存峰值及首次达到时间）。查询时窗口内部的完整桶走预聚合，
  两端不足一桶的边界扫原始点，结果与从原始点重算严格一致。
- 清理按 `ingest_date` 同时删原始点与预聚合行，无过渡期。
- CPU 以百分之一单位的整数存储与求和，平均值用整数运算做 0.005 进位的
  四舍五入，避免浮点误差。

### 测试

```bash
python3 -m pytest tests/ -q
```

- `tests/test_consistency.py`：随机多入库日、乱序补传、重复点、非法行、
  穿插清理、非对齐窗口，与独立参考实现逐项对比。
- `tests/test_rules.py`：跨入库日清理、舍入与排序、窗口/批次/时间阈值边界。
- `tests/test_perf.py`：200 容器 × 24h × 5s（约 346 万点）下批量写入与
  1h/24h 窗口查询耗时、关闭重开后首次查询。
