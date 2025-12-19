# Data Ingestion Robustness and Performance

- Define a fixed schema for empty SHAB days; skip or normalize empty-day Parquet writes safely.
- Reuse a shared requests session across date ranges, strengthen retry policy (status-forcelist, respect `Retry-After`).
- Replace per-iteration concatenation with list accumulation to avoid quadratic cost on multi-year ranges.
- Outcome: resilient ingestion with predictable schema and materially faster range processing.
