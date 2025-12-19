# Proposed Epics

## 1) Repository Hygiene and Baseline Guardrails
- Remove committed run artifacts (e.g., `jules_session_*`) and prevent recurrence via `.gitignore`.
- Align project tree so only authoritative source files remain; document expected workspace layout.
- Outcome: clean working tree, reduced reviewer confusion, fewer accidental imports of stale code.

## 2) Documentation and Runtime Alignment
- Update README to match actual persistence (Parquet, not pickle) and the Python version required by `Pipfile`.
- Add a short “supported runtimes” section that clarifies local/dev expectations vs. production deployment.
- Outcome: contributors install the correct toolchain and understand the real storage layer.

## 3) Dependency and Packaging Safety
- Remove the unintended `datetime` PyPI dependency and refresh the lockfile; audit for other shadowing risks.
- Clarify pandas/pyarrow compatibility guidance and keep the `PYARROW_IGNORE_TIMEZONE` fix prominent.
- Outcome: fewer dependency conflicts and clearer import behavior across environments.

## 4) Data Ingestion Robustness and Performance
- Define a fixed schema for empty SHAB days; skip or normalize empty-day Parquet writes safely.
- Reuse a shared requests session across date ranges, strengthen retry policy (status-forcelist, respect `Retry-After`).
- Replace per-iteration concatenation with list accumulation to avoid quadratic cost on multi-year ranges.
- Outcome: resilient ingestion with predictable schema and materially faster range processing.

## 5) Flask Execution Model Hardening
- Decouple data refresh from request handling (scheduled/CLI job) and make Flask serve only persisted artifacts.
- Add file-level locking around Parquet and plot writes; ensure multi-worker deployments do not race or duplicate work.
- Outcome: deterministic refreshes, safe concurrent access, and clearer separation of web vs. batch concerns.

## 6) UI and API Reliability
- Switch static asset references to `url_for` and tune progress reporting to reflect real phases of work.
- Guard BFS PxWeb enrichment with error handling; allow SHAB plots to render even when BFS is unavailable.
- Normalize canton parsing (explode multi-canton entries) and consider time-aware axes for plots.
- Outcome: correct asset resolution, honest progress UX, and stable endpoints even under upstream failures.
