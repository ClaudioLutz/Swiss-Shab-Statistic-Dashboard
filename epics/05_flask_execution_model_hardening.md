# Flask Execution Model Hardening

- Decouple data refresh from request handling (scheduled/CLI job) and make Flask serve only persisted artifacts.
- Add file-level locking around Parquet and plot writes; ensure multi-worker deployments do not race or duplicate work.
- Outcome: deterministic refreshes, safe concurrent access, and clearer separation of web vs. batch concerns.
