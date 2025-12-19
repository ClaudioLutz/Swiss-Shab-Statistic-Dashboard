# Repository Hygiene and Baseline Guardrails

- Remove committed run artifacts (e.g., `jules_session_*`) and prevent recurrence via `.gitignore`.
- Align project tree so only authoritative source files remain; document expected workspace layout.
- Outcome: clean working tree, reduced reviewer confusion, fewer accidental imports of stale code.
