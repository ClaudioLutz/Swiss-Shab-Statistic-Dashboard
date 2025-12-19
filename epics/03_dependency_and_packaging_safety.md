# Dependency and Packaging Safety

- Remove the unintended `datetime` PyPI dependency and refresh the lockfile; audit for other shadowing risks.
- Clarify pandas/pyarrow compatibility guidance and keep the `PYARROW_IGNORE_TIMEZONE` fix prominent.
- Outcome: fewer dependency conflicts and clearer import behavior across environments.
