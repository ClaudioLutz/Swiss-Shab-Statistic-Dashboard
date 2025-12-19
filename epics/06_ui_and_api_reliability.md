# UI and API Reliability

- Switch static asset references to `url_for` and tune progress reporting to reflect real phases of work.
- Guard BFS PxWeb enrichment with error handling; allow SHAB plots to render even when BFS is unavailable.
- Normalize canton parsing (explode multi-canton entries) and consider time-aware axes for plots.
- Outcome: correct asset resolution, honest progress UX, and stable endpoints even under upstream failures.
