"""Merchant-facing report exports — the platform ends at the egress bucket
(docs/merchant-reporting-design.md): jobs here write files to
`GCS_BUCKET_EGRESS`; delivery to merchants is the backend's problem.
"""
