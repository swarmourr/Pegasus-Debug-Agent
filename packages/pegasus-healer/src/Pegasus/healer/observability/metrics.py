from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ── Counters ──────────────────────────────────────────────────────────────────
events_consumed_total = Counter(
    "pegasus_events_consumed_total",
    "Events successfully normalised and stored",
    ["event_type"],
)
duplicate_events_total = Counter(
    "pegasus_duplicate_events_total",
    "Idempotently rejected event deliveries",
)
incidents_created_total = Counter(
    "pegasus_incidents_created_total",
    "New incidents opened",
)
fixes_by_decision_total = Counter(
    "pegasus_fixes_by_decision_total",
    "Fix proposals by policy decision",
    ["decision", "failure_type"],
)
retry_authorizations_total = Counter(
    "pegasus_retry_authorizations_total",
    "Authorised retry attempts by fix action",
    ["action"],
)
fix_outcomes_total = Counter(
    "pegasus_fix_outcomes_total",
    "Fix outcome classifications",
    ["outcome", "failure_type"],
)
retry_limit_reached_total = Counter(
    "pegasus_retry_limit_reached_total",
    "Convergence safety activations (max attempts reached)",
)

# ── Gauges ────────────────────────────────────────────────────────────────────
incidents_open = Gauge(
    "pegasus_incidents_open",
    "Currently active remediation cases",
)

# ── Histograms ────────────────────────────────────────────────────────────────
diagnosis_latency_seconds = Histogram(
    "pegasus_diagnosis_latency_seconds",
    "Time from failure event receipt to completed diagnosis",
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60],
)
diagnosis_confidence = Histogram(
    "pegasus_diagnosis_confidence",
    "Diagnosis confidence score distribution",
    ["failure_type", "source"],
    buckets=[0.0, 0.5, 0.7, 0.8, 0.9, 0.95, 1.0],
)
