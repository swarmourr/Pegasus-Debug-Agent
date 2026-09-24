from __future__ import annotations

import re

from Pegasus.healer.models.context import FailureContext
from Pegasus.healer.models.diagnosis import Diagnosis, FailureType

RULE_VERSION = "1.0.0"

# ── Signal sets ───────────────────────────────────────────────────────────────
OOM_EXIT_CODES: set[int] = {137}
OOM_SIGNALS: set[int] = {9}
OOM_HOLD_PATTERNS: list[str] = [r"memory", r"oom", r"out.of.memory", r"mem.?limit"]

DISK_HOLD_PATTERNS: list[str] = [r"disk", r"quota", r"no.space", r"disk.?exceeded"]
DISK_STDERR_PATTERNS: list[str] = [
    r"no\s+space\s+left",
    r"disk.?full",
    r"disk.?quota",
    r"errno\s*28",        # ENOSPC
    r"disk.?exceeded",
    r"diskspace",
    r"exceeded.*disk",
]

WALLTIME_PATTERNS: list[str] = [
    r"wall.?time", r"time.?limit", r"runtime.?exceeded", r"job.?killed.*time",
    r"maxwalltime", r"wall.?clock",
]

TRANSIENT_PATTERNS: list[str] = [
    r"node.?fail", r"network", r"evict", r"preempt",
    r"hold.?reason.*250",   # condor shadow exception / preemption code
    r"disconnected", r"lost.?connection",
]

SCHEDULER_ADMISSION_PATTERNS: list[str] = [
    r"invalid.?queue", r"unknown.?queue", r"unknown.?partition",
    r"qos", r"account", r"request.?exceed", r"too.?many", r"over.?limit",
    r"not.?authorized",
]


# ── Public interface ──────────────────────────────────────────────────────────

def classify(ctx: FailureContext) -> Diagnosis | None:
    """
    Run deterministic rules against the FailureContext.

    Returns a Diagnosis if a rule fires with high confidence.
    Returns None if no rule is confident enough → hand off to LLM agent.

    Rules are evaluated in priority order. The first match wins.
    Each rule records which evidence fields it relied on.
    """
    evidence_ids: list[str] = []

    # ── OUT_OF_MEMORY ─────────────────────────────────────────────────────────
    oom_signals: list[str] = []
    if ctx.exit_code in OOM_EXIT_CODES:
        oom_signals.append("exit_137")
        evidence_ids.append(f"exit_code:{ctx.exit_code}")
    if ctx.termination_signal in OOM_SIGNALS:
        oom_signals.append("signal_9")
        evidence_ids.append(f"signal:{ctx.termination_signal}")
    if _match(ctx.scheduler_reason, OOM_HOLD_PATTERNS):
        oom_signals.append("scheduler_oom_hold")
        evidence_ids.append("scheduler_reason:oom")

    mem_near_limit = _memory_near_limit(ctx)
    if mem_near_limit:
        evidence_ids.append("memory_near_limit")

    # Fire on: ≥2 OOM signals, OR 1 OOM signal + memory near limit,
    # OR exit_137 alone (128+SIGKILL = unambiguous Linux OOM kill code)
    if len(oom_signals) >= 1:
        confidence = 0.97 if (len(oom_signals) >= 2 or mem_near_limit) else 0.90
        return Diagnosis(
            failure_type=FailureType.OUT_OF_MEMORY,
            confidence=confidence,
            evidence_ids=list(evidence_ids),
            explanation=(
                f"OOM signals detected: {', '.join(oom_signals)}. "
                f"Memory near limit: {mem_near_limit}."
            ),
            recommended_fix_category="RESOURCE_MEMORY",
            source="RULE",
            rule_version=RULE_VERSION,
        )

    # ── DISK_EXCEEDED ─────────────────────────────────────────────────────────
    evidence_ids = []
    if _match(ctx.scheduler_reason, DISK_HOLD_PATTERNS):
        evidence_ids.append("scheduler_reason:disk")
    if _disk_near_limit(ctx):
        evidence_ids.append("disk_near_limit")
    if _match(ctx.stderr_excerpt, DISK_STDERR_PATTERNS):
        evidence_ids.append("stderr_disk_error")
    if evidence_ids:
        return Diagnosis(
            failure_type=FailureType.DISK_EXCEEDED,
            confidence=0.95,
            evidence_ids=evidence_ids,
            explanation=f"Disk exceeded signals: {evidence_ids}.",
            recommended_fix_category="RESOURCE_DISK",
            source="RULE",
            rule_version=RULE_VERSION,
        )

    # ── WALLTIME_EXCEEDED ─────────────────────────────────────────────────────
    evidence_ids = []
    if _match(ctx.scheduler_reason, WALLTIME_PATTERNS):
        evidence_ids.append("scheduler_reason:walltime")
    if _runtime_near_limit(ctx):
        evidence_ids.append("runtime_near_limit")
    if evidence_ids:
        return Diagnosis(
            failure_type=FailureType.WALLTIME_EXCEEDED,
            confidence=0.93,
            evidence_ids=evidence_ids,
            explanation="Walltime/runtime limit exceeded.",
            recommended_fix_category="RESOURCE_RUNTIME",
            source="RULE",
            rule_version=RULE_VERSION,
        )

    # ── TRANSIENT_INFRASTRUCTURE ──────────────────────────────────────────────
    if _match(ctx.scheduler_reason, TRANSIENT_PATTERNS):
        return Diagnosis(
            failure_type=FailureType.TRANSIENT_INFRASTRUCTURE,
            confidence=0.90,
            evidence_ids=["scheduler_reason:transient"],
            explanation="Scheduler reason indicates transient infrastructure failure.",
            recommended_fix_category="RETRY_OR_SITE",
            source="RULE",
            rule_version=RULE_VERSION,
        )

    # ── SCHEDULER_ADMISSION ───────────────────────────────────────────────────
    if _match(ctx.scheduler_reason, SCHEDULER_ADMISSION_PATTERNS):
        return Diagnosis(
            failure_type=FailureType.SCHEDULER_ADMISSION,
            confidence=0.92,
            evidence_ids=["scheduler_reason:admission"],
            explanation="Scheduler admission failure (invalid queue, QoS, or account).",
            recommended_fix_category="SCHEDULER_CONFIGURATION",
            source="RULE",
            rule_version=RULE_VERSION,
        )

    # ── MISSING_INPUT ─────────────────────────────────────────────────────────
    failed = [c for c in ctx.input_checks if not c.exists or not c.accessible]
    if failed:
        return Diagnosis(
            failure_type=FailureType.MISSING_INPUT,
            confidence=0.95,
            evidence_ids=[f"input:{c.logical_filename}" for c in failed],
            explanation=f"Missing or inaccessible input files: {[c.logical_filename for c in failed]}.",
            recommended_fix_category="DATA_BINDING",
            source="RULE",
            rule_version=RULE_VERSION,
        )

    # No rule fired → return None → LLM agent
    return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _match(text: str | None, patterns: list[str]) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(re.search(p, t) for p in patterns)


def _memory_near_limit(ctx: FailureContext, threshold: float = 0.85) -> bool:
    peak = ctx.measured_resources.peak_memory_mb
    req = ctx.requested_resources.memory_mb
    return bool(peak and req and peak >= req * threshold)


def _disk_near_limit(ctx: FailureContext, threshold: float = 0.90) -> bool:
    used = ctx.measured_resources.disk_used_mb
    req = ctx.requested_resources.disk_mb
    return bool(used and req and used >= req * threshold)


def _runtime_near_limit(ctx: FailureContext, threshold: float = 0.95) -> bool:
    rt = ctx.measured_resources.runtime_seconds
    req = ctx.requested_resources.runtime_seconds
    return bool(rt and req and rt >= req * threshold)
