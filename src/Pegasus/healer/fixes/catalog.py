from __future__ import annotations

import math
import uuid

from Pegasus.healer.models.context import FailureContext
from Pegasus.healer.models.diagnosis import Diagnosis, FailureType
from Pegasus.healer.models.fixes import FixAction, FixProposal
from Pegasus.healer.policies.loader import PolicyConfig


def lookup(
    diagnosis: Diagnosis,
    ctx: FailureContext,
    policy: PolicyConfig,
) -> FixProposal | None:
    """
    Return a known FixProposal for a known failure type.

    Returns None when the failure is too complex for a catalog entry —
    the caller should then invoke the LLM fix-planning agent.
    """
    ftype = diagnosis.failure_type

    if ftype == FailureType.OUT_OF_MEMORY:
        return _oom_fix(diagnosis, ctx, policy)

    if ftype == FailureType.DISK_EXCEEDED:
        return _disk_fix(diagnosis, ctx, policy)

    if ftype == FailureType.WALLTIME_EXCEEDED:
        return _walltime_fix(diagnosis, ctx, policy)

    if ftype == FailureType.TRANSIENT_INFRASTRUCTURE:
        return _transient_fix(diagnosis, ctx)

    if ftype == FailureType.MISSING_INPUT:
        return _missing_input_fix(diagnosis, ctx)

    # APPLICATION_ERROR, LOGIC_VALIDATION, UNKNOWN → LLM fix planner
    return None


# ── Per-type builders ─────────────────────────────────────────────────────────

def _oom_fix(diagnosis: Diagnosis, ctx: FailureContext, policy: PolicyConfig) -> FixProposal:
    ft = "OUT_OF_MEMORY"
    multiplier = policy.get_multiplier(ft)
    max_mem = policy.get_ceiling(ft, "maximum_memory_mb") or 32768
    current = ctx.requested_resources.memory_mb or 4096
    proposed = min(math.ceil(current * multiplier), max_mem)

    return FixProposal(
        fix_id=uuid.uuid4(),
        incident_id=ctx.incident_id,
        action=FixAction.INCREASE_MEMORY,
        parameters={"multiplier": multiplier, "maximum_memory_mb": max_mem},
        old_configuration={"memory_mb": current},
        proposed_configuration={"memory_mb": proposed},
        justification=(
            f"OOM diagnosis (confidence={diagnosis.confidence:.2f}). "
            f"Increasing memory {current} MB → {proposed} MB (×{multiplier})."
        ),
        evidence_ids=diagnosis.evidence_ids,
        confidence=diagnosis.confidence,
        source="RULE",
        requires_approval=False,
    )


def _disk_fix(diagnosis: Diagnosis, ctx: FailureContext, policy: PolicyConfig) -> FixProposal:
    ft = "DISK_EXCEEDED"
    multiplier = policy.get_multiplier(ft)
    max_disk = policy.get_ceiling(ft, "maximum_disk_mb") or 102400
    current = ctx.requested_resources.disk_mb or 10240
    proposed = min(math.ceil(current * multiplier), max_disk)

    return FixProposal(
        fix_id=uuid.uuid4(),
        incident_id=ctx.incident_id,
        action=FixAction.INCREASE_DISK,
        parameters={"multiplier": multiplier},
        old_configuration={"disk_mb": current},
        proposed_configuration={"disk_mb": proposed},
        justification=(
            f"Disk exceeded (confidence={diagnosis.confidence:.2f}). "
            f"Increasing disk {current} MB → {proposed} MB."
        ),
        evidence_ids=diagnosis.evidence_ids,
        confidence=diagnosis.confidence,
        source="RULE",
        requires_approval=False,
    )


def _walltime_fix(diagnosis: Diagnosis, ctx: FailureContext, policy: PolicyConfig) -> FixProposal:
    ft = "WALLTIME_EXCEEDED"
    multiplier = policy.get_multiplier(ft)
    max_rt = policy.get_ceiling(ft, "maximum_runtime_seconds") or 86400
    current = ctx.requested_resources.runtime_seconds or 3600
    proposed = min(int(current * multiplier), max_rt)

    return FixProposal(
        fix_id=uuid.uuid4(),
        incident_id=ctx.incident_id,
        action=FixAction.INCREASE_RUNTIME,
        parameters={"multiplier": multiplier},
        old_configuration={"runtime_seconds": current},
        proposed_configuration={"runtime_seconds": proposed},
        justification=(
            f"Walltime exceeded (confidence={diagnosis.confidence:.2f}). "
            f"Increasing runtime {current} s → {proposed} s."
        ),
        evidence_ids=diagnosis.evidence_ids,
        confidence=diagnosis.confidence,
        source="RULE",
        requires_approval=True,  # ASK per spec
    )


def _transient_fix(diagnosis: Diagnosis, ctx: FailureContext) -> FixProposal:
    return FixProposal(
        fix_id=uuid.uuid4(),
        incident_id=ctx.incident_id,
        action=FixAction.NO_ACTION_TRANSIENT_RETRY,
        parameters={},
        old_configuration={},
        proposed_configuration={},
        justification=(
            f"Transient infrastructure failure (confidence={diagnosis.confidence:.2f}). "
            "Retrying without configuration change."
        ),
        evidence_ids=diagnosis.evidence_ids,
        confidence=diagnosis.confidence,
        source="RULE",
        requires_approval=False,
    )


def _missing_input_fix(diagnosis: Diagnosis, ctx: FailureContext) -> FixProposal:
    return FixProposal(
        fix_id=uuid.uuid4(),
        incident_id=ctx.incident_id,
        action=FixAction.CORRECT_DATA_BINDING,
        parameters={},
        old_configuration={},
        proposed_configuration={},
        justification=(
            f"Missing input files (confidence={diagnosis.confidence:.2f}). "
            "Manual data binding correction required."
        ),
        evidence_ids=diagnosis.evidence_ids,
        confidence=diagnosis.confidence,
        source="RULE",
        requires_approval=True,  # ASK per spec
    )
