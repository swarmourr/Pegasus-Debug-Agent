from __future__ import annotations

"""
LangGraph nodes for the Pegasus remediation two-loop graph.

Each function is a pure node: it receives the current RemediationState,
performs exactly one responsibility, and returns a partial state dict.

Node categories (per spec §4):
  - Data collector   : collect_context
  - Deterministic    : run_rule_classifier, lookup_fix_catalog,
                       validate_policy, apply_fix, authorize_retry,
                       evaluate_outcome, write_memory
  - AI agent         : run_diagnosis_agent, run_fix_planner
  - Data retrieval   : retrieve_memories

Services (llm, policy, retry_controller, etc.) are injected at runtime via
LangGraph's config["configurable"] mechanism.  Each node that needs services
calls _svc() to retrieve the dict.
"""

from typing import Any
from uuid import UUID

import structlog
from langgraph.config import get_config

from Pegasus.healer.collectors.base import ContextCollector
from Pegasus.healer.scheduler.factory import get_scheduler_client
from Pegasus.healer.fixes import lookup as catalog_lookup
from Pegasus.healer.fixes.overlay import build_overlay
from Pegasus.healer.models.context import (
    DiagnosisSummary,
    FixSummary,
    FailureContext,
    PolicySnapshot,
)
from Pegasus.healer.models.diagnosis import Diagnosis
from Pegasus.healer.models.evidence import RawEvidence
from Pegasus.healer.models.fixes import FixOutcome, FixProposal, PolicyDecision
from Pegasus.healer.policies import PolicyEngine
from Pegasus.healer.rules.classifier import classify

log = structlog.get_logger(__name__)


# ── Service accessor ──────────────────────────────────────────────────────────

def _svc() -> dict[str, Any]:
    """Return the services dict injected via config['configurable']."""
    return get_config().get("configurable", {})


# ── Shared helpers ────────────────────────────────────────────────────────────

def _ctx(state: dict[str, Any]) -> FailureContext:
    return FailureContext.model_validate(state["context"])


def _diagnosis(state: dict[str, Any]) -> Diagnosis:
    return Diagnosis.model_validate(state["diagnosis"])


def _proposal(state: dict[str, Any]) -> FixProposal:
    return FixProposal.model_validate(state["proposed_fix"])


# ── Diagnostic loop nodes ─────────────────────────────────────────────────────

async def collect_context(state: dict[str, Any]) -> dict[str, Any]:
    """
    Collect all diagnostic evidence and build a FailureContext.
    Runs before both the rule classifier and (on re-entry) the LLM agent.
    """
    svc = _svc()
    incident_id     = UUID(state["incident_id"])
    job_id          = state["job_id"]
    workflow_id     = state["workflow_id"]
    job_instance_id = state.get("source_job_instance_id", 1)
    exit_code       = state.get("exit_code")
    scheduler_id    = state.get("scheduler_id")

    attempt = state.get("attempt", 1)
    if state.get("retry_outcome"):
        attempt += 1

    previous_diagnoses = [
        DiagnosisSummary.model_validate(d)
        for d in state.get("context", {}).get("previous_diagnoses", [])
    ]
    previous_fixes = [
        FixSummary.model_validate(f)
        for f in state.get("context", {}).get("previous_fixes", [])
    ]

    policy = svc.get("policy")
    policy_snapshot = (
        PolicySnapshot(policy_version=policy.version, rules=policy.as_snapshot_dict())
        if policy else None
    )

    scheduler = svc.get("scheduler_client") or get_scheduler_client()
    ctx = await ContextCollector(scheduler).collect(
        job_id=job_id,
        workflow_id=workflow_id,
        job_instance_id=job_instance_id,
        exit_code=exit_code,
        scheduler_id=scheduler_id,
        incident_id=incident_id,
        attempt_number=attempt,
        previous_diagnoses=previous_diagnoses,
        previous_fixes=previous_fixes,
        policy_snapshot=policy_snapshot,
        submit_dir=svc.get("submit_dir"),
    )

    # Carry raw evidence from services (populated by POST script endpoint)
    # Falls back to empty RawEvidence in AMQP mode
    raw_ev_dict: dict[str, Any] = svc.get("raw_evidence") or {}
    raw_evidence = RawEvidence.model_validate(raw_ev_dict)

    # Populate healer tags from the .sub file so PolicyEngine can enforce them
    if raw_evidence.sub_file_content:
        from Pegasus.healer.collectors.submit_dir import parse_healer_tags
        tags = parse_healer_tags(raw_evidence.sub_file_content)
        if tags:
            ctx = ctx.model_copy(update={"job_tags": tags})

    # Populate transformation from the .sub file so apply_fix can broadcast
    # the fix to sibling jobs that share the same transformation.
    # The scheduler (condor_history) rarely carries the transformation name
    # in POST script mode, so the .sub file is the authoritative source.
    if raw_evidence.sub_file_content and not ctx.transformation:
        from Pegasus.healer.pegasus.sibling_fixer import _parse_transformation_from_content
        _t = _parse_transformation_from_content(raw_evidence.sub_file_content)
        if _t:
            ctx = ctx.model_copy(update={"transformation": _t})

    # Populate stderr_excerpt from raw_evidence so the rule classifier can
    # pattern-match on stderr content (e.g. "No space left on device").
    if raw_evidence.stderr_content and not ctx.stderr_excerpt:
        excerpt = raw_evidence.stderr_content[-3000:]   # tail — errors at bottom
        ctx = ctx.model_copy(update={"stderr_excerpt": excerpt})

    # When raw_evidence is provided (POST script path), always use the current
    # .sub file content for resource requests. The scheduler history (condor_history)
    # records the values from the *completed* job's ClassAd, which is the original
    # value before any healer patch. The .sub file reflects what will actually be
    # submitted on the next retry — we must use it to avoid re-proposing the same fix.
    if raw_evidence.sub_file_content:
        from Pegasus.healer.agents.tools import get_resource_requests
        parsed = get_resource_requests(raw_evidence)
        if any(v is not None for v in parsed.values()):
            ctx = ctx.model_copy(
                update={"requested_resources": ctx.requested_resources.model_copy(
                    update={k: v for k, v in parsed.items() if v is not None
                            and k in ("memory_mb", "disk_mb", "cpus", "runtime_seconds")}
                )}
            )

    updates: dict[str, Any] = {
        "context": ctx.model_dump(mode="json"),
        "raw_evidence": raw_evidence.model_dump(mode="json"),
        "attempt": attempt,
    }
    # Only block on insufficient evidence when raw_evidence is absent.
    # The POST script already collected everything — classads and submit_dir
    # are not additionally required when raw_evidence has sources.
    if not ctx.has_minimum_evidence and not raw_evidence.available_sources:
        updates["insufficient_evidence_fields"] = ctx.missing_evidence
    return updates


async def retrieve_memories(state: dict[str, Any]) -> dict[str, Any]:
    """
    Pull similar past incidents from episodic memory.
    Results are passed to the LLM agent as additional context.
    """
    svc = _svc()
    memory_repo = svc.get("memory_repo")
    if memory_repo is None:
        return {"retrieved_memories": []}

    ctx = _ctx(state)
    memories = await memory_repo.find_similar(
        workflow_id=ctx.workflow_id,
        failure_type=None,  # unknown yet — broad search
        transformation=ctx.transformation,
        project_id=ctx.workflow_id,
        limit=5,
    )
    return {"retrieved_memories": [m for m in memories]}


async def run_rule_classifier(state: dict[str, Any]) -> dict[str, Any]:
    """
    Run deterministic failure rules.  No LLM involved.
    If a rule fires with high confidence, the diagnosis is set directly.
    If not, state signals the graph to route to the LLM agent.
    """
    ctx = _ctx(state)
    diagnosis = classify(ctx)

    if diagnosis is not None:
        log.info(
            "rule_classifier_match",
            failure_type=diagnosis.failure_type,
            confidence=diagnosis.confidence,
            incident_id=state["incident_id"],
        )
        return {"diagnosis": diagnosis.model_dump(mode="json")}

    log.info("rule_classifier_no_match", incident_id=state["incident_id"])
    return {}   # empty → router will send to memory_retrieval then LLM


async def run_diagnosis_agent(state: dict[str, Any]) -> dict[str, Any]:
    """
    LLM-powered ReAct diagnosis for ambiguous failures.
    Only invoked when deterministic rules did not produce a confident result.

    Runs a multi-step Reason→Act→Observe loop using the raw file evidence
    collected by the POST script (or AMQP path if available).

    Uses svc["diagnosis_llm"] when available; falls back to svc["llm"].
    """
    from Pegasus.healer.agents.diagnosis import DiagnosisAgent

    svc = _svc()
    llm = svc.get("diagnosis_llm") or svc["llm"]
    agent = DiagnosisAgent(llm)
    ctx = _ctx(state)
    memories = state.get("retrieved_memories", [])
    raw_evidence = RawEvidence.model_validate(state.get("raw_evidence") or {})

    diagnosis = await agent.run(ctx, raw_evidence, memories)
    log.info(
        "diagnosis_agent_result",
        failure_type=diagnosis.failure_type,
        confidence=diagnosis.confidence,
        steps_used="react_chain",
        incident_id=state["incident_id"],
    )
    return {"diagnosis": diagnosis.model_dump(mode="json")}


# ── Action loop nodes ─────────────────────────────────────────────────────────

async def lookup_fix_catalog(state: dict[str, Any]) -> dict[str, Any]:
    """
    Look up a known fix for the diagnosed failure type.
    Returns a FixProposal if found, or empty dict to route to LLM planner.
    """
    svc = _svc()
    ctx = _ctx(state)
    diagnosis = _diagnosis(state)
    policy = svc["policy"]

    proposal = catalog_lookup(diagnosis, ctx, policy)
    if proposal is not None:
        log.info(
            "fix_catalog_hit",
            action=proposal.action.value,
            incident_id=state["incident_id"],
        )
        return {"proposed_fix": proposal.model_dump(mode="json"), "fix_id": str(proposal.fix_id)}

    log.info("fix_catalog_miss", incident_id=state["incident_id"])
    return {}


async def run_fix_planner(state: dict[str, Any]) -> dict[str, Any]:
    """
    LLM-powered fix planning for complex/unfamiliar failure types.
    Only invoked when the catalog has no match.

    Uses svc["fix_planning_llm"] when available; falls back to svc["llm"].
    """
    from Pegasus.healer.agents.fix_planning import FixPlanningAgent

    svc = _svc()
    llm = svc.get("fix_planning_llm") or svc["llm"]
    agent = FixPlanningAgent(llm)
    ctx = _ctx(state)
    diagnosis = _diagnosis(state)
    raw_evidence = RawEvidence.model_validate(state.get("raw_evidence") or {})

    proposal = await agent.run(ctx, diagnosis, raw_evidence)
    log.info(
        "fix_planner_result",
        action=proposal.action.value,
        confidence=proposal.confidence,
        incident_id=state["incident_id"],
    )
    return {"proposed_fix": proposal.model_dump(mode="json"), "fix_id": str(proposal.fix_id)}


async def validate_policy(state: dict[str, Any]) -> dict[str, Any]:
    """
    Deterministic policy gate.  Runs after every FixProposal (catalog or LLM).
    Deterministic policy ALWAYS overrides LLM output.
    """
    svc = _svc()
    ctx = _ctx(state)
    diagnosis = _diagnosis(state)
    proposal = _proposal(state)

    policy: PolicyEngine = svc["policy_engine"]
    record = policy.validate(proposal, diagnosis, ctx)

    log.info(
        "policy_decision",
        decision=record.decision.value,
        fix_id=record.fix_id,
        incident_id=state["incident_id"],
        checks_failed=record.checks_failed,
    )
    return {
        "policy_decision": record.decision.value,
        "policy_record": record.model_dump(mode="json"),
    }


async def apply_fix(state: dict[str, Any]) -> dict[str, Any]:
    """
    Patch the failing job's .sub file and broadcast the fix to all sibling
    jobs that share the same transformation and original resource configuration.

    Sibling strategy (per HTCondor state):
      IDLE    → patch .sub + condor_qedit  (update ClassAd, no disruption)
      RUNNING → patch .sub only            (retry uses new values if it fails)
      other   → patch .sub only            (not yet submitted by DAGMan)
    """
    svc = _svc()
    proposal = _proposal(state)
    ctx = _ctx(state)
    diagnosis = _diagnosis(state)
    retry_ctrl = svc["retry_controller"]

    # 1. Patch the failing job's .sub file (stores pre-patch snapshot internally)
    overlay = await retry_ctrl.apply_overlay(proposal)
    log.info(
        "fix_applied",
        fix_id=str(proposal.fix_id),
        hash_before=overlay.hash_before,
        hash_after=overlay.hash_after,
        configs_differ=overlay.configs_differ(),
        incident_id=state["incident_id"],
    )

    # 2. Broadcast to siblings with same transformation + same resource config
    if hasattr(retry_ctrl, "broadcast_to_siblings") and ctx.transformation:
        thread_id = svc.get("thread_id", state.get("incident_id", ""))
        siblings_patched = await retry_ctrl.broadcast_to_siblings(
            fix=proposal,
            failure_type=diagnosis.failure_type,
            transformation=ctx.transformation,
            thread_id=thread_id,
        )
        if siblings_patched:
            log.info(
                "siblings_broadcast",
                count=siblings_patched,
                transformation=ctx.transformation,
                failure_type=diagnosis.failure_type,
                incident_id=state["incident_id"],
            )

    # 3. Apply script patches (ASK-path only; guarded by policy before reaching here)
    if proposal.script_patches and hasattr(retry_ctrl, "apply_script_patches"):
        patched = await retry_ctrl.apply_script_patches(proposal)
        log.info(
            "script_patches_applied",
            count=len(patched),
            paths=patched,
            fix_id=str(proposal.fix_id),
            incident_id=state["incident_id"],
        )

    return {"overlay": overlay.model_dump(mode="json")}


async def authorize_retry(state: dict[str, Any]) -> dict[str, Any]:
    """
    Release exactly ONE retry attempt and record its new job_instance_id.
    Acquires distributed lock before acting.
    """
    svc = _svc()
    incident_id = UUID(state["incident_id"])
    fix_id = UUID(state["fix_id"])
    retry_ctrl = svc["retry_controller"]
    redis = svc.get("redis")

    # Distributed lock to prevent concurrent fixes
    if redis:
        lock_key = f"incident_lock:{incident_id}"
        lock = redis.lock(lock_key, timeout=svc.get("lock_ttl", 300))
        async with lock:
            receipt = await retry_ctrl.authorize_retry(incident_id, fix_id)
    else:
        receipt = await retry_ctrl.authorize_retry(incident_id, fix_id)

    log.info(
        "retry_authorized",
        retry_job_instance_id=receipt.retry_job_instance_id,
        fix_id=receipt.fix_id,
        incident_id=state["incident_id"],
    )
    return {"retry_job_instance_id": receipt.retry_job_instance_id}


async def evaluate_outcome(state: dict[str, Any]) -> dict[str, Any]:
    """
    Classify the retry outcome as EFFECTIVE/INEFFECTIVE/PARTIALLY_EFFECTIVE/
    NEW_FAILURE/INCONCLUSIVE.

    The retry_outcome is set by the event ingestor when the matching
    JOB_SUCCEEDED or JOB_FAILED event for retry_job_instance_id arrives.
    This node reads that value and enriches it.
    """
    raw_outcome = state.get("retry_outcome", "INCONCLUSIVE")
    try:
        outcome = FixOutcome(raw_outcome)
    except ValueError:
        outcome = FixOutcome.INCONCLUSIVE

    log.info(
        "outcome_evaluated",
        outcome=outcome.value,
        incident_id=state["incident_id"],
        fix_id=state.get("fix_id"),
    )
    return {"retry_outcome": outcome.value}


async def write_memory(state: dict[str, Any]) -> dict[str, Any]:
    """
    Promote verified episode to durable long-term memory.
    Only called when outcome is EFFECTIVE.
    """
    svc = _svc()
    memory_repo = svc.get("memory_repo")
    if memory_repo is None:
        return {"terminal": True}

    ctx = _ctx(state)
    diagnosis = _diagnosis(state)
    proposal = _proposal(state)

    await memory_repo.store_episode(
        project_id=ctx.workflow_id,
        failure_type=diagnosis.failure_type.value,
        transformation=ctx.transformation,
        execution_site=ctx.execution_site,
        fix_action=proposal.action.value,
        outcome=state.get("retry_outcome", "EFFECTIVE"),
        configuration_delta=proposal.proposed_configuration,
        source_incident_id=ctx.incident_id,
        confidence=diagnosis.confidence,
    )
    log.info("memory_written", incident_id=state["incident_id"])
    return {"terminal": True}


async def generate_proposal_report(state: dict[str, Any]) -> dict[str, Any]:
    """
    Terminal node for ASK decisions.

    The agent diagnosed the failure and has a fix proposal, but cannot apply
    it automatically — either because the current scope level is insufficient
    (e.g. scope=job but fix requires catalog access) or because the failure
    type always requires human action (SCRIPT_ERROR, APPLICATION_ERROR).

    The full fix proposal is already in state (proposed_fix, policy_record).
    The POST script reads it from the final graph state and writes the agent
    report so the human can apply the fix manually.

    Exit code for this path: 0 (DAGMan does NOT retry automatically).
    """
    proposal = _proposal(state)
    diagnosis = _diagnosis(state)
    log.info(
        "proposal_report_generated",
        failure_type=diagnosis.failure_type,
        action=proposal.action.value,
        scope_reason=state.get("policy_record", {}).get("reason", ""),
        incident_id=state["incident_id"],
    )
    return {"terminal": True}


async def handle_insufficient_evidence(state: dict[str, Any]) -> dict[str, Any]:
    """Terminal node when mandatory evidence is missing."""
    log.warning(
        "insufficient_evidence",
        missing=state.get("insufficient_evidence_fields", []),
        incident_id=state["incident_id"],
    )
    return {"terminal": True}


async def escalate(state: dict[str, Any]) -> dict[str, Any]:
    """Terminal node when the system cannot safely proceed."""
    log.warning(
        "incident_escalated",
        incident_id=state["incident_id"],
        policy_decision=state.get("policy_decision"),
        errors=state.get("errors", []),
    )
    return {"terminal": True}
