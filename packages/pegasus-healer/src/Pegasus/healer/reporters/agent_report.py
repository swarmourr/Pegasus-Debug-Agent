from __future__ import annotations

"""
Agent remediation report writer.

Generates a human-readable incident report and writes it to the Pegasus
submit directory as {job_id}.agent_report after every diagnosis attempt.

The report is always written — for AUTO (audit trail), ASK (approval
instructions), STOP, and ESCALATE (manual investigation guidance).
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_W = 72   # report width

_DECISION_HEADER = {
    "RETRY":    "AUTO — Fix applied automatically",
    "ASK":      "ASK — Human approval required before applying fix",
    "STOP":     "STOP — Terminal failure, no automatic recovery",
    "ESCALATE": "ESCALATE — Agent could not determine root cause",
}

_DECISION_ICON = {
    "RETRY":    "[OK]",
    "ASK":      "[!] ",
    "STOP":     "[X] ",
    "ESCALATE": "[!] ",
}


# ── Formatting helpers ─────────────────────────────────────────────────────────

def _line(char: str = "─") -> str:
    return char * _W

def _header(title: str, char: str = "─") -> str:
    pad = (_W - len(title) - 2) // 2
    return f"{char * pad} {title} {char * (pad + (_W - len(title) - 2) % 2)}"

def _wrap(text: str, indent: int = 2) -> str:
    """Wrap long text at word boundaries."""
    prefix = " " * indent
    words = text.split()
    lines: list[str] = []
    current = prefix
    for word in words:
        if len(current) + len(word) + 1 > _W:
            lines.append(current.rstrip())
            current = prefix + word + " "
        else:
            current += word + " "
    if current.strip():
        lines.append(current.rstrip())
    return "\n".join(lines)


# ── Section builders ───────────────────────────────────────────────────────────

def _section_header(
    job_id: str,
    workflow_id: str,
    submit_dir: str,
    incident_id: str,
    attempt: int,
    max_retries: int,
    exit_code: int,
    generated_at: str,
) -> list[str]:
    return [
        "=" * _W,
        " PEGASUS AGENT REMEDIATION REPORT",
        "=" * _W,
        "",
        f"  Generated   : {generated_at}",
        f"  Incident ID : {incident_id}",
        f"  Job         : {job_id}",
        f"  Workflow    : {workflow_id}",
        f"  Attempt     : {attempt} of {max_retries}",
        f"  Exit code   : {exit_code}",
        f"  Submit dir  : {submit_dir}",
        "",
    ]


def _section_diagnosis(
    failure_type: str | None,
    confidence: float | None,
    explanation: str | None,
    evidence_sources: list[str],
) -> list[str]:
    out = [_header("DIAGNOSIS"), ""]

    if failure_type:
        conf_str = f"  [{int(confidence * 100)}% confidence]" if confidence is not None else ""
        out.append(f"  Failure type  :  {failure_type}{conf_str}")
    else:
        out.append("  Failure type  :  UNKNOWN")

    out.append("")

    if explanation:
        out.append(_wrap(explanation, indent=2))
        out.append("")

    if evidence_sources:
        out.append("  Evidence consulted:")
        for src in evidence_sources:
            out.append(f"    • {src}")
        out.append("")

    return out


def _section_fix(fix_proposed: dict[str, Any] | None) -> list[str]:
    if not fix_proposed:
        return []

    out = [_header("FIX PROPOSAL"), ""]
    action = fix_proposed.get("action", "UNKNOWN")
    out.append(f"  Action  :  {action}")

    config = fix_proposed.get("proposed_configuration") or {}
    old_config = fix_proposed.get("old_configuration") or {}

    if config:
        out.append("  Changes:")
        for key, new_val in config.items():
            old_val = old_config.get(key, "—")
            if old_val != "—" and old_val != new_val:
                out.append(f"    {key:30s}  {old_val}  →  {new_val}")
            else:
                out.append(f"    {key:30s}  {new_val}")

    out.append("")
    return out


def _section_decision(
    decision: str,
    failure_type: str | None,
    incident_id: str,
    fix_proposed: dict[str, Any] | None,
    approval_url: str | None,
    service_url: str,
    submit_dir: str,
    missing_evidence: list[str],
) -> list[str]:
    icon = _DECISION_ICON.get(decision, "    ")
    label = _DECISION_HEADER.get(decision, decision)
    out = [_header("DECISION"), "", f"  {icon}  {label}", ""]

    if decision == "RETRY":
        out += [
            "  The .sub file was patched. DAGMan will retry the job with the",
            "  new resource allocation.",
            "",
        ]

    elif decision == "ASK":
        out += [
            "  The fix has cost or policy implications and requires explicit",
            "  approval before being applied. The job will NOT be retried",
            "  automatically.",
            "",
        ]
        if fix_proposed:
            config = fix_proposed.get("proposed_configuration") or {}
            old_config = fix_proposed.get("old_configuration") or {}
            if config:
                out.append("  Proposed changes:")
                for key, new_val in config.items():
                    old_val = old_config.get(key, "—")
                    out.append(f"    {key} = {old_val}  →  {new_val}")
                out.append("")

        fix_id = fix_proposed.get("fix_id", "") if fix_proposed else ""
        url = f"{service_url}{approval_url}" if approval_url else f"{service_url}/incidents/{incident_id}/approve"
        out += [
            "  To approve:",
            f"    curl -X POST {url} \\",
            "         -H 'Content-Type: application/json' \\",
            f"         -d '{{\"fix_id\": \"{fix_id}\", \"decision\": \"APPROVE\", \"actor\": \"you\"}}'",
            "",
            "  After approval, re-submit the job:",
            f"    pegasus-run {submit_dir}",
            "",
        ]

    elif decision == "STOP":
        out += [
            f"  Policy prohibits automatic retry for {failure_type or 'this failure type'}.",
            "  The underlying issue must be resolved manually.",
            "",
        ]

    elif decision == "ESCALATE":
        out += [
            "  The failure could not be diagnosed with sufficient confidence.",
            "  This incident has been flagged for manual investigation.",
            "",
        ]
        if missing_evidence:
            out.append("  Missing evidence:")
            for item in missing_evidence:
                out.append(f"    • {item}")
            out.append("")

    return out


def _section_next_steps(
    decision: str,
    submit_dir: str,
    job_id: str,
    incident_id: str,
    failure_type: str | None,
) -> list[str]:
    out = [_header("NEXT STEPS"), ""]

    if decision == "RETRY":
        out += [
            "  No action required. DAGMan is retrying automatically.",
            "",
            "  Monitor progress:",
            f"    pegasus-status --submit-dir {submit_dir}",
        ]

    elif decision == "ASK":
        out += [
            "  1. Review the proposed change in the FIX PROPOSAL section above.",
            "  2. Approve or reject via the API (see DECISION section).",
            "  3. Re-submit the workflow after approval.",
        ]

    elif decision == "STOP":
        out += [
            "  1. Review the diagnosis and evidence above.",
            "  2. Fix the root cause in your workflow or application code.",
            "  3. Re-submit the workflow manually after fixing the issue.",
        ]

    elif decision == "ESCALATE":
        out += [
            "  1. Review stderr manually:",
            f"       cat {submit_dir}/00/00/{job_id}.err.000",
            "       (or: {job_id}_ID0000001.err)",
            "",
            "  2. Run pegasus-analyzer:",
            f"       pegasus-analyzer --submit-dir {submit_dir}",
            "",
            "  3. Check HTCondor history (if condor job ID is available):",
            "       condor_history <cluster.proc> -json",
        ]

    out += [
        "",
        f"  Incident ID : {incident_id}  (reference when contacting support)",
        "",
        "=" * _W,
    ]
    return out


# ── Public API ────────────────────────────────────────────────────────────────

def generate_report(
    *,
    job_id: str,
    workflow_id: str,
    submit_dir: str,
    incident_id: str,
    exit_code: int,
    attempt: int,
    max_retries: int,
    decision: str,
    failure_type: str | None,
    confidence: float | None,
    explanation: str | None,
    evidence_sources: list[str],
    fix_proposed: dict[str, Any] | None,
    approval_url: str | None,
    service_url: str,
    missing_evidence: list[str] | None = None,
) -> str:
    """Build and return the full report as a string."""
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    sections: list[list[str]] = [
        _section_header(
            job_id=job_id,
            workflow_id=workflow_id,
            submit_dir=submit_dir,
            incident_id=incident_id,
            attempt=attempt,
            max_retries=max_retries,
            exit_code=exit_code,
            generated_at=generated_at,
        ),
        _section_diagnosis(
            failure_type=failure_type,
            confidence=confidence,
            explanation=explanation,
            evidence_sources=evidence_sources,
        ),
        _section_fix(fix_proposed),
        _section_decision(
            decision=decision,
            failure_type=failure_type,
            incident_id=incident_id,
            fix_proposed=fix_proposed,
            approval_url=approval_url,
            service_url=service_url,
            submit_dir=submit_dir,
            missing_evidence=missing_evidence or [],
        ),
        _section_next_steps(
            decision=decision,
            submit_dir=submit_dir,
            job_id=job_id,
            incident_id=incident_id,
            failure_type=failure_type,
        ),
    ]

    lines: list[str] = []
    for section in sections:
        lines.extend(section)

    return "\n".join(lines) + "\n"


def write_report(report_text: str, submit_dir: str | Path, job_id: str) -> Path:
    """Write the report to {submit_dir}/{job_id}.agent_report. Returns the path."""
    path = Path(submit_dir) / f"{job_id}.agent_report"
    path.write_text(report_text, encoding="utf-8")
    return path
