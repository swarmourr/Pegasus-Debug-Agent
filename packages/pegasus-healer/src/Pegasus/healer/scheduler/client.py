"""
Thin bridge from Pegasus.healer to Pegasus.client.

All Pegasus CLI calls (pegasus-analyzer, pegasus-status, pegasus-remove,
pegasus-run) go through the official Pegasus.client._client.Client that
lives in packages/pegasus-common.  HTCondor job history queries use
Pegasus.client.condor utilities.

No subprocess calls live here — they all belong in pegasus-common.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import structlog

log = structlog.get_logger(__name__)

# ── Pegasus workflow-level client ──────────────────────────────────────────────

_client = None


def get_pegasus_client():
    """Return a singleton Pegasus.client._client.Client built from PATH."""
    global _client
    if _client is None:
        from Pegasus.client._client import from_env
        _client = from_env()
    return _client


# ── Workflow-level helpers (delegating to Pegasus.client) ──────────────────────

def analyze_workflow(submit_dir: Path, job_id: str | None = None) -> str | None:
    """Run pegasus-analyzer via Pegasus.client and return stdout."""
    try:
        c = get_pegasus_client()
        result = c.analyzer(str(submit_dir), job=job_id)
        return result.stdout or None
    except Exception as exc:
        log.warning("pegasus_analyzer_failed", error=str(exc))
        return None


def get_workflow_status(submit_dir: Path) -> str | None:
    """Run pegasus-status via Pegasus.client and return stdout."""
    try:
        c = get_pegasus_client()
        result = c.status(str(submit_dir), long=True)
        return result.stdout if hasattr(result, "stdout") else None
    except Exception as exc:
        log.warning("pegasus_status_failed", error=str(exc))
        return None


def remove_workflow(submit_dir: Path) -> bool:
    """Run pegasus-remove via Pegasus.client."""
    try:
        get_pegasus_client().remove(str(submit_dir))
        return True
    except Exception as exc:
        log.warning("pegasus_remove_failed", error=str(exc))
        return False


def run_workflow(submit_dir: Path) -> bool:
    """Run pegasus-run via Pegasus.client."""
    try:
        get_pegasus_client().run(str(submit_dir))
        return True
    except Exception as exc:
        log.warning("pegasus_run_failed", error=str(exc))
        return False


# ── HTCondor job history ───────────────────────────────────────────────────────

def get_condor_history(cluster_id: str) -> dict | None:
    """
    Query condor_history for a completed job and return the raw classad dict.

    Uses Pegasus.client.condor._exec so subprocess logic stays in pegasus-common.
    Falls back to a direct subprocess call if condor is not importable
    (e.g. running outside a Pegasus installation).
    """
    if not cluster_id:
        return None
    try:
        from Pegasus.client.condor import _exec
        result = _exec(["condor_history", "-json", cluster_id], stream_stdout=False)
        data = result.json
        if isinstance(data, list) and data:
            return data[0]
        return None
    except Exception:
        # Fallback: direct subprocess (safe — single, bounded call)
        try:
            proc = subprocess.run(
                ["condor_history", "-json", cluster_id],
                capture_output=True, text=True, timeout=30,
            )
            data = json.loads(proc.stdout or "[]")
            return data[0] if data else None
        except Exception as exc:
            log.warning("condor_history_failed", cluster_id=cluster_id, error=str(exc))
            return None
