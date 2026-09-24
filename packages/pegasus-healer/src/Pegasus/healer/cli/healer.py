#!/usr/bin/env python3
"""
pegasus-healer — DAGMan POST script entry point.

Invoked by DAGMan after each job failure:

    pegasus-healer $RETURN $JOB $RETRY $MAX_RETRIES /submit/dir ${wf.uuid}

Exit codes
----------
  1 → DAGMan retries the job  (AUTO fix applied to .sub)
  0 → DAGMan stops            (ASK / STOP / ESCALATE / success)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from uuid import uuid4

TAG = "[pegasus-healer]"
_log_file = None  # opened in main() next to job .out/.err


def _log(msg: str) -> None:
    line = f"{TAG} {msg}"
    print(line, file=sys.stderr, flush=True)
    if _log_file is not None:
        print(line, file=_log_file, flush=True)


def _open_log(submit_dir: Path, job_id: str) -> None:
    global _log_file
    from Pegasus.healer.collectors.files import _job_subdirs
    for jd in _job_subdirs(submit_dir):
        sub = jd / f"{job_id}.sub"
        if sub.exists():
            try:
                _log_file = open(jd / f"{job_id}.healer.log", "a")
            except OSError:
                pass
            return
    try:
        _log_file = open(submit_dir / f"{job_id}.healer.log", "a")
    except OSError:
        pass


# ── Async graph runner ────────────────────────────────────────────────────────

async def _run(args: argparse.Namespace) -> int:
    from Pegasus.healer.collectors.submit_dir import (
        collect_evidence,
        parse_instance_id,
        parse_healer_tags,
    )
    from Pegasus.healer.policies.loader import load_policy
    from Pegasus.healer.policies.engine import PolicyEngine
    from Pegasus.healer.memory.sqlite_repo import SQLiteMemoryRepo
    from Pegasus.healer.graph.graph import create_graph_with_checkpointer

    submit_dir = Path(args.submit_dir)
    instance_id = args.job_instance_id or parse_instance_id(args.job_id)

    # Evidence
    _log("collecting evidence...")
    evidence = collect_evidence(
        submit_dir=submit_dir,
        job_id=args.job_id,
        instance_id=instance_id,
        condor_job_id=args.condor_job_id,
    )
    _log(f"evidence sources: {evidence.available_sources}")

    # Healer tag early exits
    if evidence.sub_file_content:
        tags = parse_healer_tags(evidence.sub_file_content)
        if "stop" in tags:
            _log("tag 'stop': terminal")
            return 0

    # Thread ID (incident continuity across retries)
    thread_id, is_resume = _resolve_thread(submit_dir, args.job_id,
                                           args.workflow_id, instance_id)
    _log(f"{'resuming' if is_resume else 'fresh'} thread: {thread_id}")

    # Services
    policy_path = os.environ.get("POLICY_FILE", "")
    if not policy_path or not os.path.isabs(policy_path):
        # Default: policies.yaml next to the installed package
        _pkg_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
        policy_path = str(_pkg_dir / "policies.yaml")
    policy = load_policy(policy_path)
    policy_engine = PolicyEngine(policy)

    _pegasus_dir = Path.home() / ".pegasus"
    _pegasus_dir.mkdir(exist_ok=True)

    graph = await create_graph_with_checkpointer(
        sqlite_path=os.environ.get(
            "HEALER_CHECKPOINT", str(_pegasus_dir / "healer_checkpoint.db")
        )
    )

    memory_repo = SQLiteMemoryRepo(
        db_path=os.environ.get(
            "HEALER_MEMORY_DB", str(_pegasus_dir / "healer_memory.db")
        )
    )

    from Pegasus.healer.utils.llm import make_llm
    llm           = make_llm("LLM")
    diagnosis_llm = make_llm("DIAGNOSIS_LLM")
    fix_llm       = make_llm("FIX_PLANNING_LLM")

    config = {
        "configurable": {
            "thread_id":        thread_id,
            "llm":              llm,
            "diagnosis_llm":    diagnosis_llm,
            "fix_planning_llm": fix_llm,
            "policy":           policy,
            "policy_engine":    policy_engine,
            "submit_dir":       str(submit_dir),
            "raw_evidence":     evidence.model_dump(),
            "memory_repo":      memory_repo,
        }
    }

    if is_resume:
        outcome = "EFFECTIVE" if args.exit_code == 0 else "INEFFECTIVE"
        input_data: dict = {"retry_outcome": outcome}
        _log(f"retry outcome: {outcome}")
    else:
        input_data = {
            "incident_id":            str(uuid4()),
            "workflow_id":            args.workflow_id,
            "job_id":                 args.job_id,
            "source_job_instance_id": instance_id,
            "exit_code":              args.exit_code,
            "scheduler_id":           args.condor_job_id,
            "attempt":                1,
        }

    _log("running remediation graph...")
    final_state = await graph.ainvoke(input_data, config=config)

    decision = final_state.get("policy_decision")
    _log(f"decision={decision}")

    # Exit code contract
    if final_state.get("retry_outcome") == "EFFECTIVE":
        return 0
    if decision == "AUTO":
        return 1
    if decision == "ASK" and args.exit_code != 0:
        return args.exit_code
    return 0


def _resolve_thread(
    submit_dir: Path, job_id: str, workflow_id: str, instance_id: int
) -> tuple[str, bool]:
    from Pegasus.healer.collectors.files import _job_subdirs
    for jd in _job_subdirs(submit_dir):
        tf = jd / f"{job_id}.healer_thread"
        if tf.exists():
            return tf.read_text().strip(), True
    thread_id = f"{workflow_id}/{job_id}/{instance_id}"
    # Write to submit_dir root — will be found by _job_subdirs on next call
    (submit_dir / f"{job_id}.healer_thread").write_text(thread_id)
    return thread_id, False


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        prog="pegasus-healer",
        description="Pegasus/DAGMan POST script — LangGraph remediation",
    )
    parser.add_argument("exit_code",    type=int)
    parser.add_argument("job_id")
    parser.add_argument("retry_number", type=int)
    parser.add_argument("max_retries",  type=int)
    parser.add_argument("submit_dir")
    parser.add_argument("workflow_id")
    parser.add_argument("--job-instance-id", dest="job_instance_id", type=int, default=None)
    parser.add_argument("--condor-job-id",   dest="condor_job_id",   default=None)
    parser.add_argument("--transformation",  default=None)
    args = parser.parse_args()

    _open_log(Path(args.submit_dir), args.job_id)
    _log(f"exit={args.exit_code} retry={args.retry_number}/{args.max_retries} job={args.job_id}")

    if args.exit_code == 0:
        thread_file = Path(args.submit_dir) / f"{args.job_id}.healer_thread"
        if not thread_file.exists():
            return 0

    if args.retry_number >= args.max_retries:
        _log("retry budget exhausted")
        return args.exit_code

    try:
        return asyncio.run(_run(args))
    except Exception as exc:
        import traceback
        _log(f"ERROR: {type(exc).__name__}: {exc}")
        traceback.print_exc(file=sys.stderr)
        return args.exit_code


if __name__ == "__main__":
    sys.exit(main())
