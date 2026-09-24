#!/usr/bin/env python3
"""
pegasus-healer — DAGMan POST script entry point.

Installed as a console script so it can be referenced directly in
pegasus.properties without a hard-coded script path:

    pegasus.dagman.post = pegasus-healer
    pegasus.dagman.post.arguments = $RETURN $JOB $RETRY $MAX_RETRIES \\
        /path/to/submit_dir ${wf.uuid}

EXIT CODE CONTRACT
──────────────────
  1 → DAGMan retries (AUTO fix applied)
  0 → DAGMan stops   (ASK, STOP, ESCALATE, or job succeeded)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from uuid import uuid4

try:
    from dotenv import load_dotenv as _load_dotenv
    _ENV = Path.home() / ".pegasus" / "healer.env"
    if _ENV.exists():
        _load_dotenv(_ENV, override=False)
except ImportError:
    pass

TAG = "[pegasus-healer]"
_log_file = None


def _log(msg: str) -> None:
    line = f"{TAG} {msg}"
    print(line, file=sys.stderr, flush=True)
    if _log_file is not None:
        print(line, file=_log_file, flush=True)


def _find_job_subdir(submit_dir: Path, job_id: str) -> Path:
    try:
        from Pegasus.healer.collectors.submit_dir import _all_job_subdirs
        for jd in _all_job_subdirs(submit_dir):
            if (jd / f"{job_id}.sub").exists():
                return jd
    except Exception:
        pass
    return submit_dir


def _open_log(submit_dir: Path, job_id: str) -> None:
    global _log_file
    job_dir = _find_job_subdir(submit_dir, job_id)
    try:
        _log_file = open(job_dir / f"{job_id}.healer.log", "a")
    except OSError:
        pass


def _marker_fast_path(
    submit_dir: Path,
    transformation: str | None,
    exit_code: int,
    sub_content: str | None,
) -> bool:
    if not transformation or not sub_content:
        return False
    try:
        from Pegasus.healer.pegasus.sibling_fixer import check_fast_path
        from Pegasus.healer.pegasus.sub_file import read_resource_requests
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sub", delete=False) as tmp:
            tmp.write(sub_content)
            tmp_path = Path(tmp.name)
        try:
            current_resources = read_resource_requests(tmp_path)
        finally:
            tmp_path.unlink(missing_ok=True)
        return check_fast_path(submit_dir, transformation, exit_code, current_resources)
    except Exception:
        return False


def _resolve_thread(
    submit_dir: Path,
    job_id: str,
    workflow_id: str,
    instance_id: int,
) -> tuple[str, bool]:
    job_dir = _find_job_subdir(submit_dir, job_id)
    thread_file = job_dir / f"{job_id}.healer_thread"
    if thread_file.exists():
        return thread_file.read_text().strip(), True
    thread_id = f"{workflow_id}/{job_id}/{instance_id}"
    thread_file.write_text(thread_id)
    return thread_id, False


async def _run(args: argparse.Namespace) -> int:
    from Pegasus.healer.collectors.submit_dir import (
        collect_evidence, parse_instance_id, parse_healer_tags,
    )
    from Pegasus.healer.policies.loader import load_policy
    from Pegasus.healer.policies.engine import PolicyEngine
    from Pegasus.healer.pegasus.dagman_retry_controller import DAGManRetryController
    from Pegasus.healer.pegasus.sibling_fixer import _parse_transformation_from_content
    from Pegasus.healer.llm.universal_provider import UniversalProvider
    from Pegasus.healer.graph.graph import create_graph_with_checkpointer
    from Pegasus.healer.memory.sqlite_repo import SQLiteMemoryRepo

    submit_dir = Path(args.submit_dir)
    instance_id = args.job_instance_id or parse_instance_id(args.job_id)

    _log("collecting evidence...")
    evidence = collect_evidence(
        submit_dir=submit_dir,
        job_id=args.job_id,
        instance_id=instance_id,
        condor_job_id=args.condor_job_id,
    )
    _log(f"evidence: {evidence.available_sources}")

    transformation = args.transformation
    if not transformation and evidence.sub_file_content:
        transformation = _parse_transformation_from_content(evidence.sub_file_content)

    if evidence.sub_file_content:
        tags = parse_healer_tags(evidence.sub_file_content)
        if "stop" in tags:
            _log("tag 'stop': terminal — aborting workflow")
            return 0

    if _marker_fast_path(submit_dir, transformation, args.exit_code,
                         evidence.sub_file_content):
        _log("marker fast path: fix already applied → retrying with patched .sub")
        return 1

    thread_id, is_resume = _resolve_thread(
        submit_dir, args.job_id, args.workflow_id, instance_id
    )
    _log(f"{'resuming' if is_resume else 'fresh'} thread: {thread_id}")

    _policy_path = os.environ.get("POLICY_FILE") or "policies/remediation.yaml"
    if not os.path.isabs(_policy_path):
        _policy_path = str(Path(__file__).resolve().parents[4] / _policy_path)
    policy = load_policy(_policy_path)
    policy_engine = PolicyEngine(policy)

    retry_ctrl = DAGManRetryController(
        submit_dir=str(submit_dir),
        job_id=args.job_id,
        job_instance_id=instance_id,
    )

    def _llm(model_env: str, key_env: str, url_env: str) -> UniversalProvider:
        return UniversalProvider(
            model=os.environ.get(model_env) or os.environ.get("LLM_MODEL", ""),
            api_key=os.environ.get(key_env) or os.environ.get("LLM_API_KEY", ""),
            base_url=os.environ.get(url_env) or os.environ.get("LLM_BASE_URL", ""),
        )

    llm           = _llm("LLM_MODEL",            "LLM_API_KEY",            "LLM_BASE_URL")
    diagnosis_llm = _llm("DIAGNOSIS_LLM_MODEL",   "DIAGNOSIS_LLM_API_KEY",  "DIAGNOSIS_LLM_BASE_URL")
    fix_llm       = _llm("FIX_PLANNING_LLM_MODEL","FIX_PLANNING_LLM_API_KEY","FIX_PLANNING_LLM_BASE_URL")

    _pegasus_dir = Path.home() / ".pegasus"
    _pegasus_dir.mkdir(exist_ok=True)
    graph = await create_graph_with_checkpointer(
        sqlite_path=os.environ.get(
            "HEALER_CHECKPOINT", str(_pegasus_dir / "healer_checkpoint.db")
        ),
    )

    _mem_db = os.environ.get("HEALER_MEMORY_DB") or str(_pegasus_dir / "healer_memory.db")
    memory_repo = SQLiteMemoryRepo(db_path=_mem_db)
    _log(f"memory_repo: {_mem_db}")

    config = {
        "configurable": {
            "thread_id":        thread_id,
            "llm":              llm,
            "diagnosis_llm":    diagnosis_llm,
            "fix_planning_llm": fix_llm,
            "policy":           policy,
            "policy_engine":    policy_engine,
            "retry_controller": retry_ctrl,
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
    _log(f"decision={decision} terminal={final_state.get('terminal', False)}")

    if final_state.get("retry_outcome") == "EFFECTIVE":
        _log("exit 0 → effective outcome, workflow continues")
        return 0
    if decision == "AUTO":
        _log("exit 1 → DAGMan retries with patched .sub")
        return 1
    if decision == "ASK":
        if args.exit_code != 0:
            _log(f"exit {args.exit_code} → proposal written; apply fix manually")
            return args.exit_code
        _log("exit 0 → proposal in report; apply manually and re-submit")
        return 0
    if args.exit_code != 0:
        _log(f"exit {args.exit_code} → terminal, job failed")
        return args.exit_code
    _log("exit 0 → terminal")
    return 0


def _run_graph(args: argparse.Namespace) -> int:
    try:
        return asyncio.run(_run(args))
    except Exception as exc:
        import traceback
        _log(f"ERROR: remediation graph raised {type(exc).__name__}: {exc}")
        traceback.print_exc(file=sys.stderr)
        if _log_file is not None:
            traceback.print_exc(file=_log_file)
        return args.exit_code


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="pegasus-healer",
        description="Pegasus DAGMan POST script — LangGraph remediation agent",
    )
    parser.add_argument("exit_code",    type=int, help="Job exit code ($RETURN)")
    parser.add_argument("job_id",                 help="DAGMan node name ($JOB)")
    parser.add_argument("retry_number", type=int, help="Current retry ($RETRY)")
    parser.add_argument("max_retries",  type=int, help="Max retries ($MAX_RETRIES)")
    parser.add_argument("submit_dir",             help="Pegasus submit directory")
    parser.add_argument("workflow_id",            help="Pegasus workflow UUID (wf_uuid)")
    parser.add_argument("--job-instance-id", dest="job_instance_id", type=int, default=None)
    parser.add_argument("--condor-job-id",   dest="condor_job_id",   default=None)
    parser.add_argument("--execution-site",  dest="execution_site",  default=None)
    parser.add_argument("--transformation",  default=None)
    args = parser.parse_args()

    _open_log(Path(args.submit_dir), args.job_id)
    _log(
        f"invoked: exit_code={args.exit_code} "
        f"retry={args.retry_number}/{args.max_retries} job={args.job_id}"
    )

    submit_dir = Path(args.submit_dir)
    thread_file = (
        _find_job_subdir(submit_dir, args.job_id) / f"{args.job_id}.healer_thread"
    )

    if args.exit_code == 0 and not thread_file.exists():
        return 0

    if args.retry_number >= args.max_retries:
        _log("retry budget exhausted — no agent run")
        return args.exit_code

    return _run_graph(args)


if __name__ == "__main__":
    sys.exit(main())
