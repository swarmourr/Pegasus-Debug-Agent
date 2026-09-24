"""
sibling_demo.py — Sibling-fix demo workflow for PegasusAgent healer.

Failure scenario
────────────────
  Three parallel compute jobs (A, B, C) all use the same transformation
  and all share the same intentionally-low request_memory=256 MB.

  compute_A fails on attempt 0:
    stderr → "Killed" / "out of memory (signal 9)"
    exit   → 137

  Healer detects OUT_OF_MEMORY via exit code 137 (confidence ≥ 0.95),
  increases request_memory in compute_A.sub (AUTO, no human approval),
  AND broadcasts the same patch to compute_B.sub and compute_C.sub
  (sibling broadcast via sibling_fixer).

  compute_B / compute_C may still fail with exit 137 if they were
  already running when the broadcast happened — but their .sub files
  are already patched, so the fast path kicks in:
    marker exists + .sub patched + exit 137 → skip agent → exit 1 (retry)

  After all three succeed, merge aggregates their outputs → workflow
  completes (SUCCESS=*).

Usage
─────
  cd /path/to/PegasusAgent
  python workflows/sibling_demo.py
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import logging
logging.basicConfig(level=logging.DEBUG)

from Pegasus.api import (
    File, Job, Namespace, PegasusClientError,
    TransformationCatalog, TransformationSite, Transformation, Workflow,
)

BASE_DIR        = Path(".").resolve()
INPUT_DIR       = (BASE_DIR / "input").resolve()
EXECUTABLES_DIR = (BASE_DIR / "executables").resolve()
OUTPUT_DIR      = (BASE_DIR / "output").resolve()

EXEC_SITE = "local"

# ── Healer integration ────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from workflows.healer import add_healer_to_job

_post_script = (_PROJECT_ROOT / "scripts" / "pegasus_post_script.py").resolve()
_post_script.chmod(0o755)

# Remove stale pegasus.properties (auto-loaded from CWD by Pegasus)
(BASE_DIR / "pegasus.properties").unlink(missing_ok=True)

# ── Input file ────────────────────────────────────────────────────────────────
INPUT_DIR.mkdir(exist_ok=True)
(INPUT_DIR / "data.in").write_text(
    "Shared input dataset for the sibling-demo workflow.\n"
    "All three compute jobs consume this same file.\n"
)

# ── Transformation catalog ────────────────────────────────────────────────────
tc = TransformationCatalog()
tc.add_transformations(
    Transformation("compute").add_sites(
        TransformationSite("local", str(EXECUTABLES_DIR / "compute"), is_stageable=False)
    ),
    Transformation("merge").add_sites(
        TransformationSite("local", str(EXECUTABLES_DIR / "merge"), is_stageable=False)
    ),
)

# ── Workflow ──────────────────────────────────────────────────────────────────
wf = Workflow("sibling-demo")
wf.add_transformation_catalog(tc)

fin = File("data.in")

# Three intermediate outputs — one per sibling
fa = File("A.out")
fb = File("B.out")
fc = File("C.out")

# Final aggregated output
fout = File("merged.out")

# compute_A / B / C: same transformation, same low memory so healer must patch
# each one.  Sibling broadcast means only compute_A triggers the full agent;
# compute_B and compute_C use the fast path on their first retry failure.
_MEM = "256"   # intentionally below what the job needs

job_a = add_healer_to_job(
    Job("compute", _id="compute_A")
        .add_args("-T", "2", "-i", fin, "-o", fa)
        .add_inputs(fin)
        .add_outputs(fa, stage_out=False)
        .add_profiles(Namespace.CONDOR, key="request_memory", value=_MEM)
)

job_b = add_healer_to_job(
    Job("compute", _id="compute_B")
        .add_args("-T", "2", "-i", fin, "-o", fb)
        .add_inputs(fin)
        .add_outputs(fb, stage_out=False)
        .add_profiles(Namespace.CONDOR, key="request_memory", value=_MEM)
)

job_c = add_healer_to_job(
    Job("compute", _id="compute_C")
        .add_args("-T", "2", "-i", fin, "-o", fc)
        .add_inputs(fin)
        .add_outputs(fc, stage_out=False)
        .add_profiles(Namespace.CONDOR, key="request_memory", value=_MEM)
)

job_merge = add_healer_to_job(
    Job("merge")
        .add_args("-T", "2", "-i", fa, "-i", fb, "-i", fc, "-o", fout)
        .add_inputs(fa, fb, fc)
        .add_outputs(fout)
        .add_profiles(Namespace.CONDOR, key="request_memory", value="512")
)

wf.add_jobs(job_a, job_b, job_c, job_merge)

# ── Plan ──────────────────────────────────────────────────────────────────────
try:
    wf.write()
    wf.graph(include_files=True, label="xform-id", output="sibling_demo_graph.png")
except PegasusClientError as e:
    print(e)

try:
    wf.plan(
        input_dirs=[INPUT_DIR],
        sites=[EXEC_SITE],
        output_dir=OUTPUT_DIR,
        submit=False,
    )
except PegasusClientError as e:
    print(e)
    sys.exit(1)

# ── Inject healer POST script into the generated .dag ─────────────────────────
_INFRA = ("create_dir", "stage_in", "stage_out", "register", "clean_up", "cleanup")

_dag_files = sorted(BASE_DIR.glob("**/sibling-demo-0.dag"), key=lambda p: p.stat().st_mtime)
if not _dag_files:
    print("ERROR: no sibling-demo-0.dag found — cannot inject healer", file=sys.stderr)
    sys.exit(1)

_dag_file   = _dag_files[-1]
_submit_dir = _dag_file.parent
_wf_id      = _submit_dir.name

_lines   = _dag_file.read_text().splitlines()
_patched = []
for _line in _lines:
    _m = re.match(r"^SCRIPT POST\s+(\S+)\s+/usr/bin/pegasus-exitcode", _line)
    if _m and not any(p in _m.group(1) for p in _INFRA):
        _job  = _m.group(1)
        _line = (
            f"SCRIPT POST {_job} {sys.executable} {_post_script} "
            f"$RETURN {_job} $RETRY 3 {_submit_dir} {_wf_id}"
        )
    _patched.append(_line)

_dag_file.write_text("\n".join(_patched) + "\n")
print(f"\nHealer injected → {_dag_file}")

# ── Submit ────────────────────────────────────────────────────────────────────
_res = subprocess.run(["pegasus-run", str(_submit_dir)], text=True)
if _res.returncode != 0:
    print("ERROR: pegasus-run failed", file=sys.stderr)
    sys.exit(1)
