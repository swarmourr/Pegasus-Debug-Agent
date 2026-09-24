"""
disk_demo.py — Disk-exhaustion demo workflow for PegasusAgent healer.

Failure scenario
────────────────
  analyze fails on attempt 0:
    stderr → "No space left on device"
    exit   → 1

  Healer detects DISK_EXCEEDED via stderr pattern (confidence ≥ 0.95),
  increases request_disk in the .sub file (AUTO, no human approval),
  DAGMan retries → analyze succeeds → report runs → workflow completes.

Usage
─────
  cd /path/to/PegasusAgent
  python workflows/disk_demo.py
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
    "Sample dataset for the disk-demo workflow.\n"
    "Each record requires significant scratch space to process.\n"
)

# ── Transformation catalog ────────────────────────────────────────────────────
tc = TransformationCatalog()
tc.add_transformations(
    Transformation("analyze").add_sites(
        TransformationSite("local", str(EXECUTABLES_DIR / "analyze"), is_stageable=False)
    ),
    Transformation("report").add_sites(
        TransformationSite("local", str(EXECUTABLES_DIR / "report"), is_stageable=False)
    ),
)

# ── Workflow ──────────────────────────────────────────────────────────────────
wf = Workflow("disk-demo")
wf.add_transformation_catalog(tc)

fin    = File("data.in")
finter = File("data.inter")
fout   = File("data.out")

# analyze: low request_disk so the healer has room to increase it
job_analyze = add_healer_to_job(
    Job("analyze")
        .add_args("-T", "2", "-i", fin, "-o", finter)
        .add_inputs(fin)
        .add_outputs(finter, stage_out=False)
        .add_profiles(Namespace.CONDOR, key="request_memory", value="256")
        .add_profiles(Namespace.CONDOR, key="request_disk",   value="512")   # 512 MB — intentionally small
)

job_report = add_healer_to_job(
    Job("report")
        .add_args("-T", "2", "-i", finter, "-o", fout)
        .add_inputs(finter)
        .add_outputs(fout)
        .add_profiles(Namespace.CONDOR, key="request_memory", value="256")
)

wf.add_jobs(job_analyze, job_report)

# ── Plan ──────────────────────────────────────────────────────────────────────
try:
    wf.write()
    wf.graph(include_files=True, label="xform-id", output="disk_demo_graph.png")
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

_dag_files = sorted(BASE_DIR.glob("**/disk-demo-0.dag"), key=lambda p: p.stat().st_mtime)
if not _dag_files:
    print("ERROR: no disk-demo-0.dag found — cannot inject healer", file=sys.stderr)
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
