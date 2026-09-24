"""
Multi-failure demo workflow.

Demonstrates the healer handling two distinct failure types in sequence
on the same job:

  Attempt 0 → exit 137 (OOM)          → healer increases memory  → retry
  Attempt 1 → exit 1   (disk full)    → healer increases disk    → retry
  Attempt 2 → success

HOW TO RUN
──────────
    python workflows/multi_failure_demo.py

Requirements:
  - Pegasus 5.x installed and on PATH
  - HTCondor running locally (or configured execution site)
  - executables/multi_fail is chmod +x
"""

from Pegasus.api import *
import re
import subprocess
import sys
from pathlib import Path

BASE_DIR        = Path(".").resolve()
INPUT_DIR       = (BASE_DIR / "input").resolve()
EXECUTABLES_DIR = (BASE_DIR / "executables").resolve()
OUTPUT_DIR      = (BASE_DIR / "output").resolve()

EXEC_SITE = "local"

# ── Healer injection ───────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from workflows.healer import add_healer_to_job

_post_script = (_PROJECT_ROOT / "scripts" / "pegasus_post_script.py").resolve()
_post_script.chmod(0o755)

# Remove any stale pegasus.properties
(BASE_DIR / "pegasus.properties").unlink(missing_ok=True)

# ── Input file ────────────────────────────────────────────────────────────────
INPUT_DIR.mkdir(parents=True, exist_ok=True)
(INPUT_DIR / "f.in").write_text(
    "Input data for the multi-failure demo workflow.\n"
)

# ── Transformation catalog ─────────────────────────────────────────────────────
tc = TransformationCatalog()
tc.add_transformations(
    Transformation("multi_fail").add_sites(
        TransformationSite(
            EXEC_SITE,
            str(EXECUTABLES_DIR / "multi_fail"),
            is_stageable=False,
        )
    ),
)

# ── Files ──────────────────────────────────────────────────────────────────────
fin  = File("f.in")
fout = File("f.out")

# ── Job — starts with conservative resources so both fixes are meaningful ──────
job = add_healer_to_job(
    Job("multi_fail")
    .add_args("-T", "2", "-i", fin, "-o", str(fout))
    .add_inputs(fin)
    .add_outputs(fout)
    .add_profiles(Namespace.CONDOR, key="request_memory", value="256")
    .add_profiles(Namespace.CONDOR, key="request_disk",   value="1024")
)

# ── Workflow ───────────────────────────────────────────────────────────────────
wf = Workflow("multi-failure-demo")
wf.add_transformation_catalog(tc)
wf.add_jobs(job)

try:
    wf.write()
    wf.graph(include_files=True, label="xform-id", output="graph.png")
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

_dag_files = sorted(
    BASE_DIR.glob("**/multi-failure-demo-0.dag"),
    key=lambda p: p.stat().st_mtime,
)
if not _dag_files:
    print("ERROR: no .dag file found — cannot inject healer", file=sys.stderr)
    sys.exit(1)

_dag_file   = _dag_files[-1]
_submit_dir = _dag_file.parent
_wf_id      = _submit_dir.name

_lines = _dag_file.read_text().splitlines()
_patched = []
for _line in _lines:
    _m = re.match(r"^SCRIPT POST\s+(\S+)\s+/usr/bin/pegasus-exitcode", _line)
    if _m and not any(p in _m.group(1) for p in _INFRA):
        _job = _m.group(1)
        _line = (
            f"SCRIPT POST {_job} {sys.executable} {_post_script} "
            f"$RETURN {_job} $RETRY 3 {_submit_dir} {_wf_id}"
        )
    _patched.append(_line)

_dag_file.write_text("\n".join(_patched) + "\n")
print(f"\nHealer injected → {_dag_file}")

# ── Submit ─────────────────────────────────────────────────────────────────────
_res = subprocess.run(["pegasus-run", str(_submit_dir)], text=True)
if _res.returncode != 0:
    print("ERROR: pegasus-run failed", file=sys.stderr)
    sys.exit(1)
