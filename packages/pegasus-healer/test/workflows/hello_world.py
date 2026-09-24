from Pegasus.api import *
import re
import subprocess
import sys
from pathlib import Path

import logging

logging.basicConfig(level=logging.DEBUG)

# we specify directories for inputs, executables and outputs
# - directory where to pick up the inputs from a directory.
# - directory where the executables that the workflow uses are placed.
# - directory where the outputs should be placed.

BASE_DIR = Path(".").resolve()
INPUT_DIR = Path(BASE_DIR /  "input").resolve()
EXECUTABLES_DIR = Path(BASE_DIR / "executables").resolve()
OUTPUT_DIR = Path(BASE_DIR /  "output").resolve()

# the execution site where you job to run.
# local means the jobs run on ACCESS Pegasus itself.
#
# compute means the jobs execution on the compute site
# defined in your site catalog. In the ACCESS Pegasus
# setup, it means that jobs will run on a node provisioned
# from an ACCESS site such as jetstream.
EXEC_SITE = "local"

# --- Healer post script -------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from workflows.healer import add_healer_to_job

_post_script = (_PROJECT_ROOT / "scripts" / "pegasus_post_script.py").resolve()
_post_script.chmod(0o755)

# Remove any stale pegasus.properties — Pegasus auto-loads it from CWD
(BASE_DIR / "pegasus.properties").unlink(missing_ok=True)

# generate a simple input file for the workflow
INPUT_DIR.mkdir(parents=True, exist_ok=True)
with open("{}/f.in".format(INPUT_DIR), "w") as f:
    f.write("This is the contents of the input file for the hello world workflow!")

# --- Transformation catalog ---------------------------------------------------
tc = TransformationCatalog()
tc.add_transformations(
    Transformation("hello").add_sites(
        TransformationSite("local", str(EXECUTABLES_DIR / "hello"), is_stageable=False)
    ),
    Transformation("world").add_sites(
        TransformationSite("local", str(EXECUTABLES_DIR / "world"), is_stageable=False)
    ),
)

# --- Workflow -----------------------------------------------------------------
wf = Workflow("hello-world")
wf.add_transformation_catalog(tc)

fin = File("f.in")
finter = File("f.inter")
fout = File("f.out")

job_hello = add_healer_to_job(
    Job("hello")\
                    .add_args("-T", "3", "-i", fin, "-o {}".format(finter))\
                    .add_inputs(fin)\
                    .add_outputs(finter, stage_out=False)\
                    .add_profiles(Namespace.CONDOR, key="request_memory", value="256")
)

job_world = add_healer_to_job(
    Job("world")\
                    .add_args("-T", "3", "-i", finter, "-o {}".format(fout))\
                    .add_inputs(finter)\
                    .add_outputs(fout)
)

wf.add_jobs(job_hello, job_world)

# --- Visualize the Workflow ---------------------------------------------------
try:
    wf.write()
    wf.graph(include_files=True, label="xform-id", output="graph.png")
except PegasusClientError as e:
    print(e)

# --- Plan (without submit — we patch the .dag first) -------------------------
try:
    wf.plan(input_dirs=[INPUT_DIR], sites=[EXEC_SITE],\
            output_dir=OUTPUT_DIR, submit=False)
except PegasusClientError as e:
    print(e)
    sys.exit(1)

# --- Inject healer into the generated .dag ------------------------------------
_INFRA = ("create_dir", "stage_in", "stage_out", "register", "clean_up", "cleanup")

# Find the generated .dag — Pegasus submit dir location varies by site config
_dag_files = sorted(BASE_DIR.glob("**/hello-world-0.dag"), key=lambda p: p.stat().st_mtime)

if not _dag_files:
    print("ERROR: no .dag file found — cannot inject healer", file=sys.stderr)
    sys.exit(1)

_dag_file   = _dag_files[-1]           # latest run
_submit_dir = _dag_file.parent         # e.g. .../run0001/
_wf_id      = _submit_dir.name         # e.g. "run0001"

_lines = _dag_file.read_text().splitlines()
_patched = []
for _line in _lines:
    _m = re.match(r"^SCRIPT POST\s+(\S+)\s+/usr/bin/pegasus-exitcode", _line)
    if _m and not any(p in _m.group(1) for p in _INFRA):
        _job = _m.group(1)
        _line = (f"SCRIPT POST {_job} {sys.executable} {_post_script} "
                 f"$RETURN {_job} $RETRY 3 {_submit_dir} {_wf_id}")
    _patched.append(_line)

_dag_file.write_text("\n".join(_patched) + "\n")
print(f"\nHealer injected → {_dag_file}")

# --- Submit the patched DAG ---------------------------------------------------
_res = subprocess.run(["pegasus-run", str(_submit_dir)], text=True)
if _res.returncode != 0:
    print("ERROR: pegasus-run failed", file=sys.stderr)
    sys.exit(1)
