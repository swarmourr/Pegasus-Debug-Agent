"""
Pegasus healer integration helpers.

Attaches pegasus_post_script.py as a DAGMan POST script to every job
in a Pegasus workflow via Properties and per-job RETRY profiles.

Usage
─────
    from workflows.healer import configure_healer_properties, add_healer_to_job

    props = Properties()
    configure_healer_properties(props, submit_dir=SUBMIT_DIR, max_retries=3)
    props.write()

    job = add_healer_to_job(Job("my-task"), max_retries=3)
"""
from __future__ import annotations

from pathlib import Path

from Pegasus.api import Job, Namespace, Properties

# Absolute path to the post script — resolved relative to this file
_POST_SCRIPT = (Path(__file__).parent.parent / "scripts" / "pegasus_post_script.py").resolve()


def configure_healer_properties(
    props: Properties,
    submit_dir: str | Path,
    max_retries: int = 3,
) -> None:
    """
    Register the healer POST script in Pegasus properties.

    Sets ``dagman.post`` and ``dagman.post.arguments`` so Pegasus writes
    the post script into every node of the generated .dag file.

    DAGMan substitutes ``$RETURN``, ``$JOB``, ``$RETRY``, ``$MAX_RETRIES``
    at runtime.  Pegasus substitutes ``${wf.uuid}`` at planning time.

    Parameters
    ----------
    props:        Pegasus Properties object (loaded or freshly created).
    submit_dir:   Absolute path to the Pegasus submit directory for this run.
    max_retries:  Maximum DAGMan retries; written as the MAX_RETRIES argument
                  so the post script knows the retry budget.
    """
    # Keep "." as-is — DAGMan always invokes post scripts with CWD set to the
    # run's submit directory, so "." resolves correctly at runtime without us
    # needing to know the per-run path at generation time.
    submit_dir_arg = "." if str(submit_dir).strip() == "." else str(Path(submit_dir).resolve())
    props["pegasus.dagman.post"] = str(_POST_SCRIPT)
    props["pegasus.dagman.post.arguments"] = (
        f"$RETURN $JOB $RETRY {max_retries} {submit_dir_arg} ${{wf.uuid}}"
    )


def add_healer_to_job(job: Job, max_retries: int = 3) -> Job:
    """
    Add a DAGMan RETRY profile to a single job.

    The POST script itself is registered globally via
    ``configure_healer_properties``; this sets the per-job retry budget
    so DAGMan knows how many times to call the healer before giving up.

    Parameters
    ----------
    job:         Pegasus Job to configure.
    max_retries: Maximum DAGMan retries for this job.

    Returns
    -------
    The same job object (fluent API — chain with ``.add_inputs()``, etc.).
    """
    job.add_profiles(Namespace.DAGMAN, key="RETRY", value=str(max_retries))
    return job
