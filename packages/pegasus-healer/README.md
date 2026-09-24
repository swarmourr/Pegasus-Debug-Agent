# PegasusAgent — Auto-Healing for Pegasus WMS

Event-driven, LLM-assisted job failure remediation for **Pegasus WMS / HTCondor**.
Triggered by DAGMan as a POST script on every job exit. No API server, no daemon.

See [ARCHITECTURE.md](ARCHITECTURE.md) for a full technical reference with diagrams.

---

## How it works

```
Job fails (exit 137, disk full, walltime exceeded, ...)
    │
DAGMan calls POST script
    │
pegasus_post_script.py
    │
    ├── Fast path: sibling marker already applied → exit 1 (retry, no agent)
    │
    └── LangGraph remediation graph
            collect evidence (.sub, .out, .err, dagman.out, condor history)
                 │
            Rule classifier (deterministic, no LLM)
                 │ no match
            DiagnosisAgent (ReAct + tools, LLM)
                 │
            Fix catalog → Policy engine (deterministic safety gate)
                 │
            AUTO → patch .sub + sibling broadcast → exit 1 (DAGMan retries)
            STOP → exit original code (escalate)
```

---

## Prerequisites

- Python 3.11+
- Pegasus WMS 5.x installed (`pegasus-plan`, `pegasus-run`, `pegasus-status` on PATH)
- HTCondor (`condor_q`, `condor_qedit` on PATH)
- LLM API access (Anthropic, OpenAI, Azure, Ollama, or any LiteLLM provider)

---

## Installation

```bash
git clone https://github.com/swarmourr/AgentMape.git
cd AgentMape
git checkout pegasus-agent

# Create and activate virtual environment
python3 -m venv agentic
source agentic/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure LLM and optional settings
cp .env.example .env
# Edit .env — set LLM_MODEL and the matching API key
```

---

## LLM provider configuration

Set `LLM_MODEL` in `.env` to any model string supported by LiteLLM:

| Provider | `LLM_MODEL` | Key env var |
|---|---|---|
| Anthropic | `anthropic/claude-opus-4-6` | `LLM_API_KEY` |
| OpenAI | `gpt-4o` | `LLM_API_KEY` |
| Azure OpenAI | `azure/gpt-4o` | `LLM_API_KEY` + `LLM_BASE_URL` |
| AWS Bedrock | `bedrock/anthropic.claude-3-opus` | `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` |
| Ollama (local) | `ollama/llama3` | `LLM_BASE_URL=http://localhost:11434` |
| Any OAI-compat | `openai/my-model` | `LLM_BASE_URL=http://...` + `LLM_API_KEY` |

No code changes needed — just update `.env` and re-run.

---

## Wiring into your workflow

```python
from workflows.healer import add_healer_to_job
from Pegasus.api import Job, Namespace

job = add_healer_to_job(
    Job("my_transform", _id="my_job")
        .add_args(...)
        .add_inputs(...)
        .add_outputs(...)
        .add_profiles(Namespace.CONDOR, key="request_memory", value="4096")
)
```

`add_healer_to_job` injects the POST script into the job definition. The workflow
planner then writes it into the `.dag` file automatically.

---

## Demo workflows

Self-contained demos that show the healer in action on the local site:

```bash
# OOM fix: hello fails exit 137 → healer increases memory → retry succeeds
python workflows/hello_world.py

# Disk fix: analyze fails "No space left" → healer increases disk → retry succeeds
python workflows/disk_demo.py

# Sibling broadcast: 3 parallel compute jobs all fail OOM →
#   healer patches all 3 .sub files in one agent run → all retry and succeed
python workflows/sibling_demo.py
```

Monitor a running workflow:

```bash
pegasus-status --long <submit_dir>
tail -f <submit_dir>/00/00/*.healer.log
```

---

## Inspecting a failed run

```bash
# Show evidence collected for all failed jobs (no LLM)
python scripts/pegasus_inspect.py /path/to/run_dir

# Run the full agent on a failed job (requires LLM_API_KEY)
python scripts/pegasus_inspect_v3.py /path/to/run_dir --agent --verbose

# Focus on one job
python scripts/pegasus_inspect_v3.py /path/to/run_dir \
  --agent --job mifaser_mifaser_ARS --verbose
```

---

## Runtime files

The healer writes these files next to each job's `.out` / `.err`:

```
00/00/
  jobname.healer.log        trace of every POST script invocation
  jobname.healer_thread     LangGraph thread_id for checkpoint resume
  jobname.agent_report      structured JSON: diagnosis + fix + policy decision
```

Persistent databases (in `$HOME` by default, override with env vars):

```
~/.pegasus_healer_checkpoint.db   LangGraph state between invocations
~/.pegasus_healer_memory.db       episodic fix history (written on success)
```

| Env var | Default | Purpose |
|---|---|---|
| `HEALER_CHECKPOINT` | `~/.pegasus_healer_checkpoint.db` | LangGraph SQLite checkpoint |
| `HEALER_MEMORY_DB` | `~/.pegasus_healer_memory.db` | Episodic memory store |

---

## Running tests

```bash
# Unit tests — no LLM, no live services needed
pytest tests/unit tests/contract tests/graph -v

# Integration tests with real run fixtures (no LLM needed for tool tests)
pytest tests/integration/test_run0023_diagnosis.py -v -k "not llm"

# Full integration with real LLM
export LLM_MODEL=anthropic/claude-opus-4-6
export LLM_API_KEY=sk-ant-...
pytest tests/integration/ -v -s
```

---

## Safety invariants

| Invariant | Enforcement |
|---|---|
| No action without diagnosis | Graph: action loop requires `diagnosis` in state |
| Deterministic policy overrides LLM | `PolicyEngine` runs after every LLM fix proposal |
| No forbidden actions | `MODIFY_EXECUTABLE`, `MODIFY_ALGORITHM` always STOP |
| Bounded retries | Attempt counter checked before every graph re-entry |
| No scientific logic changes | Policy STOP on any executable or algorithm modification |
