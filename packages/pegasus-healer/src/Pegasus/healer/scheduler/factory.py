"""Compatibility shim — use scheduler/client.py for new code."""
from Pegasus.healer.scheduler.client import get_pegasus_client, get_condor_history

# Alias used by collectors/submit_dir.py and graph/nodes.py
def get_scheduler_client():
    return get_pegasus_client()
