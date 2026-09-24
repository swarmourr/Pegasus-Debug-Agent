"""Scheduler integration — thin bridge to Pegasus.client."""

from Pegasus.healer.scheduler.client import get_pegasus_client, get_condor_history

__all__ = ["get_pegasus_client", "get_condor_history"]
