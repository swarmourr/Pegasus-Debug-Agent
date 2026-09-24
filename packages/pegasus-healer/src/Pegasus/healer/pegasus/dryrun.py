from __future__ import annotations

"""
DryRunRetryController — prints what it would apply, touches nothing.

Used in graph tests and local development to see the full remediation
chain without patching any .sub files or touching the filesystem.
"""

from datetime import datetime, timezone
from uuid import UUID

from Pegasus.healer.fixes.overlay import build_overlay
from Pegasus.healer.models.fixes import AppliedOverlay, FixProposal
from Pegasus.healer.pegasus.adapter import AttemptState, JobAttemptRef, RetryReceipt, Verification

_W = 60


class DryRunRetryController:
    """
    Drop-in replacement for DAGManRetryController that prints every
    action instead of executing it.

    apply_overlay  → prints the configuration diff
    authorize_retry → prints the retry authorization
    """

    def __init__(self) -> None:
        self._applied: list[AppliedOverlay] = []
        self._receipts: list[RetryReceipt] = []
        self._next_instance_id = 2

    async def apply_overlay(self, fix: FixProposal) -> AppliedOverlay:
        overlay = build_overlay(fix)
        self._applied.append(overlay)

        print(f"\n{'─' * _W}")
        print(f"[DRY RUN] apply_overlay  —  action: {fix.action.value}")
        print(f"  fix_id : {fix.fix_id}")
        print(f"  reason : {fix.justification}")
        print(f"  changes to .sub file:")
        for key, new_val in fix.proposed_configuration.items():
            old_val = fix.old_configuration.get(key, "—")
            print(f"    {key:30s}  {old_val}  →  {new_val}")
        print(f"  config hash:  {overlay.hash_before[:12]}  →  {overlay.hash_after[:12]}")
        print(f"  differs: {overlay.configs_differ()}")
        print(f"{'─' * _W}")

        return overlay

    async def authorize_retry(self, incident_id: UUID, fix_id: UUID) -> RetryReceipt:
        receipt = RetryReceipt(
            incident_id=str(incident_id),
            fix_id=str(fix_id),
            retry_job_instance_id=self._next_instance_id,
            authorized_at=datetime.now(timezone.utc).isoformat(),
        )
        self._receipts.append(receipt)
        self._next_instance_id += 1

        print(f"\n[DRY RUN] authorize_retry")
        print(f"  incident_id      : {incident_id}")
        print(f"  fix_id           : {fix_id}")
        print(f"  new_instance_id  : {receipt.retry_job_instance_id}")
        print(f"  → DAGMan would exit 1 and retry with new resources")

        return receipt

    async def inspect_attempt(self, ref: JobAttemptRef) -> AttemptState:
        return AttemptState(is_terminal=True, is_failed=True, exit_code=1)

    async def verify_retry_configuration(self, receipt: RetryReceipt) -> Verification:
        return Verification(consumed_new_config=True, actual_configuration={})

    async def apply_script_patches(self, fix: FixProposal) -> list[str]:
        """Print script patches without writing to disk."""
        for patch in fix.script_patches:
            print(f"\n[DRY RUN] script_patch  —  {patch.file_path}")
            print(f"  description : {patch.patch_description}")
            print(f"  backup would: {patch.file_path}.bak")
            orig_lines = patch.original_content.count("\n")
            new_lines  = patch.patched_content.count("\n")
            print(f"  size change : {orig_lines} → {new_lines} lines")
        return [p.file_path for p in fix.script_patches]

    async def cancel_pending_retry(self, incident_id: UUID) -> None:
        print(f"\n[DRY RUN] cancel_pending_retry  incident={incident_id}")

    # ── Inspection helpers ────────────────────────────────────────────────────

    @property
    def applied_overlays(self) -> list[AppliedOverlay]:
        return list(self._applied)

    @property
    def retry_receipts(self) -> list[RetryReceipt]:
        return list(self._receipts)
