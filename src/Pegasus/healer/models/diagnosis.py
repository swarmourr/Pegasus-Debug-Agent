from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class FailureType(StrEnum):
    OUT_OF_MEMORY = "OUT_OF_MEMORY"
    DISK_EXCEEDED = "DISK_EXCEEDED"
    WALLTIME_EXCEEDED = "WALLTIME_EXCEEDED"
    TRANSIENT_INFRASTRUCTURE = "TRANSIENT_INFRASTRUCTURE"
    SCHEDULER_ADMISSION = "SCHEDULER_ADMISSION"
    MISSING_INPUT = "MISSING_INPUT"
    DATA_MISMATCH = "DATA_MISMATCH"       # wrong format, wrong dimensions, corrupt data
    SCRIPT_ERROR = "SCRIPT_ERROR"         # bug/env issue in the transformation script
    APPLICATION_ERROR = "APPLICATION_ERROR"
    LOGIC_VALIDATION = "LOGIC_VALIDATION"
    UNKNOWN = "UNKNOWN"


# Map failure type → fix category string (used by agents and catalog)
FAILURE_FIX_CATEGORY: dict[FailureType, str] = {
    FailureType.OUT_OF_MEMORY: "RESOURCE_MEMORY",
    FailureType.DISK_EXCEEDED: "RESOURCE_DISK",
    FailureType.WALLTIME_EXCEEDED: "RESOURCE_RUNTIME",
    FailureType.TRANSIENT_INFRASTRUCTURE: "RETRY_OR_SITE",
    FailureType.SCHEDULER_ADMISSION: "SCHEDULER_CONFIGURATION",
    FailureType.MISSING_INPUT: "DATA_BINDING",
    FailureType.DATA_MISMATCH: "DATA_CONVERSION",
    FailureType.SCRIPT_ERROR: "SCRIPT_PATCH",
    FailureType.APPLICATION_ERROR: "APPLICATION_DIAGNOSIS",
    FailureType.LOGIC_VALIDATION: "HUMAN_REVIEW",
    FailureType.UNKNOWN: "HUMAN_REVIEW",
}


class AlternativeCause(BaseModel):
    failure_type: FailureType
    confidence: float = Field(ge=0.0, le=1.0)
    explanation: str


class Diagnosis(BaseModel):
    failure_type: FailureType
    confidence: float = Field(ge=0.0, le=1.0)
    evidence_ids: list[str] = Field(default_factory=list)
    explanation: str
    missing_evidence: list[str] = Field(default_factory=list)
    alternative_causes: list[AlternativeCause] = Field(default_factory=list)
    recommended_fix_category: str | None = None
    requires_human_review: bool = False
    source: str = "RULE"          # "RULE" | "LLM"
    rule_version: str | None = None
    model_id: str | None = None   # populated when source == "LLM"
