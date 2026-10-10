"""Typed immutable workflow and planner contracts. No executable expressions."""

from __future__ import annotations

from enum import StrEnum
from pathlib import PurePosixPath
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator


class NodeType(StrEnum):
    START = "start"
    PLANNER = "planner"
    AGENT = "agent_task"
    CONDITION = "condition"
    FORK = "parallel_fork"
    JOIN = "parallel_join"
    TEST = "test"
    REVIEW = "review"
    APPROVAL = "approval"
    COMMIT = "git_commit"
    END = "end"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Node(StrictModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    type: NodeType
    config: dict[str, JsonValue] = Field(default_factory=dict)


class Edge(StrictModel):
    id: str | None = None
    source: str
    target: str
    source_port: Literal["out", "true", "false"] = "out"
    target_port: Literal["in"] = "in"


class Workflow(StrictModel):
    schema_version: Literal["1"]
    nodes: tuple[Node, ...] = Field(min_length=2, max_length=100)
    edges: tuple[Edge, ...] = Field(max_length=300)


class PlannedTask(StrictModel):
    id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    title: str = Field(min_length=1, max_length=240)
    description: str = Field(min_length=1, max_length=32768)
    acceptance_criteria: tuple[str, ...] = Field(min_length=1, max_length=30)
    expected_files: tuple[str, ...] = Field(min_length=1, max_length=100)
    dependency_ids: tuple[str, ...] = Field(default_factory=tuple, max_length=12)

    @field_validator("expected_files")
    @classmethod
    def safe_paths(cls, paths: tuple[str, ...]) -> tuple[str, ...]:
        for path in paths:
            candidate = PurePosixPath(path)
            if (
                candidate.is_absolute()
                or ".." in candidate.parts
                or "\\" in path
                or not path
                or len(path) > 1024
                or any(ord(character) < 32 for character in path)
            ):
                raise ValueError("Expected files must be project-relative Linux paths")
            if any(
                part in {".git", ".env", ".ssh", ".aws", ".codex", ".claude"} or part.startswith(".env.")
                for part in candidate.parts
            ):
                raise ValueError("Protected paths cannot be planned write scope")
        return paths


class Plan(StrictModel):
    tasks: tuple[PlannedTask, ...] = Field(min_length=1, max_length=12)
    unknowns: tuple[str, ...] = Field(default_factory=tuple, max_length=30)


class Limits(StrictModel):
    max_parallel_agents: int = Field(default=3, ge=1, le=3)
    max_tasks_per_plan: int = Field(default=12, ge=1, le=12)
    max_retries: int = Field(default=2, ge=0, le=2)
    max_execution_minutes: int = Field(default=30, ge=1, le=30)
    max_run_minutes: int = Field(default=180, ge=1, le=180)
    max_steps: int = Field(default=100, ge=1, le=1000)
    max_output_bytes: int = Field(default=20 * 1024 * 1024, ge=1, le=20 * 1024 * 1024)
    max_disk_bytes: int = Field(default=5 * 1024**3, ge=1, le=5 * 1024**3)


JsonObject = dict[str, JsonValue]
Status = Literal["succeeded", "failed", "cancelled", "skipped"]


class Outcome(StrictModel):
    status: Status
    execution_id: str | None = None
    result: JsonObject = Field(default_factory=dict)
    error_code: str | None = None
    selected: bool = True
