"""Immutable declarative LangGraph compiler with ledger-backed external effects."""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime
from typing import Annotated, Any, Callable, TypedDict, cast
from uuid import UUID, uuid5

from langchain_core.output_parsers import PydanticOutputParser
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt
from langsmith import tracing_context

from adf_orchestrator.ledger import Ledger, LedgerError, Record
from adf_orchestrator.models import Limits, Node, NodeType, Plan, Workflow
from adf_orchestrator.planning import graph_from_plan
from adf_orchestrator.telemetry import span
from adf_orchestrator.validation import adjacency, parse_graph, reachable, topological_order, validate_plan

TERMINAL = {"succeeded", "failed", "cancelled"}
NO_RETRY = {
    "AUTH_REQUIRED",
    "PERMISSION_DENIED",
    "CAPABILITY_UNSUPPORTED",
    "INVALID_WORKFLOW",
    "INVALID_PLAN",
    "LEASE_STALE",
    "EXECUTION_UNCERTAIN",
    "INTEGRATION_CONFLICT",
    "QUOTA_EXCEEDED",
    "OUTPUT_LIMIT",
    "DISK_LIMIT",
}
RETRYABLE = {
    "PROVIDER_RATE_LIMIT",
    "PROVIDER_TRANSIENT",
    "EXECUTION_TIMEOUT",
    "PROCESS_FAILED",
    "TEST_FAILED",
    "REVIEW_REJECTED",
}


def content_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def stable_id(run_id: str, node_id: str, attempt: int, operation: str) -> str:
    return str(uuid5(UUID(run_id), f"{node_id}:{attempt}:{operation}"))


def merge_outcomes(left: dict[str, Record], right: dict[str, Record]) -> dict[str, Record]:
    result = dict(left)
    for key, value in right.items():
        if key in result and result[key] != value:
            raise ValueError(f"Conflicting immutable node outcome: {key}")
        result[key] = value
    return result


class GraphState(TypedDict):
    run_id: str
    outcomes: Annotated[dict[str, Record], merge_outcomes]


class WorkflowEngine:
    def __init__(self, ledger: Ledger, checkpointer: Any, clock: Callable[[], float] = time.time) -> None:
        self.ledger, self.checkpointer, self.clock = ledger, checkpointer, clock
        self.cache: dict[tuple[str, str], Any] = {}

    @staticmethod
    def workflow(run: Record) -> Record:
        if run.get("graph_json"):
            return dict(run["graph_json"])
        if run.get("mode") != "planning":
            raise ValueError("Execution run requires an immutable graph")
        return {
            "schema_version": "1",
            "nodes": [{"id": "s", "type": "start"}, {"id": "plan", "type": "planner"}, {"id": "e", "type": "end"}],
            "edges": [{"source": "s", "target": "plan"}, {"source": "plan", "target": "e"}],
        }

    def compile(self, run: Record) -> Any:
        graph = parse_graph(self.workflow(run))
        graph_hash = content_hash(graph.model_dump(mode="json"))
        supplied_hash = run.get("graph_hash")
        if supplied_hash and supplied_hash != content_hash(self.workflow(run)):
            raise ValueError("Immutable graph hash mismatch")
        key = (run["id"], graph_hash)
        if key in self.cache:
            return self.cache[key]
        builder = StateGraph(GraphState)
        outgoing, incoming = adjacency(graph)
        for node in graph.nodes:

            async def operation(state: GraphState, current: Node = node) -> dict[str, dict[str, Record]]:
                # One metadata observation per checkpoint invocation, with the durable run parent.
                # Replays may produce another observation; no external operation is repeated.
                snapshot = await self.ledger.run(state["run_id"])
                context = {
                    "run_id": snapshot["id"],
                    "project_id": snapshot.get("project_id"),
                    "task_id": current.config.get("task_id") or snapshot.get("task_id"),
                    "node_run_id": stable_id(snapshot["id"], current.id, 1, "node"),
                    "traceparent": snapshot.get("traceparent"),
                }
                with span("workflow.node", context):
                    outcome = await self.run_node(graph, current, state)
                return {"outcomes": {current.id: outcome}}

            builder.add_node(node.id, operation)
        for node in graph.nodes:
            if node.type == NodeType.START:
                builder.add_edge(START, node.id)
            elif incoming[node.id]:
                # A barrier waits for selected and explicitly skipped branches alike.
                builder.add_edge(incoming[node.id], node.id)
            if node.type == NodeType.END:
                builder.add_edge(node.id, END)
        compiled = builder.compile(checkpointer=self.checkpointer)
        if len(self.cache) >= 128:
            self.cache.pop(next(iter(self.cache)))
        self.cache[key] = compiled
        return compiled

    async def run_node(self, graph: Workflow, node: Node, state: GraphState) -> Record:
        run_id, outcomes = state["run_id"], state["outcomes"]
        run = await self.ledger.run(run_id)
        outgoing, incoming = adjacency(graph)
        parent_outcomes = [outcomes[parent] for parent in incoming[node.id]]
        if run.get("cancel_requested") or run.get("control", {}).get("cancel_requested"):
            return await self.record(run_id, node, {"status": "cancelled"})
        if node.type not in {NodeType.END, NodeType.CONDITION} and any(
            outcome["status"] in {"failed", "cancelled"} for outcome in parent_outcomes
        ):
            return await self.record(run_id, node, {"status": "failed", "error_code": "PREDECESSOR_FAILED"})
        selected = not parent_outcomes or any(outcome["status"] != "skipped" for outcome in parent_outcomes)
        # Conditions send both branches through the static graph; unselected paths are skipped.
        for edge in graph.edges:
            if edge.target == node.id and edge.source in outcomes and edge.source_port in {"true", "false"}:
                if outcomes[edge.source].get("selected_port") != edge.source_port:
                    selected = False
        if not selected:
            return await self.record(run_id, node, {"status": "skipped", "selected": False})
        if node.type in {NodeType.START, NodeType.FORK, NodeType.JOIN}:
            return await self.record(run_id, node, {"status": "succeeded"})
        if node.type == NodeType.CONDITION:
            return await self.record(run_id, node, self.condition(node, parent_outcomes, outcomes))
        if node.type == NodeType.APPROVAL:
            snapshot = self.approval_snapshot(run, node, outcomes)
            approval = await self.ledger.approval(run_id, str(node.config.get("action", "execute_plan")), snapshot)
            await self.ledger.node(run_id, node.id, 1, "waiting_approval", {"approval_id": approval["id"]})
            interrupt({"kind": "approval", "approval_id": approval["id"], "snapshot_hash": approval["snapshot_hash"]})
            decision = await self.ledger.get_approval(approval["id"])
            expired = self.approval_expired(decision)
            approved = (
                decision.get("decision") == "approve"
                and decision.get("snapshot_hash") == approval["snapshot_hash"]
                and not expired
            )
            return await self.record(
                run_id,
                node,
                {
                    "status": "succeeded" if approved else "failed",
                    "approval_id": approval["id"],
                    "snapshot_hash": approval["snapshot_hash"],
                    "error_code": None if approved else "APPROVAL_EXPIRED" if expired else "APPROVAL_REJECTED",
                },
            )
        if node.type == NodeType.END:
            return await self.finish(graph, node, run, outcomes)
        operation = node.type.value
        limits = self.limits(run)
        repair_target: str | None = None
        feedback: Record = {}
        for attempt in range(1, limits.max_retries + 2):
            # On interrupt replay, the exact same intent resolves to the same execution.
            intent = self.intent(graph, node, run, outcomes, attempt, operation)
            if attempt > 1 and node.type in {NodeType.TEST, NodeType.REVIEW} and feedback:
                source = repair_target or intent["request"]["target_execution_id"]
                repair_intent = self.intent(graph, node, run, outcomes, attempt, "agent_task")
                repair_id = stable_id(run_id, "__repair_" + node.id, attempt, "agent_task")
                repair_intent.update(
                    execution_id=repair_id,
                    node_id="__repair_" + node.id,
                    task_id=intent["task_id"],
                    dependency_execution_ids=intent["dependency_execution_ids"],
                    agent_config_version_id=(
                        run.get("agents_by_role", {}).get("Developer") or run.get("agent") or {}
                    ).get("config_version_id"),
                )
                repair_intent["request"].update(
                    worktree_key=repair_id,
                    repair_source_execution_id=source,
                    repair_feedback=feedback,
                    allowed_write_paths=intent["request"]["allowed_write_paths"],
                    task_prompt=intent["request"]["task_prompt"],
                    predecessor_commits=intent["request"]["predecessor_commits"],
                )
                repaired = await self.await_execution(run_id, "__repair_" + node.id, attempt, repair_intent)
                if repaired["status"] != "succeeded":
                    return await self.record(
                        run_id,
                        node,
                        {
                            "status": repaired["status"],
                            "execution_id": repair_id,
                            "error_code": (repaired.get("result") or {}).get("error_code", "REPAIR_FAILED"),
                        },
                        attempt,
                    )
                repair_target = repair_id
                intent["request"].update(target_execution_id=repair_id, tree_sha=repaired["result"].get("tree_sha"))
                if node.type == NodeType.REVIEW:
                    test_intent = self.intent(graph, node, run, outcomes, attempt, "test")
                    test_id = stable_id(run_id, "__repair_tests_" + node.id, attempt, "test")
                    test_intent.update(
                        execution_id=test_id, node_id="__repair_tests_" + node.id, agent_config_version_id=None
                    )
                    test_intent["request"].update(
                        target_execution_id=repair_id, tree_sha=repaired["result"].get("tree_sha")
                    )
                    tested = await self.await_execution(run_id, "__repair_tests_" + node.id, attempt, test_intent)
                    if tested["status"] != "succeeded" or not self.checks_passed(
                        tested.get("result", {}).get("test_results")
                    ):
                        return await self.record(
                            run_id,
                            node,
                            {"status": "failed", "execution_id": test_id, "error_code": "REPAIR_TEST_FAILED"},
                            attempt,
                        )
                    intent["request"]["test_results"] = tested["result"]["test_results"]
            result = await self.await_execution(run_id, node.id, attempt, intent)
            if result["status"] == "succeeded":
                data: Record = dict(result.get("result") or {})
                if intent["request"].get("target_execution_id"):
                    data["target_execution_id"] = intent["request"]["target_execution_id"]
                    data["writing_node_id"] = intent["request"]["writing_node_id"]
                try:
                    await self.validate_evidence(node, run, outcomes, data, intent)
                except ValueError as exc:
                    return await self.record(
                        run_id,
                        node,
                        {"status": "failed", "execution_id": intent["execution_id"], "error_code": str(exc)},
                    )
                return await self.record(
                    run_id,
                    node,
                    {"status": "succeeded", "execution_id": intent["execution_id"], "result": data, "attempt": attempt},
                    attempt,
                )
            default_code = (
                "TEST_FAILED"
                if node.type == NodeType.TEST
                else "REVIEW_REJECTED"
                if node.type == NodeType.REVIEW
                else "PROCESS_FAILED"
            )
            code = str((result.get("result") or {}).get("error_code") or default_code)
            feedback = {
                "error_code": code,
                "test_results": (result.get("result") or {}).get("test_results", []),
                "findings": (result.get("result") or {}).get("review", {}).get("findings", []),
            }
            if (
                result["status"] == "cancelled"
                or code in NO_RETRY
                or code not in RETRYABLE
                or attempt > limits.max_retries
            ):
                return await self.record(
                    run_id,
                    node,
                    {
                        "status": result["status"],
                        "execution_id": intent["execution_id"],
                        "error_code": code,
                        "attempt": attempt,
                    },
                    attempt,
                )
            await self.ledger.node(run_id, node.id, attempt, "retry_wait", {"error_code": code})
            if node.type not in {NodeType.TEST, NodeType.REVIEW}:
                interrupt({"kind": "retry", "not_before": self.clock() + min(30, 2**attempt), "run_id": run_id})
        raise RuntimeError("Unreachable bounded retry state")

    async def record(self, run_id: str, node: Node, outcome: Record, attempt: int = 1) -> Record:
        await self.ledger.node(run_id, node.id, attempt, outcome["status"], outcome)
        return outcome

    async def await_execution(self, run_id: str, node_id: str, attempt: int, intent: Record) -> Record:
        # The interrupt position is identical whether capacity admits or defers intent.
        # Private checkpoints retain the exact intent; UI/telemetry never sees this payload.
        try:
            execution = await self.ledger.execute(run_id, intent)
            if execution.get("status") not in TERMINAL:
                await self.ledger.node(run_id, node_id, attempt, "dispatched", {"execution_id": intent["execution_id"]})
        except LedgerError as exc:
            if exc.code not in {
                "CAPACITY_EXCEEDED",
                "PATH_RESERVED",
                "QUOTA_BUSY",
                "RUN_PAUSED",
                "CONTROL_PENDING",
                "CONCURRENCY_LIMIT",
                "PATH_RESERVATION",
            }:
                raise
        interrupt({"kind": "execution", "execution_id": intent["execution_id"], "intent": intent})
        result = await self.ledger.execution(intent["execution_id"])
        if result.get("status") not in TERMINAL:
            raise ValueError("Cannot resume an execution without authoritative terminal result")
        return result

    def approval_expired(self, approval: Record) -> bool:
        expiry = approval.get("expires_at")
        return (
            isinstance(expiry, str)
            and datetime.fromisoformat(expiry.replace("Z", "+00:00")).timestamp() <= self.clock()
        )

    @staticmethod
    def limits(run: Record) -> Limits:
        raw = run.get("policy", {}).get("limits", run.get("policy", {}))
        if not raw and isinstance(run.get("policy", {}).get("manifest"), dict):
            raw = run["policy"]["manifest"].get("limits", {})
        allowed = set(Limits.model_fields)
        return Limits.model_validate({key: value for key, value in raw.items() if key in allowed})

    @staticmethod
    def checks_passed(checks: object) -> bool:
        return (
            isinstance(checks, list)
            and bool(checks)
            and all(
                isinstance(check, dict)
                and (check.get("passed") is True or check.get("status") == "passed")
                and check.get("exit_code", 0) == 0
                for check in checks
            )
        )

    @staticmethod
    def condition(node: Node, parents: list[Record], outcomes: dict[str, Record]) -> Record:
        source = outcomes.get(str(node.config.get("source_node")), parents[-1] if parents else {})
        field = str(node.config["field"])
        result = source.get("result", {})
        values = {
            "status": source.get("status"),
            "test_passed": result.get(
                "test_passed",
                WorkflowEngine.checks_passed(result.get("test_results")) if result.get("test_results") else None,
            ),
            "review_approved": result.get("review", {}).get("approved"),
            "planner_has_tasks": bool(result.get("plan", {}).get("tasks")),
        }
        actual, expected = values[field], node.config["value"]
        selected = (
            actual == expected if node.config["operator"] == "eq" else isinstance(expected, list) and actual in expected
        )
        return {"status": "succeeded", "selected_port": "true" if selected else "false"}

    @staticmethod
    def approval_snapshot(run: Record, node: Node, outcomes: dict[str, Record]) -> Record:
        tasks = list(run.get("task_snapshots", {}).values()) or [
            run.get("task", {}),
            *(run.get("plan") or {}).get("tasks", []),
        ]
        return {
            "action": node.config.get("action", "execute_plan"),
            "node_id": node.id,
            "expires_in_sec": node.config.get("expires_in_sec"),
            "graph_hash": run.get("graph_hash") or content_hash(WorkflowEngine.workflow(run)),
            "plan_hash": run.get("plan_hash"),
            "root_base_sha": run.get("root_base_sha"),
            "policy_hash": run.get("policy", {}).get("config_hash", content_hash(run.get("policy", {}))),
            "agent_config_hash": (run.get("agent") or run.get("agents_by_role", {}).get("Developer", {})).get(
                "config_hash"
            ),
            "dependency_hash": run.get("dependency_hash"),
            "agent_versions_hash": content_hash(run.get("agent_versions", run.get("agents_by_role", {}))),
            "outcomes_hash": content_hash(outcomes),
            "allowed_write_paths": sorted({path for task in tasks for path in task.get("expected_files", [])}),
            "tool_ids": (run.get("agent") or run.get("agents_by_role", {}).get("Developer", {})).get("tool_ids", []),
        }

    def intent(
        self, graph: Workflow, node: Node, run: Record, outcomes: dict[str, Record], attempt: int, operation: str
    ) -> Record:
        _, incoming = adjacency(graph)
        ancestors = reachable(node.id, incoming) - {node.id}
        ordered = [key for key in topological_order(graph) if key in ancestors]
        ancestor_results = [(key, outcomes.get(key, {})) for key in ordered]
        dependency_ids = [
            value["execution_id"]
            for _, value in ancestor_results
            if value.get("status") == "succeeded" and value.get("execution_id")
        ]
        commits = list(
            dict.fromkeys(
                value.get("result", {}).get("commit_sha")
                for _, value in ancestor_results
                if value.get("result", {}).get("commit_sha")
            )
        )
        if operation == "agent_task" and node.config.get("dependency_ids") is not None:
            required = set(cast(list[str], node.config.get("dependency_ids", [])))
            by_task = {
                str(item.config.get("task_id")): item
                for item in graph.nodes
                if item.type == NodeType.AGENT and item.config.get("task_id")
            }
            pending = list(required)
            while pending:
                current = pending.pop()
                definition = by_task.get(current)
                if definition:
                    for predecessor in cast(list[str], definition.config.get("dependency_ids", [])):
                        if str(predecessor) not in required:
                            required.add(str(predecessor))
                            pending.append(str(predecessor))
            selected_commits = []
            for key, value in ancestor_results:
                definition = next(item for item in graph.nodes if item.id == key)
                if definition.type != NodeType.COMMIT or not value.get("result", {}).get("commit_sha"):
                    continue
                writing_node = next(
                    (item for item in graph.nodes if item.id == definition.config.get("target_node")), None
                )
                if writing_node and str(writing_node.config.get("task_id")) in required:
                    selected_commits.append(value)
            dependency_ids = [value["execution_id"] for value in selected_commits]
            commits = list(dict.fromkeys(value["result"]["commit_sha"] for value in selected_commits))
            if len(selected_commits) < len(required):
                raise ValueError("Approved predecessor commits are missing from the dependency graph")
        candidates = [
            key
            for key, value in ancestor_results
            if value.get("status") == "succeeded"
            and value.get("result", {}).get("worktree_key")
            and next(item for item in graph.nodes if item.id == key).type == NodeType.AGENT
        ]
        target_node = str(node.config.get("target_node", candidates[-1] if candidates else ""))
        target = outcomes.get(target_node, {})
        target_result = target.get("result", {})
        related_results = [
            (key, value)
            for key, value in ancestor_results
            if key == target_node or value.get("result", {}).get("writing_node_id") == target_node
        ]
        inherited_target = self.last_evidence(related_results, "target_execution_id")
        limits = self.limits(run)
        task = run.get("task", {})
        target_definition = next((item for item in graph.nodes if item.id == target_node), None)
        bound_task_id = node.config.get("task_id") or (
            target_definition.config.get("task_id")
            if target_definition and operation in {"test", "review", "git_commit"}
            else None
        )
        if bound_task_id:
            task = run.get("task_snapshots", {}).get(str(bound_task_id)) or next(
                (item for item in (run.get("plan") or {}).get("tasks", []) if item["id"] == bound_task_id), task
            )
        agent_ref = node.config.get("agent_config_version_id")
        role_agent = run.get("agents_by_role", {}).get(
            "Planner" if operation == "planner" else "Reviewer" if operation == "review" else "Developer", {}
        )
        if not agent_ref and operation in {"planner", "review"}:
            agent_ref = role_agent.get("config_version_id")
        if not agent_ref and operation == "agent_task":
            agent_ref = (run.get("agent") or {}).get("config_version_id")
        selected_agent = run.get("agent_versions", {}).get(agent_ref, {})
        if not selected_agent:
            for snapshot in [role_agent, run.get("agent") or {}]:
                if snapshot.get("config_version_id") == agent_ref:
                    selected_agent = snapshot
                    break
        # Mock's public contract is a JSON payload, selected only from pinned agent snapshots.
        structured_mock = operation in {"planner", "agent_task", "review"} and selected_agent.get("provider") == "mock"
        task_prompt = str(node.config.get("description", task.get("description", task.get("title", ""))))
        if not structured_mock:
            task_prompt += "\nAcceptance criteria:\n" + "\n".join(
                str(item) for item in node.config.get("acceptance_criteria", task.get("acceptance_criteria", []))
            )
        request: Record = {
            "root_base_sha": run.get("root_base_sha"),
            "execution_base_sha": target_result.get("base_sha", run.get("root_base_sha")),
            "task_prompt": task_prompt,
            "predecessor_commits": commits,
            "check_ids": node.config.get(
                "check_ids", [check["id"] for check in run.get("project", {}).get("checks", [])]
            ),
            "allowed_write_paths": node.config.get("expected_files", task.get("expected_files", [])),
            "timeout_sec": min(
                int(cast(int, node.config.get("timeout_sec", limits.max_execution_minutes * 60))),
                limits.max_execution_minutes * 60,
            ),
            "max_steps": limits.max_steps,
            "max_output_bytes": limits.max_output_bytes,
            "max_disk_bytes": limits.max_disk_bytes,
            "worktree_key": stable_id(run["id"], node.id, attempt, operation),
            "env_allowlist": ["PATH", "HOME", "LANG", "TERM"],
        }
        if len(request["task_prompt"]) > 32768:
            raise ValueError("Task prompt and criteria exceed the bounded request budget")
        if operation in {"test", "review", "git_commit"}:
            if not target.get("execution_id"):
                raise ValueError("Execution operation requires an ancestor worktree")
            request.update(
                {
                    "target_execution_id": inherited_target or target["execution_id"],
                    "writing_node_id": target_node,
                    "tree_sha": self.last_evidence(related_results, "tree_sha") or target_result.get("tree_sha"),
                    "developer_id": target_result.get(
                        "developer_id",
                        (run.get("agent") or run.get("agents_by_role", {}).get("Developer", {})).get("id"),
                    ),
                }
            )
        if operation == "review":
            request["test_results"] = self.last_evidence(related_results, "test_results")
        if operation == "git_commit":
            request["review"] = (
                outcomes.get(str(node.config.get("review_node")), {}).get("result", {}).get("review")
                if node.config.get("review_node")
                else self.last_evidence(related_results, "review")
            )
            request["test_results"] = (
                outcomes.get(str(node.config.get("test_node")), {}).get("result", {}).get("test_results")
                if node.config.get("test_node")
                else self.last_evidence(related_results, "test_results")
            )
        if operation == "planner":
            request["plan_schema"] = {
                "tasks": [
                    {
                        "id": "task-slug",
                        "title": "...",
                        "description": "...",
                        "acceptance_criteria": ["..."],
                        "expected_files": ["relative/path"],
                        "dependency_ids": [],
                    }
                ],
                "unknowns": [],
            }
            if not structured_mock:
                request["task_prompt"] += (
                    "\n"
                    + PydanticOutputParser(pydantic_object=Plan).get_format_instructions()
                    + "\nNo repository writes."
                )
        return {
            "execution_id": stable_id(run["id"], node.id, attempt, operation),
            "node_id": node.id,
            "attempt": attempt,
            "operation": operation,
            "task_id": bound_task_id or task.get("id"),
            "agent_config_version_id": agent_ref,
            "dependency_execution_ids": dependency_ids,
            "request": request,
        }

    @staticmethod
    def last_evidence(ancestors: list[tuple[str, Record]], key: str) -> Any:
        for _, value in reversed(ancestors):
            if key in value.get("result", {}):
                return value["result"][key]
        return None

    async def validate_evidence(
        self, node: Node, run: Record, outcomes: dict[str, Record], result: Record, intent: Record
    ) -> None:
        if node.type == NodeType.PLANNER:
            plan = validate_plan(
                result.get("plan"),
                min(
                    int(cast(int, node.config.get("max_tasks", self.limits(run).max_tasks_per_plan))),
                    self.limits(run).max_tasks_per_plan,
                ),
            )
            raw_plan = plan.model_dump(mode="json")
            persisted = await self.ledger.plan(run["id"], {**raw_plan, "source_plan_hash": content_hash(raw_plan)})
            latest = await self.ledger.run(run["id"])
            proposal = graph_from_plan(persisted, latest.get("agents_by_role", {}))
            await self.ledger.graph_proposal(run["id"], latest["plan_hash"], proposal)
        elif node.type == NodeType.TEST:
            checks = result.get("test_results")
            if not self.checks_passed(checks):
                raise ValueError("TEST_EVIDENCE_MISSING")
            checks = cast(list[Record], checks)
            expected_tree = intent["request"].get("tree_sha")
            if (
                not expected_tree
                or result.get("tree_sha") != expected_tree
                or any(check.get("tree_sha") != expected_tree for check in checks)
            ):
                raise ValueError("TEST_TREE_MISMATCH")
            if {check.get("check_id", check.get("id")) for check in checks} != set(intent["request"]["check_ids"]):
                raise ValueError("TEST_CHECKS_MISSING")
        elif node.type == NodeType.REVIEW:
            review = result.get("review") or {}
            if (
                review.get("approved") is not True
                or not review.get("tree_sha")
                or not review.get("reviewer_id")
                or review.get("reviewer_id") == review.get("developer_id")
            ):
                raise ValueError("INDEPENDENT_REVIEW_MISSING")
            if review["tree_sha"] != intent["request"].get("tree_sha"):
                raise ValueError("REVIEW_TREE_MISMATCH")
        elif node.type == NodeType.COMMIT:
            if not result.get("commit_sha") or not result.get("tree_sha"):
                raise ValueError("COMMIT_EVIDENCE_MISSING")
            if result["tree_sha"] != intent["request"].get("review", {}).get("tree_sha"):
                raise ValueError("COMMIT_TREE_MISMATCH")

    async def finish(self, graph: Workflow, node: Node, run: Record, outcomes: dict[str, Record]) -> Record:
        active = [value for value in outcomes.values() if value.get("selected", True) and value["status"] != "skipped"]
        if any(value["status"] != "succeeded" for value in active):
            status = "cancelled" if any(value["status"] == "cancelled" for value in active) else "failed"
            await self.ledger.transition(run["id"], status, {"error_code": "WORKFLOW_FAILED"})
            return await self.record(run["id"], node, {"status": status})
        agent_nodes = [
            item
            for item in graph.nodes
            if item.type == NodeType.AGENT and outcomes.get(item.id, {}).get("status") == "succeeded"
        ]
        commits = [
            outcomes[key]["result"]["commit_sha"]
            for key in topological_order(graph)
            if outcomes.get(key, {}).get("result", {}).get("commit_sha")
        ]
        if not agent_nodes:
            await self.ledger.transition(
                run["id"], "completed", {**(run.get("result") or {}), "mode": "planning", "integration_status": None}
            )
            return await self.record(run["id"], node, {"status": "succeeded"})
        if len(set(commits)) < len(agent_nodes):
            await self.ledger.transition(run["id"], "failed", {"error_code": "TASK_COMMIT_MISSING"})
            return await self.record(run["id"], node, {"status": "failed", "error_code": "TASK_COMMIT_MISSING"})
        intent = self.intent(graph, node, run, outcomes, 1, "integrate")
        intent["request"]["integration_commits"] = list(dict.fromkeys(commits))
        result = await self.await_execution(run["id"], node.id, 1, intent)
        data = result.get("result") or {}
        integration_status = data.get("integration_status")
        if integration_status == "needs_resolution" or data.get("error_code") == "INTEGRATION_CONFLICT":
            current = await self.ledger.run(run["id"])
            if not (current.get("result") or {}).get("original_execution_id"):
                await self.ledger.transition(
                    run["id"],
                    "paused",
                    {**data, "integration_status": "needs_resolution", "original_execution_id": intent["execution_id"]},
                )
            await self.ledger.node(
                run["id"],
                node.id,
                1,
                "waiting_approval",
                {"error_code": "INTEGRATION_CONFLICT", "original_execution_id": intent["execution_id"]},
            )
            interrupt({"kind": "resolution", "original_execution_id": intent["execution_id"], "run_id": run["id"]})
            latest = await self.ledger.run(run["id"])
            resolution = latest.get("resolution") or (latest.get("result") or {}).get("resolution", {})
            approval = await self.ledger.get_approval(resolution["approval_id"])
            snapshot = approval.get("snapshot", {})
            if (
                approval.get("decision") != "approve"
                or snapshot.get("execution_id", snapshot.get("original_execution_id")) != intent["execution_id"]
                or snapshot.get("tree_sha", snapshot.get("resolution_tree_sha")) != resolution.get("tree_sha")
            ):
                raise ValueError("Integration resolution approval is stale")
            resumed = self.intent(graph, node, latest, outcomes, 2, "integrate")
            resumed["request"].update(
                integration_commits=list(dict.fromkeys(commits)),
                manual_resolution=True,
                target_execution_id=intent["execution_id"],
                resolution_tree_sha=resolution["tree_sha"],
                resolution_approval_id=resolution["approval_id"],
            )
            result = await self.await_execution(run["id"], node.id, 2, resumed)
            data = result.get("result") or {}
            integration_status = data.get("integration_status")
            intent = resumed
        checks = data.get("test_results") or []
        check_ids = [check["id"] for check in run.get("project", {}).get("checks", [])]
        valid_checks = (
            self.checks_passed(checks)
            and len(checks) == len(check_ids)
            and {check.get("check_id") for check in checks} == set(check_ids)
            and bool(data.get("tree_sha"))
            and all(check.get("tree_sha") == data["tree_sha"] for check in checks)
        )
        if (
            result["status"] != "succeeded"
            or integration_status != "passed"
            or not valid_checks
            or not data.get("commit_sha")
            or data.get("branch") != "factory/integration/" + run["id"]
        ):
            await self.ledger.transition(run["id"], "failed", {"error_code": "INTEGRATION_EVIDENCE_MISSING", **data})
            return await self.record(
                run["id"], node, {"status": "failed", "result": data}, attempt=int(intent["attempt"])
            )
        await self.ledger.transition(run["id"], "completed", data)
        return await self.record(
            run["id"],
            node,
            {"status": "succeeded", "result": data, "execution_id": intent["execution_id"]},
            attempt=int(intent["attempt"]),
        )

    async def advance(self, run: Record) -> Record | None:
        # Graph checkpoints/intents contain private task instructions. Disable automatic
        # LangSmith export even when a parent shell enables it; allowlisted OTel is separate.
        with tracing_context(enabled=False):
            return await self._advance(run)

    async def _advance(self, run: Record) -> Record | None:
        if (
            run.get("status") == "paused"
            and not run.get("pause_requested")
            and not run.get("cancel_requested")
            and run.get("integration_status") != "needs_resolution"
        ):
            await self.ledger.transition(run["id"], "running", {})
            run = await self.ledger.run(run["id"])
        graph = self.compile(run)
        config = {
            "configurable": {"thread_id": run.get("thread_id", run["id"])},
            "max_concurrency": 3,
            "recursion_limit": 300,
        }
        snapshot = await graph.aget_state(config)
        if not snapshot.values:
            if run.get("pause_requested") or run.get("cancel_requested"):
                return None
            await self.ledger.transition(run["id"], "running", {})
            return cast(Record, await graph.ainvoke({"run_id": run["id"], "outcomes": {}}, config))
        resume: Record = {}
        for task in snapshot.tasks:
            for pending in task.interrupts:
                value = pending.value
                if value["kind"] == "execution":
                    try:
                        execution = await self.ledger.execution(value["execution_id"])
                    except LedgerError as exc:
                        if exc.status != 404 and exc.code != "NOT_FOUND":
                            raise
                        if run.get("pause_requested") or run.get("cancel_requested"):
                            continue
                        try:
                            await self.ledger.execute(run["id"], value["intent"])
                        except LedgerError as busy:
                            if busy.code not in {
                                "CAPACITY_EXCEEDED",
                                "PATH_RESERVED",
                                "QUOTA_BUSY",
                                "RUN_PAUSED",
                                "CONTROL_PENDING",
                                "CONCURRENCY_LIMIT",
                                "PATH_RESERVATION",
                            }:
                                raise
                        continue
                    if execution.get("status") in TERMINAL:
                        resume[pending.id] = {"terminal": True}
                elif value["kind"] == "approval":
                    approval = await self.ledger.get_approval(value["approval_id"])
                    if (
                        approval.get("decision") in {"approve", "reject"}
                        or approval.get("status")
                        in {
                            "expired",
                            "rejected",
                        }
                        or self.approval_expired(approval)
                    ):
                        resume[pending.id] = {"decided": True}
                elif value["kind"] == "resolution":
                    resolution = run.get("resolution") or (run.get("result") or {}).get("resolution", {})
                    if (
                        resolution.get("approval_id")
                        and not run.get("pause_requested")
                        and run.get("status") != "paused"
                    ):
                        approval = await self.ledger.get_approval(resolution["approval_id"])
                        if approval.get("decision") == "approve" and not self.approval_expired(approval):
                            resume[pending.id] = {"resolution_accepted": True}
                elif value["kind"] == "retry":
                    if self.clock() >= value["not_before"]:
                        resume[pending.id] = {"retry_due": True}
                elif value["kind"] == "capacity":
                    if not run.get("pause_requested") or run.get("cancel_requested"):
                        resume[pending.id] = {"retry_dispatch": True}
        if resume:
            return cast(Record, await graph.ainvoke(Command(resume=resume), config))
        if snapshot.next and not any(task.interrupts for task in snapshot.tasks):
            # Crash after checkpoint but before invoking the next superstep.
            return cast(Record, await graph.ainvoke(None, config))
        return None
