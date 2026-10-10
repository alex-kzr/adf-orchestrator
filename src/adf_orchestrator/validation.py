"""Declarative DAG validation, including ports and properly nested fork scopes."""

from __future__ import annotations

from collections import deque
from typing import Mapping

from pydantic import ValidationError

from adf_orchestrator.models import NodeType, Plan, Workflow

NODE_TYPES = {node.value for node in NodeType}
CONFIG_FIELDS: dict[NodeType, set[str]] = {
    NodeType.START: set(),
    NodeType.END: set(),
    NodeType.PLANNER: {"agent_config_version_id", "max_tasks"},
    NodeType.AGENT: {
        "task_id",
        "agent_config_version_id",
        "expected_files",
        "description",
        "acceptance_criteria",
        "dependency_ids",
    },
    NodeType.CONDITION: {"field", "operator", "value", "source_node"},
    NodeType.FORK: {"join_id"},
    NodeType.JOIN: {"fork_id", "policy"},
    NodeType.TEST: {"check_ids", "target_node", "timeout_sec"},
    NodeType.REVIEW: {"agent_config_version_id", "target_node"},
    NodeType.APPROVAL: {"action", "expires_in_sec"},
    NodeType.COMMIT: {"target_node", "test_node", "review_node"},
}


def adjacency(graph: Workflow) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    outgoing: dict[str, list[str]] = {node.id: [] for node in graph.nodes}
    incoming: dict[str, list[str]] = {node.id: [] for node in graph.nodes}
    for edge in graph.edges:
        if edge.source in outgoing and edge.target in incoming:
            outgoing[edge.source].append(edge.target)
            incoming[edge.target].append(edge.source)
    return outgoing, incoming


def reachable(start: str, links: Mapping[str, list[str]], stop: str | None = None) -> set[str]:
    seen: set[str] = set()
    todo = [start]
    while todo:
        item = todo.pop()
        if item in seen:
            continue
        seen.add(item)
        if item != stop:
            todo.extend(links[item])
    return seen


def topological_order(graph: Workflow) -> list[str]:
    outgoing, incoming = adjacency(graph)
    degree = {key: len(value) for key, value in incoming.items()}
    queue = deque(sorted(key for key, value in degree.items() if value == 0))
    result: list[str] = []
    while queue:
        item = queue.popleft()
        result.append(item)
        for target in outgoing[item]:
            degree[target] -= 1
            if degree[target] == 0:
                queue.append(target)
    return result


def parse_graph(value: object) -> Workflow:
    graph = Workflow.model_validate(value)
    errors = semantic_errors(graph)
    if errors:
        raise ValueError("; ".join(errors))
    return graph


def validate_graph(value: object) -> list[str]:
    try:
        graph = Workflow.model_validate(value)
    except ValidationError as exc:
        return [
            f"{'.'.join(str(part) for part in error['loc'])}: {error['msg']}"
            for error in exc.errors(include_input=False, include_url=False)
        ]
    return semantic_errors(graph)


def semantic_errors(graph: Workflow) -> list[str]:
    errors: list[str] = []
    nodes = {node.id: node for node in graph.nodes}
    if len(nodes) != len(graph.nodes):
        return ["Node IDs must be unique"]
    starts = [node.id for node in graph.nodes if node.type == NodeType.START]
    ends = [node.id for node in graph.nodes if node.type == NodeType.END]
    if len(starts) != 1 or len(ends) != 1:
        return ["Exactly one start and end required"]
    if any(edge.source not in nodes or edge.target not in nodes for edge in graph.edges):
        return ["Edge references unknown node"]
    if len({(edge.source, edge.target) for edge in graph.edges}) != len(graph.edges):
        return ["Duplicate edge"]
    outgoing, incoming = adjacency(graph)
    if len(topological_order(graph)) != len(nodes):
        return ["Workflow has a cycle"]
    if incoming[starts[0]] or outgoing[ends[0]]:
        errors.append("Start cannot have input; end cannot have output")
    if reachable(starts[0], outgoing) != set(nodes) or reachable(ends[0], incoming) != set(nodes):
        errors.append("All nodes must be reachable from start and able to reach end")
    for edge in graph.edges:
        expected = {"true", "false"} if nodes[edge.source].type == NodeType.CONDITION else {"out"}
        if edge.source_port not in expected:
            errors.append(f"{edge.source}: invalid output port {edge.source_port}")
    for node in graph.nodes:
        unknown = set(node.config) - CONFIG_FIELDS[node.type]
        if unknown:
            errors.append(f"{node.id}: unknown configuration fields {','.join(sorted(unknown))}")
        for key in ("target_node", "test_node", "review_node", "source_node"):
            ref = node.config.get(key)
            if ref is not None and (
                not isinstance(ref, str)
                or ref not in nodes
                or ref == node.id
                or node.id not in reachable(ref, outgoing)
            ):
                errors.append(f"{node.id}: {key} must refer to an ancestor")
        for key, expected_kind in (
            ("test_node", NodeType.TEST),
            ("review_node", NodeType.REVIEW),
            ("target_node", NodeType.AGENT),
        ):
            ref = node.config.get(key)
            if isinstance(ref, str) and ref in nodes and nodes[ref].type != expected_kind:
                errors.append(f"{node.id}: {key} references the wrong node type")
        for key in ("expected_files", "check_ids", "acceptance_criteria", "dependency_ids"):
            items = node.config.get(key)
            if items is not None and (
                not isinstance(items, list)
                or len(items) > 100
                or any(not isinstance(item, str) or not item for item in items)
            ):
                errors.append(f"{node.id}: {key} must be bounded string list")
        paths = node.config.get("expected_files")
        if isinstance(paths, list) and all(isinstance(path, str) for path in paths):
            from adf_orchestrator.models import PlannedTask

            try:
                PlannedTask.safe_paths(tuple(str(path) for path in paths))
            except ValueError:
                errors.append(f"{node.id}: invalid or protected expected write path")
        for key, upper in [("timeout_sec", 1800), ("expires_in_sec", 10800), ("max_tasks", 12)]:
            value = node.config.get(key)
            if value is not None and (type(value) is not int or not 1 <= value <= upper):
                errors.append(f"{node.id}: {key} exceeds the bounded integer registry")
        if node.type == NodeType.APPROVAL and node.config.get("action", "execute_plan") not in (
            "execute_plan",
            "execution",
            "trusted-development",
        ):
            errors.append(f"{node.id}: unknown approval action")
        if node.type == NodeType.CONDITION:
            if not isinstance(node.config.get("field"), str) or node.config.get("field") not in {
                "status",
                "test_passed",
                "review_approved",
                "planner_has_tasks",
            }:
                errors.append(f"{node.id}: invalid condition field")
            if (
                not isinstance(node.config.get("operator"), str)
                or node.config.get("operator") not in {"eq", "in"}
                or "value" not in node.config
            ):
                errors.append(f"{node.id}: condition needs operator eq/in and typed value")
            if node.config.get("operator") == "in" and not isinstance(node.config.get("value"), list):
                errors.append(f"{node.id}: condition in requires a list")
            values = node.config.get("value") if node.config.get("operator") == "in" else [node.config.get("value")]
            if isinstance(values, list):
                if node.config.get("field") == "status" and any(
                    value not in ("succeeded", "failed", "cancelled", "skipped") for value in values
                ):
                    errors.append(f"{node.id}: status condition requires known status values")
                elif node.config.get("field") in ("test_passed", "review_approved", "planner_has_tasks") and any(
                    type(value) is not bool for value in values
                ):
                    errors.append(f"{node.id}: evidence condition requires boolean values")
            ports = [edge.source_port for edge in graph.edges if edge.source == node.id]
            if sorted(ports) != ["false", "true"]:
                errors.append(f"{node.id}: condition requires exactly true and false output")
        elif node.type != NodeType.FORK and len(outgoing[node.id]) > 1:
            errors.append(f"{node.id}: fan-out requires parallel_fork or condition")
        if node.type == NodeType.FORK:
            join_id = node.config.get("join_id")
            if (
                not isinstance(join_id, str)
                or join_id not in nodes
                or nodes[join_id].type != NodeType.JOIN
                or nodes[join_id].config.get("fork_id") != node.id
            ):
                errors.append(f"{node.id}: fork needs a reciprocal parallel_join")
                continue
            if len(outgoing[node.id]) < 2:
                errors.append(f"{node.id}: fork needs at least two branches")
            branches = [reachable(branch, outgoing, join_id) for branch in outgoing[node.id]]
            for scope in branches:
                if join_id not in scope or any(
                    target not in scope for member in scope - {join_id} for target in outgoing[member]
                ):
                    errors.append(f"{node.id}: every branch must converge at paired join")
            for index, scope in enumerate(branches):
                if any((scope & other) - {join_id} for other in branches[index + 1 :]):
                    errors.append(f"{node.id}: branches overlap before paired join")
            full_scope = set().union(*branches)
            for member in full_scope - {join_id}:
                if any(source not in full_scope and source != node.id for source in incoming[member]):
                    errors.append(f"{node.id}: external edge enters fork scope")
            for member in full_scope - {join_id}:
                nested = nodes[member]
                if nested.type == NodeType.FORK and nested.config.get("join_id") not in full_scope - {join_id}:
                    errors.append(f"{node.id}: fork scopes must be properly nested")
        elif node.type == NodeType.JOIN:
            fork_id = node.config.get("fork_id")
            if (
                not isinstance(fork_id, str)
                or fork_id not in nodes
                or nodes[fork_id].type != NodeType.FORK
                or nodes[fork_id].config.get("join_id") != node.id
            ):
                errors.append(f"{node.id}: join needs a reciprocal parallel_fork")
            if node.config.get("policy", "all_success") != "all_success":
                errors.append(f"{node.id}: join only supports all_success")
    return sorted(set(errors))


def validate_plan(value: object, max_tasks: int = 12) -> Plan:
    plan = Plan.model_validate(value)
    if len(plan.tasks) > max_tasks:
        raise ValueError("Plan task budget exceeded")
    ids = {task.id for task in plan.tasks}
    if len(ids) != len(plan.tasks):
        raise ValueError("Plan task IDs must be unique")
    dependencies = {task.id: list(task.dependency_ids) for task in plan.tasks}
    for task in plan.tasks:
        if (
            len(set(task.dependency_ids)) != len(task.dependency_ids)
            or task.id in task.dependency_ids
            or not set(task.dependency_ids) <= ids
        ):
            raise ValueError("Plan dependencies must be unique existing tasks")
    complete: set[str] = set()
    while len(complete) < len(ids):
        ready = {task for task in ids - complete if set(dependencies[task]) <= complete}
        if not ready:
            raise ValueError("Plan task dependencies have a cycle")
        complete.update(ready)
    return plan
