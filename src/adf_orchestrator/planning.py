"""Derive a reviewable workflow proposal; never rewrite a published version."""

from __future__ import annotations

from adf_orchestrator.ledger import Record
from adf_orchestrator.validation import validate_graph, validate_plan


def graph_from_plan(plan: Record, agents_by_role: Record | None = None) -> Record:
    fields = {"id", "title", "description", "acceptance_criteria", "expected_files", "dependency_ids"}
    typed = validate_plan(
        {
            "tasks": [{key: value for key, value in task.items() if key in fields} for task in plan["tasks"]],
            "unknowns": plan.get("unknowns", []),
        }
    )
    roles = agents_by_role or {}
    nodes: list[Record] = [
        {"id": "start", "type": "start", "config": {}},
        {"id": "approve", "type": "approval", "config": {"action": "execute_plan"}},
    ]
    edges: list[Record] = []

    def link(source: str, target: str) -> None:
        edges.append({"source": source, "target": target, "source_port": "out", "target_port": "in"})

    link("start", "approve")
    complete: set[str] = set()
    prior = "approve"
    task_index = {task.id: index for index, task in enumerate(typed.tasks)}
    wave = 0
    while len(complete) < len(typed.tasks):
        selected = [task for task in typed.tasks if task.id not in complete and set(task.dependency_ids) <= complete]
        if not selected:
            raise ValueError("Plan dependencies cannot be scheduled")
        fork, join = f"fork_{wave}", f"join_{wave}"
        if len(selected) > 1:
            nodes.extend(
                [
                    {"id": fork, "type": "parallel_fork", "config": {"join_id": join}},
                    {"id": join, "type": "parallel_join", "config": {"fork_id": fork, "policy": "all_success"}},
                ]
            )
            link(prior, fork)
        for task in selected:
            index = task_index[task.id]
            agent, check, review, commit = (f"{prefix}_{index}" for prefix in ("agent", "check", "review", "commit"))
            agent_config: Record = {
                "task_id": task.id,
                "description": task.description,
                "acceptance_criteria": list(task.acceptance_criteria),
                "expected_files": list(task.expected_files),
                "dependency_ids": list(task.dependency_ids),
            }
            review_config: Record = {"target_node": agent}
            for role, config in [("Developer", agent_config), ("Reviewer", review_config)]:
                reference = roles.get(role, {}).get("config_version_id")
                if reference:
                    config["agent_config_version_id"] = reference
            nodes.extend(
                [
                    {"id": agent, "type": "agent_task", "config": agent_config},
                    {"id": check, "type": "test", "config": {"target_node": agent}},
                    {"id": review, "type": "review", "config": review_config},
                    {
                        "id": commit,
                        "type": "git_commit",
                        "config": {"target_node": agent, "test_node": check, "review_node": review},
                    },
                ]
            )
            link(fork if len(selected) > 1 else prior, agent)
            link(agent, check)
            link(check, review)
            link(review, commit)
            if len(selected) > 1:
                link(commit, join)
            else:
                prior = commit
        if len(selected) > 1:
            prior = join
        complete.update(task.id for task in selected)
        wave += 1
    nodes.append({"id": "end", "type": "end", "config": {}})
    link(prior, "end")
    graph: Record = {"schema_version": "1", "nodes": nodes, "edges": edges}
    errors = validate_graph(graph)
    if errors:
        raise ValueError("Generated proposal failed semantic validation: " + "; ".join(errors))
    return graph
