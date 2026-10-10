import unittest

from adf_orchestrator.planning import graph_from_plan
from adf_orchestrator.validation import validate_graph


class ProposalTests(unittest.TestCase):
    def task(self, task_id: str, deps: list[str]) -> dict:
        return {
            "id": task_id,
            "title": task_id,
            "description": "Implement approved scope",
            "expected_files": [task_id + ".py"],
            "acceptance_criteria": ["Tests pass"],
            "dependency_ids": deps,
        }

    def test_parallel_plan_generates_paired_waves_and_bound_evidence_nodes(self) -> None:
        plan = {"tasks": [self.task("a", []), self.task("b", []), self.task("c", []), self.task("d", ["a", "c"])]}
        graph = graph_from_plan(plan, {"Reviewer": {"config_version_id": "review-version"}})
        self.assertEqual(validate_graph(graph), [])
        types = [node["type"] for node in graph["nodes"]]
        self.assertEqual(types.count("parallel_fork"), 1)
        self.assertEqual(types.count("git_commit"), 4)
        dependent = next(node for node in graph["nodes"] if node["id"] == "agent_3")
        self.assertEqual(dependent["config"]["dependency_ids"], ["a", "c"])
        self.assertEqual(graph_from_plan(plan), graph_from_plan(plan))

    def test_max_twelve_task_proposal_remains_within_graph_budget(self) -> None:
        graph = graph_from_plan({"tasks": [self.task(f"t{index}", []) for index in range(12)]})
        self.assertLessEqual(len(graph["nodes"]), 100)
        self.assertEqual(validate_graph(graph), [])
