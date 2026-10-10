import unittest

from adf_orchestrator.validation import validate_graph, validate_plan


class SemanticsTests(unittest.TestCase):
    def test_invalid_ports_expression_and_condition_are_rejected(self) -> None:
        graph = {
            "schema_version": "1",
            "nodes": [
                {"id": "s", "type": "start", "config": {}},
                {
                    "id": "c",
                    "type": "condition",
                    "config": {"field": "__import__", "operator": "eval", "value": "1", "script": "danger"},
                },
                {"id": "e", "type": "end", "config": {}},
            ],
            "edges": [{"source": "s", "target": "c", "source_port": "true"}, {"source": "c", "target": "e"}],
        }
        errors = validate_graph(graph)
        self.assertTrue(any("unknown configuration" in error for error in errors))
        self.assertTrue(any("invalid output port" in error for error in errors))
        self.assertTrue(any("condition" in error for error in errors))

    def test_fork_must_reciprocate_and_all_branches_converge(self) -> None:
        graph = {
            "schema_version": "1",
            "nodes": [
                {"id": "s", "type": "start"},
                {"id": "f", "type": "parallel_fork", "config": {"join_id": "j"}},
                {"id": "a", "type": "agent_task"},
                {"id": "b", "type": "agent_task"},
                {"id": "j", "type": "parallel_join", "config": {"fork_id": "f"}},
                {"id": "e", "type": "end"},
            ],
            "edges": [
                {"source": a, "target": b}
                for a, b in [("s", "f"), ("f", "a"), ("f", "b"), ("a", "j"), ("b", "j"), ("j", "e")]
            ],
        }
        self.assertEqual(validate_graph(graph), [])
        graph["edges"][4]["target"] = "e"
        self.assertTrue(any("converge" in error for error in validate_graph(graph)))

    def task(self, key: str, dependencies: list[str]) -> dict:
        return {
            "id": key,
            "title": key,
            "description": "Implement bounded change",
            "acceptance_criteria": ["Test passes"],
            "expected_files": ["src/feature.py"],
            "dependency_ids": dependencies,
        }

    def test_structured_plan_dependencies_paths_and_limits(self) -> None:
        self.assertEqual(len(validate_plan({"tasks": [self.task("a", []), self.task("b", ["a"])]}).tasks), 2)
        with self.assertRaises(ValueError):
            validate_plan({"tasks": [self.task("a", ["b"]), self.task("b", ["a"])]})
        task = self.task("a", [])
        task["expected_files"] = ["../.ssh/id_rsa"]
        with self.assertRaises(ValueError):
            validate_plan({"tasks": [task]})
        with self.assertRaises(ValueError):
            validate_plan({"tasks": [self.task(str(index), []) for index in range(13)]})
