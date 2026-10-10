import unittest
from adf_orchestrator.validation import validate_graph

class GraphInvariantTests(unittest.TestCase):
    def graph(self) -> dict:
        return {"schema_version": "1", "nodes": [{"id": "s", "type": "start"}, {"id": "a", "type": "agent_task"}, {"id": "e", "type": "end"}], "edges": [{"source": "s", "target": "a"}, {"source": "a", "target": "e"}]}
    def test_valid_and_cycle(self) -> None:
        graph = self.graph()
        self.assertEqual(validate_graph(graph), [])
        graph["edges"].append({"source": "a", "target": "s"})
        self.assertIn("cycle", validate_graph(graph)[0])
    def test_unreachable_node_rejected(self) -> None:
        graph = self.graph()
        graph["nodes"].append({"id": "orphan", "type": "test"})
        self.assertTrue(validate_graph(graph))

    def test_malformed_values_fail_cleanly(self) -> None:
        graph = self.graph()
        graph["nodes"][1]["type"] = []
        self.assertTrue(validate_graph(graph))
        graph = self.graph()
        graph["edges"][0]["source"] = []
        self.assertTrue(validate_graph(graph))
