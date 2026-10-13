"""Reproduce late-commit repair drift and missing independent capability evidence."""
from __future__ import annotations

import copy
import unittest
from unittest.mock import AsyncMock

from langgraph.checkpoint.memory import InMemorySaver
from test_engine import FakeLedger, workflow

from adf_orchestrator.engine import WorkflowEngine
from adf_orchestrator.validation import parse_graph


class RepairProvenanceTests(unittest.IsolatedAsyncioTestCase):
    def fixture(self, late_commit: bool = False):
        raw = workflow(["start", "agent_task", "test", "review", "git_commit", "review", "end"])
        raw["nodes"][1]["config"] = {"agent_config_version_id": "gamma-version"}
        raw["nodes"][3]["config"] = {"target_node": "n1"}
        raw["nodes"][4]["config"] = {"target_node": "n1", "test_node": "n2", "review_node": "n3"}
        raw["nodes"][5]["config"] = {"target_node": "n1"}
        ledger = FakeLedger(raw)
        ledger.snapshot["agent"] = {"config_version_id": "other-developer"}
        ledger.snapshot["policy"] = {"limits": {"max_retries": 1 if late_commit else 0}}
        source_request = {"run_id": ledger.snapshot["id"], "task_id": ledger.snapshot["task"]["id"],
            "root_base_sha": "1" * 40, "execution_base_sha": "1" * 40, "predecessor_commits": [],
            "task_prompt": "Change fixture\nAcceptance criteria:\n", "allowed_write_paths": ["src"],
            "agent_config_version": "gamma-version", "agent": {"config_version_id": "gamma-version", "skills": [], "mcp_servers": []}}
        if late_commit and getattr(self, "repair_case", None) == "external":
            ledger.snapshot["external_dependencies"] = [{"commit_shas": ["e" * 40]}]
            source_request["predecessor_commits"] = ["e" * 40]
        if late_commit and getattr(self, "repair_case", None) == "role":
            raw["nodes"][1]["config"] = {}
            ledger.snapshot["agent"] = {}
            ledger.snapshot["agents_by_role"]["Developer"] = {"config_version_id": "gamma-version"}
        source = {"id": "writer", "run_id": ledger.snapshot["id"], "operation": "agent_task", "status": "succeeded", "epoch": 1,
            "request": source_request, "result": {"tree_sha": "2" * 40, "base_sha": "1" * 40, "worktree_key": "writer"}}
        source["capability_audit"] = {"execution_id": "writer", "run_id": ledger.snapshot["id"], "epoch": 1,
            "tree_sha": "2" * 40, "truncated": False, "events": []}
        ledger.executions["writer"] = source
        outcomes = {"n0": {"status": "succeeded"}, "n1": {"status": "succeeded", "execution_id": "writer", "result": source["result"]},
            "n2": {"status": "succeeded", "execution_id": "check", "result": {"target_execution_id": "writer", "writing_node_id": "n1", "tree_sha": "2" * 40,
                "test_results": [{"check_id": "unit", "status": "passed", "tree_sha": "2" * 40}]}},
            "n3": {"status": "succeeded", "result": {}},
            "n4": {"status": "succeeded", "execution_id": "later-commit", "result": {"commit_sha": "4" * 40}}}
        engine = WorkflowEngine(ledger, InMemorySaver())
        engine.validate_evidence = AsyncMock()
        return ledger, engine, parse_graph(raw), outcomes

    async def test_review_repair_keeps_original_dependencies_and_developer(self):
        ledger, engine, graph, outcomes = self.fixture(True)
        requests = []
        review_count = 0
        async def complete(run_id, node_id, attempt, intent):
            nonlocal review_count
            requests.append(copy.deepcopy(intent))
            if intent["operation"] == "review":
                review_count += 1
                return {"status": "failed", "result": {"error_code": "REVIEW_REJECTED"}} if review_count == 1 else {"status": "succeeded", "result": {}}
            if intent["operation"] == "agent_task":
                if intent["request"]["predecessor_commits"] != ledger.executions["writer"]["request"]["predecessor_commits"]:
                    return {"status": "failed", "result": {"error_code": "VERSION_CONFLICT"}}
                saved = copy.deepcopy(ledger.executions["writer"])
                saved.update(id=intent["execution_id"])
                saved["result"]["tree_sha"] = "5" * 40
                saved["capability_audit"].update(execution_id=intent["execution_id"], tree_sha="5" * 40)
                ledger.executions[intent["execution_id"]] = saved
                return {"status": "succeeded", "result": {"tree_sha": "5" * 40}}
            return {"status": "succeeded", "result": {"test_results": [{"check_id": "unit", "status": "passed", "tree_sha": "5" * 40}]}}
        engine.await_execution = complete
        result = await engine.run_node(graph, graph.nodes[5], {"run_id": ledger.snapshot["id"], "outcomes": outcomes})
        self.assertEqual(result["status"], "succeeded")
        repair = next(r for r in requests if r["operation"] == "agent_task")
        self.assertEqual(repair["agent_config_version_id"], "gamma-version")
        self.assertEqual(repair["request"]["predecessor_commits"], ledger.executions["writer"]["request"]["predecessor_commits"])
        self.assertEqual(repair["request"]["execution_base_sha"], "1" * 40)
        self.assertNotIn("later-commit", repair["dependency_execution_ids"])

    async def test_reviewer_receives_bound_selected_capability_audit(self):
        ledger, engine, graph, outcomes = self.fixture()
        source = ledger.executions["writer"]
        source["request"]["agent"].update(skills=[{"manifest": {"hash": "a" * 64}}],
            mcp_servers=[{"name": "markers", "manifest": {"allowed_tools": ["read_marker"]}}])
        source["capability_audit"]["events"] = [
            {"kind": "skill_loaded", "event_id": "skill-event", "sequence": 1, "name": "selected", "sha256": "a" * 64, "native": True},
            {"kind": "mcp_tool_call", "event_id": "mcp-event", "sequence": 2, "server_id": "markers", "tool_name": "read_marker", "status": "completed"}]
        requests = []
        async def review(run_id, node_id, attempt, intent):
            requests.append(intent)
            good = "mcp-event" in intent["request"]["task_prompt"] and "a" * 64 in intent["request"]["task_prompt"]
            return {"status": "succeeded" if good else "failed", "result": {"error_code": "REVIEW_REJECTED"}}
        engine.await_execution = review
        result = await engine.run_node(graph, graph.nodes[5], {"run_id": ledger.snapshot["id"], "outcomes": outcomes})
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(requests[0]["agent_config_version_id"], "reviewer-version")
        self.assertNotIn("skills", requests[0]["request"])

    async def test_stale_source_audit_cannot_start_reviewer(self):
        ledger, engine, graph, outcomes = self.fixture()
        ledger.executions["writer"]["request"]["agent"]["skills"] = [{"manifest": {"hash": "a" * 64}}]
        ledger.executions["writer"]["capability_audit"]["epoch"] = 0
        engine.await_execution = AsyncMock(return_value={"status": "succeeded", "result": {}})
        result = await engine.run_node(graph, graph.nodes[5], {"run_id": ledger.snapshot["id"], "outcomes": outcomes})
        self.assertEqual(result["status"], "failed")
        engine.await_execution.assert_not_called()

    async def test_source_tree_and_identity_mismatch_cannot_start_reviewer(self):
        for field, value in (("id", "other"), ("run_id", "another-run"), ("status", "uncertain")):
            with self.subTest(field=field):
                ledger, engine, graph, outcomes = self.fixture()
                ledger.executions["writer"][field] = value
                engine.await_execution = AsyncMock()
                result = await engine.run_node(graph, graph.nodes[5], {"run_id": ledger.snapshot["id"], "outcomes": outcomes})
                self.assertEqual(result["status"], "failed")
                engine.await_execution.assert_not_called()

    async def test_capability_context_cannot_exceed_prompt_bound(self):
        ledger, engine, graph, outcomes = self.fixture()
        ledger.snapshot["task"]["description"] = "x" * 32700
        engine.await_execution = AsyncMock()
        result = await engine.run_node(graph, graph.nodes[5], {"run_id": ledger.snapshot["id"], "outcomes": outcomes})
        self.assertEqual(result["status"], "failed")
        engine.await_execution.assert_not_called()

    async def test_review_repair_preserves_api_pinned_external_commits(self):
        self.repair_case = "external"
        await self.test_review_repair_keeps_original_dependencies_and_developer()

    async def test_review_repair_preserves_api_role_selected_developer(self):
        self.repair_case = "role"
        await self.test_review_repair_keeps_original_dependencies_and_developer()
