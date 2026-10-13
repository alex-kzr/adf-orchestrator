"""Ledger/checkpoint crash boundaries and selected-branch invariants."""

from __future__ import annotations

import copy
import json
import unittest
from unittest.mock import patch
from uuid import uuid4

from langgraph.checkpoint.memory import InMemorySaver
from langsmith import get_tracing_context

from adf_orchestrator.engine import NO_RETRY, RETRYABLE, WorkflowEngine, stable_id
from adf_orchestrator.ledger import LedgerError, Record
from adf_orchestrator.planning import graph_from_plan
from adf_orchestrator.validation import parse_graph


def workflow(types: list[str]) -> Record:
    return {
        "schema_version": "1",
        "nodes": [{"id": f"n{index}", "type": kind, "config": {}} for index, kind in enumerate(types)],
        "edges": [
            {"source": f"n{index}", "target": f"n{index + 1}", "source_port": "out", "target_port": "in"}
            for index in range(len(types) - 1)
        ],
    }


class FakeLedger:
    def __init__(self, graph: Record) -> None:
        self.snapshot: Record = {
            "id": str(uuid4()),
            "graph_json": graph,
            "root_base_sha": "1" * 40,
            "status": "queued",
            "task": {
                "id": str(uuid4()),
                "title": "Test task",
                "description": "Change fixture",
                "expected_files": ["src"],
            },
            "project": {"checks": [{"id": "unit", "argv": ["python", "-m", "unittest"]}]},
            "policy": {"limits": {"max_retries": 2}},
            "agent": {"id": "developer", "config_hash": "a" * 64, "config_version_id": "developer-version"},
            "agent_config_version_id": "developer-version",
            "agents_by_role": {"Reviewer": {"config_version_id": "reviewer-version"}},
            "pause_requested": False,
            "cancel_requested": False,
        }
        self.executions: dict[str, Record] = {}
        self.approvals: dict[str, Record] = {}
        self.intents: dict[str, Record] = {}
        self.node_results: dict[str, Record] = {}
        self.plans: list[Record] = []
        self.reject_code: str | None = None

    async def run(self, run_id: str) -> Record:
        return copy.deepcopy(self.snapshot)

    async def execute(self, run_id: str, intent: Record) -> Record:
        key = intent["execution_id"]
        if key not in self.intents and self.reject_code:
            raise LedgerError(self.reject_code)
        if key in self.intents and intent != self.intents[key]:
            raise LedgerError("IDEMPOTENCY_CONFLICT")
        self.intents[key] = copy.deepcopy(intent)
        self.executions.setdefault(key, {"id": key, "run_id": run_id, "operation": intent["operation"], "status": "created", "result": None,
            "request": {**copy.deepcopy(intent["request"]), "task_id": intent["task_id"],
                        "agent_config_version": intent.get("agent_config_version_id"),
                        "agent": {"config_version_id": intent.get("agent_config_version_id")}}})
        return copy.deepcopy(self.executions[key])

    async def execution(self, execution_id: str) -> Record:
        if execution_id not in self.executions:
            raise LedgerError("NOT_FOUND", 404)
        return copy.deepcopy(self.executions[execution_id])

    async def approval(self, run_id: str, action: str, snapshot: Record) -> Record:
        key = stable_id(run_id, snapshot["node_id"], 1, "approval")
        self.approvals.setdefault(key, {"id": key, "decision": None, "snapshot_hash": "f" * 64, "snapshot": snapshot})
        return copy.deepcopy(self.approvals[key])

    async def get_approval(self, approval_id: str) -> Record:
        return copy.deepcopy(self.approvals[approval_id])

    async def node(self, run_id: str, node_id: str, attempt: int, status: str, result: Record) -> None:
        self.node_results[node_id] = {"status": status, "attempt": attempt, "result": copy.deepcopy(result)}

    async def transition(self, run_id: str, status: str, result: Record) -> None:
        self.snapshot["status"] = status
        self.snapshot["result"] = result

    async def plan(self, run_id: str, plan: Record) -> Record:
        self.plans.append(copy.deepcopy(plan))
        self.snapshot["plan_hash"] = "b" * 64
        return {"tasks": plan["tasks"]}

    async def graph_proposal(self, run_id: str, plan_hash: str, graph: Record) -> None:
        self.snapshot["graph_proposal"] = copy.deepcopy(graph)

    def succeed(self, operation: str, result: Record | None = None) -> str:
        for key, intent in self.intents.items():
            if intent["operation"] == operation and self.executions[key]["status"] == "created":
                self.executions[key] = {**self.executions[key], "status": "succeeded", "result": result or {}}
                return key
        raise AssertionError(f"No pending {operation}")


class EngineTests(unittest.IsolatedAsyncioTestCase):
    def test_mock_cli_intents_preserve_public_json_payload_for_each_pinned_role(self) -> None:
        payloads = {
            "planner": {"plan": {"tasks": [], "unknowns": ["fixture"]}},
            "agent_task": {"mock_files": {"src/a.py": "VALUE = 1\n"}},
            "review": {"review": {"approved": True, "findings": []}},
        }
        raw = workflow(["start", "planner", "agent_task", "test", "review", "git_commit", "end"])
        ledger = FakeLedger(raw)
        ledger.snapshot["agent"] = {"config_version_id": "native-developer", "provider": "codex"}
        ledger.snapshot["agent_versions"] = {}
        outcomes = {"n2": {"status": "succeeded", "execution_id": "writer", "result": {"worktree_key": "workspace"}}}
        engine = WorkflowEngine(ledger, InMemorySaver())
        for operation, index in [("planner", 1), ("agent_task", 2), ("review", 4)]:
            with self.subTest(operation=operation):
                version = "mock-" + operation
                encoded = json.dumps(payloads[operation], indent=2)
                raw["nodes"][index]["config"] = {"agent_config_version_id": version}
                ledger.snapshot["task"].update(
                    description=encoded, acceptance_criteria=["Do not append text to structured mock payload."]
                )
                ledger.snapshot["agent_versions"][version] = {"config_version_id": version, "provider": "mock"}
                graph = parse_graph(raw)
                intent = engine.intent(graph, graph.nodes[index], ledger.snapshot, outcomes, 1, operation)
                self.assertEqual(intent["agent_config_version_id"], version)
                self.assertEqual(intent["request"]["task_prompt"], encoded)
                self.assertEqual(json.loads(intent["request"]["task_prompt"]), payloads[operation])
                if operation == "planner":
                    self.assertIn("plan_schema", intent["request"])

    def test_mock_role_default_and_frozen_bound_task_payload_are_used(self) -> None:
        raw = workflow(["start", "agent_task", "review", "end"])
        ledger = FakeLedger(raw)
        child = "child-task"
        payload = '{"review":{"approved":true,"findings":[]}}'
        raw["nodes"][1]["config"]["task_id"] = child
        ledger.snapshot["task_snapshots"] = {
            child: {"id": child, "description": payload, "expected_files": ["src/a.py"]}
        }
        ledger.snapshot["agent"] = {"config_version_id": "native-developer", "provider": "codex"}
        ledger.snapshot["agents_by_role"]["Reviewer"]["provider"] = "mock"
        graph = parse_graph(raw)
        outcomes = {"n1": {"status": "succeeded", "execution_id": "writer", "result": {"worktree_key": "workspace"}}}
        intent = WorkflowEngine(ledger, InMemorySaver()).intent(
            graph, graph.nodes[2], ledger.snapshot, outcomes, 1, "review"
        )
        self.assertEqual(intent["request"]["task_prompt"], payload)
        self.assertEqual(intent["task_id"], child)
        self.assertEqual(intent["agent_config_version_id"], "reviewer-version")

    def test_native_cli_prompts_keep_criteria_and_planner_instructions(self) -> None:
        raw = workflow(["start", "planner", "agent_task", "review", "end"])
        ledger = FakeLedger(raw)
        ledger.snapshot["agent"] = {"config_version_id": "native-developer", "provider": "codex"}
        ledger.snapshot["agent_versions"] = {"native": {"config_version_id": "native", "provider": "codex"}}
        outcomes = {"n2": {"status": "succeeded", "execution_id": "writer", "result": {"worktree_key": "workspace"}}}
        for operation, index in [("planner", 1), ("agent_task", 2), ("review", 3)]:
            with self.subTest(operation=operation):
                raw["nodes"][index]["config"] = {"agent_config_version_id": "native"}
                ledger.snapshot["task"].update(
                    description='{"result":"native text"}', acceptance_criteria=["Preserve native acceptance criteria."]
                )
                graph = parse_graph(raw)
                intent = WorkflowEngine(ledger, InMemorySaver()).intent(
                    graph, graph.nodes[index], ledger.snapshot, outcomes, 1, operation
                )
                self.assertIn(
                    "\nAcceptance criteria:\nPreserve native acceptance criteria.", intent["request"]["task_prompt"]
                )
                if operation == "planner":
                    self.assertIn("No repository writes.", intent["request"]["task_prompt"])
                    self.assertIn("JSON schema", intent["request"]["task_prompt"])

    async def test_manual_resolution_records_the_exact_continuation_attempt(self) -> None:
        raw = workflow(["start", "agent_task", "git_commit", "end"])
        graph = parse_graph(raw)
        ledger = FakeLedger(raw)
        ledger.snapshot["policy"] = {"max_retries": 0}
        engine = WorkflowEngine(ledger, InMemorySaver())
        original = stable_id(ledger.snapshot["id"], "n3", 1, "integrate")
        ledger.snapshot["resolution"] = {"approval_id": "resolution", "tree_sha": "4" * 40}
        ledger.approvals["resolution"] = {
            "decision": "approve",
            "snapshot": {"execution_id": original, "tree_sha": "4" * 40},
        }
        outcomes = {
            "n0": {"status": "succeeded"},
            "n1": {"status": "succeeded", "execution_id": "writer", "result": {"tree_sha": "2" * 40}},
            "n2": {
                "status": "succeeded",
                "execution_id": "commit",
                "result": {"tree_sha": "2" * 40, "commit_sha": "3" * 40},
            },
        }
        dispatched: list[Record] = []

        async def dispatch(run_id: str, node_id: str, attempt: int, intent: Record) -> Record:
            dispatched.append(copy.deepcopy(intent))
            await ledger.node(run_id, node_id, attempt, "dispatched", {"execution_id": intent["execution_id"]})
            if attempt == 1:
                return {
                    "status": "failed",
                    "result": {"error_code": "INTEGRATION_CONFLICT", "integration_status": "needs_resolution"},
                }
            return {
                "status": "succeeded",
                "result": {
                    "tree_sha": "4" * 40,
                    "commit_sha": "5" * 40,
                    "branch": "factory/integration/" + run_id,
                    "integration_status": "passed",
                    "test_results": [{"check_id": "unit", "status": "passed", "tree_sha": "4" * 40}],
                },
            }

        with patch.object(engine, "await_execution", side_effect=dispatch), patch("adf_orchestrator.engine.interrupt"):
            await engine.finish(graph, graph.nodes[-1], ledger.snapshot, outcomes)
        self.assertEqual(ledger.snapshot["status"], "completed")
        self.assertEqual([intent["attempt"] for intent in dispatched], [1, 2])
        self.assertTrue(dispatched[1]["request"]["manual_resolution"])
        self.assertEqual(dispatched[1]["request"]["target_execution_id"], original)
        self.assertEqual(ledger.node_results["n3"]["attempt"], 2)
        self.assertEqual(ledger.node_results["n3"]["status"], "succeeded")
        self.assertEqual(ledger.node_results["n3"]["result"]["execution_id"], dispatched[1]["execution_id"])

    async def test_zero_retry_provider_failure_does_not_create_continuation(self) -> None:
        ledger = FakeLedger(workflow(["start", "agent_task", "end"]))
        ledger.snapshot["policy"] = {"max_retries": 0}
        engine = WorkflowEngine(ledger, InMemorySaver())
        await engine.advance(ledger.snapshot)
        key = next(iter(ledger.executions))
        ledger.executions[key] = {"id": key, "status": "failed", "result": {"error_code": "PROVIDER_TRANSIENT"}}
        await engine.advance(ledger.snapshot)
        self.assertEqual(ledger.snapshot["status"], "failed")
        self.assertEqual(len(ledger.intents), 1)
        self.assertEqual(ledger.node_results["n1"]["attempt"], 1)
        self.assertEqual(ledger.node_results["n1"]["status"], "failed")

    async def test_approval_precedes_writer_and_replay_never_duplicates_intent(self) -> None:
        ledger = FakeLedger(workflow(["start", "approval", "agent_task", "test", "review", "git_commit", "end"]))
        saver = InMemorySaver()
        engine = WorkflowEngine(ledger, saver)
        await engine.advance(ledger.snapshot)
        self.assertFalse(ledger.intents)
        approval = next(iter(ledger.approvals.values()))
        approval["decision"] = "approve"
        await engine.advance(ledger.snapshot)
        self.assertEqual(len(ledger.intents), 1)
        execution_id = ledger.succeed(
            "agent_task",
            {"tree_sha": "2" * 40, "base_sha": "1" * 40, "worktree_key": "tree1", "developer_id": "developer"},
        )
        # Replace engine/cache after result persisted and before next checkpoint.
        engine = WorkflowEngine(ledger, saver)
        await engine.advance(ledger.snapshot)
        self.assertEqual(len(ledger.intents), 2)
        self.assertEqual([intent["operation"] for intent in ledger.intents.values()].count("agent_task"), 1)
        ledger.succeed(
            "test",
            {
                "test_results": [{"check_id": "unit", "status": "passed", "tree_sha": "2" * 40, "exit_code": 0}],
                "tree_sha": "2" * 40,
            },
        )
        await engine.advance(ledger.snapshot)
        review_intent = next(intent for intent in ledger.intents.values() if intent["operation"] == "review")
        self.assertEqual(review_intent["request"]["target_execution_id"], execution_id)
        self.assertEqual(review_intent["agent_config_version_id"], "reviewer-version")
        ledger.succeed(
            "review",
            {
                "review": {
                    "reviewer_id": "reviewer",
                    "developer_id": "developer",
                    "approved": True,
                    "tree_sha": "2" * 40,
                }
            },
        )
        await engine.advance(ledger.snapshot)
        ledger.succeed("git_commit", {"commit_sha": "3" * 40, "tree_sha": "2" * 40})
        await engine.advance(ledger.snapshot)
        ledger.succeed(
            "integrate",
            {
                "integration_status": "passed",
                "branch": "factory/integration/" + ledger.snapshot["id"],
                "tree_sha": "2" * 40,
                "commit_sha": "5" * 40,
                "test_results": [{"check_id": "unit", "status": "passed", "exit_code": 0, "tree_sha": "2" * 40}],
            },
        )
        await engine.advance(ledger.snapshot)
        self.assertEqual(ledger.snapshot["status"], "completed")
        self.assertEqual(len(ledger.intents), 5)

    async def test_failed_auth_is_not_retried_and_run_fails(self) -> None:
        ledger = FakeLedger(workflow(["start", "agent_task", "end"]))
        engine = WorkflowEngine(ledger, InMemorySaver())
        await engine.advance(ledger.snapshot)
        key = next(iter(ledger.executions))
        ledger.executions[key] = {"id": key, "status": "failed", "result": {"error_code": "AUTH_REQUIRED"}}
        await engine.advance(ledger.snapshot)
        self.assertEqual(len(ledger.intents), 1)
        self.assertEqual(ledger.snapshot["status"], "failed")

    async def test_uncertain_never_becomes_replacement_writer(self) -> None:
        ledger = FakeLedger(workflow(["start", "agent_task", "end"]))
        engine = WorkflowEngine(ledger, InMemorySaver())
        await engine.advance(ledger.snapshot)
        key = next(iter(ledger.executions))
        ledger.executions[key]["status"] = "uncertain"
        self.assertIsNone(await engine.advance(ledger.snapshot))
        self.assertEqual(len(ledger.intents), 1)

    async def test_transient_retry_has_distinct_bounded_execution_ids(self) -> None:
        ledger = FakeLedger(workflow(["start", "agent_task", "end"]))
        engine = WorkflowEngine(ledger, InMemorySaver())
        await engine.advance(ledger.snapshot)
        now = [100.0]
        engine.clock = lambda: now[0]
        for index in range(3):
            key = next(key for key, value in ledger.executions.items() if value["status"] == "created")
            ledger.executions[key] = {"id": key, "status": "failed", "result": {"error_code": "PROVIDER_TRANSIENT"}}
            await engine.advance(ledger.snapshot)
            if index < 2:
                self.assertIsNone(await engine.advance(ledger.snapshot))
                now[0] += 31
                await engine.advance(ledger.snapshot)
        self.assertEqual(len(ledger.intents), 3)
        self.assertEqual({intent["attempt"] for intent in ledger.intents.values()}, {1, 2, 3})

    async def test_pause_capacity_interrupt_preserves_same_id(self) -> None:
        ledger = FakeLedger(workflow(["start", "agent_task", "end"]))
        ledger.reject_code = "RUN_PAUSED"
        engine = WorkflowEngine(ledger, InMemorySaver())
        await engine.advance(ledger.snapshot)
        self.assertFalse(ledger.intents)
        ledger.snapshot["pause_requested"] = True
        self.assertIsNone(await engine.advance(ledger.snapshot))
        ledger.snapshot["pause_requested"] = False
        ledger.reject_code = None
        await engine.advance(ledger.snapshot)
        self.assertEqual(len(ledger.intents), 1)

    async def test_three_parallel_nodes_dispatch_in_distinct_contexts(self) -> None:
        graph = workflow(["start", "parallel_fork", "agent_task", "agent_task", "agent_task", "parallel_join", "end"])
        graph["nodes"][1]["config"] = {"join_id": "n5"}
        graph["nodes"][5]["config"] = {"fork_id": "n1"}
        graph["edges"] = [
            {"source": a, "target": b}
            for a, b in [
                ("n0", "n1"),
                ("n1", "n2"),
                ("n1", "n3"),
                ("n1", "n4"),
                ("n2", "n5"),
                ("n3", "n5"),
                ("n4", "n5"),
                ("n5", "n6"),
            ]
        ]
        ledger = FakeLedger(graph)
        engine = WorkflowEngine(ledger, InMemorySaver())
        await engine.advance(ledger.snapshot)
        self.assertEqual(len(ledger.intents), 3)
        self.assertEqual(len({intent["request"]["worktree_key"] for intent in ledger.intents.values()}), 3)
        self.assertNotIn("n5", ledger.node_results)

    async def test_unselected_condition_branch_skips_without_external_job(self) -> None:
        graph = {
            "schema_version": "1",
            "nodes": [
                {"id": "s", "type": "start"},
                {
                    "id": "c",
                    "type": "condition",
                    "config": {"field": "status", "operator": "eq", "value": "succeeded", "source_node": "s"},
                },
                {"id": "yes", "type": "planner"},
                {"id": "no", "type": "agent_task"},
                {"id": "e", "type": "end"},
            ],
            "edges": [
                {"source": "s", "target": "c"},
                {"source": "c", "target": "yes", "source_port": "true"},
                {"source": "c", "target": "no", "source_port": "false"},
                {"source": "yes", "target": "e"},
                {"source": "no", "target": "e"},
            ],
        }
        ledger = FakeLedger(graph)
        engine = WorkflowEngine(ledger, InMemorySaver())
        await engine.advance(ledger.snapshot)
        self.assertEqual([intent["operation"] for intent in ledger.intents.values()], ["planner"])
        self.assertEqual(ledger.node_results["no"]["status"], "skipped")

    async def test_test_failure_dispatches_fresh_repair_before_retesting(self) -> None:
        ledger = FakeLedger(workflow(["start", "agent_task", "test", "review", "git_commit", "end"]))
        engine = WorkflowEngine(ledger, InMemorySaver())
        await engine.advance(ledger.snapshot)
        original = ledger.succeed(
            "agent_task", {"tree_sha": "2" * 40, "base_sha": "1" * 40, "worktree_key": "original"}
        )
        await engine.advance(ledger.snapshot)
        failed = next(key for key, intent in ledger.intents.items() if intent["operation"] == "test")
        ledger.executions[failed] = {
            "id": failed,
            "status": "failed",
            "result": {"error_code": "TEST_FAILED", "test_results": [{"status": "failed", "exit_code": 1}]},
        }
        await engine.advance(ledger.snapshot)
        repair = next(
            (key, intent)
            for key, intent in ledger.intents.items()
            if intent["operation"] == "agent_task" and key != original
        )
        self.assertEqual(repair[1]["request"]["repair_source_execution_id"], original)
        self.assertNotEqual(repair[0], original)
        ledger.succeed("agent_task", {"tree_sha": "4" * 40, "base_sha": "1" * 40, "worktree_key": "repaired"})
        await engine.advance(ledger.snapshot)
        latest_test = next(
            intent for intent in ledger.intents.values() if intent["operation"] == "test" and intent["attempt"] == 2
        )
        self.assertEqual(latest_test["request"]["target_execution_id"], repair[0])

    def test_patch_safety_conflicts_are_not_model_retries(self) -> None:
        for code in ("INVALID_PROVIDER_RESULT", "VERSION_CONFLICT", "IDEMPOTENCY_CONFLICT", "SECRET_DETECTED",
                     "INVALID_PATH", "LEASE_LOST", "CHECK_UNAVAILABLE", "PATCH_IO_FAILED", "EXECUTION_UNCERTAIN"):
            self.assertIn(code, NO_RETRY)
            self.assertNotIn(code, RETRYABLE)
        self.assertIn("TEST_FAILED", RETRYABLE)
        self.assertIn("REVIEW_REJECTED", RETRYABLE)

    async def test_passing_tests_on_another_tree_cannot_unlock_review(self) -> None:
        ledger = FakeLedger(workflow(["start", "agent_task", "test", "review", "git_commit", "end"]))
        engine = WorkflowEngine(ledger, InMemorySaver())
        await engine.advance(ledger.snapshot)
        ledger.succeed("agent_task", {"tree_sha": "2" * 40, "base_sha": "1" * 40, "worktree_key": "original"})
        await engine.advance(ledger.snapshot)
        ledger.succeed(
            "test",
            {
                "tree_sha": "9" * 40,
                "test_results": [{"check_id": "unit", "status": "passed", "exit_code": 0, "tree_sha": "9" * 40}],
            },
        )
        await engine.advance(ledger.snapshot)
        self.assertEqual(ledger.snapshot["status"], "failed")
        self.assertEqual(ledger.node_results["n2"]["result"]["error_code"], "TEST_TREE_MISMATCH")
        self.assertFalse(any(item["operation"] == "review" for item in ledger.intents.values()))

    async def test_fresh_repair_keeps_the_child_task_scope(self) -> None:
        graph = workflow(["start", "agent_task", "test", "review", "git_commit", "end"])
        task_id = str(uuid4())
        graph["nodes"][1]["config"] = {
            "task_id": task_id,
            "description": "Child work",
            "expected_files": ["src/child.py"],
        }
        ledger = FakeLedger(graph)
        ledger.snapshot["plan"] = {
            "tasks": [
                {
                    "id": task_id,
                    "description": "Child work",
                    "expected_files": ["src/child.py"],
                    "acceptance_criteria": ["child tests pass"],
                }
            ]
        }
        engine = WorkflowEngine(ledger, InMemorySaver())
        await engine.advance(ledger.snapshot)
        ledger.succeed("agent_task", {"tree_sha": "2" * 40, "base_sha": "1" * 40, "worktree_key": "original"})
        await engine.advance(ledger.snapshot)
        failed = next(key for key, value in ledger.intents.items() if value["operation"] == "test")
        ledger.executions[failed] = {"id": failed, "status": "failed", "result": {"error_code": "TEST_FAILED"}}
        await engine.advance(ledger.snapshot)
        repair = next(value for value in ledger.intents.values() if value["node_id"].startswith("__repair_"))
        self.assertEqual(repair["task_id"], task_id)
        self.assertEqual(repair["request"]["allowed_write_paths"], ["src/child.py"])
        self.assertIn("Child work", repair["request"]["task_prompt"])

    def test_explicit_target_ignores_unrelated_ancestor_evidence(self) -> None:
        raw = workflow(["start", "agent_task", "agent_task", "test", "review", "git_commit", "end"])
        raw["nodes"][3]["config"] = {"target_node": "n1"}
        ledger = FakeLedger(raw)
        engine = WorkflowEngine(ledger, InMemorySaver())
        outcomes = {
            "n0": {"status": "succeeded"},
            "n1": {
                "status": "succeeded",
                "execution_id": "first",
                "result": {"tree_sha": "2" * 40, "worktree_key": "first"},
            },
            "n2": {
                "status": "succeeded",
                "execution_id": "second",
                "result": {
                    "tree_sha": "3" * 40,
                    "worktree_key": "second",
                    "target_execution_id": "second",
                    "writing_node_id": "n2",
                },
            },
        }
        graph = parse_graph(raw)
        intent = engine.intent(graph, graph.nodes[3], ledger.snapshot, outcomes, 1, "test")
        self.assertEqual(intent["request"]["target_execution_id"], "first")
        self.assertEqual(intent["request"]["tree_sha"], "2" * 40)

    def test_dependency_base_excludes_unrelated_prior_wave_commits(self) -> None:
        tasks = [
            {
                "id": name,
                "title": name,
                "description": name,
                "expected_files": [f"src/{name}.py"],
                "acceptance_criteria": ["passes"],
                "dependency_ids": ["first"] if name == "last" else [],
            }
            for name in ["first", "unrelated", "last"]
        ]
        raw = graph_from_plan({"tasks": tasks})
        ledger = FakeLedger(raw)
        ledger.snapshot["plan"] = {"tasks": tasks}
        graph = parse_graph(raw)
        outcomes = {node.id: {"status": "succeeded"} for node in graph.nodes}
        for index in range(2):
            outcomes[f"agent_{index}"] = {
                "status": "succeeded",
                "execution_id": f"writer-{index}",
                "result": {"tree_sha": str(index + 2) * 40, "worktree_key": f"tree-{index}"},
            }
            outcomes[f"commit_{index}"] = {
                "status": "succeeded",
                "execution_id": f"commit-{index}",
                "result": {"commit_sha": str(index + 4) * 40},
            }
        node = next(node for node in graph.nodes if node.id == "agent_2")
        intent = WorkflowEngine(ledger, InMemorySaver()).intent(graph, node, ledger.snapshot, outcomes, 1, "agent_task")
        self.assertEqual(intent["dependency_execution_ids"], ["commit-0"])
        self.assertEqual(intent["request"]["predecessor_commits"], ["4" * 40])

    async def test_inherited_langsmith_environment_cannot_export_private_graph_state(self) -> None:
        ledger = FakeLedger(workflow(["start", "agent_task", "end"]))
        original = ledger.execute
        observed = []

        async def inspect(run_id: str, intent: Record) -> Record:
            observed.append(get_tracing_context().get("enabled"))
            return await original(run_id, intent)

        ledger.execute = inspect
        with patch.dict("os.environ", {"LANGSMITH_TRACING": "true"}):
            await WorkflowEngine(ledger, InMemorySaver()).advance(ledger.snapshot)
        self.assertEqual(observed, [False])

    async def test_expired_approval_without_a_decision_fails_without_dispatch(self) -> None:
        ledger = FakeLedger(workflow(["start", "approval", "agent_task", "end"]))
        engine = WorkflowEngine(ledger, InMemorySaver())
        await engine.advance(ledger.snapshot)
        approval = next(iter(ledger.approvals.values()))
        approval["expires_at"] = "1970-01-01T00:00:00+00:00"
        await engine.advance(ledger.snapshot)
        self.assertFalse(ledger.intents)
        self.assertEqual(ledger.snapshot["status"], "failed")
        self.assertEqual(ledger.node_results["n1"]["result"]["error_code"], "APPROVAL_EXPIRED")

    def test_approval_includes_manual_graph_bound_children_without_a_plan(self) -> None:
        raw = workflow(["start", "approval", "agent_task", "end"])
        ledger = FakeLedger(raw)
        child_id = str(uuid4())
        ledger.snapshot["task_snapshots"] = {
            ledger.snapshot["task"]["id"]: ledger.snapshot["task"],
            child_id: {"id": child_id, "expected_files": ["src/child.py"]},
        }
        approval = parse_graph(raw).nodes[1]
        snapshot = WorkflowEngine.approval_snapshot(ledger.snapshot, approval, {})
        self.assertEqual(snapshot["allowed_write_paths"], ["src", "src/child.py"])

    def test_manual_graph_bound_child_uses_its_pinned_prompt_and_scope(self) -> None:
        raw = workflow(["start", "agent_task", "end"])
        child_id = str(uuid4())
        raw["nodes"][1]["config"] = {"task_id": child_id}
        ledger = FakeLedger(raw)
        ledger.snapshot["task_snapshots"] = {
            child_id: {
                "id": child_id,
                "description": "Pinned child",
                "expected_files": ["src/child.py"],
                "acceptance_criteria": ["Child passes"],
            }
        }
        graph = parse_graph(raw)
        intent = WorkflowEngine(ledger, InMemorySaver()).intent(
            graph, graph.nodes[1], ledger.snapshot, {"n0": {"status": "succeeded"}}, 1, "agent_task"
        )
        self.assertEqual(intent["request"]["allowed_write_paths"], ["src/child.py"])
        self.assertIn("Pinned child", intent["request"]["task_prompt"])
        self.assertIn("Child passes", intent["request"]["task_prompt"])

    def test_manual_parallel_children_without_plan_keep_exact_test_target_scope(self) -> None:
        # Real-run regression: three manual graph children are pinned in task_snapshots,
        # while run.plan is absent and the root expected_files contains their whole union.
        names = ["alpha", "beta", "gamma"]
        task_ids = {name: str(uuid4()) for name in names}
        scopes = {name: [f"src/adf_demo/{name}.py", f"tests/test_{name}.py"] for name in names}
        raw = {
            "schema_version": "1",
            "nodes": [
                {"id": "s", "type": "start"},
                {"id": "f", "type": "parallel_fork", "config": {"join_id": "j"}},
                {"id": "j", "type": "parallel_join", "config": {"fork_id": "f"}},
                {"id": "e", "type": "end"},
            ],
            "edges": [{"source": "s", "target": "f"}, {"source": "j", "target": "e"}],
        }
        for name in names:
            raw["nodes"].extend(
                [
                    {
                        "id": name + "_agent",
                        "type": "agent_task",
                        "config": {"task_id": task_ids[name], "expected_files": scopes[name]},
                    },
                    {"id": name + "_test", "type": "test", "config": {"target_node": name + "_agent"}},
                ]
            )
            raw["edges"].extend(
                [
                    {"source": "f", "target": name + "_agent"},
                    {"source": name + "_agent", "target": name + "_test"},
                    {"source": name + "_test", "target": "j"},
                ]
            )
        ledger = FakeLedger(raw)
        ledger.snapshot["plan"] = None
        ledger.snapshot["task"]["expected_files"] = [path for scope in scopes.values() for path in scope]
        ledger.snapshot["task_snapshots"] = {
            ledger.snapshot["task"]["id"]: ledger.snapshot["task"],
            **{
                task_ids[name]: {"id": task_ids[name], "description": name, "expected_files": scopes[name]}
                for name in names
            },
        }
        graph = parse_graph(raw)
        outcomes = {
            "s": {"status": "succeeded"},
            "f": {"status": "succeeded"},
            **{
                name + "_agent": {
                    "status": "succeeded",
                    "execution_id": str(uuid4()),
                    "result": {"tree_sha": str(index + 1) * 40, "worktree_key": name, "base_sha": "a" * 40},
                }
                for index, name in enumerate(names)
            },
        }
        engine = WorkflowEngine(ledger, InMemorySaver())
        for index, name in enumerate(names):
            node = next(node for node in graph.nodes if node.id == name + "_test")
            intent = engine.intent(graph, node, ledger.snapshot, outcomes, 1, "test")
            self.assertEqual(intent["task_id"], task_ids[name])
            self.assertEqual(intent["request"]["allowed_write_paths"], scopes[name])
            self.assertNotEqual(intent["request"]["allowed_write_paths"], ledger.snapshot["task"]["expected_files"])
            self.assertEqual(intent["request"]["target_execution_id"], outcomes[name + "_agent"]["execution_id"])
            self.assertEqual(intent["request"]["tree_sha"], str(index + 1) * 40)
