"""Opt-in sibling API contract regressions; no provider or real Git execution.

Run: python -m pip install -e ../api && python tests/wire_api.py
SQLite tests exercise the actual authenticated routes and SQL ledger. PostgreSQL
checkpoint/restart and real CLI/Git acceptance are separate Docker/WSL checks.
"""

from __future__ import annotations

import secrets
import tempfile
import unittest
from pathlib import Path
from typing import Any

import httpx
from adf_api.config import Settings
from adf_api.db import Database
from adf_api.main import create_app
from adf_api.models import Base
from langgraph.checkpoint.memory import InMemorySaver

from adf_orchestrator.engine import WorkflowEngine
from adf_orchestrator.ledger import HttpLedger, Record
from adf_orchestrator.service import Service
from adf_orchestrator.validation import validate_graph


class SemanticValidator:
    def validate(self, graph: dict[str, Any]) -> dict[str, Any]:
        errors = validate_graph(graph)
        return {"valid": not errors, "errors": errors}


class WireTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory(prefix="adf-orchestrator-wire-")
        self.settings = Settings(
            admin_token=secrets.token_urlsafe(32),
            orchestrator_token=secrets.token_urlsafe(32),
            worker_token=secrets.token_urlsafe(32),
            allowed_roots=("/home/adf/ai-factory",),
            artifact_root=Path(self.directory.name) / "artifacts",
        )
        self.db = Database(self.settings.database_url)
        Base.metadata.create_all(self.db.engine)
        self.app = create_app(self.settings, self.db, SemanticValidator())
        self.admin = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app),
            base_url="http://api",
            headers={"Authorization": "Bearer " + self.settings.admin_token},
        )
        self.orch = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app),
            base_url="http://api",
            headers={"Authorization": "Bearer " + self.settings.orchestrator_token},
        )
        self.worker = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app),
            base_url="http://api",
            headers={"Authorization": "Bearer " + self.settings.worker_token},
        )
        self.ledger = HttpLedger(self.orch)
        self.engine = WorkflowEngine(self.ledger, InMemorySaver())
        self.project = await self.post(
            self.admin,
            "/api/projects",
            {
                "name": "wire fixture",
                "repo_path": "/home/adf/ai-factory/projects/test/repo",
                "allowed_root": "/home/adf/ai-factory",
                "trusted": True,
                "checks": [{"id": "unit", "argv": ["python", "-m", "unittest"]}],
            },
        )
        self.task = await self.post(
            self.admin,
            "/api/tasks",
            {
                "project_id": self.project["id"],
                "title": "Wire fixture",
                "description": "An isolated ledger fixture",
                "acceptance_criteria": ["fixture passes"],
                "expected_files": ["src/a.py"],
                "status": "ready",
            },
        )
        self.agents = {}
        for role in ["Planner", "Developer", "Reviewer", "Tester", "Integrator"]:
            self.agents[role] = await self.post(
                self.admin,
                "/api/agents",
                {
                    "name": role,
                    "role": role,
                    "provider": "mock",
                    "permission_profile": "observe" if role in {"Planner", "Reviewer"} else "trusted-development",
                },
            )
        self.policy = await self.post(
            self.admin,
            "/api/policies",
            {
                "name": "Fixture",
                "manifest": {"allow_commit": True, "trusted_repository": True, "allowed_write_paths": ["src/**"]},
            },
        )
        await self.post(
            self.worker,
            "/internal/workers/register",
            {
                "worker_id": "wire-worker",
                "capabilities": {"mock": {"ready": True}},
                "allowed_roots": ["/home/adf/ai-factory"],
            },
        )
        self.run: Record = {}

    async def asyncTearDown(self) -> None:
        await self.admin.aclose()
        await self.orch.aclose()
        await self.worker.aclose()
        self.db.engine.dispose()
        self.directory.cleanup()

    async def post(self, client: httpx.AsyncClient, path: str, body: Record | None = None) -> Record:
        response = await client.post(path, json=body, headers={"Idempotency-Key": secrets.token_hex(16)})
        self.assertTrue(response.is_success, f"{path}: {response.status_code} {response.text}")
        return response.json()

    async def seed_run(self) -> None:
        kinds = ["start", "approval", "agent_task", "test", "review", "git_commit", "end"]
        graph = {
            "schema_version": "1",
            "nodes": [{"id": f"n{i}", "type": kind} for i, kind in enumerate(kinds)],
            "edges": [{"source": f"n{i}", "target": f"n{i + 1}"} for i in range(len(kinds) - 1)],
        }
        workflow = await self.post(self.admin, "/api/workflows", {"name": "wire", "graph_json": graph})
        version = await self.post(self.admin, f"/api/workflows/{workflow['id']}/versions")
        result = await self.post(
            self.admin,
            "/api/runs",
            {
                "task_id": self.task["id"],
                "workflow_version_id": version["id"],
                "agent_config_version_id": self.agents["Developer"]["config_version_id"],
                "policy_id": self.policy["id"],
                "root_base_sha": "a" * 40,
            },
        )
        self.run = await self.ledger.run(result["id"])

    async def advance(self) -> Record:
        self.run = await self.ledger.run(self.run["id"])
        await self.engine.advance(self.run)
        self.run = await self.ledger.run(self.run["id"])
        return self.run

    async def approve(self, approval: Record | None = None) -> None:
        if approval is None:
            approval = (await self.ledger.run(self.run["id"]))["approvals"][0]
        await self.post(
            self.admin,
            f"/api/approvals/{approval['id']}/decision",
            {"decision": "approve", "snapshot_hash": approval["snapshot_hash"], "revision": approval["revision"]},
        )

    async def finish_job(self, operation: str, result: Record, status: str = "succeeded") -> Record:
        snapshot = await self.ledger.run(self.run["id"])
        job = next(
            item for item in snapshot["executions"] if item["operation"] == operation and item["status"] == "created"
        )
        claim = await self.post(self.worker, f"/internal/executions/{job['id']}/claim", {"worker_id": "wire-worker"})
        await self.post(
            self.worker,
            f"/internal/executions/{job['id']}/result",
            {"worker_id": "wire-worker", "epoch": claim["epoch"], "status": status, "result": result},
        )
        return claim

    @staticmethod
    def checks(tree: str = "b" * 40) -> list[Record]:
        return [{"check_id": "unit", "status": "passed", "exit_code": 0, "tree_sha": tree}]

    async def before_integration(self) -> None:
        await self.seed_run()
        await self.advance()
        self.assertEqual(self.run["executions"], [])
        await self.approve()
        await self.advance()
        self.developer_id = self.run["agents_by_role"]["Developer"]["id"]
        self.reviewer_id = self.run["agents_by_role"]["Reviewer"]["id"]
        await self.finish_job(
            "agent_task",
            {"tree_sha": "b" * 40, "base_sha": "a" * 40, "worktree_key": "fixture", "developer_id": self.developer_id},
        )
        await self.advance()
        await self.finish_job("test", {"tree_sha": "b" * 40, "test_results": self.checks()})
        await self.advance()
        await self.finish_job(
            "review",
            {
                "tree_sha": "b" * 40,
                "review": {
                    "tree_sha": "b" * 40,
                    "reviewer_id": self.reviewer_id,
                    "developer_id": self.developer_id,
                    "approved": True,
                },
            },
        )
        await self.advance()
        await self.finish_job("git_commit", {"tree_sha": "b" * 40, "commit_sha": "c" * 40})
        await self.advance()

    async def test_real_ledger_completion_never_applies_commits_twice(self) -> None:
        await self.before_integration()
        integrate = next(item for item in self.run["executions"] if item["operation"] == "integrate")
        self.assertEqual(integrate["request"]["predecessor_commits"], [])
        self.assertEqual(integrate["request"]["integration_commits"], ["c" * 40])
        await self.finish_job(
            "integrate",
            {
                "tree_sha": "b" * 40,
                "commit_sha": "d" * 40,
                "integration_status": "passed",
                "branch": "factory/integration/" + self.run["id"],
                "test_results": self.checks(),
            },
        )
        # New engine process retains checkpoint, replays exact intents against actual API.
        self.engine = WorkflowEngine(self.ledger, self.engine.checkpointer)
        await self.advance()
        self.assertEqual(self.run["status"], "completed")
        self.assertEqual(len(self.run["executions"]), 5)
        end_nodes = [node for node in self.run["nodes"] if node["node_id"] == "n6"]
        self.assertEqual([(node["attempt"], node["status"]) for node in end_nodes], [(1, "succeeded")])
        await self.advance()
        self.assertEqual(len(self.run["executions"]), 5)

    async def test_pause_at_approval_resume_returns_to_running_before_dispatch(self) -> None:
        await self.seed_run()
        await self.advance()
        await self.post(self.admin, f"/api/runs/{self.run['id']}/pause")
        await self.ledger.transition(self.run["id"], "paused", {})
        await self.approve()
        await self.post(self.admin, f"/api/runs/{self.run['id']}/resume")
        snapshot = await self.ledger.run(self.run["id"])
        self.assertEqual(snapshot["status"], "paused")
        await self.advance()
        self.assertEqual(self.run["status"], "running")
        self.assertEqual(len(self.run["executions"]), 1)

    async def test_exact_resolution_approval_and_resume_reuse_original_allocation(self) -> None:
        self.policy = await self.post(
            self.admin,
            "/api/policies",
            {
                "name": "Zero retry operator resolution fixture",
                "manifest": {
                    "max_retries": 0,
                    "allow_commit": True,
                    "trusted_repository": True,
                    "allowed_write_paths": ["src/**"],
                },
            },
        )
        await self.before_integration()
        conflict = await self.finish_job(
            "integrate",
            {
                "error_code": "INTEGRATION_CONFLICT",
                "integration_status": "needs_resolution",
                "conflict_paths": ["src/a.py"],
                "process_stopped": True,
            },
            "failed",
        )
        await self.advance()
        self.assertEqual(self.run["status"], "paused")
        denied = await self.admin.post(f"/api/runs/{self.run['id']}/resume", headers={"Idempotency-Key": "no-approval"})
        self.assertEqual(denied.status_code, 409)
        approval = await self.post(
            self.admin,
            f"/api/runs/{self.run['id']}/resolution",
            {"execution_id": conflict["execution_id"], "tree_sha": "e" * 40},
        )
        await self.approve(approval)
        await self.post(self.admin, f"/api/runs/{self.run['id']}/resume")
        await self.advance()
        jobs = [item for item in self.run["executions"] if item["operation"] == "integrate"]
        self.assertEqual(len(jobs), 2)
        resumed = next(item for item in jobs if item["attempt"] == 2)
        self.assertTrue(resumed["request"]["manual_resolution"])
        self.assertTrue(resumed["request"]["resolution_approved"])
        self.assertEqual(resumed["request"]["target_execution_id"], conflict["execution_id"])
        self.assertEqual(resumed["request"]["resolution_tree_sha"], "e" * 40)
        await self.finish_job(
            "integrate",
            {
                "tree_sha": "e" * 40,
                "commit_sha": "f" * 40,
                "integration_status": "passed",
                "branch": "factory/integration/" + self.run["id"],
                "test_results": self.checks("e" * 40),
            },
        )
        await self.advance()
        self.assertEqual(self.run["status"], "completed")
        latest_end = max(
            (node for node in self.run["nodes"] if node["node_id"] == "n6"), key=lambda node: node["attempt"]
        )
        self.assertEqual((latest_end["attempt"], latest_end["status"]), (2, "succeeded"))
        self.assertEqual(latest_end["result"]["execution_id"], resumed["id"])
        self.assertEqual(self.run["policy"]["max_retries"], 0)
        self.assertEqual(len([job for job in self.run["executions"] if job["operation"] == "agent_task"]), 1)

    async def test_planning_replay_keeps_persisted_task_ids_and_proposal_hash(self) -> None:
        response = await self.post(
            self.admin,
            f"/api/tasks/{self.task['id']}/decompose",
            {"agent_config_version_id": self.agents["Planner"]["config_version_id"], "policy_id": self.policy["id"]},
        )
        self.run = await self.ledger.run(response["id"])
        await self.ledger.request("POST", f"/internal/runs/{self.run['id']}/base", {"root_base_sha": "a" * 40})
        await self.advance()
        plan = {
            "tasks": [
                {
                    "id": "first",
                    "title": "First",
                    "description": "Implement fixture",
                    "expected_files": ["src/a.py"],
                    "acceptance_criteria": ["passes"],
                    "dependency_ids": [],
                }
            ],
            "unknowns": [],
        }
        await self.finish_job("planner", {"plan": plan})
        await self.advance()
        self.assertEqual(self.run["status"], "completed")
        self.assertEqual(self.run["result"]["graph_proposal_plan_hash"], self.run["plan_hash"])
        task_id = self.run["plan"]["tasks"][0]["id"]
        self.assertNotEqual(task_id, "first")
        self.assertFalse(validate_graph(self.run["result"]["graph_proposal"]))
        replay = await self.ledger.plan(self.run["id"], {**plan, "source_plan_hash": self.run["source_plan_hash"]})
        self.assertEqual(replay["tasks"][0]["id"], task_id)

    async def test_completed_planning_edit_refreshes_proposal_via_durable_command(self) -> None:
        await self.test_planning_replay_keeps_persisted_task_ids_and_proposal_hash()
        prior_hash = self.run["plan_hash"]
        edited = [
            {
                "id": "edited",
                "title": "Edited",
                "description": "Revised fixture",
                "expected_files": ["src/changed.py"],
                "acceptance_criteria": ["new criteria"],
                "dependency_ids": [],
            }
        ]
        response = await self.admin.patch(
            f"/api/runs/{self.run['id']}/plan",
            headers={"Idempotency-Key": "edit-completed-plan"},
            json={"revision": self.run["revision"], "tasks": edited},
        )
        self.assertTrue(response.is_success, response.text)
        self.assertNotEqual(response.json()["plan_hash"], prior_hash)
        self.assertNotIn("graph_proposal", response.json()["result"])
        await Service.apply_commands(self.ledger)
        snapshot = await self.ledger.run(self.run["id"])
        self.assertEqual(snapshot["status"], "completed")
        self.assertEqual(snapshot["result"]["graph_proposal_plan_hash"], snapshot["plan_hash"])
        node = next(node for node in snapshot["result"]["graph_proposal"]["nodes"] if node["type"] == "agent_task")
        self.assertEqual(node["config"]["task_id"], snapshot["plan"]["tasks"][0]["id"])
        self.assertEqual(node["config"]["expected_files"], ["src/changed.py"])
        commands = await self.ledger.request("GET", "/internal/orchestrator/commands")
        self.assertEqual(commands["items"], [])

    async def test_manual_external_dependency_pins_accepted_commits_and_approval_hash(self) -> None:
        await self.before_integration()
        await self.finish_job(
            "integrate",
            {
                "tree_sha": "b" * 40,
                "commit_sha": "d" * 40,
                "integration_status": "passed",
                "branch": "factory/integration/" + self.run["id"],
                "test_results": self.checks(),
            },
        )
        await self.advance()
        first_run = self.run
        child = await self.post(
            self.admin,
            "/api/tasks",
            {
                "project_id": self.project["id"],
                "title": "Dependent task",
                "description": "Uses previous accepted code",
                "expected_files": ["src/b.py"],
                "dependency_ids": [self.task["id"]],
                "status": "ready",
            },
        )
        result = await self.post(
            self.admin,
            "/api/runs",
            {
                "task_id": child["id"],
                "workflow_version_id": first_run["workflow_version"]["id"],
                "agent_config_version_id": self.agents["Developer"]["config_version_id"],
                "policy_id": self.policy["id"],
                "root_base_sha": "a" * 40,
            },
        )
        self.run = await self.ledger.run(result["id"])
        await self.advance()
        snapshot = self.run["approvals"][0]["snapshot"]
        self.assertEqual(snapshot["dependency_hash"], self.run["dependency_hash"])
        self.assertEqual(self.run["external_dependencies"][0]["run_id"], first_run["id"])
        await self.approve()
        await self.advance()
        job = next(job for job in self.run["executions"] if job["operation"] == "agent_task")
        self.assertEqual(job["request"]["predecessor_commits"], ["c" * 40])
        self.assertEqual(job["request"]["allowed_write_paths"], ["src/b.py"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
