"""Single-leader control loop, durable outbox relay and dependency readiness."""

from __future__ import annotations

import asyncio
import logging
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
import psycopg
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg.rows import dict_row
from redis.asyncio import Redis
from redis.exceptions import RedisError

from adf_orchestrator.config import Settings
from adf_orchestrator.engine import WorkflowEngine, stable_id
from adf_orchestrator.ledger import HttpLedger, LedgerError, Record
from adf_orchestrator.telemetry import span

LOG = logging.getLogger("adf.orchestrator")
LEADER_LOCK = 41023911


@dataclass
class Health:
    ready: bool = False
    leader: bool = False
    reason: str = "STARTING"
    last_tick: datetime | None = None


class Service:
    def __init__(self, settings: Settings, health: Health) -> None:
        self.settings, self.health = settings, health

    async def run(self, stop: asyncio.Event) -> None:
        failures = 0
        while not stop.is_set():
            try:
                await self.session(stop)
                return
            except (psycopg.Error, RedisError, LedgerError, httpx.TransportError, RuntimeError):
                # A disconnected advisory-lock session has no authority: rebuild every resource.
                self.health.ready, self.health.leader = False, False
                self.health.reason = "DEPENDENCY_RECONNECTING"
                failures += 1
                delay = min(30, 2 ** min(failures - 1, 5)) + secrets.randbelow(250) / 1000
                LOG.warning("Reconnecting orchestration dependencies")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except TimeoutError:
                    pass

    async def session(self, stop: asyncio.Event) -> None:
        # Separate leader connection: no checkpoint query can release session lock.
        async with await psycopg.AsyncConnection.connect(
            self.settings.database_dsn, autocommit=True, row_factory=dict_row
        ) as leader:
            async with leader.cursor() as cursor:
                await cursor.execute("SELECT pg_try_advisory_lock(%s) AS held", (LEADER_LOCK,))
                row = await cursor.fetchone()
                if not row or not row["held"]:
                    self.health.reason = "LEADER_ALREADY_ACTIVE"
                    raise RuntimeError("LEADER_ALREADY_ACTIVE")
            self.health.leader = True
            try:
                async with AsyncPostgresSaver.from_conn_string(self.settings.database_dsn) as saver:
                    await saver.setup()
                    async with httpx.AsyncClient(
                        base_url=self.settings.api_url,
                        headers={"Authorization": f"Bearer {self.settings.token}"},
                        timeout=httpx.Timeout(8),
                        limits=httpx.Limits(max_connections=12),
                    ) as client:
                        ledger = HttpLedger(client)
                        engine = WorkflowEngine(ledger, saver)
                        async with Redis.from_url(
                            self.settings.redis_url, decode_responses=True, socket_timeout=5, socket_connect_timeout=5
                        ) as redis:
                            while not stop.is_set():
                                try:
                                    await redis.ping()
                                    await leader.execute("SELECT 1")
                                    response = await client.get("/readyz")
                                    if not response.is_success:
                                        raise LedgerError("API_UNAVAILABLE")
                                    await self.tick(ledger, engine, redis)
                                    self.health.ready, self.health.reason = True, "READY"
                                    self.health.last_tick = datetime.now(UTC)
                                except (
                                    LedgerError,
                                    httpx.TransportError,
                                    RedisError,
                                    ConnectionError,
                                    TimeoutError,
                                ) as exc:
                                    self.health.ready = False
                                    self.health.reason = (
                                        exc.code if isinstance(exc, LedgerError) else "DEPENDENCY_UNAVAILABLE"
                                    )
                                    LOG.warning("Dependency unavailable: %s", self.health.reason)
                                try:
                                    await asyncio.wait_for(stop.wait(), timeout=self.settings.poll_seconds)
                                except TimeoutError:
                                    pass
            finally:
                self.health.ready, self.health.leader = False, False
                if not leader.closed:
                    try:
                        await leader.execute("SELECT pg_advisory_unlock(%s)", (LEADER_LOCK,))
                    except psycopg.Error:
                        pass  # Server released a closed session's advisory lock already.

    async def tick(self, ledger: HttpLedger, engine: WorkflowEngine, redis: Redis) -> None:
        # Relay first and again after decisions. Stable dispatch ID makes XADD replay harmless.
        await self.relay(ledger, redis)
        snapshots = await ledger.request("GET", "/internal/orchestrator/runs?status=active&limit=200")
        for run in snapshots["items"]:
            try:
                active = [
                    execution
                    for execution in run.get("executions", [])
                    if execution.get("status") not in {"succeeded", "failed", "cancelled"}
                ]
                if run.get("cancel_requested") and not active:
                    await ledger.transition(run["id"], "cancelled", {"error_code": "CANCELLED"})
                    continue
                if self.expired(run) and not run.get("cancel_requested"):
                    await ledger.request(
                        "POST", f"/internal/runs/{run['id']}/control", {"action": "cancel", "reason": "RUN_TIMEOUT"}
                    )
                    continue
                if run.get("pause_requested") and not active:
                    if run["status"] != "paused":
                        await ledger.transition(run["id"], "paused", {})
                    continue
                context = {"run_id": run["id"], "project_id": run.get("project_id"), "task_id": run.get("task_id")}
                if not run.get("traceparent"):
                    with span("workflow.run", context) as trace_context:
                        if trace_context:
                            saved = await ledger.request("POST", f"/internal/runs/{run['id']}/trace", trace_context)
                            run.update(saved)
                if (
                    run.get("mode") == "planning"
                    and run.get("plan_hash")
                    and (run.get("result") or {}).get("graph_proposal_plan_hash") != run["plan_hash"]
                ):
                    from adf_orchestrator.planning import graph_from_plan

                    await ledger.graph_proposal(
                        run["id"], run["plan_hash"], graph_from_plan(run["plan"], run.get("agents_by_role", {}))
                    )
                if not run.get("root_base_sha"):
                    if not await self.prepare(ledger, run):
                        continue
                    run = await ledger.run(run["id"])
                await engine.advance(run)
            except (LedgerError, ValueError) as exc:
                code = exc.code if isinstance(exc, LedgerError) else "INVALID_WORKFLOW"
                # A diagnostic pauses rather than claiming all OS writers stopped.
                await ledger.request(
                    "POST",
                    f"/internal/runs/{run['id']}/events",
                    {
                        "event_id": stable_id(run["id"], "diagnostic", 1, code),
                        "type": "orchestrator.warning",
                        "payload": {"code": code},
                    },
                )
                if code not in {"LEDGER_UNAVAILABLE", "VERSION_CONFLICT"}:
                    await ledger.request(
                        "POST", f"/internal/runs/{run['id']}/control", {"action": "pause", "reason": code}
                    )
        await self.relay(ledger, redis)
        await self.apply_commands(ledger)

    @staticmethod
    async def apply_commands(ledger: HttpLedger) -> None:
        commands = await ledger.request("GET", "/internal/orchestrator/commands")
        for command in commands["items"]:
            run = await ledger.run(command["run_id"])
            kind = command["kind"]
            terminal = run["status"] in {"completed", "failed", "cancelled"}
            applied = terminal
            if kind == "refresh_proposal":
                from adf_orchestrator.planning import graph_from_plan

                if run.get("mode") == "planning" and run.get("plan_hash"):
                    if (run.get("result") or {}).get("graph_proposal_plan_hash") != run["plan_hash"]:
                        await ledger.graph_proposal(
                            run["id"], run["plan_hash"], graph_from_plan(run["plan"], run.get("agents_by_role", {}))
                        )
                    applied = True
                else:
                    applied = False
            elif kind in {"start", "decompose", "resume"}:
                applied = terminal or (run["status"] == "running" and not run.get("pause_requested"))
            elif kind == "pause":
                applied = terminal or run["status"] == "paused" or not run.get("pause_requested")
            elif kind == "cancel":
                applied = terminal
            if applied:
                await ledger.request("POST", f"/internal/commands/{command['id']}/applied", {"status": run["status"]})

    @staticmethod
    def expired(run: Record) -> bool:
        created = run.get("created_at")
        if not isinstance(created, str):
            return False
        start = datetime.fromisoformat(created.replace("Z", "+00:00"))
        return (datetime.now(UTC) - start).total_seconds() > WorkflowEngine.limits(run).max_run_minutes * 60

    @staticmethod
    async def prepare(ledger: HttpLedger, run: Record) -> bool:
        execution_id = stable_id(run["id"], "__prepare", 1, "repo_inspect")
        intent: Record = {
            "execution_id": execution_id,
            "node_id": "__prepare",
            "attempt": 1,
            "operation": "repo_inspect",
            "dependency_execution_ids": [],
            "request": {
                "task_prompt": "",
                "timeout_sec": 30,
                "max_steps": 1,
                "max_output_bytes": 65536,
                "max_disk_bytes": 1,
            },
        }
        await ledger.execute(run["id"], intent)
        execution = await ledger.execution(execution_id)
        if execution["status"] == "succeeded":
            base = execution.get("result", {}).get("root_base_sha")
            if not isinstance(base, str) or len(base) != 40:
                raise ValueError("Invalid inspected base")
            await ledger.request("POST", f"/internal/runs/{run['id']}/base", {"root_base_sha": base})
            return True
        if execution["status"] in {"failed", "cancelled"}:
            raise LedgerError("PROJECT_INSPECTION_FAILED")
        return False

    @staticmethod
    async def relay(ledger: HttpLedger, redis: Redis) -> None:
        pending = await ledger.request("GET", "/internal/orchestrator/outbox?limit=200")
        for message in pending["items"]:
            payload = message.get("data", message.get("payload", message))
            execution_id = payload.get("execution_id") or message.get("execution_id")
            if not execution_id:
                continue
            stream_id = await redis.xadd(
                "adf:executions",
                {"execution_id": execution_id, "dispatch_id": message["id"]},
                maxlen=10000,
                approximate=True,
            )
            await ledger.request("POST", f"/internal/outbox/{message['id']}/delivered", {"stream_id": stream_id})
