"""Scoped async persistence facade. HTTP failure text never becomes public events."""

from __future__ import annotations

import asyncio
import secrets
from datetime import datetime, timedelta
from typing import Any, Protocol

import httpx

Record = dict[str, Any]  # The versioned JSON wire boundary is validated by API models.


class LedgerError(Exception):
    def __init__(self, code: str, status: int = 0) -> None:
        self.code, self.status = code, status
        super().__init__(code)


class Ledger(Protocol):
    async def run(self, run_id: str) -> Record: ...
    async def execute(self, run_id: str, intent: Record) -> Record: ...
    async def execution(self, execution_id: str) -> Record: ...
    async def approval(self, run_id: str, action: str, snapshot: Record) -> Record: ...
    async def get_approval(self, approval_id: str) -> Record: ...
    async def node(self, run_id: str, node_id: str, attempt: int, status: str, result: Record) -> None: ...
    async def transition(self, run_id: str, status: str, result: Record) -> None: ...
    async def plan(self, run_id: str, plan: Record) -> Record: ...
    async def graph_proposal(self, run_id: str, plan_hash: str, graph: Record) -> None: ...


class HttpLedger:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def request(self, method: str, path: str, body: Record | None = None) -> Record:
        # All writes have deterministic application keys; retry only transient transport/5xx.
        for attempt in range(4):
            try:
                response = await self.client.request(method, path, json=body)
                if response.status_code < 500:
                    break
            except (httpx.TimeoutException, httpx.TransportError):
                if attempt == 3:
                    raise LedgerError("LEDGER_UNAVAILABLE") from None
            if attempt == 3:
                raise LedgerError("LEDGER_UNAVAILABLE")
            await asyncio.sleep(min(2**attempt, 8) + secrets.randbelow(100) / 1000)
        try:
            data: Record = response.json()
        except ValueError:
            raise LedgerError("INVALID_LEDGER_RESPONSE", response.status_code) from None
        if not isinstance(data, dict):
            raise LedgerError("INVALID_LEDGER_RESPONSE")
        if not response.is_success:
            raise LedgerError(str(data.get("code", "LEDGER_REJECTED")), response.status_code)
        return data

    async def run(self, run_id: str) -> Record:
        return await self.request("GET", f"/internal/runs/{run_id}")

    async def execute(self, run_id: str, intent: Record) -> Record:
        return await self.request("POST", f"/internal/runs/{run_id}/executions", intent)

    async def execution(self, execution_id: str) -> Record:
        return await self.request("GET", f"/internal/executions/{execution_id}")

    async def approval(self, run_id: str, action: str, snapshot: Record) -> Record:
        body: Record = {"action": action, "snapshot": snapshot}
        if snapshot.get("expires_in_sec"):
            run = await self.run(run_id)
            created = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
            expiry = created + timedelta(seconds=snapshot["expires_in_sec"])
            deadline = datetime.fromisoformat(run["deadline_at"].replace("Z", "+00:00"))
            body["expires_at"] = min(expiry, deadline).isoformat()
        return await self.request("POST", f"/internal/runs/{run_id}/approvals", body)

    async def get_approval(self, approval_id: str) -> Record:
        return await self.request("GET", f"/internal/approvals/{approval_id}")

    async def node(self, run_id: str, node_id: str, attempt: int, status: str, result: Record) -> None:
        run = await self.run(run_id)
        previous = next(
            (node for node in run.get("nodes", []) if node["node_id"] == node_id and node["attempt"] == attempt), None
        )
        if previous and previous["status"] in {"succeeded", "failed", "cancelled", "skipped"}:
            if previous["status"] != status:
                return  # Interrupt replay cannot downgrade an already committed ledger outcome.
        await self.request(
            "POST",
            f"/internal/runs/{run_id}/nodes",
            {"node_id": node_id, "attempt": attempt, "status": status, "result": result},
        )

    async def transition(self, run_id: str, status: str, result: Record) -> None:
        await self.request(
            "POST",
            f"/internal/runs/{run_id}/transition",
            {"status": status, "result": result, "integration_status": result.get("integration_status")},
        )

    async def plan(self, run_id: str, plan: Record) -> Record:
        return await self.request("POST", f"/internal/runs/{run_id}/plan", plan)

    async def graph_proposal(self, run_id: str, plan_hash: str, graph: Record) -> None:
        await self.request(
            "POST", f"/internal/runs/{run_id}/graph-proposal", {"plan_hash": plan_hash, "graph_json": graph}
        )
