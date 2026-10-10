"""Dependency session recovery and bounded private HTTP diagnostics."""

from __future__ import annotations

import asyncio
import json
import secrets
import threading
import unittest
from http.client import HTTPConnection

import httpx
import psycopg

from adf_orchestrator.config import Settings
from adf_orchestrator.http_server import server
from adf_orchestrator.service import Health, Service


class ServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_lost_postgres_session_is_replaced_before_any_new_dispatch(self) -> None:
        health = Health(ready=True, leader=True)
        settings = Settings("http://fixture", "fixture", "fixture", "fixture")
        observed = []

        class Recovering(Service):
            async def session(self, stop: asyncio.Event) -> None:
                observed.append((health.ready, health.leader))
                if len(observed) == 1:
                    raise psycopg.OperationalError("Synthetic session closure")
                stop.set()

        await Recovering(settings, health).run(asyncio.Event())
        self.assertEqual(len(observed), 2)
        self.assertEqual(observed[1], (False, False))
        self.assertEqual(health.reason, "DEPENDENCY_RECONNECTING")


class HttpTests(unittest.TestCase):
    def setUp(self) -> None:
        self.health = Health()
        self.token = secrets.token_urlsafe(32)
        self.http = server("127.0.0.1", 0, self.health, self.token)
        self.thread = threading.Thread(target=self.http.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        self.thread.start()
        self.client = httpx.Client(base_url=f"http://127.0.0.1:{self.http.server_port}", timeout=2)

    def tearDown(self) -> None:
        self.client.close()
        self.http.shutdown()
        self.http.server_close()
        self.thread.join(timeout=2)

    def test_readiness_auth_invalid_body_and_semantic_validation(self) -> None:
        self.assertEqual(self.client.get("/healthz").status_code, 200)
        self.assertEqual(self.client.get("/readyz").status_code, 503)
        self.health.ready = self.health.leader = True
        self.health.reason = "READY"
        self.assertEqual(self.client.get("/readyz").status_code, 200)
        graph = {
            "schema_version": "1",
            "nodes": [{"id": "s", "type": "start"}, {"id": "e", "type": "end"}],
            "edges": [{"source": "s", "target": "e"}],
        }
        self.assertEqual(self.client.post("/validate", json={"graph_json": graph}).status_code, 401)
        headers = {"Authorization": "Bearer " + self.token}
        self.assertTrue(self.client.post("/validate", json={"graph_json": graph}, headers=headers).json()["valid"])
        self.assertEqual(self.client.post("/validate", content=b"\xff", headers=headers).status_code, 422)
        graph["edges"].append({"source": "e", "target": "s"})
        self.assertFalse(
            self.client.post("/validate", content=json.dumps({"graph_json": graph}), headers=headers).json()["valid"]
        )
        # Reject the declared size before reading a body. Sending an oversized body
        # while the server closes can cause an OS TCP reset instead of delivering 413.
        connection = HTTPConnection("127.0.0.1", self.http.server_port, timeout=2)
        try:
            connection.putrequest("POST", "/validate")
            connection.putheader("Authorization", "Bearer " + self.token)
            connection.putheader("Content-Length", str(256 * 1024 + 1))
            connection.endheaders()
            self.assertEqual(connection.getresponse().status, 413)
        finally:
            connection.close()
