"""Bounded internal validation and dependency-aware diagnostics."""

from __future__ import annotations

import hmac
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from adf_orchestrator.service import Health
from adf_orchestrator.validation import validate_graph


def handler(health: Health, token: str | None) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(5)

        def log_message(self, format: str, *args: Any) -> None:
            # No request headers, bodies, URLs or bearer values in HTTP logs.
            return

        def reply(self, status: int, body: dict[str, Any]) -> None:
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:
            if self.path == "/healthz":
                self.reply(200, {"service": "adf-orchestrator", "status": "alive"})
            elif self.path == "/readyz":
                self.reply(
                    200 if health.ready else 503,
                    {"ready": health.ready, "leader": health.leader, "code": health.reason},
                )
            else:
                self.reply(404, {"code": "NOT_FOUND"})

        def do_POST(self) -> None:
            if self.path != "/validate":
                self.reply(404, {"code": "NOT_FOUND"})
                return
            supplied = self.headers.get("Authorization", "")
            if not token or not hmac.compare_digest(supplied, f"Bearer {token}"):
                self.reply(401, {"code": "AUTH_REQUIRED"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 1 <= length <= 256 * 1024:
                    self.reply(413, {"code": "BODY_LIMIT"})
                    return
                body = json.loads(self.rfile.read(length))
                errors = validate_graph(body["graph_json"])
            except (ValueError, KeyError, TypeError):
                self.reply(422, {"code": "INVALID_REQUEST"})
                return
            self.reply(200, {"valid": not errors, "errors": errors})

    return Handler


def server(host: str, port: int, health: Health, token: str | None) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), handler(health, token))
