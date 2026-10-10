"""Validate immutable DAGs or run the PostgreSQL-backed orchestration service."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
from pathlib import Path

from adf_orchestrator.config import Settings
from adf_orchestrator.http_server import server
from adf_orchestrator.service import Health, Service
from adf_orchestrator.telemetry import initialize
from adf_orchestrator.validation import validate_graph, validate_plan


async def serve(host: str, port: int, diagnostics_only: bool = False) -> None:
    health = Health()
    settings = None if diagnostics_only else Settings.load()
    http = server(host, port, health, settings.token if settings else None)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(name, stop.set)
        except NotImplementedError:
            pass
    telemetry = initialize()
    try:
        async with asyncio.TaskGroup() as group:
            task = group.create_task(asyncio.to_thread(http.serve_forever, poll_interval=0.2))
            if settings:

                async def lifecycle() -> None:
                    try:
                        await Service(settings, health).run(stop)
                    finally:
                        stop.set()
                        await asyncio.to_thread(http.shutdown)

                service_task = group.create_task(lifecycle())
            else:
                health.reason = "DIAGNOSTICS_ONLY"
            await stop.wait()
            if settings and not service_task.done():
                service_task.cancel()
            await asyncio.to_thread(http.shutdown)
            await task
    finally:
        await asyncio.to_thread(http.shutdown)
        http.server_close()
        if telemetry:
            await asyncio.to_thread(telemetry.shutdown)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    check = sub.add_parser("validate")
    check.add_argument("path", type=Path)
    plan = sub.add_parser("validate-plan")
    plan.add_argument("path", type=Path)
    daemon = sub.add_parser("serve")
    daemon.add_argument("--host", default="127.0.0.1")
    daemon.add_argument("--port", type=int, default=8081)
    daemon.add_argument("--diagnostics-only", action="store_true")
    args = parser.parse_args()
    if args.command in {"validate", "validate-plan"}:
        try:
            data = json.loads(args.path.read_text(encoding="utf-8"))
            if args.command == "validate":
                errors = validate_graph(data)
            else:
                validate_plan(data)
                errors = []
        except (OSError, ValueError) as exc:
            errors = [str(exc)]
        print(json.dumps({"valid": not errors, "errors": errors}))
        raise SystemExit(bool(errors))
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    asyncio.run(serve(args.host, args.port, args.diagnostics_only))


if __name__ == "__main__":
    main()
