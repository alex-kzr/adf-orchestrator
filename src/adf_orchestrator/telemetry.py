"""Allowlisted metadata only; no automatic exception or prompt recording."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from adf_orchestrator.ledger import Record

IDS = ("project_id", "task_id", "run_id", "node_run_id", "execution_id", "agent_id", "provider")


def initialize() -> TracerProvider | None:
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint or os.environ.get("ADF_TELEMETRY_ENABLED", "false").lower() != "true":
        return None
    provider = TracerProvider(resource=Resource.create({"service.name": "adf-orchestrator"}))
    provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=endpoint.rstrip("/") + "/v1/traces", timeout=2),
            max_queue_size=256,
            max_export_batch_size=64,
        )
    )
    trace.set_tracer_provider(provider)
    return provider


@contextmanager
def span(name: str, context: Record) -> Iterator[Record]:
    attributes: dict[str, str | bool] = {
        "adf.telemetry.eligible": True,
        "langfuse.trace.name": "workflow.run",
        "langfuse.observation.type": "chain" if name in {"workflow.run", "workflow.node"} else "span",
    }
    for key in IDS:
        value = context.get(key) or "not-applicable"
        if isinstance(value, str):
            attributes[key] = value
            attributes[f"langfuse.trace.metadata.{key}"] = value
    if context.get("run_id"):
        attributes["langfuse.session.id"] = str(context["run_id"])
    with trace.get_tracer("adf.orchestrator").start_as_current_span(
        name,
        context=extract({"traceparent": context["traceparent"]}) if context.get("traceparent") else None,
        attributes=attributes,
        record_exception=False,
        set_status_on_exception=False,
    ) as current:
        identity = current.get_span_context()
        yield (
            {
                "trace_id": f"{identity.trace_id:032x}",
                "traceparent": f"00-{identity.trace_id:032x}-{identity.span_id:016x}-01",
            }
            if identity.is_valid
            else {}
        )
