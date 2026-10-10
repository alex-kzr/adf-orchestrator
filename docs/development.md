# Orchestrator development

Implemented: dependency-free basic DAG validator and diagnostic HTTP server. Validator checks node vocabulary/IDs, edge references, cycles, one start/end and reachability. It does **not** compile LangGraph, validate full typed ports/nested fork/join/conditions, schedule jobs, checkpoint or recover runs. Production compiler requires E3 and machine-readable schema validation.

`adf-orchestrator validate <workflow.json>` emits validation errors and exits nonzero on invalid input. `serve` provides `/healthz` 200 and `/readyz` 503 only. There is no run endpoint. Stdlib tests cover cycle/disconnected-path failure. Python >=3.12; no orchestration dependencies are installed yet. Version candidates are recorded in umbrella docs; pin tested LangGraph/LangChain/checkpointer during E3.

Next: DB leadership lock, durable commands/outbox/ledger, worker result reconciliation, dependency-base assembly, graph compiler/checkpointer and hash-bound approval. CLI subprocesses/Git operations belong in worker. Retries never blindly repeat uncertain filesystem effects. Preserve the separation between scheduler decisions and worker execution.
