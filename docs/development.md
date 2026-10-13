# Orchestrator development and operations

## Shared-patch source compatibility

The existing bounded `TEST_FAILED`/`REVIEW_REJECTED` loop still creates fresh developer repairs, retests, independently reviews exact trees, commits and integrates sequentially. Source classification now explicitly refuses retries for patch/schema/content/path/version/idempotency/lease/check-capability/I/O safety failures. This does not deploy the orchestrator container or enable a native backend. See [worker source behavior](../../../docs/shared-patch-source.md) and [separate installation/acceptance scope](../../../docs/shared-patch-deployment.md).

## Responsibilities and implementation

The orchestrator validates immutable workflow versions, compiles a real LangGraph `StateGraph`, and advances it using `AsyncPostgresSaver`. The API is the sole transition authority for control, runtime and audit state; the orchestrator owns only the PostgreSQL checkpoints schema. Redis carries execution and dispatch identifiers. Workers fetch trusted immutable snapshots from the API and own OS process supervision, worktrees, tests, review evidence and Git journals. The orchestrator never launches a coding CLI or invokes a hosted language-model API.

Source modules separate typed models and semantic validation (`models.py`, `validation.py`), plan proposals (`planning.py`), the API facade (`ledger.py`), graph decisions (`engine.py`), resource lifecycle and outbox relay (`service.py`), diagnostics (`http_server.py`) and allowlisted telemetry (`telemetry.py`). The JSON wire boundary uses an explicit `Record` alias; declarative graph and plan inputs use frozen strict Pydantic models.

## Declarative graph semantics

Version `1` supports start, planner, agent_task, condition, parallel_fork, parallel_join, test, review, approval, git_commit and end. Validation rejects duplicate IDs/edges, unknown nodes/configuration, cycles, unreachable nodes, invalid ports and wrongly typed ancestor references. There is exactly one start/end. Conditions expose exactly `true` and `false`; other nodes expose `out`, and all inputs use `in`. Fork/join pairs must be reciprocal, properly nested, disjoint before the join and fully convergent. The only join policy is `all_success`.

Conditions read a bounded ancestor status or boolean evidence field and use `eq` or `in`; they never evaluate arbitrary expressions. Both condition branches exist in the static compiled graph. An unselected branch records explicit skipped outcomes so barriers can safely converge. Failure routing can collect diagnostics, but a failed selected required task still prevents completion.

Configuration fields are the exported `CONFIG_FIELDS` registry in `validation.py`. Blank developer/test/review/commit configurations use the immutable run task, registered checks and pinned role agents. Explicit targets must be ancestors of the right node type. Review requires a separately pinned Reviewer identity, even when no override is specified. `timeout_sec` and planner `max_tasks` only reduce policy budgets. Approval `expires_in_sec` is bounded from run creation and cannot exceed its durable deadline.

The planner operation asks the real configured CLI for structured output using the LangChain Pydantic parser's format contract. Plans contain one to twelve typed tasks, criteria, relative write scope and an acyclic dependency list. Protected paths and traversal are rejected. The API first persists stable task UUIDs and a plan hash. The orchestrator then generates a separate full proposal in `run.result.graph_proposal`, tagged by `graph_proposal_plan_hash`. Parallel task waves include test, independent review and commit gates. This proposal never changes a running graph. A user reviews/publishes it and starts an execution run with the matching plan. Plan edits invalidate the old proposal/grant and enqueue a durable `refresh_proposal` command, including after planning completion.

## Durable external effects and concurrency

Execution IDs are UUIDv5 values derived from run, node, attempt and operation. The exact intent is persisted through the API before a LangGraph interrupt. Replaying the node submits identical content and reads the authoritative result. Private checkpoints may contain task instructions and exact pending intents; they are never public SSE or telemetry payloads. Approval and execution interrupts always occur at the same position during replay, including capacity deferrals. One run uses the same LangGraph thread ID across restart.

The API enforces the global parallel limit of three, provider/project limits, path reservations, immutable agent/registry snapshots and exact approvals. LangGraph additionally bounds local concurrent supersteps to three. A dependency writer receives only approved commits from its transitive predecessor tasks, plus API-pinned previously integrated external dependencies. Unrelated prior-wave commits do not silently become its base. Every writing job starts from the pinned root base plus that approved dependency snapshot. End performs one serial integration on the separate `factory/integration/<run_id>` allocation; integration receives no predecessor replay list, so task deltas apply once.

All selected writing tasks need actual local commit evidence. Registered checks bind the exact tree, review binds that tree and distinct developer/reviewer identities, and commits bind the reviewed tree. Completion requires successful serial integration, exact registered integration checks, the expected integration branch and an actual commit. The API and worker independently verify these gates.

Retries have a maximum of three total attempts, bounded backoff and execution/run wall deadlines. Authentication, permissions, unknown capabilities, stale leases and uncertain effects are never blindly retried. Test/review failures can request a fresh Developer repair allocation with the same child task scope and vetted previous changes. Review repairs rerun tests before another independent review. A worker must prove its previous supervisor stopped before repair/reassignment; uncertainty retains reservations and requires reconciliation.

Human approval snapshots bind graph, plan, root base, dependency hash, policy, every pinned agent/registry version, selected developer hash, the union of every pinned root/graph-bound/plan task write scope, tools and ancestor outcomes. Decisions are reread from the API; an interrupt resume payload is not consent. Integration conflicts pause at a separate resolution interrupt. Ordinary resume is rejected until a human accepts the exact resolution tree for the original integration execution. The next intent points to that original allocation and exact tree; the worker rechecks its journal and tree before continuing.

## Service lifecycle and readiness

Production `serve` acquires a session advisory leadership lock using a dedicated PostgreSQL connection, sets up PostgreSQL checkpoint tables, opens bounded HTTP/Redis resources and reconciles the API ledger. On PostgreSQL connection loss it closes the session and reacquires leadership with a fresh saver; it never reuses a closed lock connection. A healthy container recovers when PostgreSQL returns without relying on Docker unhealthy-container restarts. Dependency reconnect backoff is bounded at thirty seconds plus jitter. Cancellation uses structured service tasks and closes HTTP, Redis, database and telemetry resources.

Pause stops new dispatch and drains existing work before status becomes paused. Resume clears flags and explicitly transitions a drained paused run to running before any pending intent is replayed. Cancel is monotonic; the worker supervises stopped-process proof before the API releases reservations. A run deadline requests cancellation rather than claiming active OS writers have stopped. Durable start/control commands are acknowledged after the corresponding state holds. Outbox messages remain reconstructible from created executions after Redis loss; repeated dispatch IDs are harmless because API claim fencing is authoritative.

`/healthz` means the HTTP process is alive. `/readyz` is false during startup, reconnect, missing API/Redis/checkpoint dependencies or missing leadership, and true after a successful dependency-checked reconciliation tick. Diagnostics-only mode always has readiness false. The internal authenticated `POST /validate` accepts `{ "graph_json": ... }`, returns `{ "valid": bool, "errors": [...] }`, and limits bodies to 256 KiB. HTTP request bodies, URLs and bearer values are not logged. httpx/httpcore polling logs default to WARNING.

Production configuration uses:

| Setting | Purpose |
| --- | --- |
| `ADF_API_URL` | Private API origin; Compose uses `http://api:8080` |
| `ADF_ORCHESTRATOR_TOKEN_FILE` | Scoped API and validator bearer token |
| `DATABASE_URL` | `adf_orchestrator` PostgreSQL role, checkpoints search path |
| `ADF_DB_PASSWORD_FILE` | Checkpoint-role password file |
| `ADF_REDIS_URL` | Scoped Redis ACL user and database |
| `ADF_REDIS_PASSWORD_FILE` | Redis ACL password file |
| `ADF_TELEMETRY_ENABLED`, `OTEL_EXPORTER_OTLP_ENDPOINT` | Explicit metadata telemetry opt-in |

Secret values are read from files without printing DSNs or dataclass representations. Database role/schema grants are provisioned by umbrella infrastructure; this service does not change security settings. Redis commands needed by this producer are PING and XADD plus client connection negotiation. Execution CLI credentials remain solely in worker/provider configuration, outside source and orchestration prompts.

## Telemetry

The first successful durable trace-context CAS pins one W3C traceparent/trace ID per run before any repository-inspection or execution dispatch. `workflow.run` describes initialization, so its span duration is not the total run duration; total duration comes from the ledger. Node observations and worker continuations use the persisted parent. Checkpoint replay may produce repeated metadata node observations, while external intents remain idempotent. Root/node fields without an applicable execution/agent use the explicit `not-applicable` value, never invented execution IDs.

Eligible `workflow.run`/`workflow.node` observations use the shared Langfuse metadata IDs and fixed names. There are no prompt, code, stdout, event or automatic exception attributes. Automatic LangSmith graph tracing is disabled around every graph invocation with its supported tracing context, even when enabled by an inherited shell environment; only the allowlisted OTel producer is used. Transport outage may lose the initialization span: export is best effort and incomplete trace retrieval is reported honestly by the umbrella acceptance checks. Telemetry never gates correctness or approval.

## Verification

Run the README's unit, strict-type, Ruff security/async and Bandit commands. CI pins checkout/setup-python actions to verified SHAs, installs the exact runtime lock, and runs standalone tests without subscriptions. An opt-in cross-module suite exercises actual authenticated FastAPI routes, API SQL state transitions and `HttpLedger` rather than replacing them with a fake facade:

```sh
python -m pip install -e ../api
python tests/wire_api.py
```

This suite uses isolated temporary SQLite and LangGraph memory checkpoints; it verifies completion/evidence flow, drained pause/resume, exact manual resolution, integration's empty predecessor replay list, and planner UUID/hash replay. It does not prove PostgreSQL lock/checkpoint behavior, real Redis delivery, CLI capability or filesystem isolation. The matching umbrella Docker/WSL acceptance must exercise PostgreSQL/orchestrator/Redis restart, process cancellation and uncertain lease reconciliation, genuine independently reviewed worktree commits, dependency bases, conflicts, browser workflow and actual Langfuse/OTel retrieval. Full MVP acceptance is asserted only from those recorded results.

Mock execution uses the worker's existing public JSON payload contract: put `mock_files`, `review`, or `plan` in the task description. For a CLI node whose immutable pinned agent snapshot selects `provider=mock`, the orchestrator preserves that description exactly, including whitespace. Acceptance-criteria headings and planner format instructions are appended for native providers only. Scope, approval, task binding and execution limits still come from their pinned snapshots; JSON content cannot authorize permissions. The wire regression checks the public task-to-run-to-worker-claim payload; it does not claim native CLI acceptance from mock output.

## Verified upstream contracts and dependency upgrades

Pins verified against official releases on 2026-10-10 include [LangGraph 1.2.14](https://pypi.org/project/langgraph/1.2.14/), [PostgreSQL checkpoint saver 3.1.2](https://pypi.org/project/langgraph-checkpoint-postgres/3.1.2/) and [LangChain 1.4.4](https://pypi.org/project/langchain/1.4.4/). The runtime lock pins their resolved transitive dependencies. Official [persistence documentation](https://docs.langchain.com/oss/python/langgraph/persistence) , [conditional tracing documentation](https://docs.langchain.com/langsmith/conditional-tracing), and [interrupt documentation](https://docs.langchain.com/oss/python/langgraph/interrupts) establish saver setup, durable thread IDs and node restart on resume. Upgrades require graph, interrupt, wire and fault contract tests; they cannot authorize new permissions.

## Repairs and independent provenance

Repair allocates a fresh execution from the original writing node's pinned
dependency/configuration snapshot. It includes API-pinned external task commits
and the original Developer-role fallback; later review ancestors cannot add
unrelated commits. The persisted source id/run/task/agent/base/write/dependency
bindings must match before dispatch. Worker tree/snapshot fencing remains intact.

Native reviews receive the API's selected capability audit bound to the exact
source execution, run, lease epoch and tree. The prompt distinguishes materialized
instruction hashes and completed MCP calls from semantic skill consumption.
Stale, mismatched, truncated or oversized evidence fails before reviewer dispatch.
Tests/criteria still determine review; no extra skill/MCP/profile permission is
assigned to the reviewer and rejection is never converted into approval.
