# adf-orchestrator

ADF's declarative workflow compiler and durable scheduler. It compiles validated DAGs into LangGraph, persists checkpoints in PostgreSQL, and records execution decisions through the authenticated ADF API. The host worker performs coding CLI, subprocess and Git operations.

The service implements orchestration code and fault tests. End-to-end production acceptance also requires the matching API, WSL worker, registered CLI capabilities and umbrella acceptance evidence. `/readyz` reports current dependency and leadership readiness; it does not certify full product acceptance.

## Development

Python 3.12.9 is the tested runtime. Use this repository's own virtual environment.

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.lock mypy==1.19.1 ruff==0.15.7 bandit==1.9.4
python -m pip install --no-deps -e .
python -m unittest discover -s tests -v
python -m mypy src --strict
python -m ruff check src tests
python -m bandit -r src
adf-orchestrator validate ../../packages/contracts/examples/workflow.json
adf-orchestrator serve --diagnostics-only --host 127.0.0.1 --port 8081
```

On Windows, activate `.venv/Scripts/Activate.ps1`. Diagnostics mode serves health information with readiness false. Production `serve` requires the scoped secret files and services described in [development and operations](docs/development.md). Start the core through the umbrella Compose configuration, and run the execution worker separately on WSL ext4.

## Documentation

- [Development, compiler semantics, recovery and verification](docs/development.md)
- [Architecture](../../docs/architecture.md), [agent workflow](../../docs/agent-workflow.md), [reliability](../../docs/reliability.md)
- [Contracts](../../docs/contracts.md), [product and acceptance](../../docs/product.md), [security](../../docs/security.md)
- [Observability](../../docs/observability.md), [local development](../../docs/local-development.md), [repository publication](../../docs/repositories.md)

Relative umbrella links work in the initialized ADF checkout. Standalone clones contain module documentation; obtain the matching [adf-meta shared documentation](https://github.com/alex-kzr/adf-meta/tree/main/docs) for the full contract. The user supplied the real [adf-orchestrator origin](https://github.com/alex-kzr/adf-orchestrator). This file describes the implementation; publication status belongs to the umbrella delivery report.
