# adf-orchestrator

LangGraph DAG compilation, durable scheduling and recovery for ADF. This is an independent local Git repository and a pinned submodule of the ADF umbrella.

**Foundation scaffold, not a working MVP.** Read [module design](docs/development.md) for what exists and what remains. GitHub origin: [adf-orchestrator](https://github.com/alex-kzr/adf-orchestrator), supplied by the user and configured locally. Publication requires explicit user authorization; the matching adf-meta delivery report records publication and verification status.

## Umbrella documentation

- [Architecture](../../docs/architecture.md)
- [Product and acceptance](../../docs/product.md)
- [Contracts](../../docs/contracts.md)
- [Security](../../docs/security.md)
- [Backlog](../../docs/backlog.md)
- [Local development](../../docs/local-development.md)
- [Repository publication](../../docs/repositories.md)

These relative links work inside the initialized umbrella checkout. A standalone module clone contains its own `docs/development.md`; obtain the matching umbrella release for shared docs/contracts. The canonical umbrella is [adf-meta](https://github.com/alex-kzr/adf-meta), with [shared documentation](https://github.com/alex-kzr/adf-meta/tree/main/docs). These web links require the matching commits to be published.

## Scaffold commands

Run from this module directory (Python 3.12+; frontend Node 24 LTS / npm).

```sh
python -m pip install -e .
python -m unittest discover -s tests
adf-orchestrator validate ../../packages/contracts/examples/workflow.json
adf-orchestrator serve --host 127.0.0.1 --port 8081
```

Run only the host worker in WSL2 for eventual real CLI execution; it is not a Compose service. Foundation mock/probe code never launches coding agents. For Python, use a local virtual environment; install commands do not change global packages when run there. No secret is required to validate/mock the scaffold.
