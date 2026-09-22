# Data contracts

A contract is the producer's promise about a dataset: schema, owner, what each field means,
guarantees, SLOs, which fields are personal data, and who consumes it. It's versioned, and CI fails
a breaking change that doesn't bump the major version.

| Path | What it is |
|---|---|
| `dim_client.yaml`, `fct_client_activity.yaml` | the contracts for the two shared Gold tables |
| `../services/contract-api/` | the compatibility rules, as a FastAPI service and a library |
| `../scripts/validate_contracts.py` | the CI gate: each contract against the version on `main` |

```bash
make contracts-check                          # the compatibility rules' own tests
uv run python scripts/validate_contracts.py   # every contract against origin/main
```

Which changes are breaking, and how contracts are enforced:
[decision guide, section 8](../docs/decision-guide.md#8-data-contracts).
