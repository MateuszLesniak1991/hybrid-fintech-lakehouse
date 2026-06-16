# CI/CD Plan

## 1. Objective

The CI/CD process will validate code, schemas, Docker configuration and data transformations before deployment.

The initial implementation will use GitHub Actions.

---

## 2. Branching strategy

Recommended branches:

```text
main
feature/*
fix/*
chore/*
```

Workflow:

1. Create feature branch.
2. Implement change.
3. Run local tests.
4. Push branch.
5. Open Pull Request.
6. GitHub Actions validates the change.
7. Review and merge to `main`.

---

## 3. Continuous Integration

Planned GitHub Actions checks:

### Python validation

- install dependencies,
- compile Python files,
- run Ruff or Flake8,
- run formatting check,
- run unit tests.

Example:

```bash
python -m py_compile generator/*.py
python -m py_compile streaming/*.py
python -m py_compile batch/*.py
pytest
```

### Docker Compose validation

```bash
docker compose config --quiet
```

### SQL validation

- check SQL file presence,
- validate schema syntax using temporary PostgreSQL,
- create `banking_source` schema,
- verify expected tables.

### Secret scanning

- prevent `.env` commits,
- scan for Azure connection strings,
- scan for access keys,
- use GitHub secret scanning.

### Documentation validation

- check Markdown links,
- check required documents,
- optionally run Markdown lint.

---

## 4. Unit tests

Planned tests:

```text
tests/test_generator.py
tests/test_event_schema.py
tests/test_timestamp_replay.py
tests/test_partition_paths.py
tests/test_minio_export.py
tests/test_data_quality.py
```

Examples:

- generated amount is positive,
- risk score stays in range,
- replay preserves original timestamp,
- partition path matches event hour,
- transaction event contains required fields.

---

## 5. Integration tests

Planned integration environment:

- PostgreSQL service container,
- Redpanda service container,
- MinIO service container.

Test flow:

```text
generate small dataset
→ verify PostgreSQL rows
→ replay 100 events
→ verify Redpanda messages
→ export one hour
→ verify MinIO object
```

---

## 6. Cloud deployment

Planned approach:

### Azure resources

Infrastructure as Code using:

- Bicep or Terraform.

Resources:

- resource group,
- Event Hub namespace,
- Event Hub,
- Storage Account,
- ADLS container,
- identities and role assignments.

### Databricks

Planned deployment:

- Databricks Asset Bundles,
- notebooks,
- jobs,
- cluster or serverless configuration,
- environment-specific variables.

### Fabric

Fabric configuration may initially require manual setup and documentation. Automation will be added where supported.

---

## 7. Environment separation

Planned environments:

```text
dev
test
prod-demo
```

Each environment should use separate:

- Event Hub names,
- storage paths,
- consumer groups,
- configuration values.

---

## 8. Secrets

Secrets will be stored in:

- GitHub Actions Secrets,
- Azure Key Vault,
- Databricks secret scopes.

Secrets must never be stored in:

- Git history,
- `.env.example`,
- documentation screenshots,
- logs.

---

## 9. Release process

Recommended versioning:

```text
v0.1.0 — local source platform
v0.2.0 — Redpanda streaming
v0.3.0 — MinIO Bronze
v0.4.0 — Azure Event Hub and ADLS
v0.5.0 — Databricks medallion layers
v1.0.0 — complete portfolio platform
```

---

## 10. Planned pipeline stages

```text
validate
→ test
→ build
→ deploy-dev
→ integration-test
→ deploy-demo
```

Deployment to cloud should require manual approval for cost control.

