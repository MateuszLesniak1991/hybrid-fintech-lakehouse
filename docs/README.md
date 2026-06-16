# Hybrid FinTech Lakehouse Platform — Documentation

This directory contains the technical and business documentation for the **Hybrid FinTech Lakehouse Platform** portfolio project.

## Documentation map

| Document | Purpose |
|---|---|
| [01_business_scenario.md](01_business_scenario.md) | Banking scenario, actors, source systems and business use cases |
| [02_architecture.md](02_architecture.md) | Hybrid on-premises and cloud architecture |
| [03_data_model.md](03_data_model.md) | PostgreSQL source model and dataset descriptions |
| [04_pipeline_flow.md](04_pipeline_flow.md) | End-to-end batch and streaming flows |
| [05_bronze_silver_gold.md](05_bronze_silver_gold.md) | Medallion architecture and transformation rules |
| [06_local_setup.md](06_local_setup.md) | Local environment setup and execution |
| [07_cloud_setup.md](07_cloud_setup.md) | Azure, Fabric and Databricks implementation plan |
| [08_data_quality.md](08_data_quality.md) | Data quality rules, controls and validation queries |
| [09_ci_cd_plan.md](09_ci_cd_plan.md) | CI/CD roadmap and deployment strategy |
| [10_project_results.md](10_project_results.md) | Current results, scale and validation evidence |
| [adr/](adr/) | Architecture Decision Records |

## Current implementation status

| Component | Status |
|---|---|
| PostgreSQL source database | Completed |
| Historical banking data generator | Completed |
| Redpanda replay pipeline | Completed |
| Hourly Parquet export to MinIO | Completed |
| Azure Event Hub connectivity test | Completed |
| Redpanda to Event Hub bridge | Completed |
| MinIO to ADLS Gen2 synchronization | Completed |
| Microsoft Fabric Eventstream | Completed |
| Databricks Bronze/Silver/Gold | Completed |
| Power BI dashboards | Completed |
| CI/CD | Completed|

The documentation distinguishes clearly between **implemented**, **tested** and **planned** components.
