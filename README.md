# Hybrid FinTech Lakehouse Platform

Hybrid FinTech Lakehouse Platform is a portfolio Data Engineering project
simulating realistic banking operations and processing data through two
parallel pipelines:

- near real-time fraud detection,
- batch customer and risk analytics.

The platform combines on-premise components with Microsoft Azure services.


## Business problem

A bank receives data from multiple operational systems, including:

- payment gateways,
- ATM networks,
- core banking systems,
- CRM,
- KYC/AML systems,
- fraud engines,
- customer support systems.

Some events must be analyzed almost immediately for fraud detection, while
other datasets are processed in batches for customer analytics, reporting
and historical analysis.

## Architecture

![Hybrid FinTech Lakehouse Architecture](images/architecture.png)


The platform uses two processing paths:

1. Streaming path:
   PostgreSQL → Redpanda → Azure Event Hub → Microsoft Fabric

2. Batch path:
   PostgreSQL → hourly Parquet files → MinIO → ADLS Gen2 → Databricks
