# ADR 003: Use MinIO as the Local Bronze Layer

## Status

Accepted

## Context

The project needs local S3-compatible object storage before copying data to Azure.

## Decision

Use MinIO as the on-premises Bronze layer.

## Reason

- S3-compatible API,
- local object storage,
- simple Docker deployment,
- web console,
- suitable for Parquet files,
- mirrors cloud object-storage workflows.

## Consequences

Positive:

- local data lake copy,
- independent from cloud availability,
- supports testing before ADLS upload,
- preserves partition structure.

Negative:

- data exists in two locations,
- synchronization logic is required,
- object lifecycle must be managed.
