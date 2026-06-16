# ADR 004: Use Hourly Parquet Partitioning

## Status

Accepted

## Context

The batch pipeline requires incremental exports from PostgreSQL to object storage.

## Decision

Store event datasets as hourly Parquet files partitioned by:

```text
year/month/day/hour
```

## Reason

- enables incremental ingestion,
- supports partition pruning,
- simplifies reprocessing,
- creates predictable object paths,
- resembles production data lake patterns,
- works well for the 31-day portfolio dataset.

## Consequences

Positive:

- easy hourly backfills,
- easy validation,
- efficient selective reads,
- clear lineage.

Negative:

- low-volume datasets create small files,
- later compaction may be required,
- object counts are higher.
