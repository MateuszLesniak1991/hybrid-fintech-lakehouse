# ADR 005: Preserve Original Event Time During Replay

## Status

Accepted

## Context

Historical events must look fresh when demonstrated in a real-time pipeline, but their original monthly timestamps must remain available.

## Decision

During replay:

```text
event_time = current replay timestamp
original_event_time = original PostgreSQL event timestamp
```

## Reason

- supports real-time demonstrations,
- preserves historical audit context,
- allows both processing-time and event-time analysis,
- prevents loss of the source timestamp.

## Consequences

Positive:

- fresh Event Hub and Fabric stream,
- historical month remains analyzable,
- clear replay metadata.

Negative:

- downstream users must understand two timestamps,
- dashboards must select the correct timestamp for each use case.
