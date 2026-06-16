# ADR 002: Use Redpanda for Local Event Streaming

## Status

Accepted

## Context

The platform requires a Kafka-compatible local broker for event replay, consumer groups and future cloud forwarding.

## Decision

Use Redpanda as the local event broker.

## Reason

- Kafka API compatibility,
- simple Docker deployment,
- lower operational complexity for a portfolio lab,
- Redpanda Console for message inspection,
- suitable for replaying historical events.

## Consequences

Positive:

- local streaming environment,
- easy topic inspection,
- standard Kafka clients,
- restartable consumers.

Negative:

- another infrastructure component,
- topic retention and storage must be managed,
- cloud bridge must be implemented separately.
