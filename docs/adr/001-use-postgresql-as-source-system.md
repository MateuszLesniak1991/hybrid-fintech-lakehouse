# ADR 001: Use PostgreSQL as the Local Source System

## Status

Accepted

## Context

The project requires a realistic source database capable of storing customers, accounts, transactions, balances, compliance data and operational events.

SQLite was suitable for an edge device concept, but the main VM requires:

- larger data volume,
- concurrent access,
- web administration,
- efficient batch inserts,
- relational schemas,
- SQL analytics,
- integration with Python.

## Decision

Use PostgreSQL as the primary local source system.

## Consequences

Positive:

- realistic enterprise database,
- supports 250,000+ transactions,
- works well with Python,
- supports JSONB,
- supports server-side cursors,
- accessible through pgAdmin.

Negative:

- requires a running service,
- consumes more resources than SQLite,
- requires connection configuration.
