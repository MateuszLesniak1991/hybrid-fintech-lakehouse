#!/usr/bin/env python3
"""
Generate realistic banking transactions continuously.

Each transaction is inserted atomically into:
- banking_source.transactions
- banking_source.stream_events (outbox row, replayed_flag = false)

The outbox publisher sends stream_events to Redpanda separately.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import Json


LOG = logging.getLogger("realtime-banking-generator")
STOP_REQUESTED = False


def handle_signal(signum: int, frame: Any) -> None:
    del frame
    global STOP_REQUESTED
    LOG.info("Received signal %s; stopping after current transaction.", signum)
    STOP_REQUESTED = True


@dataclass(frozen=True)
class ReferenceData:
    account_id: str
    customer_id: str
    merchant_id: str | None
    atm_id: str | None


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "fintech"),
        user=os.getenv("POSTGRES_USER", "fintech_user"),
        password=required_env("POSTGRES_PASSWORD"),
        connect_timeout=10,
        application_name="realtime_banking_generator",
    )


def fetch_reference_data(conn) -> ReferenceData:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.account_id, a.customer_id
            FROM banking_source.accounts a
            ORDER BY random()
            LIMIT 1
            """
        )
        account = cur.fetchone()
        if not account:
            raise RuntimeError("No accounts found in banking_source.accounts")

        cur.execute(
            """
            SELECT merchant_id
            FROM banking_source.merchants
            ORDER BY random()
            LIMIT 1
            """
        )
        merchant = cur.fetchone()

        cur.execute(
            """
            SELECT atm_id
            FROM banking_source.atms
            ORDER BY random()
            LIMIT 1
            """
        )
        atm = cur.fetchone()

    return ReferenceData(
        account_id=account[0],
        customer_id=account[1],
        merchant_id=merchant[0] if merchant else None,
        atm_id=atm[0] if atm else None,
    )


def choose_scenario(ref: ReferenceData) -> dict[str, Any]:
    fraud = random.random() < 0.08
    use_atm = random.random() < 0.16

    if use_atm:
        channel = "ATM"
        payment_method = "cash_withdrawal"
        merchant_id = None
        atm_id = ref.atm_id
        amount = Decimal(str(round(random.uniform(20, 900), 2)))
        device_type = "atm_terminal"
        city = random.choice(["Warsaw", "Krakow", "Wroclaw", "Poznan", "Gdansk"])
    else:
        channel = random.choice(["POS", "ECOMMERCE", "MOBILE"])
        payment_method = random.choice(
            ["card", "contactless", "mobile_wallet", "bank_transfer"]
        )
        merchant_id = ref.merchant_id
        atm_id = None
        amount = Decimal(str(round(random.uniform(4, 2500), 2)))
        device_type = random.choice(["mobile", "desktop", "pos_terminal", "tablet"])
        city = random.choice(
            ["Warsaw", "Krakow", "Wroclaw", "Poznan", "Gdansk", "Lodz", "Berlin", "Prague"]
        )

    if fraud:
        risk_score = random.randint(82, 99)
        is_fraud_suspected = True
        fraud_rule_hit = random.choice(
            [
                "HIGH_VALUE_TRANSACTION",
                "IMPOSSIBLE_TRAVEL",
                "BLACKLISTED_DEVICE",
                "VELOCITY_BREACH",
                "UNUSUAL_GEOLOCATION",
            ]
        )
        if random.random() < 0.35:
            authorization_status = "DECLINED"
            decline_reason = "FRAUD_SUSPECTED"
        else:
            authorization_status = "APPROVED"
            decline_reason = None
        ip_country = random.choice(["RU", "CN", "NG", "BR", "US", "DE"])
    else:
        risk_score = random.randint(1, 74)
        is_fraud_suspected = False
        fraud_rule_hit = None
        authorization_status = "APPROVED" if random.random() < 0.96 else "DECLINED"
        decline_reason = None if authorization_status == "APPROVED" else random.choice(
            ["INSUFFICIENT_FUNDS", "CARD_EXPIRED", "LIMIT_EXCEEDED"]
        )
        ip_country = random.choice(["PL", "PL", "PL", "DE", "CZ", "SK"])

    return {
        "channel": channel,
        "payment_method": payment_method,
        "merchant_id": merchant_id,
        "atm_id": atm_id,
        "amount": amount,
        "currency": random.choice(["PLN", "PLN", "PLN", "EUR", "USD"]),
        "authorization_status": authorization_status,
        "decline_reason": decline_reason,
        "device_id": f"DEV-{uuid.uuid4().hex[:16].upper()}",
        "device_type": device_type,
        "ip_country": ip_country,
        "city": city,
        "risk_score": risk_score,
        "is_fraud_suspected": is_fraud_suspected,
        "fraud_rule_hit": fraud_rule_hit,
    }


def build_event(
    transaction_id: str,
    event_id: str,
    event_time: datetime,
    ref: ReferenceData,
    tx: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    if tx["is_fraud_suspected"]:
        event_type = "high_risk_transaction_detected"
    elif tx["authorization_status"] == "DECLINED":
        event_type = "transaction_declined"
    else:
        event_type = "transaction_authorized"

    payload = {
        "event_id": event_id,
        "event_time": event_time.isoformat(),
        "event_type": event_type,
        "source_system": "core_banking_realtime",
        "entity_type": "transaction",
        "entity_id": transaction_id,
        "transaction_id": transaction_id,
        "customer_id": ref.customer_id,
        "account_id": ref.account_id,
        "merchant_id": tx["merchant_id"],
        "atm_id": tx["atm_id"],
        "payment_method": tx["payment_method"],
        "channel": tx["channel"],
        "amount": float(tx["amount"]),
        "currency": tx["currency"],
        "authorization_status": tx["authorization_status"],
        "decline_reason": tx["decline_reason"],
        "device_id": tx["device_id"],
        "device_type": tx["device_type"],
        "ip_country": tx["ip_country"],
        "city": tx["city"],
        "risk_score": tx["risk_score"],
        "is_fraud_suspected": tx["is_fraud_suspected"],
        "fraud_rule_hit": tx["fraud_rule_hit"],
    }
    return event_type, payload


def insert_transaction_and_event(conn) -> tuple[str, str, str]:
    ref = fetch_reference_data(conn)
    tx = choose_scenario(ref)
    event_time = datetime.now(timezone.utc)
    transaction_id = f"TX-RT-{uuid.uuid4().hex.upper()}"
    event_id = f"EVT-RT-{uuid.uuid4().hex.upper()}"

    event_type, payload = build_event(
        transaction_id=transaction_id,
        event_id=event_id,
        event_time=event_time,
        ref=ref,
        tx=tx,
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO banking_source.transactions (
                    transaction_id,
                    event_time,
                    customer_id,
                    account_id,
                    merchant_id,
                    atm_id,
                    payment_method,
                    channel,
                    amount,
                    currency,
                    authorization_status,
                    decline_reason,
                    device_id,
                    device_type,
                    ip_country,
                    city,
                    risk_score,
                    is_fraud_suspected,
                    fraud_rule_hit,
                    source_system
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    transaction_id,
                    event_time,
                    ref.customer_id,
                    ref.account_id,
                    tx["merchant_id"],
                    tx["atm_id"],
                    tx["payment_method"],
                    tx["channel"],
                    tx["amount"],
                    tx["currency"],
                    tx["authorization_status"],
                    tx["decline_reason"],
                    tx["device_id"],
                    tx["device_type"],
                    tx["ip_country"],
                    tx["city"],
                    tx["risk_score"],
                    tx["is_fraud_suspected"],
                    tx["fraud_rule_hit"],
                    "core_banking_realtime",
                ),
            )
            cur.execute(
                """
                INSERT INTO banking_source.stream_events (
                    event_id,
                    event_time,
                    event_type,
                    source_system,
                    entity_type,
                    entity_id,
                    payload_json,
                    replayed_flag,
                    replayed_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, FALSE, NULL)
                """,
                (
                    event_id,
                    event_time,
                    event_type,
                    "core_banking_realtime",
                    "transaction",
                    transaction_id,
                    Json(payload),
                ),
            )

    return transaction_id, event_id, event_type


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continuously generate individual banking transactions."
    )
    parser.add_argument("--min-interval", type=float, default=2.0)
    parser.add_argument("--max-interval", type=float, default=8.0)
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Stop after N events. Use 0 for continuous operation.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to dotenv file. Default: .env",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_dotenv(Path(args.env_file), override=False)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    if args.min_interval < 0 or args.max_interval < args.min_interval:
        raise ValueError("Invalid interval range")

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    conn = connect()
    conn.autocommit = False
    produced = 0

    LOG.info(
        "Realtime generator started. Interval %.1f-%.1fs, max_events=%s",
        args.min_interval,
        args.max_interval,
        args.max_events or "unlimited",
    )

    try:
        while not STOP_REQUESTED:
            try:
                transaction_id, event_id, event_type = insert_transaction_and_event(conn)
                produced += 1
                LOG.info(
                    "[GENERATED] transaction_id=%s event_id=%s event_type=%s total=%d",
                    transaction_id,
                    event_id,
                    event_type,
                    produced,
                )
            except psycopg2.Error:
                conn.rollback()
                LOG.exception("Database operation failed; reconnecting.")
                try:
                    conn.close()
                except Exception:
                    pass
                time.sleep(3)
                conn = connect()
                conn.autocommit = False
                continue

            if args.max_events and produced >= args.max_events:
                break

            time.sleep(random.uniform(args.min_interval, args.max_interval))
    finally:
        conn.close()
        LOG.info("Realtime generator stopped. Generated=%d", produced)


if __name__ == "__main__":
    main()
