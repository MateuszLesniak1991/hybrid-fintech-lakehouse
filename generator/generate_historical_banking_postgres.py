#!/usr/bin/env python3

import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values, Json


FIRST_NAMES_M = [
    "Jan", "Piotr", "Tomasz", "Pawel", "Michal", "Krzysztof", "Adam",
    "Mateusz", "Marcin", "Lukasz", "Jakub", "Dawid", "Marek", "Rafal",
    "Bartosz", "Kamil", "Sebastian", "Wojciech", "Robert", "Daniel"
]

FIRST_NAMES_F = [
    "Anna", "Katarzyna", "Agnieszka", "Magdalena", "Monika", "Aleksandra",
    "Natalia", "Karolina", "Joanna", "Ewa", "Marta", "Paulina", "Dominika",
    "Patrycja", "Weronika", "Julia", "Zuzanna", "Klaudia", "Barbara"
]

LAST_NAMES = [
    "Kowalski", "Nowak", "Wisniewski", "Wojcik", "Kowalczyk", "Kaminski",
    "Lewandowski", "Zielinski", "Szymanski", "Wozniak", "Dabrowski",
    "Kozlowski", "Jankowski", "Mazur", "Kwiatkowski", "Krawczyk",
    "Piotrowski", "Grabowski", "Nowakowski", "Pawlowski"
]

CITIES = [
    "Warszawa", "Krakow", "Wroclaw", "Poznan", "Gdansk", "Lodz",
    "Katowice", "Lublin", "Rzeszow", "Szczecin", "Bydgoszcz",
    "Bialystok", "Torun", "Olsztyn", "Opole", "Kielce", "Gdynia",
    "Radom", "Gliwice", "Tychy"
]

MERCHANT_TYPES = [
    ("Market Polka", "grocery"),
    ("Fresh Basket", "grocery"),
    ("FuelPoint", "fuel"),
    ("ElectroNova", "electronics"),
    ("Cafe Verona", "restaurant"),
    ("Pizza Station", "restaurant"),
    ("TravelMate", "travel"),
    ("Hotel Amber", "hotel"),
    ("MedicaPlus", "healthcare"),
    ("Apteka Zdrowie", "pharmacy"),
    ("ModaLine", "fashion"),
    ("SportMax", "sport"),
    ("BookWorld", "education"),
    ("GameZone", "gaming"),
    ("StreamBox", "subscription"),
    ("RideNow", "transport"),
    ("Jewelry House", "jewelry"),
    ("CryptoDesk", "crypto_exchange"),
    ("OnlineMall", "online_marketplace"),
    ("HomeTools", "home_improvement")
]

SEGMENTS = ["student", "mass", "affluent", "premium", "private", "business", "senior"]
PAYMENT_METHODS = [
    "card_payment",
    "blik_payment",
    "online_payment",
    "bank_transfer",
    "atm_withdrawal"
]

DEVICE_TYPES = ["ios", "android", "windows", "macos", "linux", "unknown"]
IP_COUNTRIES = ["PL", "PL", "PL", "PL", "PL", "DE", "CZ", "SK", "NL", "GB", "US", "UA"]

SUPPORT_TYPES = [
    "card_issue", "account_access", "transaction_dispute",
    "limit_change", "kyc_question", "fraud_report"
]

CHARGEBACK_REASONS = [
    "fraud_suspected", "service_not_provided", "duplicate_transaction",
    "unauthorized_transaction", "product_not_received"
]


def new_id(prefix):
    return f"{prefix}-{uuid.uuid4()}"


def risk_level(score):
    if score >= 75:
        return "high"
    if score >= 40:
        return "medium"
    return "low"


def random_datetime(start_dt, end_dt):
    number_of_days = (end_dt.date() - start_dt.date()).days + 1
    selected_date = start_dt.date() + timedelta(
        days=random.randint(0, number_of_days - 1)
    )

    hours = list(range(24))
    hour_weights = [
        1, 1, 1, 1, 1, 2, 4, 7, 11, 15, 18, 20,
        22, 21, 20, 22, 26, 30, 28, 22, 16, 10, 6, 3
    ]

    hour = random.choices(hours, weights=hour_weights, k=1)[0]

    result = datetime(
        selected_date.year,
        selected_date.month,
        selected_date.day,
        hour,
        random.randint(0, 59),
        random.randint(0, 59),
        random.randint(0, 999999),
        tzinfo=timezone.utc
    )

    return min(max(result, start_dt), end_dt)


def limits_for_segment(segment):
    limits = {
        "student": (1000, 3000, 1000),
        "mass": (5000, 10000, 3000),
        "affluent": (10000, 25000, 5000),
        "premium": (20000, 50000, 10000),
        "private": (50000, 150000, 20000),
        "business": (30000, 200000, 10000),
        "senior": (3000, 8000, 2000)
    }
    return limits[segment]


def calculate_amount(payment_method, segment):
    multiplier = {
        "student": 0.5,
        "mass": 1.0,
        "affluent": 1.6,
        "premium": 2.2,
        "private": 3.5,
        "business": 3.0,
        "senior": 0.7
    }[segment]

    if payment_method == "atm_withdrawal":
        base = random.choice([50, 100, 200, 300, 500, 1000, 2000])
    elif payment_method == "bank_transfer":
        base = random.uniform(50, 20000)
    elif payment_method == "blik_payment":
        base = random.uniform(10, 1500)
    elif payment_method == "online_payment":
        base = random.uniform(20, 5000)
    else:
        base = random.uniform(5, 3000)

    return round(base * multiplier, 2)


def calculate_risk(payment_method, amount, ip_country, merchant_category):
    score = random.randint(1, 35)
    rule = None

    if amount > 10000:
        score += 35
        rule = "high_amount"

    if ip_country not in ("PL", "DE", "CZ", "SK"):
        score += 25
        rule = "unusual_country"

    if merchant_category in ("crypto_exchange", "gaming", "jewelry"):
        score += 18
        rule = "high_risk_merchant_category"

    if payment_method == "atm_withdrawal" and amount >= 2000:
        score += 15
        rule = "large_cash_withdrawal"

    return min(score, 100), rule


def add_stream_event(
    events,
    event_time,
    event_type,
    source_system,
    entity_type,
    entity_id,
    payload
):
    events.append((
        str(uuid.uuid4()),
        event_time,
        event_type,
        source_system,
        entity_type,
        entity_id,
        Json(payload),
        False,
        None
    ))


def connect():
    load_dotenv()

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "fintech"),
        user=os.getenv("POSTGRES_USER", "fintech_user"),
        password=os.getenv("POSTGRES_PASSWORD", "fintech_pass")
    )


def truncate_tables(cursor):
    cursor.execute("""
        TRUNCATE TABLE
            banking_source.stream_events,
            banking_source.daily_account_balances,
            banking_source.device_blacklist,
            banking_source.merchant_risk_scores,
            banking_source.customer_risk_scores,
            banking_source.chargeback_cases,
            banking_source.support_cases,
            banking_source.kyc_aml_checks,
            banking_source.transactions,
            banking_source.atms,
            banking_source.merchants,
            banking_source.accounts,
            banking_source.customers;
    """)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--customers", type=int, default=5000)
    parser.add_argument("--merchants", type=int, default=800)
    parser.add_argument("--atms", type=int, default=250)
    parser.add_argument("--transactions", type=int, default=250000)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)

    start_dt = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)
    end_dt = (
        datetime.fromisoformat(args.end_date).replace(tzinfo=timezone.utc)
        + timedelta(hours=23, minutes=59, seconds=59)
    )

    conn = connect()
    conn.autocommit = False
    cursor = conn.cursor()

    if args.reset:
        print("Resetting banking_source tables...")
        truncate_tables(cursor)
        conn.commit()

    customers = []
    accounts = []
    kyc_checks = []
    customer_risks = []
    initial_events = []

    account_lookup = {}
    customer_lookup = {}

    print(f"Generating {args.customers} customers...")

    for number in range(args.customers):
        customer_id = new_id("CUST")
        gender = random.choice(["M", "F"])

        first_name = random.choice(
            FIRST_NAMES_M if gender == "M" else FIRST_NAMES_F
        )
        last_name = random.choice(LAST_NAMES)
        city = random.choice(CITIES)

        segment = random.choices(
            SEGMENTS,
            weights=[10, 50, 18, 10, 2, 6, 4],
            k=1
        )[0]

        registration_time = random_datetime(
            start_dt - timedelta(days=720),
            start_dt
        )

        customers.append((
            customer_id,
            registration_time,
            first_name,
            last_name,
            gender,
            random.randint(1945, 2005),
            f"{first_name.lower()}.{last_name.lower()}.{number}@examplebank.local",
            f"+48{random.randint(500000000, 899999999)}",
            "PL",
            city,
            segment,
            "active",
            "crm_system"
        ))

        customer_lookup[customer_id] = {
            "segment": segment,
            "city": city
        }

        add_stream_event(
            initial_events,
            registration_time,
            "customer_registered",
            "crm_system",
            "customer",
            customer_id,
            {
                "customer_id": customer_id,
                "registration_time": registration_time.isoformat(),
                "city": city,
                "customer_segment": segment
            }
        )

        aml_score = random.randint(1, 100)
        kyc_time = registration_time + timedelta(
            minutes=random.randint(5, 720)
        )

        kyc_id = new_id("KYC")

        kyc_checks.append((
            kyc_id,
            customer_id,
            kyc_time,
            random.choices(
                ["verified", "pending", "rejected", "expired"],
                weights=[83, 10, 3, 4],
                k=1
            )[0],
            aml_score,
            risk_level(aml_score),
            random.random() < 0.015,
            random.choices(
                ["clear", "manual_review", "blocked"],
                weights=[96, 3, 1],
                k=1
            )[0],
            "kyc_aml_system"
        ))

        risk_id = new_id("RISK")
        customer_risks.append((
            risk_id,
            customer_id,
            kyc_time + timedelta(minutes=random.randint(1, 60)),
            aml_score,
            risk_level(aml_score),
            random.choice([
                "normal_behavior",
                "new_device_usage",
                "unusual_country_activity",
                "manual_review",
                "chargeback_history"
            ]),
            "customer-risk-v1",
            "customer_risk_engine"
        ))

        if random.random() < 0.92:
            number_of_accounts = random.choices(
                [1, 2, 3],
                weights=[78, 18, 4],
                k=1
            )[0]

            card_limit, transfer_limit, cash_limit = limits_for_segment(segment)

            for _ in range(number_of_accounts):
                account_id = new_id("ACC")
                account = (
                    account_id,
                    customer_id,
                    registration_time + timedelta(
                        minutes=random.randint(10, 1440)
                    ),
                    random.choices(
                        ["personal", "savings", "business", "credit_card"],
                        weights=[65, 15, 12, 8],
                        k=1
                    )[0],
                    random.choices(
                        ["PLN", "EUR", "USD"],
                        weights=[90, 7, 3],
                        k=1
                    )[0],
                    "active",
                    Decimal(str(card_limit)),
                    Decimal(str(transfer_limit)),
                    Decimal(str(cash_limit)),
                    "core_banking_system"
                )

                accounts.append(account)

                account_lookup.setdefault(customer_id, []).append({
                    "account_id": account_id,
                    "currency": account[4],
                    "card_limit": float(card_limit),
                    "transfer_limit": float(transfer_limit),
                    "cash_limit": float(cash_limit)
                })

    execute_values(
        cursor,
        """
        INSERT INTO banking_source.customers (
            customer_id, registration_time, first_name, last_name, gender,
            birth_year, email, phone_number, country, city,
            customer_segment, customer_status, source_system
        ) VALUES %s
        """,
        customers,
        page_size=args.batch_size
    )

    execute_values(
        cursor,
        """
        INSERT INTO banking_source.accounts (
            account_id, customer_id, opened_at, account_type, currency,
            account_status, daily_card_limit, daily_transfer_limit,
            daily_cash_withdrawal_limit, source_system
        ) VALUES %s
        """,
        accounts,
        page_size=args.batch_size
    )

    execute_values(
        cursor,
        """
        INSERT INTO banking_source.kyc_aml_checks (
            kyc_event_id, customer_id, event_time, kyc_status,
            aml_risk_score, aml_risk_level, pep_flag,
            sanctions_screening_status, source_system
        ) VALUES %s
        """,
        kyc_checks,
        page_size=args.batch_size
    )

    execute_values(
        cursor,
        """
        INSERT INTO banking_source.customer_risk_scores (
            risk_event_id, customer_id, score_time, risk_score,
            risk_level, main_risk_driver, model_version, source_system
        ) VALUES %s
        """,
        customer_risks,
        page_size=args.batch_size
    )

    conn.commit()

    print(f"Customers inserted: {len(customers)}")
    print(f"Accounts inserted: {len(accounts)}")

    merchants = []
    merchant_risks = []
    merchant_lookup = []

    print(f"Generating {args.merchants} merchants...")

    for number in range(args.merchants):
        merchant_id = new_id("MERCH")
        base_name, category = random.choice(MERCHANT_TYPES)
        city = random.choice(CITIES)

        merchants.append((
            merchant_id,
            f"{base_name} {city} {number}",
            category,
            city,
            random.choices(
                ["PL", "DE", "CZ", "SK", "NL"],
                weights=[85, 5, 4, 3, 3],
                k=1
            )[0],
            new_id("TERM"),
            random.choices(
                ["active", "watchlist", "blocked"],
                weights=[92, 7, 1],
                k=1
            )[0],
            "merchant_management_system"
        ))

        merchant_lookup.append({
            "merchant_id": merchant_id,
            "merchant_name": merchants[-1][1],
            "category": category,
            "city": city
        })

        merchant_score = random.randint(1, 100)

        merchant_risks.append((
            new_id("MRISK"),
            merchant_id,
            random_datetime(start_dt, end_dt),
            merchant_score,
            risk_level(merchant_score),
            random.choice([
                "normal_behavior",
                "chargeback_rate",
                "fraud_alerts",
                "new_merchant",
                "category_risk"
            ]),
            "merchant_risk_engine"
        ))

    execute_values(
        cursor,
        """
        INSERT INTO banking_source.merchants (
            merchant_id, merchant_name, merchant_category, city,
            country, terminal_id, merchant_status, source_system
        ) VALUES %s
        """,
        merchants,
        page_size=args.batch_size
    )

    execute_values(
        cursor,
        """
        INSERT INTO banking_source.merchant_risk_scores (
            merchant_risk_event_id, merchant_id, score_time,
            merchant_risk_score, merchant_risk_level,
            main_risk_driver, source_system
        ) VALUES %s
        """,
        merchant_risks,
        page_size=args.batch_size
    )

    atms = []
    atm_lookup = []

    print(f"Generating {args.atms} ATMs...")

    for _ in range(args.atms):
        atm_id = new_id("ATM")
        city = random.choice(CITIES)

        atms.append((
            atm_id,
            random.choice([
                "ExampleBank", "CashNet", "EuronetLike",
                "Bankomat24", "MetroCash"
            ]),
            city,
            "PL",
            random.choice([
                "bank_branch", "shopping_mall", "train_station",
                "airport", "fuel_station", "street"
            ]),
            "active",
            "atm_network_system"
        ))

        atm_lookup.append({
            "atm_id": atm_id,
            "city": city
        })

    execute_values(
        cursor,
        """
        INSERT INTO banking_source.atms (
            atm_id, operator, city, country, location_type,
            atm_status, source_system
        ) VALUES %s
        """,
        atms,
        page_size=args.batch_size
    )

    execute_values(
        cursor,
        """
        INSERT INTO banking_source.stream_events (
            event_id, event_time, event_type, source_system,
            entity_type, entity_id, payload_json,
            replayed_flag, replayed_at
        ) VALUES %s
        """,
        initial_events,
        page_size=args.batch_size
    )

    conn.commit()

    customer_ids = list(account_lookup.keys())

    transaction_batch = []
    stream_batch = []
    support_batch = []
    blacklist_batch = []
    authorized_transactions = []

    print(f"Generating {args.transactions} transactions...")

    for number in range(1, args.transactions + 1):
        customer_id = random.choice(customer_ids)
        customer = customer_lookup[customer_id]
        account = random.choice(account_lookup[customer_id])
        event_time = random_datetime(start_dt, end_dt)

        payment_method = random.choices(
            PAYMENT_METHODS,
            weights=[42, 18, 18, 12, 10],
            k=1
        )[0]

        merchant = None
        atm = None
        merchant_category = None
        channel = "pos"
        city = customer["city"]

        if payment_method == "atm_withdrawal":
            atm = random.choice(atm_lookup)
            city = atm["city"]
            channel = "atm"
        elif payment_method == "bank_transfer":
            channel = random.choice(["mobile", "web"])
        else:
            merchant = random.choice(merchant_lookup)
            merchant_category = merchant["category"]
            city = merchant["city"]
            channel = random.choice(["pos", "mobile", "web"])

        amount = calculate_amount(
            payment_method,
            customer["segment"]
        )

        ip_country = random.choice(IP_COUNTRIES)

        risk_score, fraud_rule = calculate_risk(
            payment_method,
            amount,
            ip_country,
            merchant_category
        )

        authorization_status = "authorized"
        decline_reason = None

        if (
            payment_method == "atm_withdrawal"
            and amount > account["cash_limit"]
        ):
            authorization_status = "declined"
            decline_reason = "daily_limit_exceeded"

        elif (
            payment_method in (
                "card_payment",
                "blik_payment",
                "online_payment"
            )
            and amount > account["card_limit"]
        ):
            authorization_status = "declined"
            decline_reason = "daily_limit_exceeded"

        elif (
            payment_method == "bank_transfer"
            and amount > account["transfer_limit"]
        ):
            authorization_status = "declined"
            decline_reason = "daily_limit_exceeded"

        elif risk_score >= 88 and random.random() < 0.65:
            authorization_status = "declined"
            decline_reason = "suspected_fraud"

        elif random.random() < 0.025:
            authorization_status = "declined"
            decline_reason = random.choice([
                "insufficient_funds",
                "invalid_authentication",
                "technical_error"
            ])

        fraud_suspected = (
            risk_score >= 75
            or decline_reason == "suspected_fraud"
        )

        transaction_id = new_id("TX")
        device_id = f"device-{random.randint(1000, 99999)}"

        source_system = (
            "atm_network_system"
            if payment_method == "atm_withdrawal"
            else "payment_gateway"
        )

        transaction_batch.append((
            transaction_id,
            event_time,
            customer_id,
            account["account_id"],
            merchant["merchant_id"] if merchant else None,
            atm["atm_id"] if atm else None,
            payment_method,
            channel,
            Decimal(str(amount)),
            account["currency"],
            authorization_status,
            decline_reason,
            device_id,
            random.choice(DEVICE_TYPES),
            ip_country,
            city,
            risk_score,
            fraud_suspected,
            fraud_rule,
            source_system
        ))

        payload = {
            "transaction_id": transaction_id,
            "event_time": event_time.isoformat(),
            "customer_id": customer_id,
            "account_id": account["account_id"],
            "merchant_id": merchant["merchant_id"] if merchant else None,
            "merchant_name": merchant["merchant_name"] if merchant else None,
            "merchant_category": merchant_category,
            "atm_id": atm["atm_id"] if atm else None,
            "payment_method": payment_method,
            "channel": channel,
            "amount": amount,
            "currency": account["currency"],
            "authorization_status": authorization_status,
            "decline_reason": decline_reason,
            "device_id": device_id,
            "ip_country": ip_country,
            "city": city,
            "risk_score": risk_score,
            "is_fraud_suspected": fraud_suspected,
            "fraud_rule_hit": fraud_rule
        }

        add_stream_event(
            stream_batch,
            event_time,
            f"{payment_method}_{authorization_status}",
            source_system,
            "transaction",
            transaction_id,
            payload
        )

        if fraud_suspected:
            add_stream_event(
                stream_batch,
                event_time,
                "high_risk_transaction_detected",
                "fraud_risk_engine",
                "transaction",
                transaction_id,
                payload
            )

        if authorization_status == "authorized":
            authorized_transactions.append((
                transaction_id,
                customer_id,
                merchant["merchant_id"] if merchant else None,
                amount,
                account["currency"],
                event_time
            ))

        if random.random() < 0.008:
            support_case_id = new_id("SC")

            support_batch.append((
                support_case_id,
                customer_id,
                event_time,
                random.choice(SUPPORT_TYPES),
                random.choice([
                    "open", "in_progress", "resolved", "rejected"
                ]),
                random.choices(
                    ["low", "medium", "high", "critical"],
                    weights=[42, 38, 15, 5],
                    k=1
                )[0],
                "customer_support_system"
            ))

        if random.random() < 0.003:
            blacklist_batch.append((
                new_id("BLDEV"),
                device_id,
                event_time,
                random.choice([
                    "fraud_confirmed",
                    "account_takeover",
                    "multiple_failed_attempts",
                    "suspicious_location"
                ]),
                random.choice(["medium", "high", "critical"]),
                True,
                "fraud_risk_engine"
            ))

        if len(transaction_batch) >= args.batch_size:
            execute_values(
                cursor,
                """
                INSERT INTO banking_source.transactions (
                    transaction_id, event_time, customer_id, account_id,
                    merchant_id, atm_id, payment_method, channel, amount,
                    currency, authorization_status, decline_reason,
                    device_id, device_type, ip_country, city, risk_score,
                    is_fraud_suspected, fraud_rule_hit, source_system
                ) VALUES %s
                """,
                transaction_batch,
                page_size=args.batch_size
            )

            execute_values(
                cursor,
                """
                INSERT INTO banking_source.stream_events (
                    event_id, event_time, event_type, source_system,
                    entity_type, entity_id, payload_json,
                    replayed_flag, replayed_at
                ) VALUES %s
                """,
                stream_batch,
                page_size=args.batch_size
            )

            if support_batch:
                execute_values(
                    cursor,
                    """
                    INSERT INTO banking_source.support_cases (
                        support_case_id, customer_id, created_at,
                        case_type, case_status, priority, source_system
                    ) VALUES %s
                    """,
                    support_batch
                )

            if blacklist_batch:
                execute_values(
                    cursor,
                    """
                    INSERT INTO banking_source.device_blacklist (
                        blacklist_event_id, device_id, event_time,
                        reason, severity, active_flag, source_system
                    ) VALUES %s
                    """,
                    blacklist_batch
                )

            conn.commit()

            transaction_batch.clear()
            stream_batch.clear()
            support_batch.clear()
            blacklist_batch.clear()

        if number % 25000 == 0:
            print(
                f"Progress: {number:,}/{args.transactions:,} transactions"
            )

    if transaction_batch:
        execute_values(
            cursor,
            """
            INSERT INTO banking_source.transactions (
                transaction_id, event_time, customer_id, account_id,
                merchant_id, atm_id, payment_method, channel, amount,
                currency, authorization_status, decline_reason,
                device_id, device_type, ip_country, city, risk_score,
                is_fraud_suspected, fraud_rule_hit, source_system
            ) VALUES %s
            """,
            transaction_batch
        )

    if stream_batch:
        execute_values(
            cursor,
            """
            INSERT INTO banking_source.stream_events (
                event_id, event_time, event_type, source_system,
                entity_type, entity_id, payload_json,
                replayed_flag, replayed_at
            ) VALUES %s
            """,
            stream_batch
        )

    if support_batch:
        execute_values(
            cursor,
            """
            INSERT INTO banking_source.support_cases (
                support_case_id, customer_id, created_at,
                case_type, case_status, priority, source_system
            ) VALUES %s
            """,
            support_batch
        )

    if blacklist_batch:
        execute_values(
            cursor,
            """
            INSERT INTO banking_source.device_blacklist (
                blacklist_event_id, device_id, event_time,
                reason, severity, active_flag, source_system
            ) VALUES %s
            """,
            blacklist_batch
        )

    conn.commit()

    chargeback_count = min(
        len(authorized_transactions),
        max(1000, args.transactions // 100)
    )

    print(f"Generating {chargeback_count} chargebacks...")

    chargebacks = []

    for transaction in random.sample(
        authorized_transactions,
        chargeback_count
    ):
        (
            transaction_id,
            customer_id,
            merchant_id,
            amount,
            currency,
            transaction_time
        ) = transaction

        chargebacks.append((
            new_id("CB"),
            transaction_id,
            customer_id,
            merchant_id,
            transaction_time + timedelta(
                days=random.randint(1, 14),
                hours=random.randint(0, 23)
            ),
            random.choice(CHARGEBACK_REASONS),
            random.choice([
                "open", "in_progress", "resolved", "rejected"
            ]),
            Decimal(str(amount)),
            currency,
            "chargeback_management_system"
        ))

    execute_values(
        cursor,
        """
        INSERT INTO banking_source.chargeback_cases (
            chargeback_id, transaction_id, customer_id, merchant_id,
            case_opened_at, reason_code, case_status,
            amount, currency, source_system
        ) VALUES %s
        """,
        chargebacks,
        page_size=args.batch_size
    )

    conn.commit()

    print("Generating daily balances...")

    balance_batch = []
    current_date = start_dt.date()

    while current_date <= end_dt.date():
        for account in accounts:
            if random.random() < 0.98:
                opening_balance = round(
                    random.uniform(100, 120000),
                    2
                )

                closing_balance = max(
                    opening_balance + random.uniform(-8000, 8000),
                    0
                )

                available_balance = max(
                    closing_balance - random.uniform(0, 2000),
                    0
                )

                balance_batch.append((
                    new_id("BAL"),
                    account[0],
                    current_date,
                    Decimal(str(round(opening_balance, 2))),
                    Decimal(str(round(closing_balance, 2))),
                    Decimal(str(round(available_balance, 2))),
                    account[4],
                    "core_banking_ledger",
                    datetime(
                        current_date.year,
                        current_date.month,
                        current_date.day,
                        1,
                        tzinfo=timezone.utc
                    )
                ))

                if len(balance_batch) >= args.batch_size:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO banking_source.daily_account_balances (
                            balance_id, account_id, balance_date,
                            opening_balance, closing_balance,
                            available_balance, currency,
                            source_system, created_at
                        ) VALUES %s
                        """,
                        balance_batch
                    )
                    conn.commit()
                    balance_batch.clear()

        current_date += timedelta(days=1)

    if balance_batch:
        execute_values(
            cursor,
            """
            INSERT INTO banking_source.daily_account_balances (
                balance_id, account_id, balance_date,
                opening_balance, closing_balance,
                available_balance, currency,
                source_system, created_at
            ) VALUES %s
            """,
            balance_batch
        )

    conn.commit()

    print("Creating indexes...")

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_transactions_event_time
            ON banking_source.transactions(event_time);

        CREATE INDEX IF NOT EXISTS idx_transactions_customer_id
            ON banking_source.transactions(customer_id);

        CREATE INDEX IF NOT EXISTS idx_transactions_risk
            ON banking_source.transactions(risk_score);

        CREATE INDEX IF NOT EXISTS idx_stream_events_event_time
            ON banking_source.stream_events(event_time);

        CREATE INDEX IF NOT EXISTS idx_stream_events_replayed
            ON banking_source.stream_events(replayed_flag);

        CREATE INDEX IF NOT EXISTS idx_stream_events_type
            ON banking_source.stream_events(event_type);

        CREATE INDEX IF NOT EXISTS idx_daily_balances_date
            ON banking_source.daily_account_balances(balance_date);
    """)

    conn.commit()

    cursor.close()
    conn.close()

    print("Historical generation finished successfully.")


if __name__ == "__main__":
    main()
