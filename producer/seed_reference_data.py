from faker import Faker
from dotenv import load_dotenv
from datetime import datetime, timezone
import argparse
import os
import psycopg2
import random
import time

load_dotenv()

fake = Faker("pl_PL")

# Initial baseline data
INITIAL_CUSTOMERS_MIN = 900
INITIAL_CUSTOMERS_MAX = 1200

INITIAL_ACCOUNTS_MIN = 1300
INITIAL_ACCOUNTS_MAX = 1800

INITIAL_MERCHANTS_MIN = 200
INITIAL_MERCHANTS_MAX = 300

# Incremental growth per cycle
NEW_CUSTOMERS_MIN = 1
NEW_CUSTOMERS_MAX = 8

NEW_ACCOUNTS_MIN = 1
NEW_ACCOUNTS_MAX = 12

NEW_MERCHANTS_MIN = 0
NEW_MERCHANTS_MAX = 3

# Random wait time between incremental batches
SLEEP_SECONDS_MIN = 30
SLEEP_SECONDS_MAX = 180


def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )


def get_next_id(cursor, table_name, id_column):
    cursor.execute(f"SELECT COALESCE(MAX({id_column}), 0) + 1 FROM {table_name};")
    return cursor.fetchone()[0]


def get_existing_customer_ids(cursor):
    cursor.execute("SELECT customer_id FROM customers;")
    return [row[0] for row in cursor.fetchall()]


def get_table_count(cursor, table_name):
    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
    return cursor.fetchone()[0]


def get_current_timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def generate_customer(customer_id):
    return (
        customer_id,
        fake.first_name(),
        fake.last_name(),
        fake.unique.email(),
        fake.city(),
        "Poland",
        random.choice(["standard", "premium", "business"]),
        random.randint(1, 100),
        fake.date_time_between(start_date="-5y", end_date="now"),
    )


def generate_account(account_id, customer_id):
    return (
        account_id,
        customer_id,
        random.choice(["personal", "savings", "business"]),
        random.choice(["PLN", "EUR", "USD"]),
        random.choices(
            ["active", "blocked", "closed"],
            weights=[90, 7, 3],
            k=1,
        )[0],
        fake.date_time_between(start_date="-5y", end_date="now"),
    )


def generate_merchant(merchant_id):
    categories = [
        "grocery",
        "fuel",
        "electronics",
        "restaurant",
        "travel",
        "online",
        "atm",
        "subscription",
        "entertainment",
        "healthcare",
        "insurance",
        "gaming",
        "education",
    ]

    return (
        merchant_id,
        fake.company(),
        random.choice(categories),
        fake.city(),
        "Poland",
    )


def insert_customers(cursor, start_id, count):
    for customer_id in range(start_id, start_id + count):
        cursor.execute(
            """
            INSERT INTO customers (
                customer_id, first_name, last_name, email, city, country,
                customer_segment, risk_score, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING
            """,
            generate_customer(customer_id),
        )


def insert_accounts(cursor, start_id, count, customer_ids):
    if not customer_ids:
        print("No customers found. Skipping account generation.")
        return

    for account_id in range(start_id, start_id + count):
        customer_id = random.choice(customer_ids)

        cursor.execute(
            """
            INSERT INTO accounts (
                account_id, customer_id, account_type, currency, status, opened_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (account_id) DO NOTHING
            """,
            generate_account(account_id, customer_id),
        )


def insert_merchants(cursor, start_id, count):
    for merchant_id in range(start_id, start_id + count):
        cursor.execute(
            """
            INSERT INTO merchants (
                merchant_id, merchant_name, merchant_category, city, country
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (merchant_id) DO NOTHING
            """,
            generate_merchant(merchant_id),
        )


def print_counts(cursor):
    customers_count = get_table_count(cursor, "customers")
    accounts_count = get_table_count(cursor, "accounts")
    merchants_count = get_table_count(cursor, "merchants")

    print(
        f"[{get_current_timestamp()}] Current counts | "
        f"customers={customers_count}, "
        f"accounts={accounts_count}, "
        f"merchants={merchants_count}"
    )


def run_initial_seed(cursor):
    customers_count = get_table_count(cursor, "customers")

    if customers_count > 0:
        print("Initial seed skipped: reference data already exists.")
        print_counts(cursor)
        return

    customers_to_create = random.randint(INITIAL_CUSTOMERS_MIN, INITIAL_CUSTOMERS_MAX)
    accounts_to_create = random.randint(INITIAL_ACCOUNTS_MIN, INITIAL_ACCOUNTS_MAX)
    merchants_to_create = random.randint(INITIAL_MERCHANTS_MIN, INITIAL_MERCHANTS_MAX)

    print("Running initial seed...")
    print(
        f"Initial batch size | "
        f"customers={customers_to_create}, "
        f"accounts={accounts_to_create}, "
        f"merchants={merchants_to_create}"
    )

    customer_start_id = get_next_id(cursor, "customers", "customer_id")
    account_start_id = get_next_id(cursor, "accounts", "account_id")
    merchant_start_id = get_next_id(cursor, "merchants", "merchant_id")

    insert_customers(cursor, customer_start_id, customers_to_create)

    customer_ids = get_existing_customer_ids(cursor)

    insert_accounts(cursor, account_start_id, accounts_to_create, customer_ids)
    insert_merchants(cursor, merchant_start_id, merchants_to_create)

    print("Initial seed completed.")
    print_counts(cursor)


def run_incremental_seed(cursor):
    new_customers = random.randint(NEW_CUSTOMERS_MIN, NEW_CUSTOMERS_MAX)
    new_accounts = random.randint(NEW_ACCOUNTS_MIN, NEW_ACCOUNTS_MAX)
    new_merchants = random.randint(NEW_MERCHANTS_MIN, NEW_MERCHANTS_MAX)

    print(
        f"[{get_current_timestamp()}] Incremental batch | "
        f"new_customers={new_customers}, "
        f"new_accounts={new_accounts}, "
        f"new_merchants={new_merchants}"
    )

    customer_start_id = get_next_id(cursor, "customers", "customer_id")
    account_start_id = get_next_id(cursor, "accounts", "account_id")
    merchant_start_id = get_next_id(cursor, "merchants", "merchant_id")

    insert_customers(cursor, customer_start_id, new_customers)

    customer_ids = get_existing_customer_ids(cursor)

    insert_accounts(cursor, account_start_id, new_accounts, customer_ids)
    insert_merchants(cursor, merchant_start_id, new_merchants)

    print_counts(cursor)


def run_once(mode):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        if mode == "initial":
            run_initial_seed(cursor)
        elif mode == "incremental":
            run_incremental_seed(cursor)

        conn.commit()

    except Exception as error:
        conn.rollback()
        print(f"Error during seed execution: {error}")
        raise

    finally:
        cursor.close()
        conn.close()


def run_continuous():
    print("Starting continuous reference data generator.")
    print("First step: initial seed if database is empty.")
    print("Then: random incremental batches in a loop.")
    print("Press CTRL+C to stop.")

    run_once("initial")

    try:
        while True:
            sleep_seconds = random.randint(SLEEP_SECONDS_MIN, SLEEP_SECONDS_MAX)

            print(
                f"[{get_current_timestamp()}] "
                f"Waiting {sleep_seconds} seconds before next incremental batch..."
            )

            time.sleep(sleep_seconds)

            run_once("incremental")

    except KeyboardInterrupt:
        print("\nContinuous reference data generator stopped by user.")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Reference data generator for Hybrid FinTech Lakehouse Platform"
    )

    parser.add_argument(
        "--mode",
        choices=["initial", "incremental", "continuous"],
        default="continuous",
        help="Choose initial load, one incremental batch, or continuous generator",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if args.mode == "continuous":
        run_continuous()
    else:
        run_once(args.mode)


if __name__ == "__main__":
    main()
