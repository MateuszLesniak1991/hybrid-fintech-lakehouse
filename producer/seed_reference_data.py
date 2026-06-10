from faker import Faker
from dotenv import load_dotenv
import psycopg2
import os
import random

load_dotenv()

fake = Faker("pl_PL")
Faker.seed(42)
random.seed(42)

CUSTOMERS_COUNT = 1000
ACCOUNTS_COUNT = 1500
MERCHANTS_COUNT = 250


def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )


def seed_customers(cursor):
    for customer_id in range(1, CUSTOMERS_COUNT + 1):
        cursor.execute(
            """
            INSERT INTO customers (
                customer_id, first_name, last_name, email, city, country,
                customer_segment, risk_score, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING
            """,
            (
                customer_id,
                fake.first_name(),
                fake.last_name(),
                fake.unique.email(),
                fake.city(),
                "Poland",
                random.choice(["standard", "premium", "business"]),
                random.randint(1, 100),
                fake.date_time_between(start_date="-5y", end_date="-30d"),
            ),
        )


def seed_accounts(cursor):
    for account_id in range(1, ACCOUNTS_COUNT + 1):
        cursor.execute(
            """
            INSERT INTO accounts (
                account_id, customer_id, account_type, currency, status, opened_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (account_id) DO NOTHING
            """,
            (
                account_id,
                random.randint(1, CUSTOMERS_COUNT),
                random.choice(["personal", "savings", "business"]),
                random.choice(["PLN", "EUR", "USD"]),
                random.choices(
                    ["active", "blocked", "closed"],
                    weights=[90, 7, 3],
                    k=1,
                )[0],
                fake.date_time_between(start_date="-5y", end_date="-10d"),
            ),
        )


def seed_merchants(cursor):
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
    ]

    for merchant_id in range(1, MERCHANTS_COUNT + 1):
        cursor.execute(
            """
            INSERT INTO merchants (
                merchant_id, merchant_name, merchant_category, city, country
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (merchant_id) DO NOTHING
            """,
            (
                merchant_id,
                fake.company(),
                random.choice(categories),
                fake.city(),
                "Poland",
            ),
        )


def print_counts(cursor):
    for table in ["customers", "accounts", "merchants"]:
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        count = cursor.fetchone()[0]
        print(f"{table}: {count} rows")


def main():
    conn = get_connection()
    cursor = conn.cursor()

    print("Seeding customers...")
    seed_customers(cursor)

    print("Seeding accounts...")
    seed_accounts(cursor)

    print("Seeding merchants...")
    seed_merchants(cursor)

    conn.commit()

    print("Reference data seeded successfully.")
    print_counts(cursor)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
