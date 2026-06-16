#!/usr/bin/env python3

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor


PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCAL_EXPORT_ROOT = PROJECT_ROOT / "data" / "hourly_exports"


HOURLY_DATASETS = {
    "transactions": {
        "table": "banking_source.transactions",
        "timestamp_column": "event_time",
        "prefix": "banking/transactions",
    },
    "kyc_aml_checks": {
        "table": "banking_source.kyc_aml_checks",
        "timestamp_column": "event_time",
        "prefix": "banking/compliance/kyc_aml_checks",
    },
    "support_cases": {
        "table": "banking_source.support_cases",
        "timestamp_column": "created_at",
        "prefix": "banking/support/cases",
    },
    "chargeback_cases": {
        "table": "banking_source.chargeback_cases",
        "timestamp_column": "case_opened_at",
        "prefix": "banking/fraud/chargebacks",
    },
    "customer_risk_scores": {
        "table": "banking_source.customer_risk_scores",
        "timestamp_column": "score_time",
        "prefix": "banking/risk/customer_scores",
    },
    "merchant_risk_scores": {
        "table": "banking_source.merchant_risk_scores",
        "timestamp_column": "score_time",
        "prefix": "banking/risk/merchant_scores",
    },
    "device_blacklist": {
        "table": "banking_source.device_blacklist",
        "timestamp_column": "event_time",
        "prefix": "banking/risk/device_blacklist",
    },
}


SNAPSHOT_DATASETS = {
    "customers": {
        "table": "banking_source.customers",
        "prefix": "banking/customer/master",
    },
    "accounts": {
        "table": "banking_source.accounts",
        "prefix": "banking/account/master",
    },
    "merchants": {
        "table": "banking_source.merchants",
        "prefix": "banking/reference/merchants",
    },
    "atms": {
        "table": "banking_source.atms",
        "prefix": "banking/reference/atms",
    },
}


def load_configuration() -> None:
    env_path = PROJECT_ROOT / ".env"

    if not env_path.exists():
        raise FileNotFoundError(f"Nie znaleziono .env: {env_path}")

    load_dotenv(dotenv_path=env_path)


def connect_postgres():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "fintech"),
        user=os.getenv("POSTGRES_USER", "fintech_user"),
        password=os.getenv("POSTGRES_PASSWORD", "fintech_pass"),
        connect_timeout=10,
        application_name="postgres_to_minio_export",
    )


def build_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        region_name="us-east-1",
    )


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
        print(f"Bucket istnieje: {bucket}")
    except ClientError:
        client.create_bucket(Bucket=bucket)
        print(f"Utworzono bucket: {bucket}")


def normalize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return value

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    return value


def fetch_rows(connection, query: str, parameters: tuple = ()) -> list[dict]:
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query, parameters)
        rows = cursor.fetchall()

    return [
        {key: normalize_value(value) for key, value in dict(row).items()}
        for row in rows
    ]


def write_parquet(rows: list[dict], local_path: Path) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pylist(rows)

    pq.write_table(
        table,
        local_path,
        compression="snappy",
    )


def object_exists(client, bucket: str, object_key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=object_key)
        return True
    except ClientError:
        return False


def upload_file(
    client,
    bucket: str,
    local_path: Path,
    object_key: str,
    overwrite: bool,
) -> bool:
    if not overwrite and object_exists(client, bucket, object_key):
        print(f"Pominięto istniejący plik: s3://{bucket}/{object_key}")
        return False

    client.upload_file(
        str(local_path),
        bucket,
        object_key,
        ExtraArgs={
            "ContentType": "application/vnd.apache.parquet",
        },
    )

    print(
        f"Wysłano: s3://{bucket}/{object_key} "
        f"({local_path.stat().st_size / 1024 / 1024:.2f} MB)"
    )

    return True


def floor_to_hour(value: datetime) -> datetime:
    return value.replace(minute=0, second=0, microsecond=0)


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def export_hourly_dataset(
    connection,
    client,
    bucket: str,
    dataset_name: str,
    start_time: datetime,
    end_time: datetime,
    overwrite: bool,
    keep_local: bool,
) -> tuple[int, int]:
    config = HOURLY_DATASETS[dataset_name]

    table_name = config["table"]
    timestamp_column = config["timestamp_column"]
    prefix = config["prefix"]

    current_hour = floor_to_hour(start_time)
    final_hour = floor_to_hour(end_time)

    exported_files = 0
    exported_rows = 0

    while current_hour <= final_hour:
        next_hour = current_hour + timedelta(hours=1)

        rows = fetch_rows(
            connection,
            f"""
            SELECT *
            FROM {table_name}
            WHERE {timestamp_column} >= %s
              AND {timestamp_column} < %s
            ORDER BY {timestamp_column}
            """,
            (current_hour, next_hour),
        )

        if not rows:
            current_hour = next_hour
            continue

        year = current_hour.strftime("%Y")
        month = current_hour.strftime("%m")
        day = current_hour.strftime("%d")
        hour = current_hour.strftime("%H")

        filename = (
            f"{dataset_name}_"
            f"{current_hour.strftime('%Y%m%d_%H')}.parquet"
        )

        local_path = (
            LOCAL_EXPORT_ROOT
            / dataset_name
            / f"year={year}"
            / f"month={month}"
            / f"day={day}"
            / f"hour={hour}"
            / filename
        )

        object_key = (
            f"{prefix}/"
            f"year={year}/"
            f"month={month}/"
            f"day={day}/"
            f"hour={hour}/"
            f"{filename}"
        )

        write_parquet(rows, local_path)

        uploaded = upload_file(
            client,
            bucket,
            local_path,
            object_key,
            overwrite,
        )

        if uploaded:
            exported_files += 1
            exported_rows += len(rows)

        if not keep_local:
            local_path.unlink(missing_ok=True)

        print(
            f"{dataset_name} | {current_hour.isoformat()} | "
            f"rekordy={len(rows):,}"
        )

        current_hour = next_hour

    return exported_files, exported_rows


def export_snapshot_dataset(
    connection,
    client,
    bucket: str,
    dataset_name: str,
    snapshot_date: str,
    overwrite: bool,
    keep_local: bool,
) -> tuple[int, int]:
    config = SNAPSHOT_DATASETS[dataset_name]

    rows = fetch_rows(
        connection,
        f"""
        SELECT *
        FROM {config["table"]}
        """,
    )

    if not rows:
        print(f"Brak danych snapshot: {dataset_name}")
        return 0, 0

    filename = f"{dataset_name}_{snapshot_date.replace('-', '')}.parquet"

    local_path = (
        LOCAL_EXPORT_ROOT
        / dataset_name
        / f"snapshot_date={snapshot_date}"
        / filename
    )

    object_key = (
        f'{config["prefix"]}/'
        f"snapshot_date={snapshot_date}/"
        f"{filename}"
    )

    write_parquet(rows, local_path)

    uploaded = upload_file(
        client,
        bucket,
        local_path,
        object_key,
        overwrite,
    )

    if not keep_local:
        local_path.unlink(missing_ok=True)

    return (1, len(rows)) if uploaded else (0, 0)


def export_daily_balances(
    connection,
    client,
    bucket: str,
    start_date: str,
    end_date: str,
    overwrite: bool,
    keep_local: bool,
) -> tuple[int, int]:
    current_date = datetime.fromisoformat(start_date).date()
    final_date = datetime.fromisoformat(end_date).date()

    exported_files = 0
    exported_rows = 0

    while current_date <= final_date:
        rows = fetch_rows(
            connection,
            """
            SELECT *
            FROM banking_source.daily_account_balances
            WHERE balance_date = %s
            ORDER BY account_id
            """,
            (current_date,),
        )

        if rows:
            year = current_date.strftime("%Y")
            month = current_date.strftime("%m")
            day = current_date.strftime("%d")
            date_compact = current_date.strftime("%Y%m%d")

            filename = f"daily_account_balances_{date_compact}.parquet"

            local_path = (
                LOCAL_EXPORT_ROOT
                / "daily_account_balances"
                / f"year={year}"
                / f"month={month}"
                / f"day={day}"
                / filename
            )

            object_key = (
                "banking/account/daily_balances/"
                f"year={year}/"
                f"month={month}/"
                f"day={day}/"
                f"{filename}"
            )

            write_parquet(rows, local_path)

            uploaded = upload_file(
                client,
                bucket,
                local_path,
                object_key,
                overwrite,
            )

            if uploaded:
                exported_files += 1
                exported_rows += len(rows)

            if not keep_local:
                local_path.unlink(missing_ok=True)

            print(
                f"daily_account_balances | {current_date} | "
                f"rekordy={len(rows):,}"
            )

        current_date += timedelta(days=1)

    return exported_files, exported_rows


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Eksport PostgreSQL do godzinowych plików Parquet w MinIO."
    )

    parser.add_argument(
        "--start",
        required=True,
        help="Początek zakresu, np. 2026-05-16T00:00:00+00:00",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="Koniec zakresu, np. 2026-06-15T23:59:59+00:00",
    )
    parser.add_argument(
        "--dataset",
        default="all",
        choices=[
            "all",
            *HOURLY_DATASETS.keys(),
        ],
    )
    parser.add_argument(
        "--skip-snapshots",
        action="store_true",
    )
    parser.add_argument(
        "--skip-daily-balances",
        action="store_true",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
    )
    parser.add_argument(
        "--keep-local",
        action="store_true",
    )

    args = parser.parse_args()

    load_configuration()

    start_time = parse_datetime(args.start)
    end_time = parse_datetime(args.end)

    if end_time <= start_time:
        parser.error("--end musi być późniejsze niż --start")

    connection = connect_postgres()
    minio_client = build_minio_client()

    bucket = os.getenv("MINIO_BUCKET", "bronze")

    ensure_bucket(minio_client, bucket)

    datasets = (
        list(HOURLY_DATASETS.keys())
        if args.dataset == "all"
        else [args.dataset]
    )

    total_files = 0
    total_rows = 0

    try:
        for dataset_name in datasets:
            print(f"\nEksport godzinowy: {dataset_name}")

            files, rows = export_hourly_dataset(
                connection=connection,
                client=minio_client,
                bucket=bucket,
                dataset_name=dataset_name,
                start_time=start_time,
                end_time=end_time,
                overwrite=args.overwrite,
                keep_local=args.keep_local,
            )

            total_files += files
            total_rows += rows

        snapshot_date = end_time.date().isoformat()

        if args.dataset == "all" and not args.skip_snapshots:
            for dataset_name in SNAPSHOT_DATASETS:
                print(f"\nEksport snapshotu: {dataset_name}")

                files, rows = export_snapshot_dataset(
                    connection=connection,
                    client=minio_client,
                    bucket=bucket,
                    dataset_name=dataset_name,
                    snapshot_date=snapshot_date,
                    overwrite=args.overwrite,
                    keep_local=args.keep_local,
                )

                total_files += files
                total_rows += rows

        if args.dataset == "all" and not args.skip_daily_balances:
            print("\nEksport dziennych sald")

            files, rows = export_daily_balances(
                connection=connection,
                client=minio_client,
                bucket=bucket,
                start_date=start_time.date().isoformat(),
                end_date=end_time.date().isoformat(),
                overwrite=args.overwrite,
                keep_local=args.keep_local,
            )

            total_files += files
            total_rows += rows

    finally:
        connection.close()

    print("\nEksport zakończony")
    print(f"Pliki wysłane: {total_files:,}")
    print(f"Rekordy wysłane: {total_rows:,}")
    print(f"Bucket: {bucket}")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"BŁĄD: {exc}", file=sys.stderr)
        sys.exit(1)
