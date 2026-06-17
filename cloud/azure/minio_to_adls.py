#!/usr/bin/env python3
"""
Synchronizes files from MinIO Bronze storage to Azure ADLS Gen2 / Blob Storage.

Source:
    MinIO bucket, for example:
    s3://bronze/banking/transactions/year=2026/month=05/day=16/hour=01/file.parquet

Target:
    Azure container, for example:
    abfss://bronze@storageaccount.dfs.core.windows.net/banking/transactions/year=2026/month=05/day=16/hour=01/file.parquet

Features:
- keeps the same object path structure,
- supports prefix filtering,
- supports dry-run mode,
- skips existing files by default,
- supports overwrite,
- verifies file size after upload,
- creates the ADLS container if it does not exist.
"""

import argparse
import os
import sys
import tempfile
from dataclasses import dataclass
from typing import Iterator, Optional

import boto3
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv


@dataclass
class SyncStats:
    discovered: int = 0
    uploaded: int = 0
    skipped: int = 0
    failed: int = 0
    bytes_uploaded: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync MinIO Bronze objects to Azure ADLS Gen2 / Blob container."
    )

    parser.add_argument(
        "--prefix",
        default="banking/",
        help="MinIO object prefix to sync. Default: banking/",
    )

    parser.add_argument(
        "--bucket",
        default=None,
        help="MinIO bucket name. Defaults to MINIO_BUCKET from .env or bronze.",
    )

    parser.add_argument(
        "--container",
        default=None,
        help="Azure container name. Defaults to ADLS_CONTAINER_NAME from .env or bronze.",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing blobs in Azure.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List planned operations without uploading files.",
    )

    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Optional maximum number of files to process.",
    )

    return parser.parse_args()


def get_minio_client():
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if not endpoint or not access_key or not secret_key:
        raise RuntimeError(
            "Missing MinIO configuration. Required: MINIO_ENDPOINT, "
            "MINIO_ACCESS_KEY, MINIO_SECRET_KEY."
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def get_blob_service_client() -> BlobServiceClient:
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    if not connection_string:
        raise RuntimeError(
            "Missing AZURE_STORAGE_CONNECTION_STRING in .env."
        )

    return BlobServiceClient.from_connection_string(connection_string)


def ensure_container(blob_service: BlobServiceClient, container_name: str):
    container_client = blob_service.get_container_client(container_name)

    try:
        container_client.create_container()
        print(f"[INFO] Created Azure container: {container_name}")
    except ResourceExistsError:
        print(f"[INFO] Azure container exists: {container_name}")

    return container_client


def iter_minio_objects(
    minio_client,
    bucket: str,
    prefix: str,
    max_files: Optional[int] = None,
) -> Iterator[dict]:
    token = None
    yielded = 0

    while True:
        kwargs = {
            "Bucket": bucket,
            "Prefix": prefix,
        }

        if token:
            kwargs["ContinuationToken"] = token

        response = minio_client.list_objects_v2(**kwargs)

        for item in response.get("Contents", []):
            if item["Key"].endswith("/"):
                continue

            yield item
            yielded += 1

            if max_files is not None and yielded >= max_files:
                return

        if not response.get("IsTruncated"):
            break

        token = response["NextContinuationToken"]


def blob_exists_with_same_size(container_client, blob_name: str, size: int) -> bool:
    blob_client = container_client.get_blob_client(blob_name)

    try:
        props = blob_client.get_blob_properties()
        return props.size == size
    except ResourceNotFoundError:
        return False


def upload_object(
    minio_client,
    container_client,
    bucket: str,
    key: str,
    size: int,
    overwrite: bool,
) -> int:
    blob_client = container_client.get_blob_client(key)

    with tempfile.NamedTemporaryFile() as tmp:
        minio_client.download_fileobj(bucket, key, tmp)
        tmp.flush()
        tmp.seek(0)

        blob_client.upload_blob(
            tmp,
            overwrite=overwrite,
            content_settings=ContentSettings(
                content_type="application/octet-stream"
            ),
        )

    props = blob_client.get_blob_properties()

    if props.size != size:
        raise RuntimeError(
            f"Size mismatch after upload for {key}. "
            f"MinIO={size}, Azure={props.size}"
        )

    return props.size


def main() -> int:
    load_dotenv(".env")

    args = parse_args()

    minio_bucket = args.bucket or os.getenv("MINIO_BUCKET", "bronze")
    azure_container = args.container or os.getenv("ADLS_CONTAINER_NAME", "bronze")

    print("[INFO] MinIO to ADLS sync started")
    print(f"[INFO] Source bucket: {minio_bucket}")
    print(f"[INFO] Source prefix: {args.prefix}")
    print(f"[INFO] Target container: {azure_container}")
    print(f"[INFO] Dry run: {args.dry_run}")
    print(f"[INFO] Overwrite: {args.overwrite}")

    minio_client = get_minio_client()
    blob_service = get_blob_service_client()
    container_client = ensure_container(blob_service, azure_container)

    stats = SyncStats()

    for item in iter_minio_objects(
        minio_client=minio_client,
        bucket=minio_bucket,
        prefix=args.prefix,
        max_files=args.max_files,
    ):
        key = item["Key"]
        size = item["Size"]
        stats.discovered += 1

        if not args.overwrite and blob_exists_with_same_size(
            container_client, key, size
        ):
            stats.skipped += 1
            print(f"[SKIP] {key} ({size} bytes)")
            continue

        if args.dry_run:
            print(f"[DRY-RUN] upload {key} ({size} bytes)")
            continue

        try:
            uploaded_size = upload_object(
                minio_client=minio_client,
                container_client=container_client,
                bucket=minio_bucket,
                key=key,
                size=size,
                overwrite=args.overwrite,
            )

            stats.uploaded += 1
            stats.bytes_uploaded += uploaded_size
            print(f"[UPLOAD] {key} ({uploaded_size} bytes)")

        except Exception as exc:
            stats.failed += 1
            print(f"[ERROR] {key}: {exc}", file=sys.stderr)

    print("")
    print("[SUMMARY]")
    print(f"Discovered files: {stats.discovered}")
    print(f"Uploaded files:   {stats.uploaded}")
    print(f"Skipped files:    {stats.skipped}")
    print(f"Failed files:     {stats.failed}")
    print(f"Uploaded size MB: {stats.bytes_uploaded / 1024 / 1024:.2f}")

    if stats.failed > 0:
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
