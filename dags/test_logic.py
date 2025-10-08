# dags/assignment.py
from __future__ import annotations

import json
import logging
import mimetypes
import os
import subprocess
from datetime import timedelta
from pathlib import Path
from typing import Optional

import pendulum
import requests
import boto3
from airflow.exceptions import AirflowSkipException
from airflow.sdk import DAG, task  # Airflow 3 public API

# ---------------- S3 / MinIO Config ----------------
# ตั้งค่าใน docker-compose ของ airflow-* (worker/scheduler/ฯลฯ)
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "admin")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY", "admin123456")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
S3_ADDRESSING_STYLE = os.getenv("S3_ADDRESSING_STYLE", "path")
S3_BUCKET = os.getenv("S3_BUCKET", "warehouse")
S3_PARQUET_PREFIX = os.getenv("S3_PARQUET_PREFIX", "assignment/parquet")
S3_DOCS_PREFIX = os.getenv("S3_DOCS_PREFIX", "assignment/docs")

# ตำแหน่งเก็บไฟล์ ingest (raw)
S3_RAW_PREFIX = os.getenv("S3_RAW_PREFIX", "assignment/raw")

# ---------------- Local (เฉพาะ temp) ----------------
AIRFLOW_DATA_DIR = os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/data")
TMP_DIR = Path(os.getenv("TMP_DIR", f"{AIRFLOW_DATA_DIR}/tmp"))
PARQUET_DIR = Path(os.getenv("PARQUET_DIR", f"{AIRFLOW_DATA_DIR}/parquet"))
CSV_NAME = os.getenv("CSV_NAME", "cdc_data.csv")
CSV_URL_DEFAULT = "https://data.cdc.gov/api/views/hksd-2xuw/rows.csv?accessType=DOWNLOAD"
CSV_URL = os.getenv("CSV_URL", CSV_URL_DEFAULT)

# ---------------- dbt config ----------------
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt/profiles")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_DOCS_PORT = os.getenv("DBT_DOCS_PORT", "8082")

# ---------------- HTTP config ----------------
HTTP_TIMEOUT_CONNECT = int(os.getenv("HTTP_TIMEOUT_CONNECT", "5"))
HTTP_TIMEOUT_READ = int(os.getenv("HTTP_TIMEOUT_READ", "30"))
HTTP_TIMEOUT = (HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT_READ)
CHUNK_SIZE = int(os.getenv("HTTP_CHUNK_SIZE", str(1 << 14)))  # 16KB

logger = logging.getLogger(__name__)


def _run(cmd: list[str], extra_env: Optional[dict] = None, cwd: Optional[str] = None) -> None:
    """Run a shell command; always log stdout/stderr; raise on non-zero."""
    env = {**os.environ}
    if extra_env:
        env.update(extra_env)
    if cwd is None:
        cwd = DBT_PROJECT_DIR

    logger.info("[cmd] %s", " ".join(cmd))
    p = subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )
    if p.stdout:
        logger.info("--- STDOUT ---\n%s", p.stdout)
    if p.returncode != 0:
        if p.stderr:
            logger.error("--- STDERR ---\n%s", p.stderr)
        raise subprocess.CalledProcessError(
            p.returncode, cmd, p.stdout, p.stderr)


def _ensure_tmp() -> None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)


def _s3_client():
    # ใช้ boto3 ต่อกับ MinIO (S3-compatible)
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        region_name=S3_REGION,
        config=boto3.session.Config(
            s3={"addressing_style": S3_ADDRESSING_STYLE}),
    )


def _dbt_env() -> dict[str, str]:
    """Build environment variables required for dbt commands."""
    return {
        "AWS_ACCESS_KEY_ID": S3_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": S3_SECRET_ACCESS_KEY,
        "AWS_DEFAULT_REGION": S3_REGION,
        "AWS_ENDPOINT_URL": S3_ENDPOINT_URL,
        "S3_ENDPOINT_URL": S3_ENDPOINT_URL,
        "AWS_S3_ADDRESSING_STYLE": S3_ADDRESSING_STYLE,
    }


def _upload_directory_to_s3(local_dir: str | Path, bucket: str, prefix: str) -> list[str]:
    """Upload the contents of *local_dir* to S3 under *bucket/prefix*."""
    base = Path(local_dir)
    if not base.exists():
        raise FileNotFoundError(f"Directory not found: {local_dir}")

    client = _s3_client()
    uploaded: list[str] = []
    normalized_prefix = prefix.strip("/")

    for file_path in base.rglob("*"):
        if not file_path.is_file():
            continue

        rel_key = file_path.relative_to(base).as_posix()
        key_parts = [normalized_prefix,
                     rel_key] if normalized_prefix else [rel_key]
        key = "/".join(filter(None, key_parts))

        content_type, _ = mimetypes.guess_type(str(file_path))
        extra_args = {"ContentType": content_type} if content_type else None

        if extra_args:
            client.upload_file(str(file_path), bucket,
                               key, ExtraArgs=extra_args)
        else:
            client.upload_file(str(file_path), bucket, key)

        uploaded.append(key)

    return uploaded


# ===== DAG =====
with DAG(
    dag_id="test_logic",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["assignment"],
    params={
        "csv_url": CSV_URL_DEFAULT,
        "force_download": False,
        "s3_bucket": S3_BUCKET,
        "s3_raw_prefix": S3_RAW_PREFIX,
        # สำหรับ transform
        "s3_parquet_prefix": os.getenv("S3_PREFIX", "assignment/parquet"),
    },
) as dag:
    @task()
    def test_run_dbt() -> None:
        # """รัน dbt models ทั้งหมด (full-refresh)"""
        # _run(
        #     ["dbt", "run", "--profiles-dir", f"{DBT_PROFILES_DIR}/custom-profiles.yml"],
        #     extra_env=_dbt_env(),
        # )
        print("DBT RUN completed")

    test_run_dbt()
