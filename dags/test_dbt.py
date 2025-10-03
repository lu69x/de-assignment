# dags/assignment.py
from __future__ import annotations

import json
import logging
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
        raise subprocess.CalledProcessError(p.returncode, cmd, p.stdout, p.stderr)


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
        config=boto3.session.Config(s3={"addressing_style": S3_ADDRESSING_STYLE}),
    )


# ===== DAG =====
with DAG(
    dag_id="dbt_duckdb_test",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["assignment"],
    params={
        "csv_url": CSV_URL_DEFAULT,
        "force_download": False,
        "s3_bucket": S3_BUCKET,
        "s3_raw_prefix": S3_RAW_PREFIX,
        "s3_parquet_prefix": os.getenv("S3_PREFIX", "assignment/parquet"),  # สำหรับ transform
    },
) as dag:

    @task(retries=3, retry_delay=timedelta(seconds=10), execution_timeout=timedelta(minutes=3))
    def ingest_file(force: Optional[bool] = None, csv_url: Optional[str] = None) -> str:
        """
        Download CSV to a temp file (chunked), then UPLOAD to S3 (MinIO).
        Returns: s3 URI like: s3://<bucket>/<prefix>/<CSV_NAME>
        """
        _ensure_tmp()
        force_flag = bool(force) if force is not None else bool(dag.params.get("force_download", False))
        url = csv_url or str(dag.params.get("csv_url") or CSV_URL)

        bucket = str(dag.params.get("s3_bucket") or S3_BUCKET)
        prefix = str(dag.params.get("s3_raw_prefix") or S3_RAW_PREFIX)

        s3_key = f"{prefix.rstrip('/')}/{CSV_NAME}"
        s3_uri = f"s3://{bucket}/{s3_key}"

        # ถ้าไม่ force: เช็คว่าไฟล์นี้มีใน S3 แล้วหรือยัง
        s3 = _s3_client()
        try:
            head = s3.head_object(Bucket=bucket, Key=s3_key)
            size = head.get("ContentLength", 0)
            if not force_flag and size > 0:
                msg = f"[ingest_file] exists in S3 -> skip download: {s3_uri} (size={size})"
                logger.info(msg)
                raise AirflowSkipException(msg)
        except s3.exceptions.NoSuchKey:  # type: ignore[attr-defined]
            pass
        except Exception as e:
            # ถ้าเช็คไม่ได้ เราจะพยายามดาวน์โหลดต่อ (แสดง warning)
            logger.warning("[ingest_file] cannot HEAD s3://%s/%s: %s", bucket, s3_key, e)

        # โหลดเป็นไฟล์ชั่วคราว (.part) แล้วค่อยอัปขึ้น S3
        tmp_path = TMP_DIR / (CSV_NAME + ".part")
        final_path = TMP_DIR / CSV_NAME

        headers = {"User-Agent": "airflow-assignment/1.0"}
        logger.info("[ingest_file] GET %s", url)
        with requests.Session() as session:
            with session.get(url, headers=headers, stream=True, timeout=HTTP_TIMEOUT) as r:
                r.raise_for_status()
                with tmp_path.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)

        os.replace(tmp_path, final_path)
        size = final_path.stat().st_size
        if size == 0:
            raise ValueError(f"[ingest_file] downloaded empty file: {final_path}")

        # อัปโหลดขึ้น S3
        logger.info("[ingest_file] upload to %s (%d bytes)", s3_uri, size)
        s3.upload_file(str(final_path), bucket, s3_key)

        # จะเก็บไฟล์ temp ไว้หรือไม่ก็ได้—ที่นี่ลบเพื่อลดพื้นที่
        try:
            final_path.unlink(missing_ok=True)
        except Exception as e:
            logger.warning("[ingest_file] cannot remove temp file: %s", e)

        return s3_uri

    @task(retries=1, retry_delay=timedelta(seconds=30), execution_timeout=timedelta(minutes=5))
    def cleansing_data(csv_s3_uri: str) -> str:
        """
        Placeholder cleansing: เราเก็บไว้เป็น S3 URI (ไม่ดึงลงมา)
        ในขั้นนี้แค่ validate format คร่าว ๆ ว่าเป็น s3://bucket/key
        """
        if not csv_s3_uri.startswith("s3://"):
            raise ValueError(f"[cleansing_data] not an S3 URI: {csv_s3_uri}")
        logger.info("[cleansing_data] input: %s", csv_s3_uri)
        return csv_s3_uri

    @task(retries=1, retry_delay=timedelta(seconds=30), execution_timeout=timedelta(minutes=25))
    def transform_data(csv_s3_uri: str, s3_parquet_prefix: Optional[str] = None) -> str:
        """
        Run dbt → สร้างตาราง/ไฟล์ (Parquet/Iceberg) ลง S3 ผ่าน Trino/Nessie.
        dbt models ควรอ่าน CSV จาก S3 (csv_s3_uri) และเขียนผลไปที่ s3://{bucket}/{s3_parquet_prefix}
        """
        if not csv_s3_uri.startswith("s3://"):
            raise ValueError(f"[transform_data] not an S3 URI: {csv_s3_uri}")

        bucket = str(dag.params.get("s3_bucket") or S3_BUCKET)
        parquet_prefix = s3_parquet_prefix or str(dag.params.get("s3_parquet_prefix") or "assignment/parquet")
        parquet_prefix = parquet_prefix.strip("/")

        # dbt post-hooks copy DuckDB tables to local Parquet files; ensure the
        # destination folder exists so COPY does not fail with "No such file".
        PARQUET_DIR.mkdir(parents=True, exist_ok=True)

        s3_env = {
            "AWS_ACCESS_KEY_ID": S3_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": S3_SECRET_ACCESS_KEY,
            "AWS_DEFAULT_REGION": S3_REGION,
            "AWS_ENDPOINT_URL": S3_ENDPOINT_URL,
            "S3_ENDPOINT_URL": S3_ENDPOINT_URL,
            "AWS_S3_ADDRESSING_STYLE": S3_ADDRESSING_STYLE,
        }

        # Debug/Deps
        _run(["dbt", "--version"], extra_env=s3_env)
        _run(["dbt", "debug", "--profiles-dir", DBT_PROFILES_DIR, "--project-dir", DBT_PROJECT_DIR], extra_env=s3_env)
        _run(["dbt", "deps", "--profiles-dir", DBT_PROFILES_DIR, "--project-dir", DBT_PROJECT_DIR], extra_env=s3_env)

        # ส่งตัวแปรให้ dbt ใช้
        #  - csv_s3_uri   → path ของไฟล์ CSV บน S3
        #  - s3_bucket    → bucket สำหรับเขียนผล
        #  - s3_prefix    → โฟลเดอร์สำหรับเขียน Parquet/Iceberg
        vars_json = json.dumps({
            "csv_s3_uri": csv_s3_uri,
            "s3_bucket": bucket,
            "s3_prefix": parquet_prefix,
        })

        _run(
            [
                "dbt", "run",
                "--profiles-dir", DBT_PROFILES_DIR,
                "--project-dir", DBT_PROJECT_DIR,
                "--vars", vars_json,
            ],
            extra_env=s3_env,
        )
        _run(
            [
                "dbt", "test",
                "--profiles-dir", DBT_PROFILES_DIR,
                "--project-dir", DBT_PROJECT_DIR,
                "--vars", vars_json,
            ],
            extra_env=s3_env,
        )

        if parquet_prefix:
            out_uri = f"s3://{bucket}/{parquet_prefix}"
        else:
            out_uri = f"s3://{bucket}"
        logger.info("[transform_data] completed. Output in: %s", out_uri)
        return out_uri

    @task(retries=1, retry_delay=timedelta(minutes=1), execution_timeout=timedelta(minutes=5))
    def load_data(s3_out_uri: str) -> None:
        """
        Publishing placeholder: ตอนนี้ผลอยู่บน S3 แล้ว แค่ log ปลายทาง
        """
        logger.info("[load_data] published to: %s", s3_out_uri)

    # Wiring
    csv_s3 = ingest_file()
    cleaned = cleansing_data(csv_s3)
    s3_out = transform_data(cleaned)
    load_data(s3_out)
