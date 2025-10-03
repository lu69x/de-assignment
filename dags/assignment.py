# dags/assignment.py
from airflow.sdk import DAG, task
import pendulum
import os
import subprocess
from datetime import timedelta
import requests
import json
from typing import Optional
import boto3
import mimetypes
from pathlib import Path

# ---------- S3 Config ----------
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "admin")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY", "admin123456")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
S3_ADDRESSING_STYLE = os.getenv("S3_ADDRESSING_STYLE", "path")
S3_BUCKET = os.getenv("S3_BUCKET", "warehouse")
S3_PARQUET_PREFIX = os.getenv("S3_PARQUET_PREFIX", "assignment/parquet")
S3_DOCS_PREFIX = os.getenv("S3_DOCS_PREFIX", "assignment/docs")

# ---------- Constants ----------
AIRFLOW_DATA_DIR = "/opt/airflow/data"
RAW_DIR = os.path.join(AIRFLOW_DATA_DIR, "raw")
CSV_NAME = "cdc_data.csv"
CSV_URL = "https://data.cdc.gov/api/views/hksd-2xuw/rows.csv?accessType=DOWNLOAD"

DBT_PROFILES_DIR = "/home/airflow/.dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt"

HTTP_TIMEOUT = (5, 30)            # (connect, read) seconds
CHUNK_SIZE = 1 << 14               # 16KB


# ---------- Helpers ----------
def _run(cmd: list[str], extra_env: Optional[dict[str, str]] = None) -> None:
    """Run a shell command, print stdout/stderr always; raise on non-zero."""
    print(f"[cmd] {' '.join(cmd)}")
    env = {**os.environ}
    if extra_env:
        env.update(extra_env)
    p = subprocess.run(
        cmd,
        cwd=DBT_PROJECT_DIR,
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )
    if p.stdout:
        print("--- STDOUT ---\n" + p.stdout)
    if p.returncode != 0:
        # show stderr clearly to Airflow logs
        if p.stderr:
            print("--- STDERR ---\n" + p.stderr)
        raise subprocess.CalledProcessError(p.returncode, cmd, p.stdout, p.stderr)


def _ensure_dirs() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)


def _dbt_env() -> dict[str, str]:
    return {
        "AWS_ACCESS_KEY_ID": S3_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": S3_SECRET_ACCESS_KEY,
        "AWS_DEFAULT_REGION": S3_REGION,
        "AWS_ENDPOINT_URL": S3_ENDPOINT_URL,
        "S3_ENDPOINT_URL": S3_ENDPOINT_URL,
        "AWS_S3_ADDRESSING_STYLE": S3_ADDRESSING_STYLE,
    }


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        region_name=S3_REGION,
        config=boto3.session.Config(s3={"addressing_style": S3_ADDRESSING_STYLE}),
    )


def _upload_directory_to_s3(local_dir: str, bucket: str, prefix: str) -> list[str]:
    base = Path(local_dir)
    if not base.exists():
        raise FileNotFoundError(f"Directory not found: {local_dir}")

    client = _s3_client()
    uploaded: list[str] = []

    for file_path in base.rglob("*"):
        if not file_path.is_file():
            continue
        rel = file_path.relative_to(base).as_posix()
        key = "/".join([p for p in [prefix.strip("/"), rel] if p])
        content_type, _ = mimetypes.guess_type(str(file_path))
        extra_args = {"ContentType": content_type} if content_type else None
        if extra_args:
            client.upload_file(str(file_path), bucket, key, ExtraArgs=extra_args)
        else:
            client.upload_file(str(file_path), bucket, key)
        uploaded.append(key)

    return uploaded


# ---------- DAG ----------
with DAG(
    dag_id="data_engineer_assignment",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,            # manual run only
    catchup=False,
    tags=["assignment"],
) as dag:

    @task(
        retries=3,
        retry_delay=timedelta(seconds=10),
        execution_timeout=timedelta(minutes=2),
    )
    def ingest_file(force: Optional[bool] = False) -> str:
        """
        Download CSV to RAW_DIR. If file exists and not empty, skip unless 'force' is True.
        Returns: absolute file path.
        """
        _ensure_dirs()
        output_path = os.path.join(RAW_DIR, CSV_NAME)

        if not force and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            print(f"[ingest_file] exists, skip download: {output_path}")
            return output_path

        headers = {"User-Agent": "airflow-assignment/1.0"}
        with requests.get(CSV_URL, headers=headers, stream=True, timeout=HTTP_TIMEOUT) as r:
            r.raise_for_status()
            tmp_path = output_path + ".part"
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
            os.replace(tmp_path, output_path)

        size = os.path.getsize(output_path)
        print(f"[ingest_file] downloaded: {output_path} ({size} bytes)")
        return output_path

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=5),
    )
    def cleansing_data(csv_path: str) -> str:
        """
        Placeholder for lightweight cleansing (e.g. re-encode, strip BOM, filter columns).
        Currently passes through.
        """
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"[cleansing_data] file not found: {csv_path}")
        print(f"[cleansing_data] input: {csv_path}")
        return csv_path

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=25),
    )
    def transform_data(csv_path: str) -> str:
        """
        Run dbt to build staging/marts. Models should read the CSV via:
          read_csv_auto('{{ var("csv_path", "/opt/airflow/data/raw/cdc_data.csv") }}')
        และเขียนผลลัพธ์เป็น Parquet ลง S3 โดยใช้ตัวแปร s3_bucket/s3_prefix.
        """
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"[transform_data] CSV not found: {csv_path}")

        s3_prefix = S3_PARQUET_PREFIX.strip("/")
        s3_env = _dbt_env()
        s3_env = {
            "AWS_ACCESS_KEY_ID": S3_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": S3_SECRET_ACCESS_KEY,
            "AWS_DEFAULT_REGION": S3_REGION,
            "AWS_ENDPOINT_URL": S3_ENDPOINT_URL,
            "S3_ENDPOINT_URL": S3_ENDPOINT_URL,
            "AWS_S3_ADDRESSING_STYLE": S3_ADDRESSING_STYLE,
        }

        # Show versions & environment status first (helps debugging profiles)
        _run(["dbt", "--version"], extra_env=s3_env)
        _run(["dbt", "debug", "--profiles-dir", DBT_PROFILES_DIR, "--project-dir", DBT_PROJECT_DIR], extra_env=s3_env)
        
        # ใน dags/assignment.py (ภายใน transform_data ก่อน dbt run/test)
        _run(["dbt", "deps", "--profiles-dir", DBT_PROFILES_DIR, "--project-dir", DBT_PROJECT_DIR], extra_env=s3_env)


        # Run models
        vars_json = json.dumps({
            "csv_path": csv_path,
            "s3_bucket": S3_BUCKET,
            "s3_prefix": s3_prefix,
        })
        _run([
            "dbt", "run",
            "--profiles-dir", DBT_PROFILES_DIR,
            "--project-dir", DBT_PROJECT_DIR,
            "--vars", vars_json,
        ], extra_env=s3_env)

        # Optional: run tests
        _run([
            "dbt", "test",
            "--profiles-dir", DBT_PROFILES_DIR,
            "--project-dir", DBT_PROJECT_DIR,
            "--vars", vars_json,
        ], extra_env=s3_env)

        if s3_prefix:
            out_uri = f"s3://{S3_BUCKET}/{s3_prefix}"
        else:
            out_uri = f"s3://{S3_BUCKET}"
        print(f"[transform_data] completed. Output in: {out_uri}")
        return out_uri

    @task(
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
    )
    def publish_lineage_docs(_: str) -> str:
        """Generate dbt docs (manifest + catalog) and upload to S3 for lineage browsing."""

        s3_env = _dbt_env()
        _run([
            "dbt",
            "docs",
            "generate",
            "--profiles-dir",
            DBT_PROFILES_DIR,
            "--project-dir",
            DBT_PROJECT_DIR,
        ], extra_env=s3_env)

        docs_dir = os.path.join(DBT_PROJECT_DIR, "target")
        timestamp = pendulum.now("UTC").format("YYYYMMDDTHHmmss")
        base_prefix = S3_DOCS_PREFIX.strip("/")
        versioned_prefix = "/".join(filter(None, [base_prefix, timestamp]))
        uploaded = _upload_directory_to_s3(docs_dir, S3_BUCKET, versioned_prefix)

        if not uploaded:
            raise RuntimeError("No docs were uploaded; check dbt docs generate output")

        latest_prefix = "/".join(filter(None, [base_prefix, "latest"]))
        _upload_directory_to_s3(docs_dir, S3_BUCKET, latest_prefix)

        docs_uri = f"s3://{S3_BUCKET}/{versioned_prefix}/index.html"
        print(f"[publish_lineage_docs] Uploaded {len(uploaded)} files under s3://{S3_BUCKET}/{versioned_prefix}")
        print(f"[publish_lineage_docs] Latest docs alias at s3://{S3_BUCKET}/{latest_prefix}/index.html")
        return docs_uri

    @task(
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=5),
    )
    def load_data(s3_out_uri: str, docs_uri: str) -> None:
        """
        Placeholder for publishing step. ตอนนี้ Parquet อยู่บน S3 แล้ว แค่ list objects.
        """
        prefix = S3_PARQUET_PREFIX.strip("/")
        client = _s3_client()
        list_kwargs = {"Bucket": S3_BUCKET}
        if prefix:
            list_kwargs["Prefix"] = f"{prefix}/"
        resp = client.list_objects_v2(**list_kwargs)
        keys = [item["Key"] for item in resp.get("Contents", [])]
        print(f"[load_data] published to: {s3_out_uri}")
        print(f"[load_data] objects: {keys}")
        print(f"[load_data] lineage docs: {docs_uri}")

    # Wiring
    csv = ingest_file()                 # or ingest_file(force=True) if you need re-download
    cleaned = cleansing_data(csv)
    s3_uri = transform_data(cleaned)
    docs_uri = publish_lineage_docs(s3_uri)
    load_data(s3_uri, docs_uri)
