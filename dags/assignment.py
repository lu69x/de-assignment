# dags/assignment.py
from airflow.sdk import DAG, task
import pendulum
import os
import subprocess
from datetime import timedelta
import requests
import json
from typing import Optional

# ---------- Constants ----------
AIRFLOW_DATA_DIR = "/opt/airflow/data"
RAW_DIR = os.path.join(AIRFLOW_DATA_DIR, "raw")
PARQUET_DIR = os.path.join(AIRFLOW_DATA_DIR, "parquet")
CSV_NAME = "cdc_data.csv"
CSV_URL = "https://data.cdc.gov/api/views/hksd-2xuw/rows.csv?accessType=DOWNLOAD"

DBT_PROFILES_DIR = "/home/airflow/.dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt"

HTTP_TIMEOUT = (5, 30)            # (connect, read) seconds
CHUNK_SIZE = 1 << 14               # 16KB


# ---------- Helpers ----------
def _run(cmd: list[str]) -> None:
    """Run a shell command, print stdout/stderr always; raise on non-zero."""
    print(f"[cmd] {' '.join(cmd)}")
    p = subprocess.run(
        cmd,
        cwd=DBT_PROJECT_DIR,
        text=True,
        capture_output=True,
        env={**os.environ},
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
    os.makedirs(PARQUET_DIR, exist_ok=True)


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
    def transform_data(csv_path: str) -> None:
        """
        Run dbt to build staging/marts. Models should read the CSV via:
          read_csv_auto('{{ var("csv_path", "/opt/airflow/data/raw/cdc_data.csv") }}')
        dbt_project.yml should set post-hook on marts to COPY Parquet into PARQUET_DIR.
        """
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"[transform_data] CSV not found: {csv_path}")

        # Show versions & environment status first (helps debugging profiles)
        _run(["dbt", "--version"])
        _run(["dbt", "debug", "--profiles-dir", DBT_PROFILES_DIR, "--project-dir", DBT_PROJECT_DIR])
        
        # ใน dags/assignment.py (ภายใน transform_data ก่อน dbt run/test)
        _run(["dbt", "deps", "--profiles-dir", DBT_PROFILES_DIR, "--project-dir", DBT_PROJECT_DIR])


        # Run models
        vars_json = json.dumps({"csv_path": csv_path})
        _run([
            "dbt", "run",
            "--profiles-dir", DBT_PROFILES_DIR,
            "--project-dir", DBT_PROJECT_DIR,
            "--vars", vars_json,
        ])

        # Optional: run tests
        _run([
            "dbt", "test",
            "--profiles-dir", DBT_PROFILES_DIR,
            "--project-dir", DBT_PROJECT_DIR,
        ])

    @task(
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=5),
    )
    def load_data() -> None:
        """
        Placeholder for publishing step (e.g., move Parquet to lake/Iceberg).
        For now, just ensure Parquet dir exists and list files if any.
        """
        _ensure_dirs()
        files = []
        if os.path.exists(PARQUET_DIR):
            files = sorted(os.listdir(PARQUET_DIR))
        print(f"[load_data] parquet dir: {PARQUET_DIR}")
        print(f"[load_data] files: {files}")

    # Wiring
    csv = ingest_file()                 # or ingest_file(force=True) if you need re-download
    cleaned = cleansing_data(csv)
    transform_data(cleaned) >> load_data()
