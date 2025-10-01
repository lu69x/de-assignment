from airflow.decorators import dag, task
import pendulum
import subprocess


@dag(
    dag_id="dbt_duckdb_test",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["assignment"]
)
def dbt_duckdb_test():
    @task
    def run_dbt():
        subprocess.run(
            [
                "dbt", "run",
                "--profiles-dir", "/home/airflow/.dbt",
                "--project-dir", "/opt/airflow/dbt",
                "--vars", f"csv_path:/opt/airflow/data/raw/cdc_data.csv"
            ],
            check=True,
        )

    run_dbt()


dbt_duckdb_test()
