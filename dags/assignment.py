from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import requests
import os

# Define a DAG
with DAG(
    dag_id="data_engineer_assignment",
    start_date=datetime(2023, 1, 1),
    schedule=None,  # run manually
    catchup=False,
    tags=["assignment"],
) as dag:

    @task
    def ingest_file():
        url = "https://data.cdc.gov/api/views/hksd-2xuw/rows.csv?accessType=DOWNLOAD"  # example file
        output_dir = "/opt/airflow/dags/data"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "cdc_data.csv")

        response = requests.get(url)
        response.raise_for_status()  # fail if not 200
        with open(output_path, "wb") as f:
            f.write(response.content)

        print(f"File downloaded to: {output_path}")

    @task
    def cleansing_data():
        print("Data cleansing task - to be implemented")
        pass

    @task
    def transform_data():
        print("Data transformation task - to be implemented")
        pass

    @task
    def load_data():
        print("Data loading task - to be implemented")
        pass

    ingest_file() >> cleansing_data() >> transform_data() >> load_data()
