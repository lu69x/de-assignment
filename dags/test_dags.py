from airflow.decorators import dag, task
import pendulum
from time import sleep


@dag(
    dag_id="test_dags",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,   # manual only
    catchup=False,
    tags=["assignment"],
)
def test_dags():
    @task
    def task_a():
        sleep(5)  # Simulate a task taking some time
        print("Hello from task_a!")

    @task
    def task_b():
        sleep(7)  # Simulate a task taking some time
        print("Hello from task_b!")

    @task
    def task_c():
        sleep(10)  # Simulate a task taking some time
        print("Hello from task_c!")
        
    @task
    def task_d():
        sleep(3)  # Simulate a task taking some time
        print("Hello from task_d!")

    a = task_a()
    b = task_b()
    c = task_c()
    d = task_d()

    a >> [b, c] >> d  # task_a must run before task_b, task_c, and task_d


test_dags()
