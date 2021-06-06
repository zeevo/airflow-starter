from datetime import timedelta
from typing import Any, Dict

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from constants.relationships import SUCCESS, FAILURE


@dag(
    "one-to-three",
    # schedule_interval=timedelta(seconds=60),
    start_date=days_ago(2),
    tags=["example"],
    catchup=False,
)
def one_two_three_taskflow():
    @task
    def one() -> Dict[str, Any]:
        return {SUCCESS: 1}

    @task
    def two(data) -> Dict[str, Any]:
        print(data)
        assert data == 1
        return {SUCCESS: 2}

    @task
    def three(data) -> Dict[str, Any]:
        print(data)
        assert data == 2
        return {SUCCESS: 2}

    @task
    def failure(data):
        print(data)
        print("Failure")

    results_one = one()
    results_two = two(results_one[SUCCESS])
    results_three = three(results_two[SUCCESS])

    failure(results_one[FAILURE])
    failure(results_two[FAILURE])
    failure(results_three[FAILURE])


one_two_three_dag = one_two_three_taskflow()
