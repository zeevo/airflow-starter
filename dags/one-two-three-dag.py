from datetime import timedelta
from typing import Any, Dict

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from constants.relationships import SUCCESS, FAILURE

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


@dag(
    "one-to-three",
    default_args=default_args,
    schedule_interval=timedelta(seconds=60),
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

    result_one = one()
    result_two = two(result_one[SUCCESS])
    result_three = three(result_two[SUCCESS])

    failure(result_one[FAILURE])
    failure(result_two[FAILURE])
    failure(result_three[FAILURE])


one_two_three_dag = one_two_three_taskflow()
