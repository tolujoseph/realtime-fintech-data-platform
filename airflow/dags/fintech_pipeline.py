from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="fintech_data_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["fintech", "data-engineering"],
) as dag:

    run_silver = BashOperator(
        task_id="run_silver",
        bash_command="python /opt/airflow/pipelines/silver/transform.py",
        cwd="/opt/airflow",
        retries=2,
    )

    run_gold = BashOperator(
        task_id="run_gold",
        bash_command="python /opt/airflow/pipelines/gold/transform.py",
        cwd="/opt/airflow",
        retries=2,
    )

    load_clickhouse = BashOperator(
        task_id="load_clickhouse",
        bash_command="python /opt/airflow/pipelines/clickhouse/load.py",
        cwd="/opt/airflow",
        retries=2,
    )

    run_silver >> run_gold >> load_clickhouse