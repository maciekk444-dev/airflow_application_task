from pathlib import Path
from pendulum import datetime
from datetime import timedelta

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator

from include.nbp_silver.tasks import _dag_init, _silver_transform

BASE_DIR = Path(__file__).resolve().parent.parent

BRONZE_STORAGE_PATH = BASE_DIR / "gcs_bucket" / "bronze" / "nbp"
SILVER_STORAGE_PATH = BASE_DIR / "gcs_bucket" / "silver" / "nbp"


@dag(
    dag_id="nbp_silver",
    start_date=datetime(2026, 4, 20, tz="Europe/Warsaw"),
    schedule=None,
    catchup=False,
    tags=["transformation", "silver", "nbp"],
    max_active_runs=1,
    dagrun_timeout=3600,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }
)
def pipeline():

    dag_init = PythonOperator(
        task_id="dag_init", python_callable=_dag_init, trigger_rule="all_success"
    )

    run_script = PythonOperator(
        task_id="run_python_script",
        python_callable=_silver_transform,
        op_kwargs={
            "bronze_path": BRONZE_STORAGE_PATH,
            "parquet_file": SILVER_STORAGE_PATH / "nbp.parquet",
            "current_date": "{{ ds_nodash }}",
            "date": "{{ ds }}",
            "ingestion_timestamp": "{{ ts }}",
            "ingestion_run_id": "{{ run_id }}",
            "ingestion_dag_run": "{{ dag_run }}",
        },
        trigger_rule="all_success",
    )

    dag_end = DummyOperator(task_id="dag_end", trigger_rule="all_success")

    error_email = EmailOperator(
        task_id="error_email",
        to="test@gmail.com",
        subject="Alert Airflow Mail",
        html_content=""" Failed Silver Nbp transformation {{ ts_nodash }} """,
        trigger_rule="one_failed",
    )

    dag_init >> run_script >> dag_end
    run_script >> error_email


pipeline()
