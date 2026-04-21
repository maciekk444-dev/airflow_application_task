from pathlib import Path
from pendulum import datetime
from datetime import timedelta

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from include.nbp_bronze.tasks import _dag_init, _get_file

BASE_DIR = Path(__file__).resolve().parent.parent
URL_NBP_ARCHIVE = "https://static.nbp.pl/dane/kursy/xml/dir.aspx?tt=A"
URL_NPB_XML = "https://static.nbp.pl/dane/kursy/xml/"
BRONZE_STORAGE_PATH = BASE_DIR / "gcs_bucket" / "bronze" / "nbp"


@dag(
    dag_id="nbp_bronze",
    start_date=datetime(2026, 4, 20, tz="Europe/Warsaw"),
    schedule="0 7 * * *",
    catchup=False,
    tags=["ingestion", "bronze", "nbp"],
    max_active_runs=1,
    dagrun_timeout=3600,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    },
)
def pipeline():

    dag_init = PythonOperator(task_id="dag_init", python_callable=_dag_init)

    file_get = PythonOperator(
        task_id="file_get",
        python_callable=_get_file,
        op_kwargs={
            "url_archive": URL_NBP_ARCHIVE,
            "url_xml": URL_NPB_XML,
            "current_date": "{{ ds_nodash }}",
            "raw_path": BRONZE_STORAGE_PATH,
        },
        trigger_rule="all_success",
    )

    trigger_silver_processing = TriggerDagRunOperator(
        task_id="trigger_silver_processing",
        trigger_dag_id="nbp_silver",
        trigger_rule="all_success",
        execution_date="{{ execution_date }}",
    )

    dag_end = DummyOperator(task_id="dag_end", trigger_rule="all_success")

    error_email = EmailOperator(
        task_id="error_email",
        to="test@gmail.com",
        subject="Alert Airflow Mail",
        html_content=""" Failed Bronze Nbp Xml ingestion {{ ts_nodash }} """,
        trigger_rule="one_failed",
    )

    dag_init >> file_get >> trigger_silver_processing >> dag_end
    [file_get, trigger_silver_processing] >> error_email


pipeline()
