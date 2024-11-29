from airflow.decorators import dag, task
from airflow.utils.context import Context
from airflow.utils.email import send_email
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowFailException
from datetime import datetime

# Fungsi untuk mengirim email ketika DAG atau task gagal
def send_email_on_failure(context: Context):
    send_email(
        to=["galuh.ramaditya13@gmail.com"],
        subject="Airflow Failed!",
        html_content=f"""
        <center><h1>!!! DAG RUN FAILED !!!</h1></center>
        <b>Dag</b> : <i>{context['ti'].dag_id}</i><br>
        <b>Task</b> : <i>{context['ti'].task_id}</i><br>
        <b>Log URL</b>: <i>{context['ti'].log_url}</i><br>
        """
    )

# Dekorator untuk mendefinisikan DAG dengan callback ketika gagal
@dag(
    schedule_interval=None,
    start_date=datetime(2023, 11, 5),
    on_failure_callback=send_email_on_failure,
    catchup=False
)
def notification_dag_end_success():
    start_task = EmptyOperator(task_id="start_task")

    # Task yang akan dibuat gagal untuk menguji notifikasi email
    @task
    def failed_task():
        raise AirflowFailException("Ini task yang gagal")

    # Task terakhir yang memiliki trigger_rule ALL_DONE agar dijalankan
    # meskipun ada task yang gagal sebelumnya
    end_task = EmptyOperator(
        task_id="end_task",
        trigger_rule=TriggerRule.ALL_DONE
    )

    start_task >> failed_task() >> end_task

# Inisialisasi DAG
notification_dag_end_success()

