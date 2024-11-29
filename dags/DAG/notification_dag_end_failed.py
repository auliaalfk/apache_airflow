from airflow.decorators import dag, task
from airflow.utils.context import Context
from airflow.utils.email import send_email
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime

# Fungsi untuk mengirim email jika terjadi kegagalan
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

# Dekorator untuk DAG dengan callback saat gagal
@dag(
    schedule_interval=None,
    start_date=datetime(2023, 11, 5),
    on_failure_callback=send_email_on_failure,
    catchup=False
)
def notification_dag_end_failed():
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")

    @task
    def failed_task():
        raise AirflowFailException("ini task yang gagal")

    start_task >> failed_task() >> end_task

# Inisialisasi DAG
notification_dag_end_failed()
