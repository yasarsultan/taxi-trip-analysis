from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extract_load import extractLoad
from process_load import process, load
from logging import getLogger

logger = getLogger(__name__)


year, month = str(datetime.now().year), str(datetime.now().month - 2).zfill(2)
file_name = f"yellow_tripdata_{year}-{month}.parquet"

# A function to check the output of each task
def print_outputs(ti):
    extractLoad_op = ti.xcom_pull(task_ids='extract_and_load')
    logger.info(f"The extract and load operation output is: {extractLoad_op}")

    transform_op = ti.xcom_pull(task_ids='transform_data')
    logger.info(f"The transform operation output is: {transform_op}")

    load_op = ti.xcom_pull(task_ids='load_data')
    logger.info(f"The load operation output is: {load_op}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(dag_id="batch_datapipeline", default_args=default_args, schedule="@monthly") as dag:
    # Define tasks
    extract_load_task = PythonOperator(
        task_id = "extract_and_load",
        python_callable = extractLoad,
        op_kwargs = {"file_name": file_name}
    )

    data_processing_task = PythonOperator(
        task_id = "transform_data",
        python_callable = process,
        op_kwargs = {"month": month}
    )

    load_data_task = PythonOperator(
        task_id = "load_data",
        python_callable = load
    )

    # print_output = PythonOperator(
    #     task_id = "print_task_output",
    #     python_callable = print_outputs,
    #     op_args = ['{{ ti }}'],
    # )


    # Set task dependencies
    extract_load_task >> data_processing_task >> load_data_task #>> print_output