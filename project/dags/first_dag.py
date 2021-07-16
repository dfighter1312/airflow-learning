try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
except Exception as e:
    print(f"Error {e}")

def helloworld(*args, **kwargs):
    var = kwargs.get("name", "Value not found")
    print(f"Hello {var}")
    return f"Hello {var}"

with DAG(
    dag_id = "first_dag",        # The DAG id should be the same as the filename
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2020, 7, 16)
    },
    catchup=False   # If the start date is declared before the current date
                    # and the catchup is True, all the DAG from start date
                    # to current date will be computed. Default is False.
) as f:
    helloworld = PythonOperator(
        task_id="helloworld",
        python_callable=helloworld,
        op_kwargs={"name":"Dung Dore"}
    )