try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
except Exception as e:
    print(f"Error {e}")

def first_function_execute(**context):
    print("First Function Execute")
    context['ti'].xcom_push(key='mykey', value="Please say Hello")

def anna_say_hello(**context):
    instance = context.get("ti").xcom_pull(key="mykey")
    print(f"instance is {instance}")
    if instance == "Please say Hello":
        print("Anna: Hello")
    else:
        print("Anna: Why did you call me for nothing?")

with DAG(
    dag_id = "2-function-interact",        # The DAG id should be the same as the filename
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=4),
        "start_date": datetime(2021, 7, 16)
    },
    catchup=False   # If the start date is declared before the current date
                    # and the catchup is True, all the DAG from start date
                    # to current date will be computed. Default is False.
) as f:

    first_function_execute=PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute,
        provide_context=True,
        op_kwargs={"name":"Dung Dore"}
    )

    anna_say_hello = PythonOperator(
        task_id="anna_say_hello",
        python_callable=anna_say_hello,
        provide_context=True
    )

# first_function_execute should be run before anna_say_hello
first_function_execute >> anna_say_hello