# Introduction to Airflow
Resource captured from Datacamp course - Introduction to Airflow

# Table of contents
1. Intro to Airflow
2. Implementing Airflow DAGs
3. Maintaining and monitoring Airflow workflows
4. Building production pipelines in Airflow

# 1. Intro to Airflow

## Introduction to Airflow

- *Workflow*: A set of steps to accomplish a given data engineering task (e.g., downloading files, copying data, filtering information)
- *Airflow*:
  - A platform to program workflows including creation, scheduling and monitoring.
  - Written in Python.
  - Implements workflows as DAGs (Directed Acyclic Graphs).
  - Accessed via code, command-line or via web interface.
- *DAG code example*:
```python
etl_dag = DAG(
  dag_id='etl_pipeline',
  default_args={"start_date": "2020-01-08"}
)
```
- *Running a simple Airflow task*: `airflow run <dag_id> <task_id> <start_date>`. For example:
```bash
airflow run example-etl download-file 2020-01-10
```

## Airflow DAG
- *DAG*: has the following attributes
  - Directed
  - Acyclic (does not loop / cycle / repeat)
  - Graph (actual set of component)
- *Defining a DAG*
```python
from airflow.models import DAG
from datetime import datetime

default_arguments = {
  'owner': 'jdoe',
  'email': 'jdoe@datacamp.com',
  'start_date': datetime(2020, 1, 20)
 }
 
 etl_dag = DAG('etl_workflow', default_args=default_arguments)
 ```
 - *Command line vs Python*:

| **Command line**                                                                    | **Python**                                               |
|--------------------------------------------------------------------------------------|------------------------------------------------------|
| Start Airflow processes | Create a DAG
| Manually run DAGs/tasks |  Edit the individual properties of a DAG |
| Get logging information from Airflow | |

## Starting the Airflow webserver
- *Web UI vs command line*: In most cases:
  - Equally powerful depending on needs
  - Web UI is easier
  - Command line tool may be easier to access depending on settings

- *Start a server*: `airflow webserver -p <port>`

# 2. Implementing Airflow DAGs

## Airflow operators
- Represent a single task in a workflow.
- Run independently (usually).
- Generally do not share information.
- Various operators to perform different tasks.
- *Not guaranteed to run in the same location / environment*.
- *May require extensive use of Environment variables*.
- *Can be difficult to run tasks with elevated privileges*.

### Bash Operator
- Executes a given Bash command or script. For example
```python
from airflow.operators.bash_operator import BashOpertor

example_task = BashOperator(
  task_id='bash_example',
  bash_command='cat addresses.txt | awk "NF==10" > cleaned.txt',
  dag=etl_dag
)
```
- Runs the command in a temporary directory.
- Can specify environment variables for the command.

### Python Operator
- Executes a Python function / callable.
- Operates similarly to the `BashOperator`, with more options.
- Can pass in arguments to the Python code.
```python
from airflow.operators.python_operator import PythonOperator

def printme(content):
  print(f"This goes in the logs: {content}!")
  
python_task = PythonOperator(
  task_id='simple_print',
  python_callable=printme,
  dag=example_dag,
  op_kwargs={'content': 'Hello'}
)
```
- Supports positional and keyword argument (keyword by using `op_kwargs`)

### Email Operator
- Sends an email
- Can contain typical components (HTML content, attachments)
- Does require the Airflow system to be configured with email server details
```python
from airflow.operators.email_operator import EmailOperator

email_task = EmailOperator(
  task_id='email_sales_report',
  to='sales_manager@example.com',
  subject='Automated Sales Report',
  html_content='Attached is the latest sales report',
  files='latest_sales.xlsx',
  dag=example_dag
)
```

## Airflow tasks
- Instances of operators.
- Usually assigned to a variable in Python (e.g., `example_task`).
- Referred to by the `task_id` within the Airflow tools.

## Task dependencies
- Use bitshift operator. For example:
```python
task1 = BashOperator(
  task_id='first_task',
  bash_command='echo 1',
  dag=example_dag
)

task2 = BashOperator(
  task_id='second_task',
  bash_command='echo 2',
  dag=example_dag
)

# Set first_task to run before second_task
task1 >> task2
# or task2 << task1
```
- Multiple dependencies:
  - Chained dependencies: `task1 >> task2 >> task3 >> task4`
  - Mixed dependencies: `task1 >> task2 << task3` or `task1 >> task2 \n task3 >> task2`

## Airflow scheduling
- A specific instance of a workflow at a point in time.
- Can be run manually or via `schedule_interval`.
- Maintain state for each workflow and the tasks within: running, failed, success.
- Attributes of note:
  - `start_date
  - `end_date`
  - `max_tries`
  - `schedule_interval` (can be defined via cron style syntax `* * * * *` or via built-in presets).

# 3. Maintaining and monitoring Airflow workflows

## Airflow sensors
- An operator that waits for a certain condition to be true (e.g., certain response from a web request).
- Can define how often to check for the condition to be true.
- Are assigned to tasks.
- Arguments:
  - `mode`: How to check for the condition - `poke` (run repeatedly, default) or `reschedule` (give up task slot and try again later)
  - `poke_interval`: How often to wait between checks
  - `timeout`: How long to wait before failing task
  - Normal operators for tasks
- Use when:
  - Uncertain when it will be true
  - If failure not immediately desired
  - To add task repetition without loops

### FileSensor
- Checks for the existence of a file at a certain location
- Can also check if any files exist within a directory
```python
from airflow.contrib.sensors.file_sensor import FileSensor

file_sensor_task = FileSensor(
  task_id='file_sense',
  filepath='salesdata.csv',
  poke_interval=300,
  dag=sales_report_dag
)

init_sales_cleanup >> file_sensor_task >> generate_report
```

### ExternalTaskSensor
- Wait for a task in another DAG to complete

### HttpSensor
- Request a web URL and check for content

### SqlSensor
- Runs a query to check for content

## Airflow executors
- Executors run tasks
- Different executors handle running the tasks differently

### SequentialExecutor
- The default Airflow executor
- Runs one task at a time
- Useful for debugging
- While functional, not really recommended for production

### LocalExecutor
- Runs on a single system
- Treats tasks as processes
- *Parallelism* defined by the user
- Can utilize all resources of a given host system

### CeleryExecutor
- Uses a Celery backend as task manager
- Multiple worker systems can be defined
- Is significantly more difficult to setup & configure.
- Extremely powerful method for organizations with extensive workflows

### Determine the executor
#1:
- Via `airflow.cfg` file
- Look for the `executor=` line

#2: 
- First line of `airflow list_dags`

## SLA
- SLA: Service Level Agreement - defines the amount of time a task or a DAG should require to run.
- An SLA Miss is any time the task/DAG does not meet the expected timing. If that's the case, an email is sent out and a log is stored.
- Can view SLA misses in the web UI.
- Defining SLA via the `sla` argument on the task, or in `default_args` dictionary.
```python
task1 = BashOpeator(
  task_id='sla_task',
  bash_command='runcode.sh',
  sla=timedelta(seconds=30),
  dag=dag
)
```

# 4. Building production pipelines in Airflow

## Templates
- Allow substituting information during a DAG run
- Provide added flexibility when defining tasks
- Using `Jinja` templating language
```python
templated_command = """
  echo "Reading {{ params.filename }}"
"""

t1 = BashOperator(
  task_id='template_task',
  bash_command=templated_command,
  params={'filename': 'file1.txt'}
  dag=example.dag
)
t2 = BashOperator(
  task_id='template_task',
  bash_command=templated_command,
  params={'filename': 'file2.txt'}
  dag=example_dag
)
```

- Working with lists and loops:
```python
templated_command = """
{% for filename in params.filenames %}
  echo "Reading {{ filename }}"
{% endfor %}
"""

t1 = BashOperator(
  task_id='template_task',
  bash_command=templated_command,
  params={'filenames': ['file1.txt', 'file2.txt']}
  dag=example_dag
)
```

- Built-in runtime variables:
  - `ds`: YYYY-MM-DD (date of current DAG run)
  - `ds_nodash`: YYYYMMDD
  - `prev_ds`: YYYY-MM-DD (date of previous DAG run)
  - `prev_ds_nodash`: YYYYMMDD
  - `dag`: full DAG object
  - `conf`: Airflow config object
  - ...
  - `macros.datetime`
  - `macros.timedelta`
  - `macros.uuid`
  - `macros.ds_add('2020-04-15', 5)`
  - ...

## Branching
- Provides conditional logic
- Using `BranchPythonOperator`
```python
from airflow.operator.python_operator import BranchPythonOperator

def branch_test(**kwargs):
  if int(kwagrs['ds_nodash']) % 2 == 0:
    return 'even_day_task'
  else:
    return 'odd_day_task'

branch_task = BranchPythonOperator(
  task_id='branch_task',
  dag=dag,
  provide_context=True,  # Tells Airflow to provide access to the runtime variables and macros to the function
  python_callable=branch_test
)

# Define the up/downstreams
start_task >> branch_task >> even_day_task >> even_day_task2
branch_task >> odd_day_task >> odd_day_task2
# If even day, run start_task >> branch_task >> even_day_task >> even_day_task2
# If odd day, run start_task >> branch_task >> odd_day_task >> odd_day_task2
```

## Create a production pipeline
- Running a task: `airflow run <dag_id> <task_id> <date>`
- Running a full DAG: `airflow trigger_dag -e <date> <dag_id>`
