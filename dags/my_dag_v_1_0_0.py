from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import cross_downstream

from datetime import datetime, timedelta

# 3 levels of email notification:
# 1. airflow.cfg: default value for all the DAGs
# 2. default_args: default value for all the tasks in the DAG
# 3. task level: value for the specific task
# default_args is a dictionary to define default parameters for the DAG
# all the parameters will apply to all the tasks in the DAG
default_args = {	
	"email": ["meee-airflow@yopmail.com"],
	"email_on_retry": False, # do not send email on retry
	"email_on_failure": False, # do not send email on failure
}

def _my_func(execution_date):
	if execution_date.day == 5:
		raise ValueError("Execution date is 5th")

# Use version in the DAG ID to not overwrite the historical DAG
# cacthup=False to not run the historical DAG
# e.g., current date is 2021-01-05, the historical DAG will run from 2021-01-01 to 2021-01-04
# if catchup=True, the historical DAG will run from 2021-01-01 to 2021-01-05
# if catchup=False, the historical DAG will run from 2021-01-05 to 2021-01-05
with DAG("my_dag_v_1_0_0",
		 default_args=default_args,
		 start_date=datetime(2024, 11, 1), # recommended to use datetime object
		 schedule_interval='@daily',
		 catchup=False) as dag:  # catchup=False to not run the historical DAG
	
	extract_a = BashOperator(
		task_id="extract_a",
		bash_command="echo 'task_a!' && sleep 10",
		wait_for_downstream=True, 	# wait for the downstream tasks to complete before the current task runs
									# task_a will run only if task_a in the previous DAG run is completed
	)
	
	extract_b = BashOperator(
		task_id="extract_b",
		bash_command="echo 'task_a!' && sleep 10",
		wait_for_downstream=True, 	# wait for the downstream tasks to complete before the current task runs
									# task_a will run only if task_a in the previous DAG run is completed
	)

	process_a = BashOperator(
		task_id="process_a",
		email=["meee-airflow@yopmail.com"],
		email_on_retry=True, # do not send email on retry
		email_on_failure=True, # do not send email on failure
		retries=3, # retry 3 times, default is 0. Set default_task_retries in the airflow.cfg to set the default value
		retry_exponential_backoff=True, # retry with exponential backoff
		retry_delay=timedelta(seconds=10), # retry after 10 seconds
		bash_command=" echo '{{ ti.try_number }}' && sleep 20",
		pool="process_tasks",
		priority_weight=2, # set the priority of the task
	)

	process_b = BashOperator(
		task_id="process_b",
		email=["meee-airflow@yopmail.com"],
		email_on_retry=True, # do not send email on retry
		email_on_failure=True, # do not send email on failure
		retries=3, # retry 3 times, default is 0. Set default_task_retries in the airflow.cfg to set the default value
		retry_exponential_backoff=True, # retry with exponential backoff
		retry_delay=timedelta(seconds=10), # retry after 10 seconds
		bash_command=" echo '{{ ti.try_number }}' && sleep 20",
		pool="process_tasks",
		priority_weight=1, # set the priority of the task
	)

	process_c = BashOperator(
		task_id="process_c",
		email=["meee-airflow@yopmail.com"],
		email_on_retry=True, # do not send email on retry
		email_on_failure=True, # do not send email on failure
		retries=3, # retry 3 times, default is 0. Set default_task_retries in the airflow.cfg to set the default value
		retry_exponential_backoff=True, # retry with exponential backoff
		retry_delay=timedelta(seconds=10), # retry after 10 seconds
		bash_command=" echo '{{ ti.try_number }}' && sleep 20",
		pool="process_tasks",
		priority_weight=3, # set the priority of the task
	)

	store = PythonOperator(
		task_id="store",
		python_callable=_my_func,
		depends_on_past=True, # if the previous task is failed, the current task will not run
	)

	cross_downstream([extract_a, extract_b], [process_a, process_b, process_c])
	[process_a, process_b, process_c] >> store