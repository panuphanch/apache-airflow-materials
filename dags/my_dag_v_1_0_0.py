from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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
		 dagrun_timeout=40, 	# timeout for the DAG. best practice to set this value to avoid the infinite loop or dead lock
								# if not set and exception occurs, the dag will be stuck in max_active_runs_per_dag which is 16 by default
		 catchup=True) as dag:  # catchup=False to not run the historical DAG
	
	task_a = BashOperator(
		task_id="task_a",
		bash_command="echo 'task_a!' && sleep 10"
	)

	task_b = BashOperator(
		task_id="task_b",
		email=["meee-airflow@yopmail.com"],
		email_on_retry=True, # do not send email on retry
		email_on_failure=True, # do not send email on failure
		retries=3, # retry 3 times, default is 0. Set default_task_retries in the airflow.cfg to set the default value
		retry_exponential_backoff=True, # retry with exponential backoff
		retry_delay=timedelta(seconds=10), # retry after 10 seconds
		bash_command=" echo '{{ ti.try_number }}' && exit 0"
	)

	task_c = PythonOperator(
		task_id="task_c",
		python_callable=_my_func,
		depends_on_past=True, # if the previous task is failed, the current task will not run
	)

	task_a >> task_b >> task_c