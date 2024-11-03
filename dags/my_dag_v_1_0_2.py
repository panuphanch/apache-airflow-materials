from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from datetime import datetime

with DAG("my_dag_v_1_0_2",
		 start_date=days_ago(5), 			# 5 days ago from the current date
		 schedule_interval='*/1 * * * *', 	# every minute
		 catchup=False) as dag:
	
	task_a = BashOperator(
		task_id="task_a",
		start_date=datetime(2024, 11, 2), # no usecase found, just for example
		bash_command="echo 'task_a!'"
	)

	task_b = BashOperator(
		task_id="task_b",
		start_date=datetime(2024, 11, 2), # no usecase found, just for example
		bash_command="echo 'task_b!'"
	)

	task_a >> task_b