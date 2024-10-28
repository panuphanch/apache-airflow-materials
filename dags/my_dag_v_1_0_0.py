from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime

# Use version in the DAG ID to not overwrite the historical DAG
with DAG("my_dag_v_1_0_0", start_date=datetime(2021, 1, 1), schedule_interval='@daily', catchup=False) as dag:
	
	task_a = BashOperator(
		task_id="task_a",
		bash_command="echo 'task_a!'"
	)

	task_b = BashOperator(
		task_id="task_b",
		bash_command="echo 'task_b!'"
	)

	task_a >> task_b