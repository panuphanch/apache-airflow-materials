from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime

# Define the default arguments for the DAG
default_args = {
	# The owner of the DAG
	"owner": "Meee",
}

with DAG("my_dag_v_1_0_1", 
		start_date=datetime(2021, 1, 1), 
		default_args=default_args,
		schedule_interval='@daily', catchup=False) as dag:
	
	task_a = BashOperator(
		task_id="task_a",
		bash_command="echo 'task_a!'"
	)

	# Specify owner in the task itself
	# Owner is not the same as the DAG owner
	task_c = BashOperator(
		owner="Not me",
		task_id="task_c",
		# When the task is executed, it not executed by the owner of the task.
		# It is executed by the user who runs the Airflow instance.
		bash_command="echo 'task_c!'"
	)

	task_b = BashOperator(
		task_id="task_b",
		bash_command="echo 'task_b!'"
	)

	task_a >> task_c >> task_b