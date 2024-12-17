from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator

from datetime import datetime

def _process(path, filename, ds):
	print(f'{path} / {filename} - {ds}')

with DAG('my_python_dag', 
		 schedule_interval='@daily',
		 start_date=datetime(2020, 1, 1),
		 catchup=False) as dag:
	
	task_a = PythonOperator(
		task_id='task_a',
		python_callable=_process,
		# op_args=['/usr/local/airflow', 'data.csv'], # pass arguments to the python callable (Order matters)
		# op_kwargs={ # pass keyword arguments to the python callable (Order doesn't matter better for readability)
		# 	'path': '{{ var.value.path }}',
		# 	'filename': '{{ var.value.filename }}'
		# },
		op_kwargs=Variable.get("my_settings", deserialize_json=True), # get the variable from the airflow UI
	)