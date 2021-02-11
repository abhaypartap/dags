from datetime import datetime
from airflow import DAG

from airflow.operators.email_operator import EmailOperator

default_args = {
 'owner': 'owner',
 'depends_on_past': False,
 'start_date': datetime(2020, 10, 13),
 'email': ['abhaypratap.singh@capfront.in'],
 'email_on_failure': True
}

dag = DAG('email_send',
 schedule_interval='@hourly',
 default_args=default_args,
 start_date=datetime(2020, 10, 13))

with dag:
	send_email = EmailOperator(
	 task_id='send_email',
	 to='abhaypratap.singh@capfront.in',
	 subject='Airflow Alert',
	 html_content='This is trial mail',
	 dag=dag
	 )


