import pyodbc
import airflow

from datetime import timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
#from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator


def LoadOperationStates():
    connectionString = Variable.get('password_sql')
    cnxn = pyodbc.connect(connectionString)
    cursor = cnxn.cursor()
    cursor.execute('SELECT Id, Code from dbo.OperationState')
    row = cursor.fetchone()
    while row:
        print(row[0])
        row = cursor.fetchone()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['eflorespalma@gmail.com'],
    'email_on_failure': False,
    'email _on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'tutorial',
    default_args=default_args,
    description='Airflow Playground',
    schedule_interval=timedelta(days=1)
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['eflorespalma@gmail.com'],
    'email_on_failure': False,
    'email _on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'playground2',
    default_args=default_args,
    description='Airflow Playground',
    schedule_interval=timedelta(days=1)
)

# t1, t2, and t3 are examples of tasks created by instatiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    dag=dag
)


template_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=template_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag
)

t4 = PythonOperator(
    task_id='readSQL',
    python_callable=LoadOperationStates,
    dag=dag,
)

# t4 = MsSqlOperator(
#     task_id='queryinfo',
#     mssql_conn_id='sqltoro',
#     sql='use toro-sqldatabase-uat; SELECT Id, Code from OperationState',
#     dag=dag
# )

t1 >> [t2, t3] >> t4
