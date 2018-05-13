from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.qubole_operator import QuboleOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.models import Variable, Connection
from urlparse import urlparse
import json

DB_TABLE = {
    'db_tap_variable':'MYSQL_DBTAP_ID',
    'mysql_table':'customers',
    'hive_table':'customers'
}

def variable_exists(key):
    if Variable.get(key) is None:
        return False
    else:
        return True

DAG_DEFAULTS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1),
    'email': ['data-ops@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('import from mysql', DAG_DEFAULTS)

#Read the configuration file
conf = json.loads(open(dag.folder + "/conf.json").read())

start = DummyOperator(
    task_id='start',
    dag=dag
)

import_data = QuboleOperator(
    task_id='db_import',
    command_type='dbimportcmd',
    mode=1,
    hive_table=DB_TABLE['hive_table'],
    db_table=DB_TABLE['mysql_table'],
    where_clause='id < 10',
    parallelism=2,
    dbtap_id=Variable.get("MYSQL_DBTAP_ID"),
    dag=dag
)

check_variable_exists = BranchPythonOperator (
    task_id='check_variable_exists',
    python_callable=variable_exists,
    op_kwargs={"key":DB_TABLE['MYSQL_DBTAP_ID']},
    trigger_rule=False,
    dag=dag
)

email_missing_variable = EmailOperator (
    task_id='email_missing_variable',
    to="someone@somewhere.org",
    subject="Missing Variable in Dag: {}".format(dag.dag_id),
    html_content="<h1>Missing Variable</h1>",
    dag=dag

)

end = DummyOperator(
    task_id='end',
    dag=dag
)

start >> check_variable_exists
check_variable_exists >> import_data >> end
check_variable_exists >> email_missing_variable >> end
