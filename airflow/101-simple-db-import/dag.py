from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.qubole_operator import QuboleOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import json

DAG_DEFAULTS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 10),
    'email': ['me@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('import_from_mysql_jk', default_args=DAG_DEFAULTS, schedule_interval="@daily")

conf = {
  "db_tap_variable":"MYSQL_DBTAP_ID",
  "source_db_schema":"",
  "target_hive_schema":"public",
  "tables": [
    { "name":"categories", "parallelism":1 },
    { "name":"customers", "parallelism":1 },
    { "name":"departments", "parallelism":1 },
    { "name":"order_items", "parallelism":1 },
    { "name":"orders", "parallelism":1 },
    { "name":"orders_temp", "parallelism":1 },
    { "name":"products", "parallelism":1 },
    { "name":"salesperson", "parallelism":1 }
  ]
}

import_table_array = []

start = DummyOperator(
    task_id='start',
    dag=dag
)

for table in conf['tables']:
    task_id = "db_import_%s" % (table['name'])
    import_table_array.append(
        QuboleOperator(
            task_id=task_id,
            command_type='dbimportcmd',
            mode=1,
            db_name="",
            hive_table=table['name'],
            db_table=table['name'],
            parallelism=table['parallelism'],
            dbtap_id=Variable.get(conf['db_tap_variable']),
            dag=dag
        )
    )

end = DummyOperator(
    task_id='end',
    dag=dag
)


start >> import_table_array >> end
