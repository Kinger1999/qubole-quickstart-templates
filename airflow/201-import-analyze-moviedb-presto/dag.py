from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.qubole_operator import QuboleOperator
from airflow.operators.dummy_operator import DummyOperator


web_import_script = """
mkdir ~/tmp_movielens
cd ~/tmp_movielens

wget http://files.grouplens.org/datasets/movielens/ml-latest.zip
unzip ml-latest.zip
cd ml-latest

aws s3 mb s3://qbol-moviedb-data/
aws s3 cp ~/tmp_movielens/ml-latest/movies.csv s3://qbol-movie-data/movies/movies.csv

"""

create_hive_table = """
CREATE EXTERNAL TABLE movies (
    movieid int,
    title string,
    genres string
)
STORED AS TEXTFILE
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://qbol-movie-data/movies/'
"""

count_movie_table = """
SELECT count(1)
FROM movies
"""

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

dag = DAG('import and analyze movies', DAG_DEFAULTS)

start = DummyOperator(
    task_id='start',
    dag=dag
)

import_from_web = QuboleOperator(
    task_id='import_web_data',
    command_type="shellcmd",
    script=web_import_script,
    dag=dag
)

make_hive_table = QuboleOperator(
    task_id='create_movies_table',
    command_type='hivecmd',
    query=create_hive_table,
    cluster_label='default',
    tags='create_movies_table',  # Attach tags to Qubole command, auto attaches 3 tags - dag_id, task_id, run_id
    qubole_conn_id='qubole_default',  # Connection ID to submit commands inside QDS, if not set **qubole_default** is used
    dag=dag
)

query_hive_table = QuboleOperator(
    task_id='count_movies_table',
    command_type='hivecmd',
    query=count_movie_table,
    cluster_label='default',
    tags='count_movie_table',  # Attach tags to Qubole command, auto attaches 3 tags - dag_id, task_id, run_id
    qubole_conn_id='qubole_default',  # Connection ID to submit commands inside QDS, if not set **qubole_default** is used
    dag=dag
)

end = DummyOperator(
    task_id='start',
    dag=dag
)

start >> import_from_web >> make_hive_table >> query_hive_table
