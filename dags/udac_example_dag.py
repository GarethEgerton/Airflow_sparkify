from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
# from helpers import SqlQueries


##################################
## EXTRA imrports

from airflow.operators import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import logging
import sys
logging.basicConfig(level=logging.DEBUG)
# log = logging.getLogger("airflow.task.operators")
# handler = logging.StreamHandler(sys.stdout)
# handler.setLevel(logging.INFO)
# log.addHandler(handler)
#################################

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 1, 1),
    # 'end_date': datetime(2019, 2, 12),
    'aws_credentials_id': 'aws_credentials',
    'aws_iam_role': 'arn:aws:iam::543250950086:role/dwhRole',
    'aws_region': 'us-west-2',
    'redshift_conn_id': 'redshift',
    # 'depends_on_past': False,
    'catchup_by_default': False,
    'catchup': False
    # 'retries': '3',
    # 'retry_delay': timedelta(minutes=5),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          catchup=False,
          # catchup_by_default=False,
          schedule_interval=timedelta(days=10),
          # start_date=datetime(2019, 1, 12)
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

drop_tables = PostgresOperator(
    task_id='drop_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.drop_tables
)

create_tables = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_tables
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='S3_stage_events_to_Redshift',
    dag=dag,
    destination_table='staging_events',
    s3_bucket_path='s3://udacity-dend/log_data',
    schema_path='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='S3_stage_songs_to_Redshift',
    dag=dag,
    destination_table='staging_songs',
    s3_bucket_path='s3://udacity-dend/song_data'
)

load_songplays_fact_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    destination_table='songplays',
    sql=SqlQueries.songplay_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    destination_table='song',
    sql=SqlQueries.song_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    destination_table='users',
    sql=SqlQueries.user_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    destination_table='artist',
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    destination_table='public.time',
    sql=SqlQueries.time_table_insert,
    load_method='delete_load'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables=['staging_events',
            'staging_songs',
            'artist',
            'song',
            'songplays',
            'time',
            'users'],
    tests={f'SELECT COUNT(*) FROM {a};': 0 for a in ['staging_events',
                                                           'staging_songs',
                                                           'artist',
                                                           'song',
                                                           'songplays',
                                                           'time',
                                                           'users']},
    test_condition='not_equal'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> drop_tables >> create_tables
create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift
stage_songs_to_redshift >> load_songplays_fact_table
stage_events_to_redshift >> load_songplays_fact_table
load_songplays_fact_table >> load_song_dimension_table
load_songplays_fact_table >> load_user_dimension_table
load_songplays_fact_table >> load_artist_dimension_table
load_songplays_fact_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator


# todo try and make time dimension table load using dimension operator
# todo make dictionary input for data quality checks, {sql:key}
# todo edit fact operator with parameter for truncate-insert pattern vs just inserting
#
#
############################################################
############################################################
############################################################

# python_test_task = PythonOperator(
#     task_id='python_test_task_lolo',
#     python_callable=python_test,
#     retries=3,
#     dag=dag
# )
#
# list_keys_task = PythonOperator(
#     task_id='list_keys_task',
#     python_callable=list_keys,
#     retries=3,
#     dag=dag
# )
# def python_test():
#     print('hi')
#     logging.info('test_log_1, testing, testing, 123')
#     return ('hi')
#
#
# def list_keys():
#     hook = S3Hook(aws_conn_id='aws_credentials')
#     # bucket = Variable.get('s3_bucket') if getting bucket from variables
#     # prefix = Variable.get('s3_prefix')
#     bucket = 'gigi'
#     logging.info(f'Listing Keys from {bucket}')
#     keys = hook.list_keys(bucket)
#     for key in keys:
#         logging.info(f'- s3://{bucket}/{key}')