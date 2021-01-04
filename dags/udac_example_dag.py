from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
#from helpers import SqlQueries


##################################
## EXTRA imrports

from airflow.operators import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import logging
logging.basicConfig(level=logging.DEBUG)
#################################


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'aws_credentials_id': 'aws_credentials',
    'aws_iam_role': 'arn:aws:iam::543250950086:role/dwhRole',
    'aws_region': 'us-west-2'
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None,
          #start_date=datetime.datetime.now()
        )

def python_test():
    print('hi')
    logging.info('test_log_1, testing, testing, 123')
    return('hi')

def list_keys():
    hook = S3Hook(aws_conn_id='aws_credentials')
    #bucket = Variable.get('s3_bucket') if getting bucket from variables
    #prefix = Variable.get('s3_prefix')
    bucket = 'gigi'
    logging.info(f'Listing Keys from {bucket}')
    keys = hook.list_keys(bucket)
    for key in keys:
        logging.info(f'- s3://{bucket}/{key}')

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

drop_tables_task = PostgresOperator(
    task_id='drop_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.drop_tables
    )

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_tables
    )

stage_events_to_redshift_task = StageToRedshiftOperator(
    task_id='S3_stage_events_to_Redshift',
    dag=dag,
    redshift_conn_id='redshift',
    destination_table='staging_events',
    S3_bucket_path='s3://udacity-dend/log_data',
    schema_path='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift_task = StageToRedshiftOperator(
    task_id='S3_stage_songs_to_Redshift',
    dag=dag,
    redshift_conn_id='redshift',
    destination_table='staging_songs',
    S3_bucket_path='s3://udacity-dend/song_data'
)


start_operator >> create_tables_task
# drop_tables_task >> 
create_tables_task >> stage_events_to_redshift_task
create_tables_task >> stage_songs_to_redshift_task



python_test_task = PythonOperator(
    task_id='python_test_task_lolo',
    python_callable=python_test,
    retries = 3,
    dag=dag
)

list_keys_task = PythonOperator(
    task_id='list_keys_task',
    python_callable=list_keys,
    retries = 3,
    dag=dag
)






# stage_songs_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_songs',
#     dag=dag
# )

# load_songplays_table = LoadFactOperator(
#     task_id='Load_songplays_fact_table',
#     dag=dag
# )

# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag
# )

# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag
# )

# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag
# )

# end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Begin_execution >> Stage_events
#Begin_execution >> Stage_songs


