from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    '''
    Load any JSON formatted files from S3 to Amazon Redshift. 
    Creates and runs a SQL COPY statement based on the parameters provided. 
    Parameters specify: 
        > Where in S3 the file is loaded
        > What is the target table.
    The parameters should be used to distinguish between JSON file. 
    Containing a templated field that allows it to load timestamped files 
    from S3 based on the execution time and run backfills.
    '''
    ui_color = '#358140'

    stage_sql_template='''
        COPY {destination_table} FROM '{S3_bucket_path}'
        CREDENTIALS 'aws_iam_role={aws_iam_role}'
        COMPUPDATE OFF region '{aws_region}'
        TIMEFORMAT as 'epochmillisecs'
        FORMAT JSON '{schema_path}';
        '''  
 
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id='',
                 destination_table='',
                 aws_credentials_id='',
                 S3_bucket_path='',
                 aws_iam_role='',
                 aws_region='',
                 schema_path='auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.aws_credentials_id = aws_credentials_id
        self.S3_bucket_path = S3_bucket_path
        self.aws_iam_role = aws_iam_role
        self.aws_region = aws_region
        self.schema_path = schema_path

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id,
                                aws_credentials_id=self.aws_credentials_id)

        stage_sql = StageToRedshiftOperator.stage_sql_template.format(
            destination_table=self.destination_table,
            S3_bucket_path=self.S3_bucket_path,
            aws_iam_role=self.aws_iam_role,
            aws_region=self.aws_region,
            schema_path=self.schema_path
        )
        
        redshift.run(stage_sql)
        self.log.info('StageToRedshiftOperator not implemented yet')







