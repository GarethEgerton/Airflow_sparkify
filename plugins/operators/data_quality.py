from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    '''
    Runs checks on the data. 
    Receive one or more SQL based test cases along with the expected results 
    and execute the tests. 
    
    For each the test, the test result and expected result needs to be checked 
    and if there is no match, the operator should raise an exception and the
     task should retry and fail eventually.

    For example one test could be a SQL statement that checks if certain column 
    contains NULL values by counting all the rows that have NULL in the column. 
    We do not want to have any NULLs so expected result would be 0 and the test 
    would compare the SQL statement's outcome to the expected result.
    '''

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                conn_id = 'your_connection_name',


                redshift_conn_id='',
                table='',
                *args, **kwargs):

                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.conn_id = conn_id
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        ## gg - copy paste data check code