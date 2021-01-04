from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    '''
    Takes a 
        > SQL statement 
        > Target database to run the query against 
        > Target table that will contain the results of the transformation

    Parameter to distingush between:

        > Dimension load is done with truncate-insert pattern where the 
          target table is emptied before the load. 
        
        > Fact tables should only allow append type functionality.
    '''

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
