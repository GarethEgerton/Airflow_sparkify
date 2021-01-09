from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# noinspection SqlNoDataSourceInspection
class LoadFactOperator(BaseOperator):
    """
    Takes target database and destination table to insert into based on specified SQL statement.
    """
    load_fact_sql_template = '''
        INSERT INTO {destination_table} {sql}
        '''

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 destination_table='',
                 sql='',
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        load_dimension = LoadFactOperator.load_fact_sql_template.format(
            destination_table=self.destination_table,
            sql=self.sql
        )

        redshift.run(load_dimension)
        self.log.info('Data loaded from ')
