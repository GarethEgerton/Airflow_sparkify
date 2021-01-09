from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# noinspection SqlNoDataSourceInspection
class LoadDimensionOperator(BaseOperator):
    """
    Takes target database and destination table to insert into based on specified SQL statement.
    load_method can be 'append' to append data to table, or 'delete_load' to drop and create table
    before insertion.
    """
    ui_color = '#80BD9E'

    delete_load_sql_template = '''
        DROP TABLE IF EXISTS {destination_table};
        CREATE TABLE {destination_table} AS {sql} 
        '''

    dimension_append_sql_template = '''
        INSERT INTO {destination_table} {sql}
        '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 destination_table='',
                 sql='',
                 load_method='append',
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql = sql
        self.load_method = load_method

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.load_method == 'append':
            load_dimension = LoadDimensionOperator.dimension_append_sql_template.format(
                destination_table=self.destination_table,
                sql=self.sql
            )
        elif self.load_method == 'delete_load':
            load_dimension = LoadDimensionOperator.delete_load_sql_template.format(
                destination_table=self.destination_table,
                sql=self.sql
            )
        else:
            raise ValueError(f"Incorrect load_method, please enter 'append' or 'delete_load'")

        redshift.run(load_dimension)