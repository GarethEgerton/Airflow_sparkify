from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# noinspection SqlNoDataSourceInspection
class DataQualityOperator(BaseOperator):
    """
    Takes a dictionary of sql_statements (keys) and the string condition 'equal' or 'not_equal' to evaluate
    against results (values in the dictionary) and executes the tests returning errors if any tests fail.
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tests={},
                 test_condition='equal',
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests
        self.test_condition = test_condition

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test, expected in self.tests.items():
            records = redshift.get_records(test)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {test}: {self.test_condition} to {expected}," +
                                 f" returned no results")
            num_records = records[0][0]

            if self.test_condition == 'not_equal':
                if num_records == expected:
                    raise ValueError(f"Data quality check failed. {test}: {self.test_condition} to {expected} " +
                                     f"returned {num_records} rows")
                self.log.info(f"Data quality on {test}: {self.test_condition} to {expected} passed with {records[0][0]} records")

            elif self.test_condition == 'equal':
                if num_records != expected:
                    raise ValueError(f"Data quality check failed. {test}: {self.test_condition} to {expected} " +
                                     f"returned {num_records} rows")
                self.log.info(f"Data quality on {test}: {self.test_condition} to {expected} passed with {records[0][0]} records")

