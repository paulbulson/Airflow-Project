from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.aws_credenitals_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('Confirming data quality')
        
        for table in self.tables:
            redshift_hook = PostgresHook(self.redshift_conn_id)
            sql = f"SELECT COUNT(1) FROM {table}"
            self.log.info(f"Executing {sql}")
            records = redshift_hook.get_records(sql)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"{table} has no values ERROR")

            if records[0][0] < 1:
                raise ValueError(f"{table} contains 0 rows ERROR")

            self.log.info(f"{table} contains {records[0][0]} rows")
        