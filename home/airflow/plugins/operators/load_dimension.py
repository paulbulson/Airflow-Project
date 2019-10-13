from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 aws_credentials_id="",
                 redshift_conn_id="",
                 sql_query="",
                 truncate=0,
                 create_query="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.aws_credenitals_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.create_query = create_query
        self.truncate = truncate

    def execute(self, context):
        self.log.info(f"processing {self.table}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info(f"truncating {self.table}")
            redshift.run(f"TRUNCATE {self.table}")            
        
        self.log.info(f"creating table {self.table} with {self.create_query}")
        redshift.run(self.create_query)
        
        self.log.info(f'Loading {self.table}')  
        formatted_sql = f"INSERT INTO {self.table} {self.sql_query}" 
        self.log.info(formatted_sql)
        redshift.run(formatted_sql)      
