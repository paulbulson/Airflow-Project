from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table="",
                 aws_credentials_id="",
                 redshift_conn_id="",
                 sql_query="",
                 truncate=0,
                 create_SQL="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = 'songplays'
        self.aws_credenitals_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.truncate = truncate
        self.create_SQL = create_SQL

    def execute(self, context):
        self.log.info(f"load_fact processing {self.table}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Creating {self.table}")
        redshift.run(self.create_SQL)        
        
        if self.truncate:
            self.log.info(f"truncating {self.table}")
            redshift.run(f"TRUNCATE {self.table}")            

        self.log.info(f"loading {self.table}")  
        formatted_sql = f"INSERT INTO {self.table} {self.sql_query}" 
        self.log.info(formatted_sql)
        redshift.run(formatted_sql)      
               

