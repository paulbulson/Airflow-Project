from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {} '{}'
"""

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_format = 'JSON',
                 file_struct = 'auto',
                 create_SQL = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.file_format = file_format
        self.file_struct = file_struct
        self.execution_date = kwargs.get('execution_date')
        self.create_SQL = create_SQL
        
    def execute(self, context):
        self.log.info(f"StageToRedshiftOperator processing {self.table}")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Creating {self.table}")
        redshift.run(self.create_SQL)

        self.log.info(f"Clearing data from {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info(f"Copying data from S3 to Redshift table {self.table}")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.file_format,
            self.file_struct
        )
        self.log.info(formatted_sql)
        redshift.run(formatted_sql)





