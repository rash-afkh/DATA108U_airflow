from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(S3ToRedshiftOperator):
    ui_color = "#358140"

    @apply_defaults
    def __init__(
        self,
        table="",
        redshift_conn_id="",
        aws_credentials_id="",
        s3_bucket="",
        s3_key="",
        region="us-west-2",
        extra_params=None,
        *args,
        **kwargs,
    ):
        super(StageToRedshiftOperator, self).__init__(
            schema=table,
            table=table,
            copy_options=[],
            aws_conn_id=aws_credentials_id,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_source_aws_conn_id=aws_credentials_id,
            copy_source_s3_key=s3_key,
            schema_fields=[],
            *args,
            **kwargs,
        )

        self.region = region
        self.extra_params = extra_params

    def execute(self, context):
        self.log.info(f"Copying data from S3 to Redshift staging {self.schema} table")
        self.log.info(f"Rendered Key: {self.s3_key}")

        self.log.info(
            f"Executing query to copy data from '{self.s3_bucket}/{self.s3_key}' to '{self.schema}'"
        )
        super().execute(context)
