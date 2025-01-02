from typing import Any
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

class OracleToS3Operator(BaseOperator):
    template_fields=('sql', 'file_path', 's3_bucket', 's3_key')

    def __init__(self, oracle_conn_id, sql, file_path, aws_conn_id, s3_bucket, s3_key, **kwargs):
        super().__init__(**kwargs)
        self.oracle_conn_id = oracle_conn_id
        self.sql = sql
        self.file_path = file_path
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def _fetch_data(self):
        from airflow.providers.oracle.hooks.oracle import OracleHook
        self.log.info(f"sql: {self.sql}")
        self.log.info(f"file_path: {self.file_path}")
        hook = OracleHook(self.oracle_conn_id)
        df = hook.get_pandas_df(sql=self.sql)
        df.to_csv(self.file_path, index=False, encoding='utf-8')    # header=False 헤더 제거

    def execute(self, context: Context) -> Any:
        import os
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        self._fetch_data()

        hook = S3Hook(self.aws_conn_id)
        hook.load_file(filename=self.file_path,
                       key=self.s3_key,
                       bucket_name=self.s3_bucket,
                       gzip=True,
                       replace=True
                       )
        self.log.info(f"key: {self.s3_key}")
        os.remove(self.file_path)


