from airflow.hooks.base import BaseHook
import pymysql
import pandas as pd

class CustomMysqlHook(BaseHook):

    def __init__(self, mysql_conn_id, **kwargs):
        self.mysql_conn_id = mysql_conn_id

    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.mysql_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        self.mysql_conn = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.dbname, port=self.port)
        return self.mysql_conn
    
    def bulk_load(self, table_name, file_name, delimiter: str, col_list, is_header: bool, is_replace: bool, dtype: dict=None, charset: str='utf8'):
        from sqlalchemy import create_engine

        self.log.info('적재 대상파일: ' + file_name)
        self.log.info('테이블: ' + table_name)
        self.get_conn()
        header = 0 if is_header else None                   # is_header = True 이면 0, False이면 None
        if_exists = 'replace' if is_replace else 'append'   # is_replace = True 이면 replace, False이면 append
        if len(col_list)>0:
            file_df = pd.read_csv(file_name, header=None, delimiter=delimiter, names=col_list, dtype=dtype)
        else:
            file_df = pd.read_csv(file_name, header=header, delimiter=delimiter)

        file_df = file_df.replace('\r\n', '', regex=True)

        print('적재 건수:' + str(len(file_df)))
        uri = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}?charset={charset}'
        engine = create_engine(uri)
        file_df.to_sql(name=table_name,
                        con=engine.connect(),
                        if_exists=if_exists,
                        schema=self.dbname,
                        index=False)
 



    