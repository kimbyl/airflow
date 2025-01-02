from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt')
    
    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'        
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm 
        self.base_dt = base_dt

    def execute(self, context):
        import os
        import pandas as pd

        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

        total_row_data = []
        start_row = 1
        end_row = 1000

        while True:
            self.log.info(f'start_row: {start_row}, end_row: {end_row}')
            row_data = self._call_api(self.base_url, start_row, end_row)
            total_row_data.extend(row_data)
            if len(row_data) < 1000:
                break
            else:
                start_row = end_row + 1
                end_row += 1000
        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        total_row_df = pd.DataFrame(total_row_data)
        total_row_df.to_csv(f'{self.path}/{self.file_name}.csv', encoding='utf-8', index=False)
        self.log.info(f'{self.path}/{self.file_name}.csv is created')

    def _call_api(self, base_url, start_row, end_row):
        import requests
        import json

        headers = {'Content-Type': 'application/json',
                   'charset': 'utf-8',
                   'Accept': '*/*'}

        url = f'{base_url}/{start_row}/{end_row}/'
        self.log.info(f'url: {url}')
        if self.base_dt:
            url += f'?base_date={self.base_dt}'
        response = requests.get(url, headers=headers)
        self.log.info(f'response: {response.text}')
        contents = json.loads(response.text)

        key_name = list(contents.keys())[0]
        row_data = contents[key_name]['row']        
        return row_data
                