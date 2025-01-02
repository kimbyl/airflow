from airflow.sensors.base import BaseSensorOperator
from airflow.hooks.base import BaseHook

class SeoulApiDateSensor(BaseSensorOperator):
    template_fields = ('endpoint', )
    def __init__(self, dataset, base_dt, day_off=0, **kwargs):
        '''
        dataset: 서울시 공공데이터 포털에서 센싱하고자 하는 데이터 셋 명
        base_dt: 센싱 기준 컬럼 (yyyy.mm.dd... or yyyy/mm/dd... 만 가능)
        day_off: 배치일 기준 생성 여부를 확인하고자 하는 날짜 차이를 입력 (기본값: 0)
        '''

        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset + '/1/100'    # 100건만 추출
        self.base_dt = base_dt
        self.day_off = day_off


    def poke(self, context):
        import requests
        import json
        from dateutil.relativedelta import relativedelta
        
        connection = BaseHook.get_connection(self.http_conn_id)
        url = f'http://{connection.host}:{connection.port}/{self.endpoint}'
        self.log.info(f'request url: {url}')
        response = requests.get(url)

        contents = json.loads(response.text)
        key = list(contents.keys())[0]
        rows = contents.get(key).get('row')
        last_dt = rows[0].get(self.base_dt)
        last_date = last_dt.replace('(', '').replace(')', '').replace('.', '-').replace('/', '-')
        last_date = last_date[:10]
        self.log.info(f'last_dt: {last_dt}, date: {last_date}')
        search_date = (context.get('data_interval_end').in_timezone('Asia/Seoul') + relativedelta(days=self.day_off)).strftime('%Y-%m-%d')

        try:
            import pendulum
            pendulum.from_format(last_date, 'YYYY-MM-DD')
        except:
            from airflow.exceptions import AirflowException
            AirflowException(f'{self.base_dt} 컬럼은 YYYY.MM.DD 또는 YYYY/MM/DD 형태가 아닙니다.')

        if last_date >= search_date:
            self.log.info(f'생성 확인(기준 날짜: {search_date} / API Last 날짜: {last_date})')
            return True
        else:
            self.log.info(f'Update 미완료 (기준 날짜: {search_date} / API Last 날짜:{last_date})')
            return False            